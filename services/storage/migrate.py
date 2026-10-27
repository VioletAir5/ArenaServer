#!/usr/bin/env python3
"""Apply the reviewed PostgreSQL 001 and 002 migrations atomically through psql."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys


STORAGE = Path(__file__).resolve().parent
SUPPORTED_MIGRATIONS = ((1, "001_initial"), (2, "002_asset_transfer"))
MAX_SQL_BYTES = 1024 * 1024
PROCESS_TIMEOUT = 90


class MigrationError(Exception):
    """A diagnostic that contains no connection parameters or SQL text."""


class Parser(argparse.ArgumentParser):
    """Keep invalid command arguments out of error logs."""

    def error(self, message):
        self.print_usage(sys.stderr)
        self.exit(2, "migrate.py: invalid arguments; use --help\n")


def read_migration(name, direction):
    """Read and hash the exact reviewed bytes, including line endings."""
    path = STORAGE / "migrations" / f"{name}.{direction}.sql"
    try:
        with path.open("rb") as source:
            raw = source.read(MAX_SQL_BYTES + 1)
        if not raw or len(raw) > MAX_SQL_BYTES or b"\0" in raw:
            raise MigrationError("migration file is empty, oversized or invalid")
        sql = raw.decode("utf-8")
        if not sql.strip():
            raise MigrationError("migration file is empty")
    except (OSError, UnicodeError) as error:
        raise MigrationError("cannot read the fixed UTF-8 migration files") from error
    return sql, hashlib.sha256(raw).hexdigest()


def dollar_literal(value):
    """Quote nested SQL without interpreting its own dollar-quoted bodies."""
    index = 0
    while f"$arena_literal_{index}$" in value:
        index += 1
    tag = f"$arena_literal_{index}$"
    return tag + value + tag


def connection_environment():
    """Require an explicit target rather than libpq's implicit local defaults."""
    environment = os.environ.copy()
    database = environment.get("PGDATABASE", "")
    host = environment.get("PGHOST", "")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", database):
        raise MigrationError("PGDATABASE must be an explicit simple database name")
    if (not host or host != host.strip() or len(host) > 1024
            or any(character in host for character in "\0\r\n=,")):
        raise MigrationError("PGHOST must be one explicit host or socket directory")
    timeout = environment.get("PGCONNECT_TIMEOUT", "5")
    if not re.fullmatch(r"[0-9]{1,2}", timeout) or not 1 <= int(timeout) <= 60:
        raise MigrationError("PGCONNECT_TIMEOUT must be between 1 and 60 seconds")
    environment["PGCONNECT_TIMEOUT"] = timeout
    environment["PGCLIENTENCODING"] = "UTF8"
    environment["PGAPPNAME"] = "arena-storage-migrate"
    # A service or host-address override must not silently redirect the target.
    for name in ("PGSERVICE", "PGSERVICEFILE", "PGHOSTADDR"):
        environment.pop(name, None)
    return environment


def psql_executable():
    """Accept a PostgreSQL bin directory, one executable, or the current PATH."""
    configured = os.environ.get("PG_BIN")
    if configured:
        executable = Path(configured)
        if executable.is_dir():
            executable /= "psql.exe" if os.name == "nt" else "psql"
        if not executable.is_file():
            raise MigrationError("PG_BIN does not select a psql executable")
        return str(executable.resolve())
    executable = shutil.which("psql")
    if not executable:
        raise MigrationError("psql is unavailable; set PG_BIN or PATH")
    return executable


def transaction(action, migrations):
    """Validate an applied prefix and update it under one transaction-level lock."""
    expected_rows = ",\n".join(
        f"({m['version']}, '{m['name']}', '{m['up_hash']}', '{m['down_hash']}')"
        for m in migrations)
    apply_steps = []
    rollback_steps = []
    for migration in migrations:
        version, name = migration["version"], migration["name"]
        apply_steps.append(f"""
        IF applied_version < {version} THEN
            EXECUTE {dollar_literal(migration['up_sql'])};
            IF pg_catalog.to_regnamespace('arena') IS NULL THEN
                RAISE EXCEPTION 'up migration did not retain the arena schema';
            END IF;
            INSERT INTO public.arena_schema_migrations
                (version, name, up_sha256, down_sha256)
            VALUES ({version}, '{name}', '{migration['up_hash']}',
                    '{migration['down_hash']}');
            applied_version := {version};
            changed := true;
        END IF;
""")
    for migration in reversed(migrations):
        version = migration["version"]
        rollback_steps.append(f"""
        IF applied_version >= {version} THEN
            EXECUTE {dollar_literal(migration['down_sql'])};
            DELETE FROM public.arena_schema_migrations WHERE version = {version};
            applied_version := {version - 1};
            changed := true;
        END IF;
""")
    def reported_hash(key):
        cases = " ".join(
            f"WHEN {m['version']} THEN '{m[key]}'" for m in migrations)
        return f"CASE applied_version {cases} ELSE '{migrations[-1][key]}' END"
    body = f"""
DECLARE
    ledger_oid oid := pg_catalog.to_regclass('public.arena_schema_migrations');
    arena_oid oid := pg_catalog.to_regnamespace('arena');
    history_count bigint := 0;
    applied_version integer := 0;
    changed boolean := false;
    column_names text[];
    column_types oid[];
    columns_required boolean;
BEGIN
    IF pg_catalog.current_setting('server_version_num')::integer < 140000 THEN
        RAISE EXCEPTION 'PostgreSQL 14 or later is required';
    END IF;

    IF ledger_oid IS NOT NULL THEN
        IF NOT EXISTS (
            SELECT 1 FROM pg_catalog.pg_class
            WHERE oid = ledger_oid AND relkind = 'r'
              AND relpersistence = 'p' AND NOT relispartition
        ) THEN
            RAISE EXCEPTION 'migration ledger relation drift';
        END IF;
        SELECT pg_catalog.array_agg(attname::text ORDER BY attnum),
               pg_catalog.array_agg(atttypid ORDER BY attnum),
               pg_catalog.bool_and(attnotnull AND attgenerated = ''
                                   AND attidentity = '')
          INTO column_names, column_types, columns_required
          FROM pg_catalog.pg_attribute
         WHERE attrelid = ledger_oid AND attnum > 0 AND NOT attisdropped;
        IF column_names IS DISTINCT FROM
              ARRAY['version', 'name', 'up_sha256', 'down_sha256', 'applied_at']
           OR column_types IS DISTINCT FROM ARRAY[
              'pg_catalog.int2'::regtype::oid, 'pg_catalog.text'::regtype::oid,
              'pg_catalog.text'::regtype::oid, 'pg_catalog.text'::regtype::oid,
              'pg_catalog.timestamptz'::regtype::oid]
           OR columns_required IS DISTINCT FROM true
           OR NOT EXISTS (
               SELECT 1 FROM pg_catalog.pg_constraint
               WHERE conrelid = ledger_oid AND contype = 'p'
                 AND conkey = ARRAY[1]::smallint[]
           ) THEN
            RAISE EXCEPTION 'migration ledger column or primary-key drift';
        END IF;

        SELECT pg_catalog.count(*), coalesce(pg_catalog.max(version), 0)
          INTO history_count, applied_version
          FROM public.arena_schema_migrations;
        IF history_count <> applied_version OR EXISTS (
            SELECT 1 FROM public.arena_schema_migrations actual
            LEFT JOIN (VALUES {expected_rows})
                AS expected(version, name, up_sha256, down_sha256)
              ON expected.version = actual.version
            WHERE expected.version IS NULL
               OR actual.name IS DISTINCT FROM expected.name
               OR actual.up_sha256 IS DISTINCT FROM expected.up_sha256
               OR actual.down_sha256 IS DISTINCT FROM expected.down_sha256
        ) THEN
            RAISE EXCEPTION 'migration version, order or checksum drift';
        END IF;
    END IF;

    IF (applied_version > 0) IS DISTINCT FROM (arena_oid IS NOT NULL) THEN
        RAISE EXCEPTION 'business schema and migration ledger disagree';
    END IF;

    IF '{action}' = 'up' THEN
        IF ledger_oid IS NULL THEN
            CREATE TABLE public.arena_schema_migrations (
                version smallint NOT NULL PRIMARY KEY CHECK (version > 0),
                name text NOT NULL,
                up_sha256 text NOT NULL CHECK (up_sha256 ~ '^[0-9a-f]{{64}}$'),
                down_sha256 text NOT NULL CHECK (down_sha256 ~ '^[0-9a-f]{{64}}$'),
                applied_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp()
            );
        END IF;
        {''.join(apply_steps)}
    ELSIF '{action}' = 'down' THEN
        {''.join(rollback_steps)}
        IF pg_catalog.to_regnamespace('arena') IS NOT NULL THEN
            RAISE EXCEPTION 'down migrations left the arena schema behind';
        END IF;
        IF ledger_oid IS NOT NULL THEN
            DROP TABLE public.arena_schema_migrations;
            changed := true;
        END IF;
    END IF;

    PERFORM pg_catalog.set_config('arena_migration.result',
        pg_catalog.json_build_object(
            'action', '{action}',
            'state', CASE WHEN applied_version > 0 THEN 'applied' ELSE 'unapplied' END,
            'version', nullif(applied_version, 0),
            'changed', changed,
            'up_sha256', {reported_hash('up_hash')},
            'down_sha256', {reported_hash('down_hash')}
        )::text, true);
END;
"""
    # The lock is a separate statement, so validation sees a transaction that
    # committed while this process waited for the same database-scoped lock.
    return f"""BEGIN ISOLATION LEVEL READ COMMITTED;
SET LOCAL lock_timeout = '10s';
SET LOCAL statement_timeout = '60s';
SET LOCAL search_path = pg_catalog;
SET LOCAL client_min_messages = warning;
DO $arena_lock$ BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(1095910734, 1296648018);
END $arena_lock$;
DO {dollar_literal(body)};
SELECT pg_catalog.current_setting('arena_migration.result');
COMMIT;
"""


def run(action):
    """Return a report only after psql has acknowledged the final COMMIT."""
    environment = connection_environment()
    migrations = []
    for version, name in SUPPORTED_MIGRATIONS:
        up_sql, up_hash = read_migration(name, "up")
        down_sql, down_hash = read_migration(name, "down")
        migrations.append(dict(version=version, name=name, up_sql=up_sql,
                               up_hash=up_hash, down_sql=down_sql, down_hash=down_hash))
    command = [psql_executable(), "--no-psqlrc", "--no-password", "--quiet",
               "--no-align", "--tuples-only", "--set=ON_ERROR_STOP=1",
               "--set=VERBOSITY=sqlstate", "--file=-"]
    try:
        result = subprocess.run(
            command, input=transaction(action, migrations),
            text=True, encoding="utf-8", capture_output=True, env=environment,
            timeout=PROCESS_TIMEOUT, check=False)
    except subprocess.TimeoutExpired as error:
        raise MigrationError("psql timed out; inspect status before retrying") from error
    except (OSError, UnicodeError) as error:
        raise MigrationError("cannot execute or communicate with psql") from error
    if result.returncode != 0:
        # libpq errors can contain connection details; never forward raw stderr.
        match = re.search(r"\b(?:ERROR|FATAL):\s+([0-9A-Z]{5})\b", result.stderr)
        suffix = f"; SQLSTATE {match.group(1)}" if match else ""
        raise MigrationError("PostgreSQL migration failed" + suffix)
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise MigrationError("psql did not return one valid migration report") from error
    if (not isinstance(report, dict) or report.get("action") != action
            or report.get("state") not in ("applied", "unapplied")
            or type(report.get("changed")) is not bool):
        raise MigrationError("psql returned an inconsistent migration report")
    version = report.get("version")
    applied = report["state"] == "applied"
    if (applied and (type(version) is not int
                     or version not in dict(SUPPORTED_MIGRATIONS))
            or not applied and version is not None
            or action == "up" and version != SUPPORTED_MIGRATIONS[-1][0]
            or action == "down" and applied
            or action == "status" and report["changed"]):
        raise MigrationError("psql returned an inconsistent migration report")
    head = next((m for m in migrations if m["version"] == version), migrations[-1])
    if (report.get("up_sha256") != head["up_hash"]
            or report.get("down_sha256") != head["down_hash"]):
        raise MigrationError("psql returned an inconsistent migration report")
    return report


def main():
    parser = Parser(description=__doc__)
    parser.add_argument("action", choices=("up", "down", "status"))
    arguments = parser.parse_args()
    try:
        report = run(arguments.action)
    except MigrationError as error:
        print(f"migrate.py: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("migrate.py: interrupted; inspect status before retrying", file=sys.stderr)
        return 2
    print(json.dumps(report, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
