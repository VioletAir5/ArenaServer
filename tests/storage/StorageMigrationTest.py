#!/usr/bin/env python3
"""Real PostgreSQL storage acceptance using an isolated disposable cluster."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
sys.dont_write_bytecode = True
from storage_cases import negative_cases

TABLES = ("account", "player", "item", "inventory", "game_match", "match_player", "settlement", "outbox", "asset_transfer")
COUNTS = dict(zip(TABLES, (5,4,3,5,6,11,4,2,0)))
CATALOG = """
SELECT jsonb_build_object(
 'schemas',(SELECT jsonb_agg(jsonb_build_array(nspname,nspacl::text) ORDER BY nspname)
 FROM pg_namespace WHERE nspname IN ('arena','public')),
 'relations',(SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname,c.relkind,c.relpersistence,
 c.relrowsecurity,c.relforcerowsecurity,c.reloptions,c.relacl::text) ORDER BY n.nspname,c.relname)
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('arena','public')),
 'columns',(SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname,a.attnum,a.attname,
 format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,
 pg_get_expr(d.adbin,d.adrelid),co.collname) ORDER BY n.nspname,c.relname,a.attnum)
 FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
 LEFT JOIN pg_collation co ON co.oid=a.attcollation
 WHERE n.nspname IN ('arena','public') AND a.attnum>0 AND NOT a.attisdropped),
 'constraints',(SELECT jsonb_agg(jsonb_build_array(n.nspname,c.conname,c.contype,
 c.conrelid::regclass::text,c.contypid::regtype::text,c.condeferrable,c.condeferred,c.convalidated,
 pg_get_constraintdef(c.oid,true)) ORDER BY n.nspname,c.conname,c.conrelid::regclass::text)
 FROM pg_constraint c JOIN pg_namespace n ON n.oid=c.connamespace WHERE n.nspname IN ('arena','public')),
 'indexes',(SELECT jsonb_agg(jsonb_build_array(n.nspname,c.relname,pg_get_indexdef(i.indexrelid),
 i.indisvalid,i.indisready) ORDER BY n.nspname,c.relname)
 FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname IN ('arena','public')),
 'types',(SELECT jsonb_agg(jsonb_build_array(n.nspname,t.typname,t.typtype,t.typcategory,t.typnotnull,
 format_type(t.typbasetype,t.typtypmod),t.typdefault) ORDER BY n.nspname,t.typname)
 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname IN ('arena','public')),
 'functions',(SELECT jsonb_agg(pg_get_functiondef(p.oid) ORDER BY p.proname,pg_get_function_identity_arguments(p.oid))
 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname IN ('arena','public')),
 'triggers',(SELECT jsonb_agg(pg_get_triggerdef(t.oid,true) ORDER BY n.nspname,c.relname,t.tgname)
 FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname IN ('arena','public') AND NOT t.tgisinternal))::text;
"""

class Failure(Exception):
    pass

def require(value, message):
    if not value:
        raise Failure(message)

def digest(value):
    if not isinstance(value, str):
        value = json.dumps(value, sort_keys=True, separators=(",",":"))
    return hashlib.sha256(value.encode()).hexdigest()

def run(command, env, sql=None, timeout=35):
    return subprocess.run([str(p) for p in command], input=sql, text=True, encoding="utf-8",
                          capture_output=True, env=env, timeout=timeout, check=False)

class Session:
    """Interactive psql with explicit barriers, no timing-based SQL interleaving."""
    def __init__(self, cluster, database, name):
        env = cluster.environment(database)
        env["PGAPPNAME"] = name
        self.p = subprocess.Popen(cluster.psql_command(), stdin=subprocess.PIPE,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  bufsize=0, env=env)
    def barrier(self, sql):
        self.p.stdin.write((sql+"\n\\echo ARENA_SESSION_READY\n").encode())
        self.p.stdin.flush()
        end = time.monotonic()+10
        output = b""
        while time.monotonic()<end:
            with selectors.DefaultSelector() as selector:
                selector.register(self.p.stdout, selectors.EVENT_READ)
                if not selector.select(max(0,end-time.monotonic())):
                    break
            chunk = os.read(self.p.stdout.fileno(), 65536)
            if not chunk:
                break
            output += chunk
            if b"ARENA_SESSION_READY\n" in output:
                return
        raise Failure("psql did not reach its explicit session barrier")
    def finish(self, sql):
        out,err = self.p.communicate((sql+"\n").encode(), timeout=15)
        return self.p.returncode,out.decode(),err.decode()
    def close(self):
        if self.p.poll() is None:
            try:
                self.p.communicate(b"ROLLBACK;\n",timeout=5)
            except (OSError,subprocess.TimeoutExpired):
                self.p.kill()
                self.p.communicate()

class Cluster:
    """Owns one private data directory and Unix socket; never accepts an external DSN."""
    max_connections = 16

    def __init__(self,args):
        self.args=args
        self.tmp=tempfile.TemporaryDirectory(prefix="arena-pg-",dir="/tmp")
        self.root=Path(self.tmp.name)
        self.data,self.socket=self.root/"data",self.root/"socket"
        self.socket.mkdir(mode=0o700)
        self.log=self.root/"postgres.log"
        self.base={k:v for k,v in os.environ.items() if not k.startswith("PG")}
        self.base.update(LC_ALL="C",TZ="UTC")
        if args.pg_lib:
            self.base["LD_LIBRARY_PATH"]=str(args.pg_lib)+(":"+self.base["LD_LIBRARY_PATH"] if self.base.get("LD_LIBRARY_PATH") else "")
        self.databases={"postgres"}
    def tool(self,name):
        path=self.args.pg_bin/name
        require(path.is_file(),"missing PostgreSQL tool "+name)
        return path
    def environment(self,database="arena_migration_test"):
        require(database in self.databases,"database not owned by harness")
        return dict(self.base,PGHOST=str(self.socket),PGPORT="5432",PGUSER="arena_test",
                    PGDATABASE=database,PG_BIN=str(self.args.pg_bin),PGCONNECT_TIMEOUT="5",
                    PGAPPNAME="arena-storage-acceptance",PGTZ="UTC")
    def psql_command(self):
        return [str(self.tool("psql")),"-X","--no-password","--quiet","--no-align",
                "--tuples-only","--set=ON_ERROR_STOP=1","--set=VERBOSITY=verbose"]
    def sql(self,sql,database="arena_migration_test",expected=0,single=False):
        command=self.psql_command()+(["--single-transaction"] if single else [])
        result=run(command,self.environment(database),sql)
        require(result.returncode==expected,f"psql exit {result.returncode}, expected {expected}: {result.stderr[:1800]}")
        return result
    def create(self,name):
        require(re.fullmatch(r"arena_[a-z_]+",name),"unsafe database name")
        self.sql(f'CREATE DATABASE "{name}";',"postgres")
        self.databases.add(name)
    def start(self):
        require(os.name=="posix" and os.geteuid()!=0,"run private PostgreSQL as an unprivileged Linux user")
        cmd=[self.tool("initdb"),"-D",self.data,"-U","arena_test","--no-locale",
             "--encoding=UTF8","--auth-local=trust","--auth-host=reject"]
        if self.args.pg_share:
            cmd+=["-L",self.args.pg_share]
        result=run(cmd,self.base,timeout=40)
        require(result.returncode==0,"private initdb failed: "+result.stderr[:1500])
        options=f"-c listen_addresses='' -c unix_socket_directories='{self.socket}' -c unix_socket_permissions=0700 -c port=5432 -c max_connections={self.max_connections} -c log_min_messages=warning -c timezone=UTC"
        result=run([self.tool("pg_ctl"),"-D",self.data,"-l",self.log,"-w","-t","30","-o",options,"start"],self.base,timeout=40)
        require(result.returncode==0,"private pg_ctl start failed: "+result.stderr[:1500])
        self.create("arena_migration_test")
    def close(self):
        if (self.data/"postmaster.pid").exists():
            for mode in ("fast","immediate"):
                result=run([self.tool("pg_ctl"),"-D",self.data,"-m",mode,"-w","-t","15","stop"],self.base,timeout=20)
                if result.returncode==0:
                    break
            else:
                raise Failure("cannot stop harness-owned cluster: "+str(self.root))
        self.tmp.cleanup()

class Acceptance:
    def __init__(self,args,cluster):
        self.args,self.cluster=args,cluster
        self.storage=args.repository_root/"services"/"storage"
        self.fixture=(self.storage/"fixtures"/"seed.sql").read_text()
        self.report=dict(passed=False,cycles=[],negative_cases=[],migration_cases=[],concurrency=[])
    def migrate(self,action,database="arena_migration_test",storage=None,expected=0,changed=None,environment=None,version=None):
        result=run([sys.executable,(storage or self.storage)/"migrate.py",action],
                   environment if environment is not None else self.cluster.environment(database),timeout=95)
        require(result.returncode==expected,f"migration {action} exit {result.returncode}, expected {expected}: {result.stderr[:1000]}")
        if expected:
            return result
        value=json.loads(result.stdout)
        require(value["action"]==action and value["state"] in ("applied","unapplied"),"migration JSON mismatch")
        if value["state"] == "applied":
            require(value["version"] in (1,2),"bad applied migration version")
        else:
            require(value["version"] is None,"unapplied migration has a version")
        if version is not None:
            require(value["version"] == version,"unexpected migration head version")
        head = "001_initial" if value["version"] == 1 else "002_asset_transfer"
        for key,direction in (("up_sha256","up"),("down_sha256","down")):
            artifact=(storage or self.storage)/"migrations"/(head+"."+direction+".sql")
            require(value[key] == hashlib.sha256(artifact.read_bytes()).hexdigest(),
                    "bad migration head hash")
        if changed is not None:
            require(value["changed"] is changed,"unexpected changed flag for "+action)
        return value
    def schema(self,database="arena_migration_test"):
        result=run([self.cluster.tool("pg_dump"),"--schema-only","--no-owner","--no-privileges"],self.cluster.environment(database))
        require(result.returncode==0,"pg_dump failed: "+result.stderr[:1000])
        # Only strip pg_dump's random client restriction nonce, never DDL.
        return "\n".join(line for line in result.stdout.splitlines() if not line.startswith(("\\restrict ","\\unrestrict ")))+"\n"
    def catalog(self,database="arena_migration_test"):
        return json.loads(self.cluster.sql(CATALOG,database).stdout)
    def data(self,database="arena_migration_test"):
        names=json.loads(self.cluster.sql("""SELECT coalesce(jsonb_agg(
          format('%I.%I',schemaname,tablename) ORDER BY tablename),'[]'::jsonb)
          FROM pg_catalog.pg_tables WHERE schemaname='arena';""",database).stdout)
        names += ["public.arena_schema_migrations","public.unrelated_guard"]
        parts=[f"'{n.replace(chr(39),chr(39)*2)}',(SELECT coalesce(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text),'[]'::jsonb) FROM {n} t)" for n in names]
        return json.loads(self.cluster.sql("SELECT jsonb_build_object("+",".join(parts)+")::text;",database).stdout)
    def same(self,actual,expected,label):
        require(actual==expected,f"{label} changed: actual={digest(actual)}, expected={digest(expected)}")
    def seed(self,database="arena_migration_test"):
        self.cluster.sql(self.fixture,database,single=True)
        parts=[f"'{t}',(SELECT count(*) FROM arena.{t})" for t in TABLES]
        counts=json.loads(self.cluster.sql("SELECT jsonb_build_object("+",".join(parts)+")::text;",database).stdout)
        self.same(counts,COUNTS,"fixture counts")
        facts=self.cluster.sql("""SELECT
          (SELECT count(*) FROM arena.account WHERE account_name IN ('alice','Alice')),
          (SELECT player_id FROM arena.player WHERE player_id=18446744073709551615),
          (SELECT final_checksum FROM arena.game_match WHERE match_id=1001),
          (SELECT count(*) FROM arena.outbox o JOIN arena.settlement s ON s.settlement_id=o.settlement_id WHERE s.state='committed');""",database).stdout.strip()
        require(facts=="2|18446744073709551615|18446744073709551615|2","fixture narrowed unsigned values or case-sensitive identities")
    def negatives(self):
        baseline=self.data()
        for case in negative_cases():
            result=self.cluster.sql("BEGIN;\n"+case["sql"]+"\nROLLBACK;",expected=3)
            state=re.search(r"ERROR:\s+([0-9A-Z]{5}):",result.stderr)
            require(state and state.group(1)==case["state"],case["name"]+": wrong SQLSTATE "+result.stderr[:1500])
            constraint=case.get("constraint")
            if constraint:
                require(re.search(r"CONSTRAINT NAME:\s+"+re.escape(constraint)+r"\b",result.stderr),case["name"]+": wrong constraint "+result.stderr[:1500])
            self.same(self.data(),baseline,case["name"]+" rollback")
            self.report["negative_cases"].append(dict(case=case["name"],sqlstate=state.group(1),constraint=constraint))
        self.cluster.sql("BEGIN; SELECT 0::arena.uint64,18446744073709551615::arena.uint64; UPDATE arena.inventory SET quantity=9223372036854775807 WHERE player_id=42 AND item_id=1; ROLLBACK;")
        self.same(self.data(),baseline,"legal integer limits")

    def drift_and_rollback(self,schema,catalog):
        data=self.data()
        with tempfile.TemporaryDirectory(prefix="arena-migration-copy-") as directory:
            copy=Path(directory)/"storage"
            shutil.copytree(self.storage,copy)
            down=copy/"migrations"/"001_initial.down.sql"
            raw=down.read_bytes()
            anchor=b"DROP TABLE arena.account;"
            require(raw.count(anchor)==1,"down fault anchor changed")
            faulty=raw.replace(anchor,b"SELECT 1/0;\n"+anchor)
            down.write_bytes(faulty)
            self.cluster.sql("UPDATE public.arena_schema_migrations SET down_sha256='"+
                             hashlib.sha256(faulty).hexdigest()+"' WHERE version=1;")
            injected_data=self.data()
            result=self.migrate("down",storage=copy,expected=2)
            require("22012" in result.stderr,"down fault rejected before late DDL injection")
            self.same(self.schema(),schema,"failed reverse-version down schema")
            self.same(self.catalog(),catalog,"failed reverse-version down catalog")
            self.same(self.data(),injected_data,"failed reverse-version down data")
            self.report["migration_cases"].append("mid-down-DDL-22012-atomic-across-002-001")
            self.cluster.sql("UPDATE public.arena_schema_migrations SET down_sha256='"+
                             hashlib.sha256(raw).hexdigest()+"' WHERE version=1;")
            self.same(self.data(),data,"down fault restoration")
        original=data["public.arena_schema_migrations"]
        def restore_history():
            statements=["DELETE FROM public.arena_schema_migrations;"]
            for row in original:
                values=[str(row["version"])]+[
                    "'"+row[key].replace("'","''")+"'"
                    for key in ("name","up_sha256","down_sha256","applied_at")]
                statements.append("INSERT INTO public.arena_schema_migrations "
                                  "(version,name,up_sha256,down_sha256,applied_at) VALUES("
                                  +",".join(values)+");")
            self.cluster.sql("\n".join(statements),single=True)
        for label,mutate in (
            ("checksum-001","UPDATE public.arena_schema_migrations SET up_sha256=repeat('0',64) WHERE version=1;"),
            ("checksum-002","UPDATE public.arena_schema_migrations SET down_sha256=repeat('0',64) WHERE version=2;"),
            ("version","UPDATE public.arena_schema_migrations SET version=3 WHERE version=2;"),
            ("name","UPDATE public.arena_schema_migrations SET name='002_other' WHERE version=2;"),
            ("order","INSERT INTO public.arena_schema_migrations(version,name,up_sha256,down_sha256) SELECT 3,'003_other',up_sha256,down_sha256 FROM public.arena_schema_migrations WHERE version=2;"),
            ("gap","DELETE FROM public.arena_schema_migrations WHERE version=1;"),
            ("shape","ALTER TABLE public.arena_schema_migrations ADD COLUMN extra integer;")):
            self.cluster.sql(mutate)
            before=self.schema(),self.catalog(),self.data()
            for action in ("status","up","down"):
                self.migrate(action,expected=2)
                self.same((self.schema(),self.catalog(),self.data()),before,label+" drift refusal")
            if label=="shape":
                self.cluster.sql("ALTER TABLE public.arena_schema_migrations DROP COLUMN extra;")
            else:
                restore_history()
            self.same(self.data(),data,label+" drift restoration")
            self.report["migration_cases"].append(label+"-drift-refused")
        for name in ("001_initial","002_asset_transfer"):
            for direction in ("up","down"):
                with tempfile.TemporaryDirectory(prefix="arena-artifact-drift-") as directory:
                    copy=Path(directory)/"storage"
                    shutil.copytree(self.storage,copy)
                    artifact=copy/"migrations"/(name+"."+direction+".sql")
                    artifact.write_bytes(artifact.read_bytes()+b"\n-- changed reviewed bytes\n")
                    before=self.schema(),self.catalog(),self.data()
                    for action in ("status","up","down"):
                        self.migrate(action,storage=copy,expected=2)
                        self.same((self.schema(),self.catalog(),self.data()),before,
                                  name+" "+direction+" artifact refusal")
                    self.report["migration_cases"].append(name+"-"+direction+"-artifact-drift-refused")

    def legacy_prefix(self):
        """Deploy the previous five-column 001 history independently of the new CLI."""
        db="arena_legacy_upgrade"
        self.cluster.create(db)
        self.cluster.sql("CREATE TABLE public.unrelated_guard(id integer PRIMARY KEY,value text NOT NULL);"
                         "INSERT INTO public.unrelated_guard VALUES(1,'preserve me');",db)
        empty=self.schema(db),self.catalog(db)
        up=self.storage/"migrations"/"001_initial.up.sql"
        down=self.storage/"migrations"/"001_initial.down.sql"
        legacy=up.read_text()+"""
          CREATE TABLE public.arena_schema_migrations (
            version smallint NOT NULL PRIMARY KEY CHECK(version>0),
            name text NOT NULL,
            up_sha256 text NOT NULL CHECK(up_sha256 ~ '^[0-9a-f]{64}$'),
            down_sha256 text NOT NULL CHECK(down_sha256 ~ '^[0-9a-f]{64}$'),
            applied_at timestamptz NOT NULL DEFAULT pg_catalog.clock_timestamp());
        """+("INSERT INTO public.arena_schema_migrations(version,name,up_sha256,down_sha256) "
             "VALUES(1,'001_initial','"+hashlib.sha256(up.read_bytes()).hexdigest()+"','"
             +hashlib.sha256(down.read_bytes()).hexdigest()+"');")
        self.cluster.sql(legacy,db,single=True)
        self.cluster.sql(self.fixture,db,single=True)
        before=self.schema(db),self.catalog(db),self.data(db)
        self.migrate("status",db,changed=False,version=1)
        self.same((self.schema(db),self.catalog(db),self.data(db)),before,
                  "legacy prefix status is read-only")
        with tempfile.TemporaryDirectory(prefix="arena-upgrade-fault-") as directory:
            copy=Path(directory)/"storage"
            shutil.copytree(self.storage,copy)
            artifact=copy/"migrations"/"002_asset_transfer.up.sql"
            artifact.write_bytes(artifact.read_bytes()+b"\nSELECT 1/0;\n")
            result=self.migrate("up",db,storage=copy,expected=2)
            require("22012" in result.stderr,"002 upgrade fault was not reached")
            self.same((self.schema(db),self.catalog(db),self.data(db)),before,
                      "failed 002 upgrade preserves deployed 001 and data")
        self.migrate("up",db,changed=True,version=2)
        after=self.data(db)
        for table,rows in before[2].items():
            if table=="public.arena_schema_migrations":
                rows_after=[row for row in after[table] if row["version"]==1]
            else:
                rows_after=after[table]
            self.same(rows_after,rows,"001 data/history survives additive upgrade: "+table)
        self.same(after["arena.asset_transfer"],[],"new transfer receipt table")
        self.migrate("status",db,changed=False,version=2)
        self.migrate("up",db,changed=False,version=2)
        self.migrate("down",db,changed=True)
        self.same((self.schema(db),self.catalog(db)),empty,"upgraded prefix down to empty")
        self.cluster.sql(legacy,db,single=True)
        self.cluster.sql(self.fixture,db,single=True)
        self.migrate("down",db,changed=True)
        self.same((self.schema(db),self.catalog(db)),empty,"001-only prefix down to empty")
        self.report["migration_cases"] += ["deployed-001-prefix-readonly-status",
            "failed-002-upgrade-preserves-001", "001-to-002-preserves-data-and-history",
            "001-only-prefix-reverse-down"]

    def concurrency(self):
        setup="""INSERT INTO arena.game_match(match_id,room_id,created_at)
          VALUES(9001,9001,'2026-10-26 00:00:00+00');
          INSERT INTO arena.match_player(match_id,player_id,slot,session_id,membership_epoch,initial_mmr)
          VALUES(9001,42,1,501,1,1000),(9001,43,2,502,1,1200);"""
        start="UPDATE arena.game_match SET state='running',started_at='2026-10-26 00:00:01+00' WHERE match_id=9001;"
        for kind in ("repeatable_read","read_committed","start_wins"):
            db="arena_"+kind
            self.cluster.create(db)
            self.migrate("up",db)
            self.seed(db)
            self.cluster.sql(setup,db)
            a,b=Session(self.cluster,db,"arena-session-a"),Session(self.cluster,db,"arena-session-b")
            try:
                if kind=="repeatable_read":
                    a.barrier("BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT count(*) FROM arena.match_player WHERE match_id=9001;")
                    b.barrier("BEGIN; DELETE FROM arena.match_player WHERE match_id=9001 AND slot=2;")
                    require(b.finish("COMMIT;")[0]==0,"RR roster-removal commit failed")
                    code,_,error=a.finish(start+"COMMIT;")
                    require(code==3 and re.search(r"ERROR:\s+40001:",error),"RR stale roster escaped serialization: "+error[:1000])
                    state,count,sqlstate="forming",1,"40001"
                else:
                    if kind=="read_committed":
                        b.barrier("BEGIN; DELETE FROM arena.match_player WHERE match_id=9001 AND slot=2;")
                        waiting,owner,statement,app=a,b,start,"arena-session-a"
                    else:
                        a.barrier("BEGIN;"+start)
                        waiting,owner,statement,app=b,a,"DELETE FROM arena.match_player WHERE match_id=9001 AND slot=2;","arena-session-b"
                    waiting.p.stdin.write((statement+"\n").encode())
                    waiting.p.stdin.flush()
                    end=time.monotonic()+8
                    while time.monotonic()<end:
                        locks=self.cluster.sql("SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='"+app+"' AND wait_event_type='Lock';",db).stdout.strip()
                        if locks=="1":
                            break
                        time.sleep(0.01)
                    else:
                        raise Failure("RC race did not reach the expected parent-row lock")
                    require(owner.finish("COMMIT;")[0]==0,"RC lock-owner commit failed")
                    code,_,error=waiting.finish("")
                    require(code==3 and re.search(r"ERROR:\s+23514:",error),"RC roster race escaped constraints: "+error[:1000])
                    state,count,sqlstate=("forming",1,"23514") if kind=="read_committed" else ("running",2,"23514")
                actual=self.cluster.sql("SELECT state,(SELECT count(*) FROM arena.match_player WHERE match_id=9001) FROM arena.game_match WHERE match_id=9001;",db).stdout.strip()
                require(actual==f"{state}|{count}","bad committed concurrent roster "+actual)
                self.report["concurrency"].append(dict(case=kind,sqlstate=sqlstate,final=actual))
            finally:
                a.close()
                b.close()

    def execute(self):
        c=self.cluster
        self.report["server_version"]=c.sql("SHOW server_version;").stdout.strip()
        require(int(c.sql("SHOW server_version_num;").stdout.strip())>=140000,"PostgreSQL14+ required")
        c.sql("CREATE TABLE public.unrelated_guard(id integer PRIMARY KEY,value text NOT NULL); INSERT INTO public.unrelated_guard VALUES(1,'preserve me');")
        empty_schema,empty_catalog=self.schema(),self.catalog()
        self.migrate("status",changed=False)
        self.same((self.schema(),self.catalog()),(empty_schema,empty_catalog),"read-only empty status")
        for key in ("PGHOST","PGDATABASE"):
            environment=c.environment()
            environment.pop(key)
            self.migrate("up",expected=2,environment=environment)
        self.report["migration_cases"].append("explicit-connection-required")
        self.migrate("down",changed=False)
        self.legacy_prefix()
        schema=catalog=None
        for cycle in range(self.args.cycles):
            self.migrate("up",changed=True)
            actual=self.schema(),self.catalog()
            if schema is None:
                schema,catalog=actual
            self.same(actual,(schema,catalog),"cycle up catalog/schema")
            if cycle==0:
                before=self.data()
                faulty=self.fixture.replace("INSERT INTO arena.inventory","SELECT 1/0;\nINSERT INTO arena.inventory",1)
                result=c.sql(faulty,expected=3,single=True)
                require(re.search(r"ERROR:\s+22012:",result.stderr),"seed failure injection missed")
                self.same(self.data(),before,"failed seed transaction")
                self.report["migration_cases"].append("mid-seed-22012-atomic")
            self.seed()
            if cycle==0:
                self.migrate("up",changed=False)
                self.negatives()
                self.drift_and_rollback(schema,catalog)
                before=self.data()
                result=c.sql(self.fixture,expected=3,single=True)
                require(re.search(r"ERROR:\s+23505:",result.stderr),"fixture replay was not a unique failure")
                self.same(self.data(),before,"duplicate seed rollback")
            self.migrate("down",changed=True)
            self.same((self.schema(),self.catalog()),(empty_schema,empty_catalog),"cycle down catalog/schema")
            self.migrate("up",changed=True)
            self.same((self.schema(),self.catalog()),(schema,catalog),"cycle second up catalog/schema")
            self.migrate("down",changed=True)
            self.same((self.schema(),self.catalog()),(empty_schema,empty_catalog),"cycle final down catalog/schema")
            self.report["cycles"].append(dict(cycle=cycle+1,schema_sha256=digest(schema),catalog_sha256=digest(catalog)))
        with tempfile.TemporaryDirectory(prefix="arena-up-fault-") as directory:
            copy=Path(directory)/"storage"
            shutil.copytree(self.storage,copy)
            up=copy/"migrations"/"001_initial.up.sql"
            raw=up.read_bytes()
            anchor=b"CREATE TABLE arena.outbox"
            require(raw.count(anchor)==1,"up failure anchor changed")
            up.write_bytes(raw.replace(anchor,b"SELECT 1/0;\n"+anchor))
            result=self.migrate("up",storage=copy,expected=2)
            require("22012" in result.stderr,"up failure injection missed late DDL point")
            self.same((self.schema(),self.catalog()),(empty_schema,empty_catalog),"mid-up DDL rollback")
            self.report["migration_cases"].append("mid-up-DDL-22012-atomic")
        with tempfile.TemporaryDirectory(prefix="arena-002-up-fault-") as directory:
            copy=Path(directory)/"storage"
            shutil.copytree(self.storage,copy)
            artifact=copy/"migrations"/"002_asset_transfer.up.sql"
            artifact.write_bytes(artifact.read_bytes()+b"\nSELECT 1/0;\n")
            result=self.migrate("up",storage=copy,expected=2)
            require("22012" in result.stderr,"002 empty-database fault was not reached")
            self.same((self.schema(),self.catalog()),(empty_schema,empty_catalog),
                      "002 failure rolls back the preceding 001 in the same up")
            self.report["migration_cases"].append("002-failure-rolls-back-001-in-same-up")
        command=[sys.executable,str(self.storage/"migrate.py"),"up"]
        processes=[subprocess.Popen(command,env=c.environment(),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True) for _ in range(4)]
        values=[]
        for process in processes:
            out,err=process.communicate(timeout=95)
            require(process.returncode==0,"concurrent migration failed: "+err[:1000])
            values.append(json.loads(out))
        require(sum(v["changed"] for v in values)==1,"concurrent up applied more than once")
        self.same((self.schema(),self.catalog()),(schema,catalog),"concurrent up schema/catalog")
        self.report["migration_cases"].append("four-concurrent-up-one-commit")
        self.migrate("down",changed=True)
        self.same((self.schema(),self.catalog()),(empty_schema,empty_catalog),"post-concurrent down")
        self.concurrency()
        self.report.update(passed=True,supported_migrations=[1,2],hard_acceptance=self.args.cycles>=20,cycle_count=len(self.report["cycles"]),schema_sha256=digest(schema),catalog_sha256=digest(catalog))
        if self.args.report:
            self.args.report.with_suffix(".schema.sql").write_text(schema)
            self.args.report.with_suffix(".catalog.json").write_text(json.dumps(catalog,indent=2,sort_keys=True)+"\n")
        return self.report

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository-root",type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument("--pg-bin",type=Path,required=True)
    parser.add_argument("--pg-share",type=Path)
    parser.add_argument("--pg-lib",type=Path)
    parser.add_argument("--cycles",type=int,default=20)
    parser.add_argument("--report",type=Path)
    args=parser.parse_args()
    require(args.cycles>=1,"migration cycles must be positive")
    args.repository_root=args.repository_root.resolve()
    args.pg_bin=args.pg_bin.resolve()
    if args.pg_bin.is_file():
        args.pg_bin=args.pg_bin.parent
    if args.report:
        args.report=args.report.resolve()
        args.report.parent.mkdir(parents=True,exist_ok=True)
    signal.signal(signal.SIGTERM,lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    cluster=Cluster(args)
    acceptance=Acceptance(args,cluster)
    result=acceptance.report
    try:
        cluster.start()
        result=acceptance.execute()
    except (Failure,OSError,ValueError,subprocess.SubprocessError,KeyboardInterrupt) as error:
        result.update(passed=False,error=str(error))
        if cluster.log.exists():
            result["server_log_tail"]=cluster.log.read_text(errors="replace")[-4000:]
    finally:
        try:
            cluster.close()
        except (Failure,OSError,subprocess.SubprocessError) as error:
            result.update(passed=False,cleanup_error=str(error))
    text=json.dumps(result,indent=2,sort_keys=True)
    if args.report:
        args.report.write_text(text+"\n")
    print(text)
    return 0 if result["passed"] else 1

if __name__=="__main__":
    try:
        sys.exit(main())
    except Failure as error:
        print(json.dumps(dict(passed=False,error=str(error))))
        sys.exit(2)
