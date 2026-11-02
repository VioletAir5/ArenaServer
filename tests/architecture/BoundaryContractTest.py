#!/usr/bin/env python3
"""Exercise architecture boundaries against current sources and isolated violations."""
import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root', type=Path, required=True)
    parser.add_argument('--report', type=Path, required=True)
    parser.add_argument('--linter', type=Path)
    parser.add_argument('--contract', type=Path)
    parser.add_argument('--policy', type=Path)
    args = parser.parse_args()
    root = args.repository_root.resolve()
    linter = args.linter or root/'tools/architecture/boundary_lint.py'
    contract = args.contract or root/'services/storage/contract.json'
    policy = args.policy or root/'tools/architecture/boundary_policy.json'
    spec = importlib.util.spec_from_file_location('arena_boundary', linter)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    cases = []
    started = time.monotonic()

    def check(value, message):
        if not value:
            raise AssertionError(message)

    def text(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value, encoding='utf-8')

    def contract_change(tree, edit):
        path = tree/'services/storage/contract.json'
        value = json.loads(path.read_text())
        edit(value)
        text(path, json.dumps(value))

    baseline = module.analyze(root, contract, policy)
    check(baseline['passed'], str(baseline['issues']))
    check(set(baseline['contracts']['declared']) == {
        'account.find','player.get','auth.authenticate','health.ready','match.allocate_ids',
        'match.save_result','match.get','settlement.get','lobby.execute','lobby.detach','lobby.heartbeat'},
        'actual Gateway/Auth/Lobby/Storage method contract set changed')
    check(baseline['contracts']['declared'] == baseline['contracts']['registered'],
          'a declared method is missing an actual dispatch registration')
    check(all(not cycles for cycles in baseline['cycles'].values()), 'baseline dependency cycle')
    check({entry['method'] for entry in baseline['contracts']['native_and_python_calls']} ==
          {'account.find','player.get','auth.authenticate','match.allocate_ids','match.save_result',
           'match.get','settlement.get','health.ready','lobby.execute','lobby.detach','lobby.heartbeat'},
          'current native method coverage must follow real source calls')
    check(baseline['contracts']['qualified_declared'] == baseline['contracts']['qualified_registered']
          and set(baseline['contracts']['qualified_declared']) == {
              'storage/health.ready','storage/account.find','storage/player.get','storage/match.allocate_ids',
              'storage/match.save_result','storage/match.get','storage/settlement.get',
              'auth/health.ready','auth/auth.authenticate',
              'lobby/health.ready','lobby/lobby.execute','lobby/lobby.detach','lobby/lobby.heartbeat'},
          'service-qualified registrations must include every real service health and business handler')
    check({'auth/auth.authenticate','auth/health.ready','storage/health.ready','lobby/health.ready'} <=
          set(baseline['contracts']['qualified_declared']), 'native listener coverage missing')
    messages, _ = module._proto((root/'shared/proto/auth.proto').read_text())
    check(messages['AuthenticateRequest']['proposed_epoch'] == ('uint64', 1)
          and messages['AuthenticateRequest']['credential_kind'] == ('CredentialKind', 2),
          'nested enum must not hide its enclosing message fields')
    cases.append(dict(name='current_production_sources_and_thirteen_qualified_rpc_contracts', passed=True))

    def mutation(name, mutate, code):
        with tempfile.TemporaryDirectory(prefix='arena-boundary-') as directory:
            tree = Path(directory)
            for source in ('services', 'shared', 'domain'):
                shutil.copytree(root/source, tree/source,
                    ignore=shutil.ignore_patterns('__pycache__', '*.pyc'))
            shutil.copyfile(root/'CMakeLists.txt', tree/'CMakeLists.txt')
            text(tree/'services/storage/contract.json', contract.read_text(encoding='utf-8-sig'))
            policy_copy = tree/'tools/architecture/boundary_policy.json'
            text(policy_copy, policy.read_text(encoding='utf-8-sig'))
            mutate(tree)
            result = module.analyze(tree)
            codes = {item['code'] for item in result['issues']}
            check(not result['passed'] and code in codes,
                  f'{name}: expected {code}, got {result["issues"]}')
            cases.append(dict(name=name, passed=True, expected_rejection=code,
                              diagnostics=result['issues'], cycles=result['cycles']))

    mutation('lobby_includes_room_private_implementation',
        lambda tree: text(tree/'services/lobby/src/Bad.cpp',
                          '#include "../../room/src/RoomActor.cpp"\n'), 'private_implementation')
    mutation('auth_directly_reads_player_table',
        lambda tree: text(tree/'services/auth/Bad.py',
                          'def read(connection):\n    return connection.execute("SELECT player_id FROM arena.player")\n'),
        'sql_boundary')
    mutation('auth_imports_storage_repository',
        lambda tree: text(tree/'services/auth/Bad.py', 'from services.storage.repositories import AccountRepository\n'),
        'import_boundary')
    mutation('auth_imports_database_driver',
        lambda tree: text(tree/'services/auth/Bad.py', 'import psycopg\n'), 'database_driver')
    mutation('auth_reads_unqualified_player_with_split_sql',
        lambda tree: text(tree/'services/auth/Bad.py',
                          'def read(connection):\n    return connection.execute("SELECT player_id " + "FROM player")\n'),
        'sql_boundary')
    mutation('gateway_embeds_sql_in_adjacent_cpp_literals',
        lambda tree: text(tree/'services/gateway/src/net/Bad.cpp',
                          'const char* query = "SELECT player_id " "FROM arena.player";\n'),
        'sql_boundary')
    mutation('storage_orchestrator_builds_sql',
        lambda tree: text(tree/'services/storage/application.py',
                          'def bypass(connection):\n    return connection.execute("SELECT * FROM arena.inventory")\n'),
        'sql_boundary')

    def build_cycle(tree):
        path = tree/'shared/CMakeLists.txt'
        path.write_text(path.read_text()+'\ntarget_link_libraries(arena_shared PRIVATE arena_gateway)\n')
    mutation('cmake_service_dependency_cycle', build_cycle, 'build_cycle')

    def include_cycle(tree):
        text(tree/'services/auth/include/auth/BoundaryA.h', '#include "auth/BoundaryB.h"\n')
        text(tree/'services/auth/include/auth/BoundaryB.h', '#include "auth/BoundaryA.h"\n')
    mutation('public_header_include_cycle', include_cycle, 'include_cycle')
    mutation('dynamic_service_rpc_method_is_not_silently_ignored',
        lambda tree: text(tree/'services/auth/Bad.py',
                          'async def forward(client, method):\n    return await client.call(method)\n'),
        'dynamic_rpc_method')

    def reverse_rpc(tree):
        source = 'services/gateway/cycle_fixture.py'
        text(tree/source,
            'class GatewayService:\n'
            '    def __init__(self):\n'
            '        self.handlers = {}\n'
            '        self.handlers["gateway.echo"] = self._echo\n'
            '    async def _echo(self, payload, context):\n'
            '        return b""\n')
        text(tree/'services/storage/reverse_rpc_fixture.py',
             'async def reverse(client, context):\n'
             '    return await client.call("gateway.echo", context=context)\n')
        value = json.loads((tree/'services/storage/contract.json').read_text())
        value.update(service='gateway', implementation=source, service_class='GatewayService')
        value.pop('physical_sql_owner', None)
        value.pop('data_owners', None)
        value['methods'] = [dict(name='gateway.echo', owner='gateway', logical_owner='gateway',
            callers=['storage'], handler='_echo', request=None, response=None,
            deadline_ms=5000, errors=[], idempotency=dict(kind='read_only', key=None, automatic_retries=0),
            cancellation='cooperative')]
        text(tree/'services/gateway/contract.json', json.dumps(value))
    mutation('two_services_make_synchronous_rpc_cycle', reverse_rpc, 'rpc_cycle')

    mutation('rpc_owner_declaration_missing',
        lambda tree: contract_change(tree, lambda value: value['methods'][1].pop('owner')),
        'contract_fields')
    mutation('rpc_deadline_exceeds_transport',
        lambda tree: contract_change(tree, lambda value: value['methods'][1].update(deadline_ms=5001)),
        'contract_deadline')
    mutation('rpc_unknown_error_enum',
        lambda tree: contract_change(tree, lambda value: value['methods'][1]['errors'].append('SECRET_SQL_ERROR')),
        'contract_errors')
    mutation('non_idempotent_id_allocation_enables_retries',
        lambda tree: contract_change(tree, lambda value: value['methods'][3]['idempotency'].update(automatic_retries=1)),
        'contract_idempotency')
    mutation('declared_request_type_disagrees_with_real_registration',
        lambda tree: contract_change(tree, lambda value: value['methods'][1].update(request='PlayerGetRequest')),
        'contract_registration')
    mutation('declared_reply_disagrees_with_real_handler',
        lambda tree: contract_change(tree, lambda value: value['methods'][1].update(response='PlayerGetReply')),
        'contract_response')
    mutation('registered_api_has_no_contract',
        lambda tree: contract_change(tree, lambda value: value['methods'].pop(1)), 'contract_orphan')
    mutation('new_production_sql_file_outside_storage',
        lambda tree: text(tree/'services/auth/Bad.sql', 'SELECT * FROM arena.player;\n'), 'sql_file_boundary')

    def changed_envelope(tree):
        path = tree/'shared/proto/rpc.proto'
        path.write_text(path.read_text().replace('uint64 deadline_unix_ms = 3;', 'uint64 deadline_unix_ms = 9;'))
    mutation('deadline_wire_tag_is_changed', changed_envelope, 'rpc_envelope')

    mutation('cmake_dynamic_dependency_is_not_silently_omitted',
        lambda tree: text(tree/'services/auth/Dynamic/CMakeLists.txt',
            'target_link_libraries(arena_auth PRIVATE ${HIDDEN_DEPENDENCY})\n'),
        'cmake_dynamic_dependency')
    mutation('ambiguous_public_header_is_not_arbitrarily_resolved',
        lambda tree: text(tree/'services/auth/include/net/ConnectionChannel.h', '#pragma once\n'),
        'ambiguous_project_include')
    mutation('new_table_requires_explicit_logical_owner',
        lambda tree: text(tree/'services/storage/migrations/006_unowned.up.sql',
            'CREATE TABLE arena.unowned_table (id bigint PRIMARY KEY);\n'), 'data_owner')
    mutation('existing_item_table_loses_owner',
        lambda tree: contract_change(tree, lambda value: value['data_owners'].pop('arena.item')),
        'data_owner')
    mutation('invalid_reply_type_is_reported_without_checker_crash',
        lambda tree: contract_change(tree, lambda value: value['methods'][1].update(response=[])),
        'contract_proto')
    def wrapped_registration(tree):
        path = tree/'services/storage/server.py'
        value = path.read_text()
        value = value.replace('        self.handlers = {}',
            '        self.handlers = {}\n'
            '        self.handlers["player.peek"] = self._handler("PlayerGetRequest", self._player)', 1)
        path.write_text(value)
    mutation('new_wrapped_handler_requires_a_contract', wrapped_registration, 'contract_orphan')
    def registry_update(tree):
        path = tree/'services/storage/server.py'
        value = path.read_text().replace('        self.handlers = {}',
            '        self.handlers = {}\n'
            '        self.handlers.update({"player.peek": self._handler("PlayerGetRequest", self._player)})', 1)
        path.write_text(value)
    mutation('unsupported_handler_map_update_fails_closed', registry_update, 'contract_registration_dynamic')
    def mismatched_loop_binding(tree):
        path = tree/'services/storage/server.py'
        value = path.read_text()
        old = 'self.handlers[method] = self._handler(request, handler)'
        check(old in value, 'actual registration syntax changed; review fixture')
        value = value.replace(old,
            'self.handlers[method] = self._handler("PlayerGetRequest", self._player)', 1)
        path.write_text(value)
    mutation('literal_registration_rows_must_match_actual_loop_binding',
             mismatched_loop_binding, 'contract_registration_dynamic')
    def native_source_change(tree, old, new):
        path = tree/'services/auth/app/main.cpp'
        value = path.read_text()
        check(old in value, 'native registration fixture no longer matches actual source')
        path.write_text(value.replace(old, new, 1))

    def auth_contract_change(tree, edit):
        path = tree/'services/auth/contract.json'
        value = json.loads(path.read_text())
        edit(value)
        text(path, json.dumps(value))

    mutation('native_method_must_have_a_contract',
        lambda tree: native_source_change(tree, '"auth.authenticate",', '"auth.extra",'),
        'contract_orphan')
    mutation('native_dynamic_method_cannot_hide_a_registration',
        lambda tree: native_source_change(tree, '"auth.authenticate",', 'methodName,'),
        'contract_registration_dynamic')
    mutation('native_request_type_matches_actual_parser',
        lambda tree: native_source_change(tree, 'arena::auth::AuthenticateRequest credential;',
                                         'arena::storage::PlayerGetRequest credential;'),
        'contract_registration')
    mutation('native_reply_type_matches_actual_serialization',
        lambda tree: auth_contract_change(tree, lambda value: value['methods'][1].update(
            response='arena.storage.PlayerGetReply')), 'contract_response')
    mutation('native_unconsumed_map_is_not_treated_as_a_listener',
        lambda tree: native_source_change(tree, 'std::move(handlers)', 'Server::Handlers{}'),
        'contract_registration_dynamic')
    mutation('native_map_removal_is_rejected',
        lambda tree: native_source_change(tree, '    Server server(',
            '    handlers.erase("auth.authenticate");\n    Server server('),
        'contract_registration_dynamic')
    mutation('native_duplicate_method_is_rejected',
        lambda tree: native_source_change(tree, '    Server server(',
            '    handlers.emplace("health.ready", [](auto, auto, auto) {});\n    Server server('),
        'contract_registration_dynamic')
    mutation('native_service_cannot_omit_its_contract',
        lambda tree: text(tree/'services/lobby/app/Uncatalogued.cpp',
            'void bind() { Server::Handlers handlers; '
            'handlers.emplace("lobby.extra", [](auto, auto, auto) {}); '
            'Server server(executor, config, std::move(handlers)); }\n'),
        'contract_native_undeclared')
    mutation('native_declared_budget_must_match_real_server',
        lambda tree: auth_contract_change(tree, lambda value: value['transport'].update(
            max_timeout_ms=2000)), 'contract_transport')
    mutation('native_explicit_wrong_target_is_not_inferred_away',
        lambda tree: text(tree/'services/lobby/src/BadTarget.cpp',
            'void forward() { arena::rpc::Request wire; wire.set_method("auth.authenticate"); '
            'wire.set_target_service("storage"); }\n'), 'rpc_undeclared')

    def ambiguous_payload(tree):
        text(tree/'shared/proto/ambiguous.proto',
             'syntax = "proto3"; package arena.other; message AuthenticateReply { uint64 id=1; }\n')
        auth_contract_change(tree, lambda value: value['methods'][1].update(response='AuthenticateReply'))
    mutation('ambiguous_short_proto_name_requires_qualification', ambiguous_payload, 'contract_proto')
    mutation('native_protocol_version_tag_is_stable',
        lambda tree: text(tree/'shared/proto/rpc.proto',
            (tree/'shared/proto/rpc.proto').read_text().replace(
                'uint32 protocol_version = 8;', 'uint32 protocol_version = 15;')),
        'rpc_envelope')

    mutation('malformed_native_payload_schema_reports_a_diagnostic',
        lambda tree: text(tree/'shared/proto/auth.proto',
            'syntax = "proto3"; package arena.auth; message AuthenticateRequest { uint64 epoch=1;'),
        'proto_parse')

    def native_registration_decoy(tree):
        path = tree/'services/auth/app/main.cpp'
        value = path.read_text()
        start = value.index('    handlers.emplace(\n        "auth.authenticate",')
        begin = value.index('(', start)
        end = module._balanced_end(module._cpp(value), begin, '(', ')') + 2
        decoy = '    const char* decoy = R"ARENA(' + value[start:end] + ')ARENA";\n'
        path.write_text(value[:start] + decoy + value[end:])
    mutation('raw_string_cannot_replace_an_actual_native_registration',
             native_registration_decoy, 'contract_registration')
    mutation('explicit_dynamic_rpc_target_cannot_be_inferred_away',
        lambda tree: text(tree/'services/lobby/src/BadDynamicTarget.cpp',
            'void forward() { arena::rpc::Request wire; wire.set_method("auth.authenticate"); '
            'wire.set_target_service(runtimeTarget); }\n'), 'dynamic_rpc_target')
    def serialized_reply_decoy(tree):
        path = 'services/auth/DecoyReply.cpp'
        text(tree/path, 'const char* decoy = R"(arena::auth::AuthenticateReply grant; '
                       'grant.SerializeAsString();)";\n')
        auth_contract_change(tree, lambda value: value['methods'][1].update(response_implementation=path))
    mutation('reply_serialization_inside_a_string_is_not_a_handler',
             serialized_reply_decoy, 'contract_response')

    report = dict(passed=True, scope='normal source boundary and executable contract acceptance',
        cases=cases, elapsed_seconds=time.monotonic()-started,
        source_inventory=baseline['inventory'], baseline_cycles=baseline['cycles'],
        baseline_contracts=baseline['contracts'], limitations=baseline['limitations'])
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')
    print(f'ArchitectureBoundaryContract: {len(cases)} cases passed')


if __name__ == '__main__':
    main()