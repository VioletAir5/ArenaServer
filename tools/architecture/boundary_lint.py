"""Inspect source dependencies, SQL ownership and declared RPC contracts.

This is a conservative repository boundary check, not a general C++ compiler or
proof of arbitrary dynamically constructed SQL/RPC. Unsupported dynamic service
method selection is rejected rather than silently accepted.
"""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
import json
from pathlib import Path, PurePosixPath
import re
import shlex

SUFFIXES = {".h", ".hpp", ".hh", ".cpp", ".cc", ".c", ".py", ".sql", ".proto"}
CPP = {".h", ".hpp", ".hh", ".cpp", ".cc", ".c"}
SQL = re.compile(r"\b(?:SELECT\s+(?:[^;]*\bFROM\b|(?:pg_catalog\.)?[a-z_]+\s*\()|INSERT\s+INTO\s+|UPDATE\s+(?:[a-z_]+\.)?[a-z_]+\s+SET\s+|DELETE\s+FROM\s+|(?:CREATE|ALTER|DROP)\s+TABLE\s+)", re.I | re.S)
CPP_TOKEN = re.compile(r'//[^\n]*|/\*.*?\*/|R"(?P<delim>[^ ()\\\t\r\n]{0,16})\(.*?\)(?P=delim)"|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'', re.S)
INCLUDE = re.compile(r'^\s*#\s*include\s*[<"]([^>"\n]+)[>"]', re.M)
COMMON_REQUEST = {"request_id": ("uint64",1), "correlation_id": ("string",2), "deadline_unix_ms": ("uint64",3), "method": ("string",4), "idempotency_key": ("string",5), "payload": ("bytes",6), "service_token": ("string",7), "protocol_version": ("uint32",8), "caller_service": ("string",9), "service_path": ("string",10), "target_service": ("string",11)}
COMMON_RESPONSE = {"request_id": ("uint64",1), "correlation_id": ("string",2), "status": ("Status",3), "retryable": ("bool",4), "payload": ("bytes",5), "protocol_version": ("uint32",6)}


def _line(text, offset):
    return text.count("\n", 0, offset) + 1


def _owner(path):
    bits = PurePosixPath(path).parts
    return bits[1] if bits and bits[0] == "services" and len(bits)>1 else (bits[0] if bits else '')


def _cpp(text):
    """Blank comments while preserving source positions and string contents."""
    def replace(match):
        value = match.group()
        return re.sub(r"[^\n]", " ", value) if value.startswith(("//", "/*")) else value
    return CPP_TOKEN.sub(replace, text)


def _cpp_code(text):
    """Mask literals for code-position discovery while retaining exact offsets."""
    return CPP_TOKEN.sub(lambda match: re.sub(r"[^\n]", " ", match.group()), _cpp(text))


def _strings(text):
    clean=_cpp(text)
    previous_end=None
    accumulated=''
    start_line=1
    for match in CPP_TOKEN.finditer(clean):
        value=match.group()
        if value.startswith('R"'):
            value=value[value.index("(")+1:value.rindex(")")]
        elif value.startswith('"'):
            try:
                value=ast.literal_eval(value)
            except (SyntaxError,ValueError):
                value=value[1:-1]
        else:
            previous_end=None
            accumulated=''
            continue
        line=_line(text,match.start())
        yield value,line
        gap=clean[previous_end:match.start()] if previous_end is not None else 'reset'
        if re.fullmatch(r"\s*(?:u8|u|U|L)?",gap):
            accumulated+=value
            yield accumulated,start_line
        else:
            accumulated=value
            start_line=line
        previous_end=match.end()


def _constant_string(node):
    if isinstance(node,ast.Constant) and isinstance(node.value,str):
        return node.value
    if isinstance(node,ast.BinOp) and isinstance(node.op,ast.Add):
        left,right=_constant_string(node.left),_constant_string(node.right)
        return left+right if left is not None and right is not None else None
    if isinstance(node,ast.JoinedStr):
        return ''.join(value.value if isinstance(value,ast.Constant) else ' dynamic ' for value in node.values)
    return None


def _scc(edges):
    """Tarjan SCC; return only cycles, including self edges."""
    graph = defaultdict(set)
    for a,b in edges:
        graph[a].add(b)
        graph[b]
    index = 0
    indices, low, stack, active, found = {}, {}, [], set(), []
    def visit(node):
        nonlocal index
        indices[node] = low[node] = index
        index += 1
        stack.append(node)
        active.add(node)
        for child in sorted(graph[node]):
            if child not in indices:
                visit(child)
                low[node] = min(low[node], low[child])
            elif child in active:
                low[node] = min(low[node], indices[child])
        if low[node] == indices[node]:
            part=[]
            while True:
                child=stack.pop()
                active.remove(child)
                part.append(child)
                if child==node:
                    break
            if len(part)>1 or node in graph[node]:
                found.append(sorted(part))
    for node in sorted(graph):
        if node not in indices:
            visit(node)
    return sorted(found)


def _cmake_commands(text):
    # The repository uses plain commands/quoted arguments. Ignore comments,
    # preserve parenthesized generator expressions and quoted whitespace.
    clean=re.sub(r"#[^\n]*", "", text)
    for match in re.finditer(r"\b(add_library|add_executable|target_link_libraries|add_dependencies)\s*\(",clean,re.I):
        start=match.end()
        pos,depth,quote=start,1,False
        while pos<len(clean) and depth:
            ch=clean[pos]
            if ch=='"' and (pos==0 or clean[pos-1]!='\\'):
                quote=not quote
            if not quote:
                depth += (ch=='(')-(ch==')')
            pos+=1
        if depth:
            raise ValueError("unterminated CMake command")
        yield match.group(1).lower(),shlex.split(clean[start:pos-1],posix=True),_line(clean,match.start())


def _balanced_end(text, at, opening, closing):
    """Match one source delimiter while ignoring string and character literals."""
    depth = 0
    for_position = at
    while for_position < len(text):
        token = CPP_TOKEN.match(text, for_position)
        if token:
            for_position = token.end()
            continue
        value = text[for_position]
        if value == opening:
            depth += 1
        elif value == closing:
            depth -= 1
            if depth == 0:
                return for_position
        for_position += 1
    raise ValueError('unclosed source delimiter')


def _proto(text):
    clean = _cpp(text)
    result = {}
    for match in re.finditer(r"\bmessage\s+(\w+)\s*\{", clean):
        begin = match.end() - 1
        end = _balanced_end(clean, begin, '{', '}')
        body = clean[begin + 1:end]
        # Nested enum/message fields are not fields of this message.
        direct = []
        position = 0
        while position < len(body):
            if body[position] == '{':
                position = _balanced_end(body, position, '{', '}') + 1
            else:
                direct.append(body[position])
                position += 1
        result[match.group(1)] = {
            m.group(2): (m.group(1), int(m.group(3)))
            for m in re.finditer(r"(?:repeated\s+|optional\s+)?([\w.]+)\s+(\w+)\s*=\s*(\d+)\s*;", ''.join(direct))}
    statuses = set()
    match = re.search(r"\benum\s+Status\s*\{([^{}]*)\}", clean, re.S)
    if match:
        statuses = {m.group(1) for m in re.finditer(r"(\w+)\s*=\s*\d+\s*;", match.group(1))}
    return result, statuses


def _native_registrations(text):
    """Inspect actual native Server handler maps; unsupported mutation fails closed."""
    clean = _cpp(text)
    code = _cpp_code(text)
    registered, handlers, problems = {}, {}, []
    maps = list(re.finditer(r'\b(?:arena::internal_rpc::)?Server::Handlers\s+(\w+)\s*;', code))
    constructors = []
    for constructor in re.finditer(r'\b(?:arena::internal_rpc::)?Server\s+\w+\s*\(', code):
        begin = constructor.end() - 1
        try:
            end = _balanced_end(clean, begin, '(', ')')
            constructors.append(code[begin:end + 1])
        except ValueError:
            problems.append((_line(clean, begin), 'unclosed native Server construction'))
    for declaration in maps:
        registry = declaration.group(1)
        if not any(re.search(r'\bstd::move\s*\(\s*' + re.escape(registry) + r'\s*\)', body)
                   for body in constructors):
            problems.append((_line(clean, declaration.start()), 'native handler registry is not consumed by a Server'))
        pattern = r'\b' + re.escape(registry) + r'\s*(?:\.\s*(\w+)\s*\(|(\[)|=)'
        for mutation in re.finditer(pattern, code):
            line = _line(clean, mutation.start())
            if mutation.group(1) != 'emplace':
                problems.append((line, 'unsupported native handler map mutation'))
                continue
            begin = mutation.end() - 1
            try:
                end = _balanced_end(clean, begin, '(', ')')
            except ValueError:
                problems.append((line, 'unclosed native handler registration'))
                continue
            body = clean[begin + 1:end]
            body_code = code[begin + 1:end]
            first = re.match(r'\s*"([a-z][a-z0-9_.]*)"\s*,\s*(\[)', body)
            if not first:
                problems.append((line, 'native registration requires a literal method and inline handler'))
                continue
            name = first.group(1)
            if name in registered:
                problems.append((line, 'duplicate native handler registration'))
            parsed = []
            for candidate in re.finditer(r'\b((?:\w+::)+\w+)\s+(\w+)\s*;', body_code):
                if re.search(r'\b' + re.escape(candidate.group(2)) + r'\s*\.\s*ParseFromString\s*\(', body_code):
                    parsed.append(candidate.group(1).replace('::', '.'))
            if len(parsed) > 1:
                problems.append((line, 'native handler parses multiple unsupported request types'))
            registered[name] = (parsed[0] if len(parsed) == 1 else None, name, line)
            handlers[name] = (body, line)
    return registered, handlers, problems


def _module_path(node, path):
    if isinstance(node,ast.Import):
        return [item.name for item in node.names]
    if not isinstance(node,ast.ImportFrom):
        return []
    if node.level:
        parts=list(PurePosixPath(path).parent.parts)
        parts=parts[:len(parts)-node.level+1]
        return [".".join(parts+([node.module] if node.module else []))]
    return [node.module or ""]


def _registrations(tree, class_name="StorageService"):
    """Read literal dispatch bindings and reject unsupported registry mutation."""
    registrations={}
    problems=[]
    service=next((node for node in tree.body if isinstance(node,ast.ClassDef) and node.name==class_name),None)
    if service is None:
        return registrations,{},problems
    methods={node.name:node for node in service.body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))}
    init=methods.get('__init__')
    checked_loop_assignments=set()

    def is_registry(node):
        return isinstance(node,ast.Attribute) and isinstance(node.value,ast.Name) and node.value.id=='self' and node.attr=='handlers'

    def record(name,request,handler,line):
        if name in registrations:
            problems.append((line,'duplicate dispatch registration: '+name))
        registrations[name]=(request,handler,line)

    def binding(name,value,line):
        if not isinstance(name,ast.Constant) or not isinstance(name.value,str):
            problems.append((line,'dynamic dispatch key cannot be checked against its exact loop binding'))
            return
        if isinstance(value,ast.Attribute) and isinstance(value.value,ast.Name) and value.value.id=='self':
            record(name.value,None,value.attr,line)
        elif (isinstance(value,ast.Call) and isinstance(value.func,ast.Attribute) and value.func.attr=='_handler' and len(value.args)==2
              and isinstance(value.args[0],ast.Constant) and isinstance(value.args[0].value,str)
              and isinstance(value.args[1],ast.Attribute) and isinstance(value.args[1].value,ast.Name) and value.args[1].value.id=='self'):
            record(name.value,value.args[0].value,value.args[1].attr,line)
        else:
            problems.append((line,'unsupported dispatch binding for '+name.value))

    if init:
        # Accept the current literal loop only when its actual assignment
        # binds the same method/request/handler variables from those rows.
        # An unused row table or an unrelated dynamic write proves no API.
        for node in ast.walk(init):
            if not (isinstance(node,ast.For) and isinstance(node.iter,(ast.Tuple,ast.List)) and isinstance(node.target,(ast.Tuple,ast.List)) and len(node.target.elts)==3 and all(isinstance(item,ast.Name) for item in node.target.elts)):
                continue
            key_name,request_name,handler_name=(item.id for item in node.target.elts)
            matching=[]
            for statement in node.body:
                if not isinstance(statement,ast.Assign) or len(statement.targets)!=1:
                    continue
                target,value=statement.targets[0],statement.value
                if (isinstance(target,ast.Subscript) and is_registry(target.value)
                        and isinstance(target.slice,ast.Name) and target.slice.id==key_name
                        and isinstance(value,ast.Call) and isinstance(value.func,ast.Attribute)
                        and isinstance(value.func.value,ast.Name) and value.func.value.id=='self'
                        and value.func.attr=='_handler' and len(value.args)==2 and not value.keywords
                        and isinstance(value.args[0],ast.Name) and value.args[0].id==request_name
                        and isinstance(value.args[1],ast.Name) and value.args[1].id==handler_name):
                    matching.append(statement)
            if len(matching)!=1:
                continue
            rows=[]
            for row in node.iter.elts:
                if not isinstance(row,(ast.Tuple,ast.List)) or len(row.elts)!=3:
                    break
                method,request,handler=row.elts
                if not (isinstance(method,ast.Constant) and isinstance(method.value,str) and isinstance(request,ast.Constant) and isinstance(request.value,str) and isinstance(handler,ast.Attribute) and isinstance(handler.value,ast.Name) and handler.value.id=='self'):
                    break
                rows.append((method.value,request.value,handler.attr,row.lineno))
            else:
                checked_loop_assignments.add(id(matching[0]))
                for row in rows:
                    record(*row)
        for node in ast.walk(init):
            if isinstance(node,ast.Assign):
                for target in node.targets:
                    if isinstance(target,ast.Subscript) and is_registry(target.value):
                        if id(node) not in checked_loop_assignments:
                            binding(target.slice,node.value,node.lineno)
                    elif is_registry(target):
                        if isinstance(node.value,ast.Dict):
                            for key,value in zip(node.value.keys,node.value.values):
                                binding(key,value,node.lineno)
                        else:
                            problems.append((node.lineno,'dispatch registry must be constructed from checked literal bindings'))
            elif isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and is_registry(node.func.value):
                problems.append((node.lineno,'unsupported dispatch registry mutation: '+node.func.attr))
            elif isinstance(node,(ast.AugAssign,ast.AnnAssign,ast.Delete)):
                targets=node.targets if isinstance(node,ast.Delete) else [node.target]
                if any(is_registry(target) or isinstance(target,ast.Subscript) and is_registry(target.value) for target in targets):
                    problems.append((node.lineno,'unsupported dispatch registry mutation'))
    for name,method in methods.items():
        if name=='__init__':
            continue
        for node in ast.walk(method):
            if isinstance(node,ast.Assign) and any(is_registry(target) or isinstance(target,ast.Subscript) and is_registry(target.value) for target in node.targets):
                problems.append((node.lineno,'dispatch registry mutation outside constructor requires explicit analysis'))
            elif isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute) and is_registry(node.func.value):
                problems.append((node.lineno,'dispatch registry mutation outside constructor requires explicit analysis'))
    return registrations,methods,problems


def analyze(root, contract_path=None, policy_path=None):
    """Return JSON-serializable diagnostics and the source-derived graphs."""
    root=Path(root).resolve()
    contract_path=Path(contract_path) if contract_path else root/'services/storage/contract.json'
    policy_path=Path(policy_path) if policy_path else root/'tools/architecture/boundary_policy.json'
    contract=json.loads(contract_path.read_text(encoding='utf-8-sig'))
    contract_files=[(contract_path,contract)]
    for candidate in sorted((root/'services').glob('*/contract.json')):
        if candidate.resolve()!=contract_path.resolve() and candidate.parent.name!=(contract.get('service') if isinstance(contract,dict) else None):
            contract_files.append((candidate,json.loads(candidate.read_text(encoding='utf-8-sig'))))
    policy=json.loads(policy_path.read_text(encoding='utf-8-sig'))
    issues=[]
    def issue(code,path,line,message):
        issues.append(dict(code=code,path=str(path),line=line,message=message))
    sources={}
    for source_root in policy['source_roots']:
        for path in sorted((root/source_root).rglob('*')):
            if path.is_file() and (path.suffix in SUFFIXES or path.name=='CMakeLists.txt'):
                relative=path.relative_to(root).as_posix()
                sources[relative]=path.read_text(encoding='utf-8-sig')
    allowed=policy['module_dependencies']
    sql_files=set(policy['sql_implementation_files'])
    includes,imports,build,rpc_calls=[],[],[],[]
    include_index={}
    for path in sources:
        bits=PurePosixPath(path).parts
        if 'include' in bits:
            at=bits.index('include')
            include_name='/'.join(bits[at+1:])
            if include_name in include_index and include_index[include_name]!=path:
                issue('ambiguous_project_include',path,1,f'{include_name} also resolves to {include_index[include_name]}')
            else:
                include_index[include_name]=path
        elif path.endswith('.proto'):
            include_index[PurePosixPath(path).stem+'.pb.h']=path
    trees={}
    def dependency(kind,path,line,target):
        owner,other=_owner(path),_owner(target)
        if owner==other:
            return
        if other not in allowed.get(owner,[]):
            issue(kind+'_boundary',path,line,f'{owner} must not depend on {other}: {target}')
        if kind=='include' and '/src/' in target:
            issue('private_implementation',path,line,f'cross-module private source include: {target}')
    for path,text in sources.items():
        owner=_owner(path)
        if owner not in allowed:
            issue('unknown_module',path,1,f'new module {owner} requires explicit policy')
        is_sql_allowed=path in sql_files
        strings=[]
        if PurePosixPath(path).suffix in CPP:
            clean=_cpp(text)
            code=_cpp_code(text)
            for match in INCLUDE.finditer(clean):
                if not code[match.start():match.end()].lstrip().startswith('#'):
                    continue
                name=match.group(1)
                line=_line(text,match.start())
                if any(name==header or header.endswith('/') and name.startswith(header) for header in policy['database_headers']) and not is_sql_allowed:
                    issue('database_driver',path,line,f'database implementation header {name} is private to Storage')
                candidate=(PurePosixPath(path).parent/name).as_posix()
                # Resolve relative ../ paths without requiring the included file
                # to exist on the host's native path syntax.
                candidate=Path(root/candidate).resolve().relative_to(root).as_posix() if Path(root/candidate).resolve().is_relative_to(root) else ''
                target=candidate if candidate in sources else include_index.get(name)
                if target:
                    includes.append(dict(source=path,target=target,line=line))
                    dependency('include',path,line,target)
                elif name.startswith(('net/','auth/','match/','room/','arena/','services/')):
                    issue('unresolved_project_include',path,line,f'project include not found: {name}')
            strings=list(_strings(text))
            if not is_sql_allowed:
                for execution in re.finditer(r'\b(?:PQexec(?:Params|Prepared)?|sqlite3_exec|mysql_query)\s*\(',code):
                    issue('database_driver',path,_line(text,execution.start()),'direct native database execution belongs to Storage persistence')
            for match in re.finditer(r'\b(\w+)\.set_method\s*\((.*?)\)',code,re.S):
                args=clean[match.start(2):match.end(2)].strip()
                literal=re.fullmatch(r'"([a-z][a-z0-9_.]*)"',args)
                if literal:
                    targets = set()
                    for target_match in re.finditer(r'\b' + re.escape(match.group(1)) + r'\.set_target_service\s*\((.*?)\)', code, re.S):
                        argument = clean[target_match.start(1):target_match.end(1)].strip()
                        target_literal = re.fullmatch(r'"([a-z][a-z0-9_-]*)"', argument)
                        if target_literal:
                            targets.add(target_literal.group(1))
                        else:
                            issue('dynamic_rpc_target', path, _line(text, target_match.start()),
                                  'explicit service target must be a declared literal')
                    if len(targets) > 1:
                        issue('dynamic_rpc_target', path, _line(text, match.start()), 'one request variable has ambiguous service targets')
                    rpc_calls.append(dict(source=path,caller=owner,method=literal.group(1),target=next(iter(targets)) if len(targets)==1 else None,line=_line(text,match.start())))
                elif owner!='shared':
                    issue('dynamic_rpc_method',path,_line(text,match.start()),'service RPC method must be a declared literal')
        elif path.endswith('.py'):
            try:
                tree=ast.parse(text,filename=path)
                trees[path]=tree
            except SyntaxError as error:
                issue('parse_error',path,error.lineno or 1,str(error))
                continue
            docs={id(node.value) for node in ast.walk(tree) if isinstance(node,ast.Expr) and isinstance(node.value,ast.Constant) and isinstance(node.value.value,str)}
            for node in ast.walk(tree):
                for module in _module_path(node,path):
                    first=module.split('.')[0]
                    if first in policy['database_driver_modules'] and not is_sql_allowed:
                        issue('database_driver',path,node.lineno,f'database driver {module} is private to Storage persistence')
                    if module.startswith(('services.','shared.','domain.')):
                        target=module.replace('.','/')+'.py'
                        imports.append(dict(source=path,target=target,line=node.lineno))
                        dependency('import',path,node.lineno,target)
                if isinstance(node,ast.Constant) and isinstance(node.value,str) and id(node) not in docs:
                    strings.append((node.value,node.lineno))
                if isinstance(node,(ast.BinOp,ast.JoinedStr)):
                    value=_constant_string(node)
                    if value is not None:
                        strings.append((value,node.lineno))
                if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                    if node.func.attr in ('execute','executemany','cursor','copy') and not is_sql_allowed and path not in policy.get('redis_implementation_files',[]):
                        issue('sql_boundary',path,node.lineno,'direct database execution belongs to Storage persistence')
                    elif node.func.attr=='call' and owner!='shared':
                        argument=node.args[0] if node.args else next((k.value for k in node.keywords if k.arg=='method'),None)
                        method=_constant_string(argument)
                        if method is not None and re.fullmatch('[a-z][a-z0-9_.]*',method) and '.' in method:
                            rpc_calls.append(dict(source=path,caller=owner,method=method,line=node.lineno))
                        else:
                            issue('dynamic_rpc_method',path,node.lineno,'service RPC method must be a declared literal')
        elif path.endswith('.sql'):
            if not any(path.startswith(prefix+'/') for prefix in policy['sql_data_roots']):
                issue('sql_file_boundary',path,1,'SQL file must belong to Storage migrations or fixtures')
        if not is_sql_allowed:
            for value,line in strings:
                if SQL.search(value):
                    issue('sql_boundary',path,line,'SQL belongs to Storage persistence; business code must use a repository/RPC')
    # Build graph derives declared project targets and literal links from all
    # production CMake files, including newly added services.
    targets={}
    commands=[]
    for path,text in sources.items():
        if PurePosixPath(path).name!='CMakeLists.txt':
            continue
        try:
            for command,args,line in _cmake_commands(text):
                commands.append((command,args,line,path))
                if command in ('add_library','add_executable') and args:
                    if '$' in args[0] or 'ALIAS' in args:
                        issue('cmake_dynamic_dependency',path,line,'dynamic/alias project target declarations require explicit supported resolution')
                    targets[args[0]]=path
        except ValueError as error:
            issue('cmake_parse',path,1,str(error))
    utility_targets={match.group(1) for path,text in sources.items() if PurePosixPath(path).name=='CMakeLists.txt' for match in re.finditer(r'\badd_custom_target\s*\(\s*([A-Za-z_][A-Za-z_0-9]*)',_cpp(text))}
    for command,args,line,path in commands:
        if command not in ('target_link_libraries','add_dependencies') or not args:
            continue
        source=args[0]
        for target in args[1:]:
            if '$' in target or (target.startswith('arena_') and target not in targets and target not in utility_targets):
                issue('cmake_dynamic_dependency',path,line,f'unresolved project dependency: {target}')
            if target in targets:
                build.append(dict(source=source,target=target,path=path,line=line))
                dependency('build',targets.get(source,path),line,targets[target])
    # Validate contract identity and every registration against real handlers,
    # proto types, envelope fields and public status enum.
    try:
        proto,statuses=_proto(sources.get('shared/proto/rpc.proto',''))
    except ValueError as error:
        issue('proto_parse', 'shared/proto/rpc.proto', 1, str(error))
        proto, statuses = {}, set()
    payload_proto = {}
    short_proto = defaultdict(list)
    for proto_path, proto_source in sources.items():
        if not proto_path.endswith('.proto'):
            continue
        try:
            messages, _ = _proto(proto_source)
        except ValueError as error:
            issue('proto_parse', proto_path, 1, str(error))
            continue
        package = re.search(r'\bpackage\s+([\w.]+)\s*;', _cpp(proto_source))
        prefix = package.group(1) + '.' if package else ''
        for message in messages:
            payload_proto[prefix + message] = proto_path
            short_proto[message].append(proto_path)
    for message, paths in short_proto.items():
        if len(paths) == 1:
            payload_proto[message] = paths[0]
    native_implementations = {c.get('implementation') for _, c in contract_files
                              if isinstance(c, dict) and c.get('registration_kind') == 'native_server'}
    for path, source in sources.items():
        if PurePosixPath(path).suffix in CPP and re.search(r'\bServer::Handlers\s+\w+', _cpp_code(source)) and re.search(r'\bServer\s+\w+\s*\(', _cpp_code(source)):
            if path not in native_implementations:
                issue('contract_native_undeclared', path, 1, 'native Server handler registration requires a service contract')
    for name,expected in (('Request',COMMON_REQUEST),('Response',COMMON_RESPONSE)):
        for field,spec in expected.items():
            if proto.get(name,{}).get(field)!=spec:
                issue('rpc_envelope','shared/proto/rpc.proto',1,f'{name}.{field} must be {spec}')
    declared={}
    all_registered={}
    for current_path,current in contract_files:
        if not isinstance(current,dict):
            issue('contract_fields',str(current_path),1,'service contract must be an object')
            continue
        service=current.get('service')
        implementation=current.get('implementation','')
        if not isinstance(service,str) or not isinstance(implementation,str):
            issue('contract_fields',str(current_path),1,'service/implementation must be nonempty strings')
            continue
        if service not in allowed or _owner(implementation)!=service:
            issue('contract_owner',str(current_path),1,'contract service and actual implementation owner must agree')
        if service=='storage' and current.get('physical_sql_owner')!='storage':
            issue('contract_owner',str(current_path),1,'Storage owns physical SQL')
        native = current.get('registration_kind') == 'native_server'
        if native:
            registered, handlers, registration_problems = _native_registrations(sources.get(implementation, ''))
            default = re.search(r'\bmaxBudget\s*\{\s*(\d+)\s*\}', _cpp_code(sources.get('shared/include/rpc/Server.h', '')))
            actual_maximum = int(default.group(1)) if default else None
            implementation_source = _cpp_code(sources.get(implementation, ''))
            assignments = list(re.finditer(r'\b\w+\.maxBudget\s*=\s*([^;]+);', implementation_source))
            for assignment in assignments:
                literal = re.fullmatch(r'(\d+)ms|std::chrono::milliseconds\s*\(\s*(\d+)\s*\)', assignment.group(1).strip())
                if not literal:
                    actual_maximum = None
                    break
                actual_maximum = int(literal.group(1) or literal.group(2))
            if current.get('transport', {}).get('max_timeout_ms') != actual_maximum:
                issue('contract_transport', implementation, 1, 'native Server budget differs from its service contract')
        else:
            registered,handlers,registration_problems=_registrations(trees.get(implementation,ast.Module(body=[],type_ignores=[])),current.get('service_class','StorageService'))
        for line,message in registration_problems:
            issue('contract_registration_dynamic',implementation,line,message)
        all_registered.update({(service, name): value for name, value in registered.items()})
        local_declared={}
        methods=current.get('methods',[])
        if not isinstance(methods,list) or not methods:
            issue('contract_fields',str(current_path),1,'service contract must declare a nonempty method list')
            methods=[]
        for method in methods:
            if not isinstance(method,dict):
                issue('contract_fields',str(current_path),1,'method declaration must be an object')
                continue
            name=method.get('name')
            if not isinstance(name,str) or not name or (service, name) in declared:
                issue('contract_duplicate',str(current_path),1,f'duplicate/empty method {name}')
                continue
            declared[(service, name)]=method
            local_declared[name]=method
            required=('owner','logical_owner','callers','handler','request','response','deadline_ms','errors','idempotency','cancellation')
            if any(key not in method for key in required):
                issue('contract_fields',str(current_path),1,f'{name} missing explicit owner/deadline/error/idempotency contract')
                continue
            if (not isinstance(method['owner'],str) or method['owner']!=service or not isinstance(method['logical_owner'],str) or method['logical_owner'] not in allowed):
                issue('contract_owner',str(current_path),1,f'{name} owner is invalid')
            if (not isinstance(method['callers'],list) or not method['callers'] or any(not isinstance(caller,str) or not caller for caller in method['callers']) or not isinstance(method['cancellation'],str) or not method['cancellation'].strip()):
                issue('contract_fields',str(current_path),1,f'{name} caller/cancellation contract is empty or invalid')
            deadline=method['deadline_ms']
            transport=current.get('transport',{})
            maximum=transport.get('max_timeout_ms',0) if isinstance(transport,dict) else 0
            if type(deadline) is not int or type(maximum) is not int or not 0<deadline<=maximum:
                issue('contract_deadline',str(current_path),1,f'{name} deadline exceeds transport maximum')
            common_errors=current.get('common_errors',[])
            if (not isinstance(common_errors,list) or not isinstance(method['errors'],list) or any(not isinstance(error,str) for error in common_errors+method['errors'])):
                issue('contract_errors',str(current_path),1,f'{name} errors must be arrays of enum names')
                errors=set()
            else:
                errors=set(common_errors)|set(method['errors'])
            if not errors or not errors<=statuses or 'OK' in errors or 'OUTCOME_UNKNOWN' not in errors:
                issue('contract_errors',str(current_path),1,f'{name} declares unknown/missing public errors including unknown outcome')
            idem=method['idempotency']
            if not isinstance(idem,dict):
                issue('contract_idempotency',str(current_path),1,f'{name} idempotency must be an object')
                idem={}
            if idem.get('kind') not in ('read_only','non_idempotent','semantic_key') or idem.get('automatic_retries')!=0:
                issue('contract_idempotency',str(current_path),1,f'{name} has unsupported idempotency/retry policy')
            if idem.get('kind')=='semantic_key' and not idem.get('key'):
                issue('contract_idempotency',str(current_path),1,f'{name} semantic deduplication requires a key')
            for direction in ('request','response'):
                if method[direction] is not None and (not isinstance(method[direction],str) or method[direction] not in payload_proto):
                    issue('contract_proto',str(current_path),1,f'{name} {direction} type does not exist: {method[direction]}')
            actual=registered.get(name)
            if actual is None:
                issue('contract_registration',implementation,1,f'declared method {name} is not registered')
                continue
            request,handler,line=actual
            if request!=method['request'] or handler!=method['handler']:
                issue('contract_registration',implementation,line,f'{name} actual request/handler differs from declaration')
            node=handlers.get(handler) if isinstance(handler,str) else None
            if native:
                if node is None:
                    issue('contract_handler', implementation, line, f'{name} native handler missing')
                if method['response'] is not None:
                    response_path = method.get('response_implementation', implementation)
                    if not isinstance(response_path, str) or _owner(response_path) != service or response_path not in sources:
                        issue('contract_response', implementation, line, f'{name} response implementation must belong to its service')
                    else:
                        response_text = _cpp_code(sources[response_path])
                        response_type = str(method['response']).replace('.', '::')
                        emitted = any(re.search(r'\b' + re.escape(match.group(1)) + r'\s*\.\s*Serialize(?:AsString|ToString|ToArray)\s*\(', response_text)
                                      for match in re.finditer(r'\b' + re.escape(response_type) + r'\s+(\w+)\s*;', response_text))
                        if not emitted:
                            issue('contract_response', response_path, 1, f'{name} does not serialize declared reply {method["response"]}')
                continue
            if node is None:
                issue('contract_handler',implementation,line,f'{name} handler missing')
            else:
                replies={n.args[0].value for n in ast.walk(node) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='message' and n.args and isinstance(n.args[0],ast.Constant)}
                if method['response'] is not None and (not isinstance(method['response'],str) or method['response'] not in replies):
                    issue('contract_response',implementation,node.lineno,f'{name} does not produce declared reply {method["response"]}')
        for name,(_,_,line) in registered.items():
            if name not in local_declared:
                issue('contract_orphan',implementation,line,f'registered method {name} lacks contract')
    # SQL ownership declarations must cover actual schema creation, including
    # migration metadata. Do not silently exclude newly added tables.
    tables=set()
    for path,text in sources.items():
        if path.endswith('.up.sql') or path=='services/storage/migrate.py':
            for match in re.finditer(r'\bCREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?((?:arena|public)\.[a-z_][a-z_0-9]*)',text,re.I):
                tables.add(match.group(1).lower())
    owners=contract.get('data_owners',{})
    if not isinstance(owners,dict):
        issue('data_owner',str(contract_path),1,'data_owners must be an object')
        owners={}
    for name in sorted(tables):
        if name not in owners or not isinstance(owners[name],str) or owners[name] not in allowed:
            issue('data_owner',str(contract_path),1,f'table {name} has no valid logical owner')
    for name in sorted(set(owners)-tables):
        issue('data_owner_orphan',str(contract_path),1,f'declared table {name} does not exist in migrations')
    # Transport declarations must match the actual reusable server defaults,
    # not merely agree with other declarations.
    rpc_tree=trees.get('shared/runtime/rpc.py')
    if rpc_tree:
        server=next((n for n in rpc_tree.body if isinstance(n,ast.ClassDef) and n.name=='Server'),None)
        init=next((n for n in server.body if isinstance(n,ast.FunctionDef) and n.name=='__init__'),None) if server else None
        defaults={arg.arg:value for arg,value in zip(init.args.kwonlyargs,init.args.kw_defaults)} if init else {}
        maximum=defaults.get('max_timeout')
        if not isinstance(maximum,ast.Constant) or maximum.value*1000!=(contract.get('transport',{}).get('max_timeout_ms') if isinstance(contract.get('transport'),dict) else None):
            issue('contract_transport','shared/runtime/rpc.py',getattr(init,'lineno',1),'Server max_timeout differs from declared deadline ceiling')
    registered=all_registered
    runtime=[]
    for call in rpc_calls:
        candidates = [value for (service, name), value in declared.items()
                      if name == call['method'] and call['caller'] in value.get('callers', [])
                      and (not call.get('target') or service == call['target'])]
        if len(candidates) != 1:
            named = [value for (service, name), value in declared.items() if name == call['method'] and (not call.get('target') or service == call['target'])]
            if len(named) == 1:
                candidates = named
        method = candidates[0] if len(candidates) == 1 else None
        if method is None:
            issue('rpc_undeclared',call['source'],call['line'],f'RPC {call["method"]} has no owner contract')
            continue
        callee=method.get('owner')
        callers=method.get('callers')
        if not isinstance(callee,str) or not isinstance(callers,list):
            continue
        runtime.append(dict(source=call['caller'],target=callee,method=call['method'],path=call['source'],line=call['line']))
        if call['caller'] not in callers:
            issue('rpc_caller',call['source'],call['line'],f'{call["caller"]} is not declared caller of {call["method"]}')
    cycles={}
    for kind,edges in (('build',[(e['source'],e['target']) for e in build]),('include',[(e['source'],e['target']) for e in includes]),('rpc',[(e['source'],e['target']) for e in runtime]),('python_import',[(e['source'],e['target']) for e in imports])):
        cycles[kind]=_scc(edges)
        for cycle in cycles[kind]:
            issue(kind+'_cycle','',1,' -> '.join(cycle))
    issues.sort(key=lambda i:(i['path'],i['line'],i['code'],i['message']))
    return dict(schema_version=1,passed=not issues,issues=issues,inventory=dict(files=len(sources),modules=sorted({_owner(p) for p in sources})),contracts=dict(declared=sorted({name for service,name in declared}),registered=sorted({name for service,name in registered}),qualified_declared=sorted(service+'/'+name for service,name in declared),qualified_registered=sorted(service+'/'+name for service,name in registered),native_and_python_calls=rpc_calls,data_tables=sorted(tables)),graphs=dict(build=build,include=includes,python_import=imports,rpc=runtime),cycles=cycles,limitations=['Static literal source policy; arbitrary macro-generated or runtime-generated source requires separate compiler/runtime checks.','Gateway forwards business requests to independent Lobby; Match/Room owners are local to Lobby and are not independent network services.','Caller declarations are build contracts, not a substitute for runtime caller identity authorization.'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,required=True)
    parser.add_argument('--contract',type=Path)
    parser.add_argument('--policy',type=Path)
    parser.add_argument('--report',type=Path)
    parser.add_argument('--dot',type=Path)
    args=parser.parse_args()
    result=analyze(args.root,args.contract,args.policy)
    if args.report:
        args.report.parent.mkdir(parents=True,exist_ok=True)
        args.report.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    if args.dot:
        args.dot.parent.mkdir(parents=True,exist_ok=True)
        lines=['digraph boundaries {']
        for kind in ('build','rpc'):
            for edge in result['graphs'][kind]:
                lines.append(f'  {json.dumps(kind+":"+edge["source"])} -> {json.dumps(kind+":"+edge["target"])};')
        args.dot.write_text('\n'.join(lines+['}'])+'\n',encoding='utf-8')
    print(json.dumps(dict(passed=result['passed'],issues=result['issues'],files=result['inventory']['files']),ensure_ascii=False))
    return 0 if result['passed'] else 1


if __name__=='__main__':
    raise SystemExit(main())