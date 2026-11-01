#!/usr/bin/env python3
"""Use identical documents against the actual native and Python snapshot parsers."""
import argparse
import copy
from dataclasses import asdict
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def fixtures():
    base = dict(version='1',desiredReady=True,drainGraceMs=5000,
        nodes=[dict(service='storage',nodeId='node-a',host='127.0.0.1',port=7105,ready=True)])
    values = []
    def add(name,value,accepted):
        values.append((name,json.dumps(value,separators=(',',':')) if not isinstance(value,str) else value,accepted))
    add('minimal_valid',base,True)
    for version in ('18446744073709551615','9007199254740993'):
        value=copy.deepcopy(base);value['version']=version;add('full_uint64_'+version,value,True)
    for version in (9007199254740993,18446744073709551615):
        value=copy.deepcopy(base);value['version']=version;add('numeric_uint64_'+str(version),value,True)
    value=copy.deepcopy(base);value.update(desiredReady=False,drainGraceMs=0)
    value['nodes'][0].update(host='::1',ready=False);add('explicit_false_zero_ipv6',value,True)
    value=copy.deepcopy(base);value['nodes'].insert(0,dict(service='storage',nodeId='node-z',host='::1',port=65535,ready=True))
    add('canonical_sort',value,True)
    add('snake_aliases',json.dumps(base).replace('desiredReady','desired_ready').replace('drainGraceMs','drain_grace_ms').replace('nodeId','node_id'),True)
    for key in ('version','desiredReady','drainGraceMs','nodes'):
        value=copy.deepcopy(base);value.pop(key);add('missing_'+key,value,False)
    for key in ('service','nodeId','host','port','ready'):
        value=copy.deepcopy(base);value['nodes'][0].pop(key);add('missing_node_'+key,value,False)
    for key,bad in [('version','0'),('version','18446744073709551616'),('version','-1'),
                    ('desiredReady',None),('desiredReady','true'),('desiredReady','false'),('drainGraceMs',10001),
                    ('drainGraceMs',-1),('nodes',[]),('nodes',[base['nodes'][0]]*129)]:
        value=copy.deepcopy(base);value[key]=bad;add('range_'+key+'_'+str(bad)[:12],value,False)
    for key,bad in [('port',0),('port',65536),('port',True),('ready',None),('ready','true'),('ready','false'),
                    ('service','Storage'),('service','a'*33),('nodeId','0node'),('nodeId','a'*65),
                    ('host','localhost'),('host','0.0.0.0'),('host','192.0.2.1')]:
        value=copy.deepcopy(base);value['nodes'][0][key]=bad;add('bad_node_'+key+'_'+str(bad),value,False)
    value=copy.deepcopy(base);value['nodes'].append(copy.deepcopy(value['nodes'][0]));add('duplicate_node_identity',value,False)
    value=copy.deepcopy(base);value['extra']=1;add('unknown_top_field',value,False)
    value=copy.deepcopy(base);value['nodes'][0]['extra']=1;add('unknown_node_field',value,False)
    text=json.dumps(base,separators=(',',':'))
    add('duplicate_version','{"version":"2",'+text[1:],False)
    add('escaped_duplicate','{"ver\\u0073ion":"2",'+text[1:],False)
    add('alias_duplicate',text.replace('"nodeId":','"node_id":"other","nodeId":'),False)
    for name,raw in [('empty',''),('partial',text[:-1]),('trailing',text+'{}'),('array','[]'),
                     ('null','null'),('deep','['*32+'0'+']'*32),('huge',' '*65537)]:
        add(name,raw,False)
    return values


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repository-root',type=Path,required=True)
    parser.add_argument('--descriptor',type=Path,required=True)
    parser.add_argument('--probe',type=Path,required=True)
    parser.add_argument('--report',type=Path,required=True)
    args=parser.parse_args()
    sys.path.insert(0,str(args.repository_root))
    from shared.runtime.config import ConfigSnapshot,ConfigError
    from shared.runtime.protocol import Protocol
    protocol=Protocol(args.descriptor)
    report=dict(passed=False,cases=[])
    try:
        for name,text,expected in fixtures():
            native=subprocess.run([str(args.probe)],input=text,text=True,capture_output=True,timeout=5)
            assert native.returncode==0,(name,native.stderr[-1500:])
            actual=json.loads(native.stdout)
            try:
                value=ConfigSnapshot.parse(protocol,text)
            except ConfigError:
                python=dict(accepted=False)
            else:
                python=dict(accepted=True,version=str(value.version),desired_ready=value.desired_ready,
                            drain_grace_ms=value.drain_grace_ms,nodes=[asdict(node) for node in value.nodes])
            assert actual==python,(name,actual,python)
            assert actual['accepted']==expected,(name,'wrong acceptance policy',actual)
            report['cases'].append(dict(name=name,accepted=expected,passed=True))
        report['passed']=True
    except BaseException as error:
        report['error']=str(error)
    report['source_hashes']={name:hashlib.sha256((args.repository_root/name).read_bytes()).hexdigest()
        for name in ['shared/proto/runtime_config.proto','shared/src/runtime/RuntimeConfig.cpp',
                     'shared/runtime/config.py','tests/runtime/ConfigSnapshotProbe.cpp','tests/shared/runtime/ConfigParityTest.py']}
    args.report.parent.mkdir(parents=True,exist_ok=True)
    args.report.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({key:report[key] for key in ['passed','error'] if key in report}))
    return 0 if report['passed'] else 1


if __name__=='__main__':
    raise SystemExit(main())