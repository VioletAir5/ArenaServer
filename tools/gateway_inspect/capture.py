#!/usr/bin/env python3
"""Bounded, isolated four-worker profiling. Requires Linux, debug symbols and tools."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import sys
import time

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[1]

def dump(path,value):
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2),encoding="utf-8")

def port_number():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1",0))
        return sock.getsockname()[1]

def workload(port,pid,connections,result):
    sockets=[]
    outcome=dict(pid=pid,connections=connections,verified_bytes=0,rounds=8)
    try:
        deadline=time.monotonic()+40
        for _ in range(connections):
            while True:
                try:
                    sock=socket.create_connection(("127.0.0.1",port),timeout=15)
                    sockets.append(sock)
                    break
                except ConnectionRefusedError:
                    if time.monotonic()>deadline:
                        raise
                    time.sleep(.05)
        time.sleep(.5) # let all accepts reach the owner queues
        payload=bytes(range(256))*64
        for round_id in range(8):
            for sock in sockets:
                sock.sendall(payload)
            for sock in sockets:
                received=bytearray()
                while len(received)<len(payload):
                    data=sock.recv(len(payload)-len(received))
                    if not data:
                        raise RuntimeError("unexpected EOF")
                    received.extend(data)
                if received!=payload:
                    raise RuntimeError("echo mismatch")
                outcome["verified_bytes"]+=len(received)
            time.sleep(.05)
        time.sleep(.3)
        for sock in sockets:
            sock.shutdown(socket.SHUT_WR)
            if sock.recv(1)!=b"":
                raise RuntimeError("unexpected trailing bytes")
        outcome["status"]="passed"
    except Exception as exc:
        outcome.update(status="failed",error=str(exc))
    finally:
        for sock in sockets:
            sock.close()
        dump(result,outcome)
        try:
            os.kill(pid,signal.SIGTERM)
        except ProcessLookupError:
            pass
    return outcome

def target_pid(process):
    # strace forks its tracee; Valgrind executes the target in the same PID.
    children=Path(f"/proc/{process.pid}/task/{process.pid}/children")
    deadline=time.monotonic()+10
    while time.monotonic()<deadline:
        values=children.read_text().split() if children.exists() else []
        if values:
            return int(values[0])
        if process.poll() is not None:
            raise RuntimeError("profiler exited before spawning server")
        time.sleep(.02)
    raise RuntimeError("strace did not publish its test-host child")

def run_profile(command,out,name,port,n,trace=False):
    started=dt.datetime.now().astimezone().isoformat()
    with (out/(name+".log")).open("w") as log:
        process=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        pid=None
        try:
            pid=target_pid(process) if trace else process.pid
            result=workload(port,pid,n,out/(name+"-workload.json"))
            code=process.wait(timeout=45)
            if code or result["status"]!="passed":
                raise RuntimeError(f"{name} failed: exit={code}, workload={result}")
            return dict(name=name,pid=pid,profiler_pid=process.pid,start=started,
                        command=command,exit_code=code,workload=result)
        finally:
            if process.poll() is None:
                if pid is not None:
                    try: os.kill(pid,signal.SIGTERM)
                    except ProcessLookupError: pass
                try: process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid,signal.SIGKILL)
                    process.wait()

def parse_massif(path):
    snapshots=[]
    current=None
    for line in path.read_text().splitlines():
        if line.startswith("snapshot="):
            current={"index":int(line.split("=")[1]),"tree":[]}
            snapshots.append(current)
        elif current is not None:
            if re.match(r"(time|mem_heap_B|mem_heap_extra_B|mem_stacks_B)=",line):
                key,value=line.split("=")
                current[key]=int(value)
            elif line.startswith("heap_tree="):
                current["heap_tree"]=line.split("=")[1]
            elif re.match(r"\s*n\d+: ",line):
                match=re.match(r"(\s*)n(\d+): (\d+) (.*)",line)
                current["tree"].append(dict(depth=len(match[1]),children=int(match[2]),
                                            bytes=int(match[3]),label=match[4]))
    if not snapshots:
        raise RuntimeError("empty Massif output")
    detailed=[s for s in snapshots if s["tree"]]
    peak=max(snapshots,key=lambda s:s.get("mem_heap_B",0))
    detail=max(detailed,key=lambda s:s.get("mem_heap_B",0)) if detailed else peak
    return dict(unit="ms",snapshots=snapshots,peak=peak,detailed_peak=detail,
                scope="Massif heap + allocator overhead + estimated stacks; excludes kernel socket buffers and untracked mmap")

def parse_callgrind(path):
    names={}
    edges={}
    current="?"
    callee=None
    pending=0
    def name(text):
        match=re.match(r"\((\d+)\)(?: (.*))?$",text)
        if match:
            key=match[1]
            if match[2] is not None:
                names[key]=match[2]
            return names.get(key,"<unresolved "+key+">")
        return text
    for line in path.read_text().splitlines():
        if line.startswith("fn="):
            current=name(line[3:])
        elif line.startswith("cfn="):
            callee=name(line[4:])
        elif line.startswith("calls="):
            pending=int(line[6:].split()[0])
            if callee is not None:
                key=(current,callee)
                edges[key]=edges.get(key,0)+pending
    project=[]
    for (caller,callee),calls in edges.items():
        if "service::" in caller or "service::" in callee:
            project.append(dict(caller=caller,callee=callee,calls=calls))
    return dict(edges=sorted(project,key=lambda x:x["calls"],reverse=True),
                scope="Instrumented dynamic caller/callee counts, aggregated across threads; not wall-clock timings")

def parse_strace(out):
    aggregate={}
    events=[]
    pids=[]
    for path in sorted(out.glob("syscalls.*")):
        if not path.suffix[1:].isdigit():
            continue
        tid=int(path.suffix[1:])
        pids.append(tid)
        unfinished={}
        for line in path.read_text(errors="replace").splitlines():
            match=re.match(r"(\d+\.\d+) (\w+)\((.*)",line)
            if match:
                timestamp=float(match[1]); call=match[2]
                if "<unfinished ...>" in line:
                    unfinished[call]=(timestamp,line)
                    continue
            else:
                match=re.match(r"(\d+\.\d+) <\.\.\. (\w+) resumed>(.*)",line)
                if not match:
                    continue
                call=match[2]
                timestamp,first=unfinished.pop(call,(float(match[1]),""))
                if first:
                    line=first.replace("<unfinished ...>","")+match[3]
            duration=re.search(r"<(\d+\.\d+)>$",line)
            seconds=float(duration[1]) if duration else 0
            row=aggregate.setdefault(call,dict(name=call,count=0,errors=0,seconds=0,threads={}))
            row["count"]+=1
            error=re.search(r"= -1 (\w+)",line)
            row["errors"]+=bool(error)
            row.setdefault("errno",{})
            if error:
                row["errno"][error[1]]=row["errno"].get(error[1],0)+1
            row["seconds"]+=seconds
            row["threads"][str(tid)]=row["threads"].get(str(tid),0)+1
            events.append(dict(timestamp=timestamp,tid=tid,name=call,seconds=seconds,
                               text=line[:700]))
        # incomplete calls are kept in the raw trace, not counted as completions
    return dict(summary=sorted(aggregate.values(),key=lambda r:r["count"],reverse=True),
                tids=pids,events=sorted(events,key=lambda e:e["timestamp"]),
                scope="strace -ff -ttt -T, all syscalls; traced wall durations include blocking and tracing overhead")

def capture(args):
    binary=Path(args.binary).resolve()
    if "build-asan" in str(binary) or "build-tsan" in str(binary):
        raise RuntimeError("use the ordinary Debug test host; Valgrind cannot combine with ASan/TSan")
    if not binary.is_file():
        raise RuntimeError("test host missing: "+str(binary))
    out=Path(args.output).resolve()
    out.mkdir(parents=True,exist_ok=False)
    for tool in ("gdb","valgrind","strace"):
        if not shutil.which(tool):
            raise RuntimeError("missing tool: "+tool)
    stages=[]
    env=dict(os.environ,ARENA_INSPECT_OUT=str(out),ARENA_INSPECT_CONNECTIONS=str(args.connections),
             ARENA_INSPECT_DRIVER=str(HERE/"capture.py"),
             ARENA_INSPECT_HOST_SOURCE=str(REPO/"tests/gateway/net/TcpServerTestHost.cpp"))
    port=port_number()
    env["ARENA_INSPECT_PORT"]=str(port)
    print("GDB: real virtual mappings, stacks and owners",flush=True)
    gdb_command=["gdb","-q","-batch","-ex","set pagination off","-ex","set confirm off",
                 "-ex","set disable-randomization off","-ex","source "+str(HERE/"capture_gdb.py"),
                 "--args",str(binary)]
    with (out/"gdb.log").open("w") as log:
        p=subprocess.Popen(gdb_command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        try:
            code=p.wait(timeout=100)
        finally:
            if p.poll() is None:
                identity_path=out/"gdb-process.json"
                if identity_path.exists():
                    identity=json.loads(identity_path.read_text())
                    stat_path=Path(f"/proc/{identity['pid']}/stat")
                    if stat_path.exists():
                        stat=stat_path.read_text()
                        ticks=stat[stat.rfind(")")+2:].split()[19]
                        if ticks==identity["start_ticks"]:
                            os.kill(identity["pid"],signal.SIGKILL)
                os.killpg(p.pid,signal.SIGKILL)
                p.wait()
    identity_path=out/"gdb-process.json"
    if identity_path.exists():
        identity=json.loads(identity_path.read_text())
        stat_path=Path(f"/proc/{identity['pid']}/stat")
        if stat_path.exists():
            stat=stat_path.read_text()
            start_ticks=stat[stat.rfind(")")+2:].split()[19]
            if start_ticks==identity["start_ticks"]:
                os.kill(identity["pid"],signal.SIGKILL)
                raise RuntimeError("GDB left test host alive; cleaned up exact process")
    completion_path=out/"gdb-completion.json"
    if code or not (out/"snapshot.json").exists() or not completion_path.exists():
        raise RuntimeError("GDB capture failed; see "+str(out/"gdb.log"))
    snapshot=json.loads((out/"snapshot.json").read_text())
    result=json.loads((out/"gdb-workload.json").read_text())
    if result["status"]!="passed":
        raise RuntimeError("GDB workload failed: "+str(result))
    stages.append(dict(name="gdb",pid=snapshot["pid"],command=gdb_command,exit_code=code,workload=result))
    for name,command,trace in (
        ("massif",["valgrind","--tool=massif","--time-unit=ms","--stacks=yes",
                   "--detailed-freq=1","--max-snapshots=60","--massif-out-file="+str(out/"massif.out")],False),
        ("callgrind",["valgrind","--tool=callgrind","--callgrind-out-file="+str(out/"callgrind.out")],False),
        ("strace",["strace","-ff","-ttt","-T","-o",str(out/"syscalls")],True),
    ):
        print(name+f": bounded {args.connections}-connection echo workload",flush=True)
        port=port_number()
        stages.append(run_profile(command+[str(binary),str(port),"60000","4"],
                                  out,name,port,args.connections,trace))
    data=dict(snapshot=snapshot,massif=parse_massif(out/"massif.out"),
              calls=parse_callgrind(out/"callgrind.out"),syscalls=parse_strace(out),
              captures=stages,created_at=dt.datetime.now().astimezone().isoformat(),
              binary=str(binary),binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
              git_head=subprocess.check_output(["git","rev-parse","HEAD"],cwd=REPO,text=True).strip(),
              working_tree_sha256=hashlib.sha256(subprocess.check_output(["git","diff","HEAD"],cwd=REPO)).hexdigest(),
              warning="Separate PIDs/ASLR runs. GDB map/object/stack data belong to one same snapshot. Other profiles are aggregate observations from equivalent workloads, not that address snapshot.")
    validate(data)
    write_report(out,data)
    dump(out/"data.json",data)
    # Self-contained artifact: no API/service needed once generated.
    html=(HERE/"dashboard.html").read_text()
    embedded=json.dumps(data,ensure_ascii=False).replace("<","\\u003c")
    (out/"index.html").write_text(html.replace("__CAPTURE_DATA__",embedded),encoding="utf-8")
    print(str(out/"index.html"),flush=True)
    return data

def validate(data):
    snapshot=data["snapshot"]
    def mapped(address):
        return any(int(m["start"],16)<=int(address,16)<int(m["end"],16)
                   for m in snapshot["maps"])
    if len(snapshot["threads"])!=5 or len(snapshot["workers"])!=4:
        raise RuntimeError("expected base + four worker threads")
    if any(not mapped(n["address"]) for n in snapshot["nodes"]):
        raise RuntimeError("object outside captured maps")
    if any(not mapped(t["sp"]) for t in snapshot["threads"]):
        raise RuntimeError("thread stack pointer outside captured maps")
    if sum(w["connections"] for w in snapshot["workers"])!=data["captures"][0]["workload"]["connections"]:
        raise RuntimeError("connection snapshot count mismatch")
    if not data["calls"]["edges"] or not data["syscalls"]["events"] or not data["massif"]["detailed_peak"]["tree"]:
        raise RuntimeError("empty runtime profile")

def write_report(out,data):
    s=data["snapshot"]
    virtual=sum(int(m["end"],16)-int(m["start"],16) for m in s["maps"])
    rss=sum(m["metrics"].get("Rss",0) for m in s["maps"])
    lines=["# ArenaServer 进程透视报告","",
           "## 采集对象","",
           f"一个测试服务进程中的 base + 4 个 worker 线程。{data['captures'][0]['workload']['connections']} 条持有连接，每轮发送和核对 16 KiB echo，8 轮；每个分析阶段为独立短时运行。没有恢复长期 churn。",
           "",f"- 二进制：{data['binary']}",
           f"- SHA256：{data['binary_sha256']}",
           f"- Git HEAD：{data['git_head']}（存在未提交工作树改动）",
           f"- GDB 快照：{s['captured_at']}；PID {s['pid']}",
           "","## 实测","",
           f"- {len(s['maps'])} 段映射，虚拟大小 {virtual:,} B，RSS {rss:,} B。",
           f"- {len(s['nodes'])} 个已识别对象/成员/存储节点；全部地址及线程 SP 均在 maps 中。",
           f"- worker 分布：{[(w['tid'],w['connections']) for w in s['workers']]}。",
           f"- Massif 活跃堆峰值：{data['massif']['peak']['mem_heap_B']:,} B。",
           f"- Callgrind：{len(data['calls']['edges'])} 条涉及 service:: 的动态调用边。",
           f"- strace：{len(data['syscalls']['events'])} 个完成事件，TID {data['syscalls']['tids']}。",
           "","## 各阶段验证","",
           "| 阶段 | PID | 验证字节 | 退出码 |",
           "| --- | ---: | ---: | ---: |"]
    lines.extend(f"| {c['name']} | {c['pid']} | {c['workload']['verified_bytes']:,} | {c['exit_code']} |" for c in data["captures"])
    lines+=["","## 如何使用","",
            "打开 index.html：地址定位支持十六进制；可选择映射中的对象，查看所有权和所属线程；选择调用图函数可展开上下游；点击分配树矩形看完整路径；系统调用支持 TID / 调用名筛选和原始返回值查看。",
            "","## 解释边界","",
            "- maps / smaps 是完整映射目录，没有导出整个进程的内存字节内容。",
            "- 对象图是通过当前 C++ 字段读取的部分对象图，不能扫描出全部任意对象或证明 shared_ptr 的最终释放者。",
            "- 图中 borrowed / weak / member / shared 语义依据源码；地址、容量、FD、调用栈来自同一 GDB 停止快照。",
            "- worker 栈用实际 SP 所在映射识别；不能只依赖 maps 的 [stack] 标记。",
            "- 虚拟预留空间不等于驻留页。anon 区域没有强行命名为 glibc arena；其余匿名分配来源仍未知。",
            "- GDB、Massif、Callgrind、strace 使用不同 PID。分配树及系统调用不得覆盖到 GDB 的绝对地址；每轮 ASLR 和观测开销不同。",
            "- Massif 只描述其跟踪的活跃请求内存、额外内存和估计栈使用；不包括全部 RSS / 内核 socket 缓冲区。",
            "- Callgrind 是插桩调用次数；strace 耗时包含阻塞和跟踪开销，EAGAIN 是非阻塞路径的预期结果。这不是吞吐量或生产延迟报告。",
            "- 此负载没有制造输出高水位，也不是泄漏、TSan 或长期稳定性验收。",
            "","## 原始依据","",
            "snapshot.json、gdb-maps.txt、gdb-smaps.txt、gdb-status.txt、massif.out、callgrind.out、syscalls.<tid>、各阶段日志和 workload.json 均保留。"]
    (out/"report.md").write_text("\n".join(lines)+"\n",encoding="utf-8")

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary",default=str(REPO/"build/tests/gateway/net/arena_tcp_server_test_host"))
    parser.add_argument("--output")
    parser.add_argument("--connections",type=int,default=32)
    parser.add_argument("--workload",action="store_true",help=argparse.SUPPRESS)
    parser.add_argument("--port",type=int)
    parser.add_argument("--pid",type=int)
    parser.add_argument("--result",type=Path)
    args=parser.parse_args()
    if not 4<=args.connections<=128:
        parser.error("connections must be in [4,128]")
    if args.workload:
        if None in (args.port,args.pid,args.result):
            parser.error("internal workload needs port,pid,result")
        return 0 if workload(args.port,args.pid,args.connections,args.result)["status"]=="passed" else 1
    if not args.output:
        parser.error("--output is required and must be a new directory")
    capture(args)
    return 0

if __name__=="__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(str(exc),file=sys.stderr)
        sys.exit(1)
