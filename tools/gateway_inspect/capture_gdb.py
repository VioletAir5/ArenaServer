# Module: Inspects gateway runtime state for diagnostics.
"""GDB-side read-only snapshot; loaded by inspect.py, not standalone."""
import gdb
import json
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, "/usr/share/gcc/python")
from libstdcxx.v6.printers import register_libstdcxx_printers
register_libstdcxx_printers(None)
OUT = Path(os.environ["ARENA_INSPECT_OUT"])
N = int(os.environ["ARENA_INSPECT_CONNECTIONS"])
errors = []
server_pointer = None
captured = False
io_hits = 0

def address(value):
    return hex(int(value.address))

def children(value):
    printer = gdb.default_visualizer(value)
    if printer is None:
        raise RuntimeError("No C++ pretty printer for " + str(value.type))
    return [v for _, v in printer.children()]

def items(value):
    # GDB 12 prefixes template argument types with "struct"; the GCC
    # pretty-printer lookup then fails against GCC 15 debug symbols.
    import re
    table=value["_M_h"]
    value_type=table.type.template_argument(1)
    name=re.sub(r"\b(struct|class) ", "", str(value_type))
    cached=bool(table.type.template_argument(9).template_argument(0))
    node_type=gdb.lookup_type("std::__detail::_Hash_node<"+name+", "+("true" if cached else "false")+">")
    pointer=table["_M_before_begin"]["_M_nxt"]
    result=[]
    for _ in range(int(table["_M_element_count"])):
        if int(pointer)==0:
            raise RuntimeError("short hash chain")
        node=pointer.cast(node_type.pointer()).dereference()
        pair=node["_M_storage"].address.cast(value_type.pointer()).dereference()
        result.append((pair["first"],pair["second"]))
        pointer=node["_M_nxt"]
    return result

def shared(value):
    return value["_M_ptr"].dereference()

def vector(value):
    impl = value["_M_impl"]
    start, end = impl["_M_start"], impl["_M_finish"]
    return [start[i] for i in range(int(end-start))]

def storage(value):
    impl = value["m_storage"]["_M_impl"]
    start = impl["_M_start"]
    size = int(impl["_M_finish"]-start)
    capacity = int(impl["_M_end_of_storage"]-start)
    return dict(address=hex(int(start)),size=size,capacity=capacity,
                readable=int(value["m_writeIndex"]-value["m_readIndex"]),
                read_index=int(value["m_readIndex"]),write_index=int(value["m_writeIndex"]))

def proc_snapshot(pid):
    base = Path("/proc") / str(pid)
    maps = []
    current = None
    for line in (base/"smaps").read_text().splitlines():
        parts=line.split()
        if "-" in parts[0] and len(parts)>=5:
            start,end=parts[0].split("-")
            current=dict(start="0x"+start,end="0x"+end,perms=parts[1],
                         offset=parts[2],device=parts[3],inode=parts[4],
                         path=" ".join(parts[5:]),metrics={})
            maps.append(current)
        elif current is not None and ":" in line:
            key,value=line.split(":",1)
            if value.strip().split() and value.strip().split()[0].isdigit():
                current["metrics"][key]=int(value.strip().split()[0])*1024
    fds=[]
    for fd in sorted((base/"fd").iterdir(),key=lambda x:int(x.name)):
        try:
            info=(base/"fdinfo"/fd.name).read_text()
            fds.append(dict(fd=int(fd.name),target=os.readlink(fd),info=info))
        except OSError:
            pass
    for name in ("maps","smaps","status"):
        (OUT/("gdb-"+name+".txt")).write_text((base/name).read_text())
    return maps,fds

def take_snapshot():
    global captured
    server = server_pointer.dereference()
    workers=[shared(v) for v in vector(server["m_workers"])]
    if sum(int(w["connections"]["_M_h"]["_M_element_count"]) for w in workers) < N:
        return False
    inferior=gdb.selected_inferior()
    maps,fds=proc_snapshot(inferior.pid)
    threads=[]
    for thread in inferior.threads():
        thread.switch()
        frames=[]
        frame=gdb.newest_frame()
        while frame is not None and len(frames)<40:
            sal=frame.find_sal()
            frames.append(dict(function=frame.name() or "??",pc=hex(int(frame.pc())),
                file=sal.symtab.fullname() if sal.symtab else None,line=sal.line))
            frame=frame.older()
        sp=hex(int(gdb.parse_and_eval("$sp")))
        threads.append(dict(tid=thread.ptid[1],gdb_id=thread.num,sp=sp,
                            name=thread.name,frames=frames))
    nodes=[]
    edges=[]
    def node(value,kind,label,**extra):
        a=address(value)
        nodes.append(dict(id=a,address=a,size=int(value.type.sizeof),
                          kind=kind,label=label,**extra))
        return a
    def edge(a,b,kind,label):
        edges.append(dict(source=a,target=b,kind=kind,label=label))
    def loop_node(loop,label):
        a=node(loop,"loop",label)
        for field,kind in (("m_wakeupChannel","channel"),("m_timerQueue","timer")):
            # GCC unique_ptr is a tuple; GDB pretty-printer gives the owned value.
            printer=gdb.default_visualizer(loop[field])
            ptr=list(printer.children())[0][1]
            v=ptr.dereference()
            b=node(v,kind,field)
            edge(a,b,"unique",field)
        for field in ("m_poller",):
            b=node(loop[field],"poller",field,fd=int(loop[field]["m_epollFd"]["_fd"]))
            edge(a,b,"member",field)
        return a
    s=node(server,"server","TcpServer (base stack)")
    base=server["m_loop"].dereference()
    base_id=loop_node(base,"base EventLoop")
    edge(s,base_id,"borrowed","m_loop")
    pool=node(server["m_threadPool"],"pool","EventLoopThreadPool")
    edge(s,pool,"member","m_threadPool")
    acceptor=node(server["m_acceptor"],"acceptor","Acceptor",
                  fd=int(server["m_acceptor"]["m_listenSocket"]["fd_"]["_fd"]))
    edge(s,acceptor,"member","m_acceptor")
    edge(acceptor,base_id,"borrowed","m_loop")
    wrappers=[]
    for ptr in vector(server["m_threadPool"]["m_threads"]):
        printer=gdb.default_visualizer(ptr)
        thread=list(printer.children())[0][1].dereference()
        wrappers.append(node(thread,"thread","EventLoopThread"))
        edge(pool,wrappers[-1],"unique","m_threads")
    counts=[]
    for i,w in enumerate(workers):
        wid=node(w,"worker","Worker "+str(i+1))
        edge(s,wid,"shared","m_workers")
        lid=loop_node(w["loop"].dereference(),"worker "+str(i+1)+" EventLoop (thread stack)")
        edge(wid,lid,"borrowed","loop")
        if i<len(wrappers):
            edge(wrappers[i],lid,"borrowed","m_loop (published)")
        owner=next((t["tid"] for t in threads
            if any(int(m["start"],16)<=int(lid,16)<int(m["end"],16)
                   and int(m["start"],16)<=int(t["sp"],16)<int(m["end"],16)
                   for m in maps)),None)
        for n in nodes:
            if n["id"]==lid or n["id"]==wid:
                n["tid"]=owner
        count=0
        for cid,sptr in items(w["connections"]):
            conn=shared(sptr)
            count+=1
            c=node(conn,"connection","connection #"+str(int(cid)),
                tid=owner,connection_id=int(cid),state=str(conn["m_state"]),
                fd=int(conn["m_socket"]["fd_"]["_fd"]))
            edge(wid,c,"shared","connections")
            edge(c,lid,"borrowed","m_loop")
            channel=node(conn["m_channel"],"channel","Channel fd="+str(int(conn["m_channel"]["m_fd"])),tid=owner)
            edge(c,channel,"member","m_channel")
            edge(channel,c,"weak","tie(): weak_ptr")
            socket=node(conn["m_socket"],"socket","Socket",tid=owner,fd=int(conn["m_socket"]["fd_"]["_fd"]))
            edge(c,socket,"member","m_socket")
            for field in ("m_inputBuffer","m_outputBuffer"):
                buf=conn[field]
                bid=node(buf,"buffer",field,tid=owner)
                edge(c,bid,"member",field)
                data=storage(buf)
                did=data["address"]
                nodes.append(dict(id=did,address=did,size=data["capacity"],
                    kind="storage",label=field+" storage",tid=owner,**{k:v for k,v in data.items() if k not in ("address","size")}))
                edge(bid,did,"unique","vector storage")
        counts.append(dict(worker=i+1,tid=owner,connections=count,loop=lid,worker_address=wid))
    data=dict(pid=inferior.pid,threads=threads,maps=maps,fds=fds,
              nodes=nodes,edges=edges,workers=counts,errors=errors,
              captured_at=__import__("datetime").datetime.now().astimezone().isoformat(),
              method="GDB all-stop breakpoint in TcpConnection::send; no inferior function calls")
    (OUT/"snapshot.json").write_text(json.dumps(data,ensure_ascii=False,indent=2))
    captured=True
    return True

class IoBreakpoint(gdb.Breakpoint):
    def stop(self):
        global io_hits
        io_hits += 1
        return io_hits >= N * 3

class ReadyBreakpoint(gdb.Breakpoint):
    def stop(self):
        global server_pointer
        server_pointer=gdb.parse_and_eval("&server")
        pid=gdb.selected_inferior().pid
        stat=Path(f"/proc/{pid}/stat").read_text()
        start_ticks=stat[stat.rfind(")")+2:].split()[19]
        (OUT/"gdb-process.json").write_text(json.dumps(dict(pid=pid,start_ticks=start_ticks)))
        subprocess.Popen(["/usr/bin/python3",os.environ["ARENA_INSPECT_DRIVER"],
            "--workload","--port",os.environ["ARENA_INSPECT_PORT"],"--pid",str(pid),
            "--connections",str(N),"--result",str(OUT/"gdb-workload.json")],
            stdout=open(OUT/"gdb-client.log","w"),stderr=subprocess.STDOUT)
        IoBreakpoint("service::gateway::TcpConnection::send")
        self.enabled=False
        return False

host=Path(os.environ["ARENA_INSPECT_HOST_SOURCE"])
line=next(i for i,s in enumerate(host.read_text().splitlines(),1) if "loop.loop();" in s)
ReadyBreakpoint(str(host)+":"+str(line))
gdb.execute("run "+os.environ["ARENA_INSPECT_PORT"]+" 60000 4")
try:
    if not take_snapshot():
        raise RuntimeError("Not all connections reached workers")
except Exception:
    (OUT/"gdb-snapshot-error.txt").write_text(__import__("traceback").format_exc())
    raise
for breakpoint in gdb.breakpoints():
    breakpoint.delete()
gdb.execute("continue")
exit_code=int(gdb.parse_and_eval("$_exitcode"))
(OUT/"gdb-completion.json").write_text(json.dumps(dict(exit_code=exit_code)))
if exit_code!=0:
    raise RuntimeError("test host exited with "+str(exit_code))
if not captured:
    raise RuntimeError("No complete object snapshot; inspect gdb log")
