"""Freeze authored v0.2 code as readable Markdown; no build or server execution."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "code-analysis"
REV = "v0.2"
def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])
sha = git("rev-parse", REV + "^{commit}").decode().strip()
paths = git("ls-tree", "-r", "--name-only", sha).decode().splitlines()

import re
import sys
if len(sys.argv) == 3 and sys.argv[1] == "--render":
    file = OUT / sys.argv[2]
    body = file.read_text(encoding="utf-8")
    def excerpt(match):
        path, first, last = match.group(1), int(match.group(2)), int(match.group(3))
        original = git("show", sha + ":" + path).decode("utf-8")
        lines = original.splitlines(keepends=True)
        if first < 1 or last < first or last > len(lines):
            raise ValueError((path, first, last, len(lines)))
        text = "".join(lines[first-1:last])
        lang = "python" if path.endswith(".py") else "protobuf" if path.endswith(".proto") else "cmake" if path.endswith("CMakeLists.txt") else "cpp"
        return (f"<!-- verified-excerpt {sha} {path} {first}-{last} -->\n"
                f"原文：`{path}:L{first}–L{last}`；[完整文件](source/{path}.md)。\n\n"
                + "~~~" + lang + "\n" + text + ("" if text.endswith("\n") else "\n") + "~~~")
    body, count = re.subn(r"\{\{code:([^:]+):(\d+)-(\d+)\}\}", excerpt, body)
    file.write_text(body, encoding="utf-8")
    print(f"Rendered {count} source excerpts in {file.name}")
    sys.exit(0)

def included(path):
    p = Path(path)
    return p.suffix in {".h", ".hpp", ".cpp", ".py", ".proto", ".html", ".dsl"} or p.name == "CMakeLists.txt" or path in {"CMakePresets.json", ".vscode/c_cpp_properties.json", ".gitignore"}
def category(path):
    if path == "services/gateway/app/main.cpp": return "build"
    if path.startswith("tests/"): return "tests"
    if path.startswith("tools/"): return "tools"
    if path.endswith("CMakeLists.txt") or path.endswith(".json") or path == ".gitignore": return "build"
    if path.endswith(".dsl"): return "planned-design"
    if path.startswith("shared/include/concurrent/") or path.startswith("shared/src/concurrent/"): return "shared"
    if path.startswith("shared/include/os/") or path.startswith("shared/src/os/"): return "shared"
    if path.startswith("shared/"): return "protocol"
    name = Path(path).stem
    if name in {"EventLoop", "EpollPoller", "Channel", "Acceptor", "Accptor", "EventLoopThread", "EventLoopThreadPool", "Timer", "TimerQueue", "Socket", "InetAddress"}: return "reactor"
    if name in {"TcpConnection", "TcpServer", "AsioConnect", "AsioTcpServer", "WebSocketServer", "IConnection", "ITcpServer", "ConnectionId"}: return "transports"
    return "runtime-routing"
chapters = {
    "shared": "01-shared.md",
    "reactor": "02-reactor.md",
    "protocol": "03-protocol.md",
    "transports": "04-transports.md",
    "runtime-routing": "05-runtime-routing.md",
    "build": "06-build-and-entry.md",
    "planned-design": "06-build-and-entry.md",
    "tests": "07-tests.md",
    "tools": "08-observability-tools.md",
}
inventory = []
for path in paths:
    if not included(path): continue
    data = git("show", sha + ":" + path)
    text = data.decode("utf-8")
    cat = category(path)
    lang = {".cpp":"cpp", ".h":"cpp", ".hpp":"cpp", ".py":"python", ".proto":"protobuf", ".html":"html", ".dsl":"text", ".json":"json"}.get(Path(path).suffix, "cmake" if path.endswith("CMakeLists.txt") else "text")
    target = OUT / "source" / (path + ".md")
    target.parent.mkdir(parents=True, exist_ok=True)
    # Relative analysis link stays valid in both repo and vault mirrors.
    relative = Path(*([".."] * (len(Path(path).parts)))) / chapters[cat]
    header = f"# {path}\n\n固定源码：{sha}；{len(text.splitlines())}行。\n\n[对应分析]({relative.as_posix()})。这是完整原始源码附件，机制解释见对应分析章；不把原文附件当分析本身。\n\n## 完整源码（保留原始内容）\n\n"
    fence = "~" * max(3, max((len(line) for line in text.splitlines() if line and set(line) == {"~"}), default=0) + 1)
    rendered = header + fence + lang + "\n" + text + ("" if text.endswith("\n") else "\n") + fence + "\n"
    target.write_text(rendered, encoding="utf-8")
    inventory.append({"path":path, "category":cat, "analysis":chapters[cat], "source_note":"source/"+path+".md", "source_lines":len(text.splitlines()), "source_sha256":hashlib.sha256(data).hexdigest(), "git_blob":git("rev-parse", sha+":"+path).decode().strip()})
OUT.mkdir(parents=True, exist_ok=True)
(OUT / "inventory.json").write_text(json.dumps({"source_commit":sha, "selection":"All authored C++/headers/Python/proto/HTML/DSL/CMake and root build/editor settings at v0.2; generated render assets, prose and measured data excluded.", "count":len(inventory), "source_lines":sum(x["source_lines"] for x in inventory), "files":inventory}, ensure_ascii=False, indent=2)+"\n",encoding="utf-8")
rows = ["# v0.2 全代码文件索引", "", "固定版本 "+sha+"。源码附件覆盖作者维护的代码/构建/工具/测试；生成的图像与测量数据不重复当代码。每文件链接到所属分析章。", "", "| 源码 | 行数 | 分析 | 完整原文 |", "| --- | ---: | --- | --- |"]
for x in inventory:
    rows.append(f"| `{x['path']}` | {x['source_lines']} | [{x['category']}]({x['analysis']}) | [源码]({x['source_note']}) |")
(OUT / "source-index.md").write_text("\n".join(rows)+"\n",encoding="utf-8")
print(f"Frozen {len(inventory)} files, {sum(x['source_lines'] for x in inventory)} source lines at {sha}")
