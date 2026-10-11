"""Audit fixed source evidence and local links; it cannot prove prose semantics."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/learning/code-analysis"
VAULT = Path("/mnt/c/Users/wuzhitao/Documents/Obsidian Vault/ArenaServer")
def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])
def text(p):
    return p.read_bytes().decode()
def prose(p):
    rows, fence = [], None
    for line in text(p).splitlines():
        match = re.match(r"^([~`]{3,})", line)
        if match:
            if fence is None:
                fence=match[1]
            elif line.strip()==fence:
                fence=None
            continue
        if fence is None:
            rows.append(line)
    return "\n".join(rows)
def fenced_bytes(p):
    raw=p.read_bytes()
    opening=re.search(rb"(?m)^(~{3,})[A-Za-z]*\n",raw)
    assert opening,p
    closing=raw.rfind(b"\n"+opening[1]+b"\n")
    assert closing>opening.end(),p
    return raw[opening.end():closing+1]
def links(paths):
    issues=[]
    count=0
    for file in paths:
        for target in re.findall(r"(?<!!)\[[^\]]*\]\(([^\n)]+)\)",prose(file)):
            target=target.strip().strip("<>")
            if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://",target) or target.startswith(("mailto:","#")):
                continue
            location, _, anchor = target.partition("#")
            location=unquote(location)
            resolved=(file.parent/location).resolve()
            count+=1
            if not resolved.is_file():
                issues.append(f"{file}: {target}")
            elif anchor and anchor.startswith("commit-") and 'id="'+anchor+'"' not in text(resolved):
                issues.append(f"Missing anchor in {file}: {target}")
    return count,issues

inv=json.loads(text(OUT/"inventory.json"))
sha=git("rev-parse","v0.2^{commit}").decode().strip()
assert sha==inv["source_commit"]=="b6dd2835a050f3b118e2739ff65f992ed44bca5e"
tree=git("ls-tree","-r","--name-only",sha).decode().splitlines()
# Independently enumerate the documented inclusion rule.
expected={p for p in tree if Path(p).suffix in {".h",".hpp",".cpp",".py",".proto",".html",".dsl"}
          or Path(p).name=="CMakeLists.txt"
          or p in {"CMakePresets.json",".vscode/c_cpp_properties.json",".gitignore"}}
assert expected=={x["path"] for x in inv["files"]}
assert len(expected)==152 and sum(x["source_lines"] for x in inv["files"])==26296
coverage=[]
for x in inv["files"]:
    raw=git("show",sha+":"+x["path"])
    assert hashlib.sha256(raw).hexdigest()==x["source_sha256"],x
    assert len(raw.decode().splitlines())==x["source_lines"],x
    assert git("rev-parse",sha+":"+x["path"]).decode().strip()==x["git_blob"],x
    assert fenced_bytes(OUT/x["source_note"])==raw+(b"" if raw.endswith(b"\n") else b"\n"),x
    chapter=x["analysis"]
    path=x["path"]
    # Explicit secondary analysis locations for aliases and build targets.
    if Path(path).name=="CMakeLists.txt":
        chapter="06-build-and-entry.md"
    elif Path(path).stem=="PacketBuffer":
        chapter="03-protocol.md"
    elif path.endswith(("TcpServerChurnMonitor.py","TcpServerChurnDashboard.html")):
        chapter="08-observability-tools.md"
    chapter_text=prose(OUT/chapter)
    stem=Path(path).stem
    needle=path if Path(path).name=="CMakeLists.txt" else Path(path).name
    if needle not in chapter_text:
        needle=stem
    assert needle in chapter_text,(path,chapter,"No explicit manual analysis mention")
    headings=[m for m in re.finditer(r"(?m)^#{2,3} (.+)$",chapter_text)]
    # Pick first explicit reference; a file map reference points to the same chapter.
    pos=chapter_text.index(needle)
    nearby=[m for m in headings if m.start()<=pos]
    section=nearby[-1][1] if nearby else "章首/文件地图"
    coverage.append({"path":path,"chapter":chapter,"section":section})
current_excerpts=0
for chapter in sorted(OUT.glob("[0-9][0-9]-*.md")):
    for m in re.finditer(r"<!-- verified-excerpt (\w+) (\S+) (\d+)-(\d+) -->.*?\n~~~\w*\n(.*?)\n~~~",text(chapter),re.S):
        rev,path,first,last,body=m.groups()
        assert rev==sha
        original=git("show",rev+":"+path).decode().splitlines(keepends=True)
        expected_body="".join(original[int(first)-1:int(last)])
        if expected_body.endswith("\n"):
            expected_body=expected_body[:-1]
        assert body==expected_body,(chapter,path,first,last)
        current_excerpts+=1
history=json.loads(text(OUT/"history-inventory.json"))
assert [r["sha"] for r in history["records"]]==git("rev-list","--reverse","v0.1..v0.2").decode().splitlines()
assert len(history["records"])==28
companion=text(OUT/"09-commit-code.md")
for r in history["records"]:
    raw=git("show","--format=","--no-ext-diff","--no-renames","--binary","--unified=3",r["sha"])
    assert hashlib.sha256(raw).hexdigest()==r["patch_sha256"]
    assert fenced_bytes(OUT/r["patch_note"])==raw+(b"" if raw.endswith(b"\n") else b"\n"),r
    expected_body="".join(raw.decode().splitlines(keepends=True)[r["first"]-1:r["last"]]).rstrip("\n")
    marker="<!-- verified-history "+r["sha"]+" "+r["patch_sha256"]+" "+str(r["first"])+"-"+str(r["last"])+" -->"
    m=re.search(re.escape(marker)+r".*?\n~~~diff\n(.*?)\n~~~",companion,re.S)
    assert m and m[1]==expected_body,r
    assert r["explanation"] in companion and r["limit"] in companion,r
repo_files=list((ROOT/"docs/learning").rglob("*.md"))
repo_count,repo_issues=links(repo_files)
vault_files=list((VAULT/"architecture/code-analysis").rglob("*.md"))
vault_files+=list((VAULT/"architecture").glob("v01-v02-*.md"))+list((VAULT/"architecture").glob("v02-current-design.md"))
vault_count,vault_issues=links(vault_files)
for issue in repo_issues+vault_issues:
    print(issue)
if repo_issues or vault_issues:
    raise SystemExit("Unresolved Markdown links: "+str(len(repo_issues)+len(vault_issues)))
# Excerpts and the full evidence files mirror byte-for-byte.
for file in OUT.rglob("*"):
    if file.is_file() and file.name!="verification.json":
        mirrored=VAULT/"architecture/code-analysis"/file.relative_to(OUT)
        assert mirrored.is_file(),mirrored
        assert mirrored.read_bytes().replace(b"../references/reports/",b"../../reports/")==file.read_bytes(),mirrored
tests=json.loads(text(OUT/"test-case-inventory.json"))
assert tests["source_commit"]==sha
test_paths={x["path"] for x in inv["files"] if x["path"].startswith("tests/") and x["path"].endswith((".cpp",".py"))}
assert test_paths=={x["path"] for x in tests["files"]}
assert len(tests["files"])==43 and sum(len(x["entries"]) for x in tests["files"])==378
for file in tests["files"]:
    lines=git("show",sha+":"+file["path"]).decode().splitlines()
    for entry in file["entries"]:
        assert 1<=entry["line"]<=len(lines),(file["path"],entry)
        line=lines[entry["line"]-1]
        assert (entry["kind"] in line),(file["path"],entry,line)
report={"source_commit":sha,"authored_files":152,"source_lines":26296,
        "current_excerpts":current_excerpts,"historical_commits":28,"complete_diffs":28,
        "test_files_cpp_python":43,"test_macro_python_entries":378,
        "repo_markdown_links":repo_count,"vault_markdown_links":vault_count,
        "scope":"Static source/patch/link/coverage audit; no execution of historical tests or profilers",
        "coverage":coverage}
if "--write" in sys.argv:
    (OUT/"verification.json").write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print(json.dumps({k:v for k,v in report.items() if k!="coverage"},ensure_ascii=False,indent=2))
