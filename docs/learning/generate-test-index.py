"""Index fixed v0.2 test entry points; this does not execute the tests."""
import ast
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "code-analysis"
REV = subprocess.check_output(["git", "-C", str(ROOT), "rev-parse", "v0.2^{commit}"]).decode().strip()
inventory = json.loads((OUT / "inventory.json").read_text())["files"]

def source(path):
    return subprocess.check_output(["git", "-C", str(ROOT), "show", REV + ":" + path]).decode()

rows = ["# 固定v0.2测试入口索引", "",
        "自动从Git对象定位TEST/TEST_F/TEST_P及Python函数；源码是权威。C++按宏入口分段，断言数量是静态文本统计，非执行次数；参数化case不是展开后的CTest数量。Python包含helper与actor，不把每个函数当独立验收用例。机制分析见[测试章](07-tests.md)。", ""]
metadata = []
for item in inventory:
    path = item["path"]
    if not path.startswith("tests/") or not path.endswith((".cpp", ".py")):
        continue
    text = source(path)
    entries = []
    if path.endswith(".cpp"):
        matches = list(re.finditer(r"\b(TEST(?:_F|_P)?)\s*\(\s*(\w+)\s*,\s*(\w+)\s*\)", text))
        for i, match in enumerate(matches):
            last = matches[i + 1].start() if i + 1 < len(matches) else len(text)
            body = text[match.end():last]
            checks = re.findall(r"\b(?:EXPECT|ASSERT)_[A-Z_]+\s*\(", body)
            examples = []
            for line in body.splitlines():
                if re.search(r"\b(?:EXPECT|ASSERT)_[A-Z_]+", line):
                    value = line.strip().replace("|", "\\|").replace("~", "")
                    examples.append(value[:180])
            entries.append({"name":match.group(2)+"."+match.group(3), "kind":match.group(1),
                            "line":text.count("\n",0,match.start())+1,
                            "checks_static":len(checks), "check_example":" / ".join(examples[:2])})
    else:
        tree = ast.parse(text, filename=path)
        def walk(block, prefix=""):
            for node in block:
                if isinstance(node, ast.ClassDef):
                    walk(node.body, prefix+node.name+".")
                elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    assertions = [a for a in ast.walk(node) if isinstance(a, ast.Assert)]
                    example = ast.get_source_segment(text, assertions[0]) if assertions else ""
                    entries.append({"name":prefix+node.name, "kind":"async def" if isinstance(node,ast.AsyncFunctionDef) else "def",
                                    "line":node.lineno, "checks_static":len(assertions),
                                    "check_example":re.sub(r"\s+"," ",example or "")[:200].replace("|","\\|")})
                    walk(node.body, prefix+node.name+".")
        walk(tree.body)
    rows += ["## "+path, "", "[完整源码]("+item["source_note"]+")", "",
             "| 入口 | 行 | 形式 | 静态检查数 | 断言原文举例（可能多行，详见源码） |",
             "| --- | ---: | --- | ---: | --- |"]
    for entry in entries:
        rows.append("| "+entry["name"]+" | "+str(entry["line"])+" | "+entry["kind"]+" | "+str(entry["checks_static"])+" | "+entry["check_example"]+" |")
    if not entries:
        rows.append("| 无TEST宏或Python函数入口；这是宿主/支持代码 | — | — | — | — |")
    rows.append("")
    metadata.append({"path":path, "entries":entries})
(OUT/"test-case-index.md").write_text("\n".join(rows),encoding="utf-8")
(OUT/"test-case-inventory.json").write_text(json.dumps({"source_commit":REV,"files":metadata},ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print(f"Indexed {sum(len(x['entries']) for x in metadata)} entries in {len(metadata)} files")
for item in metadata:
    print(item["path"]+": "+", ".join(x["name"].split(".")[-1] for x in item["entries"]))
