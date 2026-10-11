"""Render curated commit analysis against exact historical diffs, not current source."""
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
OUT = HERE / "code-analysis"
def git(*args):
    return subprocess.check_output(["git", "-C", str(ROOT), *args])
manifest = json.loads((HERE / "v01-v02-commit-manifest.json").read_text())
specs = json.loads((HERE / "history-excerpts.json").read_text())
actual = git("rev-list", "--reverse", "v0.1..v0.2").decode().splitlines()
assert actual == [c["sha"] for c in manifest["commits"]]
assert len(specs) == len(actual) == 28
history = OUT / "history"
history.mkdir(exist_ok=True)
rows = [
    "# 09 28次commit：代码对照与机制分析", "",
    "固定范围 v0.1..v0.2。片段来自每次提交相对第一父提交的原始diff；+是新增，-是删除，空格是上下文。节选可能从hunk中间开始，不是可单独应用的patch；完整diff逐提交保留。不能把这些历史片段当当前v0.2源码。", "",
    "每项结合[原逐提交讲解](../v01-v02-index.md)阅读，并链接当前对应机制章。纯注释/清理/报告提交如实说明，没有为其虚构运行行为。完整源码与测试见[总导航](README.md)。", "",
]
records = []
for c, s in zip(manifest["commits"], specs):
    assert c["ordinal"] == s["ordinal"]
    sha, ordinal = c["sha"], c["ordinal"]
    assert s["path"] in c["changed_files"], s
    patch = git("show", "--format=", "--no-ext-diff", "--no-renames", "--binary", "--unified=3", sha)
    lines = patch.decode().splitlines(keepends=True)
    start = next(i for i, x in enumerate(lines) if x.startswith("diff --git a/" + s["path"] + " b/"))
    end = next((i for i in range(start+1, len(lines)) if lines[i].startswith("diff --git ")), len(lines))
    hits = [i for i in range(start, end) if s["needle"] in lines[i]]
    assert hits, s
    hit = hits[s["occurrence"]]
    first, last = max(start, hit-s["before"]), min(end, hit+s["after"])
    excerpt = "".join(lines[first:last])
    patch_hash = hashlib.sha256(patch).hexdigest()
    filename = f"{ordinal:02d}-{sha[:7]}.patch.md"
    full = (
        f"# {ordinal:02d} · {sha[:7]} 完整diff\n\n"
        f"提交：{sha}；父提交：{git('rev-parse', sha+'^').decode().strip()}。\n\n"
        f"原题：{c['subject']}。日期：{c['date']}。\n\n"
        f"SHA256（原始diff字节）：{patch_hash}。\n\n"
        f"[代码分析](../09-commit-code.md#commit-{ordinal:02d})。使用git show --format= --no-ext-diff --no-renames --binary --unified=3 {sha}生成。\n\n"
        "~~~diff\n" + patch.decode() + ("" if patch.endswith(b"\n") else "\n") + "~~~\n"
    )
    (history / filename).write_text(full, encoding="utf-8")
    old = "01" if ordinal <= 11 else "02" if ordinal <= 23 else "03"
    rows += [
        f'<a id="commit-{ordinal:02d}"></a>', "",
        f"## {ordinal:02d} · {sha[:7]} · {c['subject']}", "",
        f"日期：{c['date']}；[原设计理由](../v01-v02-commits-{old}.md)；[当前机制]({s['chapter']})；[完整diff](history/{filename})。", "",
        s["explanation"], "",
        f"<!-- verified-history {sha} {patch_hash} {first+1}-{last} -->",
        f"节选文件：`{s['path']}`，原始完整diff第{first+1}–{last}行。", "",
        "~~~diff", excerpt.rstrip("\n"), "~~~", "", s["limit"], "",
    ]
    records.append({"ordinal":ordinal, "sha":sha, "parent":git("rev-parse",sha+"^").decode().strip(),
                    "path":s["path"], "patch_note":"history/"+filename, "patch_sha256":patch_hash,
                    "patch_lines":len(lines), "first":first+1, "last":last,
                    "explanation":s["explanation"], "limit":s["limit"]})
(OUT / "09-commit-code.md").write_text("\n".join(rows)+"\n", encoding="utf-8")
(OUT / "history-inventory.json").write_text(json.dumps(
    {"range":manifest["range"],"count":len(records),"records":records},
    ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
print(f"Rendered {len(records)} commit explanations and complete diffs")
