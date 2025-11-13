import s3fs, gzip, json, re, unicodedata, sys
from collections import defaultdict

# 读期刊名（统一规范化，去大小写/多空格）
def norm(s):
    s = unicodedata.normalize("NFKC", s).strip()
    s = re.sub(r"\s+", " ", s)
    return s.upper()

targets = [line.strip() for line in open("journals.txt", "r", encoding="utf-8") if line.strip()]
targets_norm = {norm(x): x for x in targets}          # 规范名 → 原始名
wanted = set(targets_norm.keys())

src = s3fs.S3FileSystem(anon=True)  # 公共桶匿名读取
paths = src.glob("openalex/data/sources/updated_date=*/**/*.gz")

hits = []                     # 匹配成功的记录
by_norm = defaultdict(list)   # 规范名 → 候选（用于排歧义）

for p in paths:
    with src.open(p, "rb") as fin, gzip.open(fin, "rt", encoding="utf-8", errors="ignore") as f:
        for line in f:
            try:
                o = json.loads(line)
            except:
                continue
            name = o.get("display_name") or ""
            n = norm(name)
            if n in wanted:
                by_norm[n].append(o)

# 规则：完全同名若返回多个，优先 type=journal / 学术期刊；否则取结果数最多的那个 source
def is_better(a, b):
    # 先按有无 issn 判；再按被引数/作品数（粗略）；最后按 id 字符串稳定排序
    ai = len(a.get("issn") or []) > 0
    bi = len(b.get("issn") or []) > 0
    if ai != bi: return ai
    ac = a.get("works_count") or 0
    bc = b.get("works_count") or 0
    if ac != bc: return ac > bc
    return (a.get("id") or "") < (b.get("id") or "")

for n, lst in by_norm.items():
    # 选最好的一条
    best = lst[0]
    for cand in lst[1:]:
        if is_better(cand, best):
            best = cand
    hits.append({
        "display_name_input": targets_norm[n],
        "display_name_matched": best.get("display_name"),
        "source_id": best.get("id"),
        "issn": "|".join(best.get("issn") or []),
        "works_count": best.get("works_count"),
        "host_org": best.get("host_organization_name"),
    })

# 输出：命中 & 未命中
import csv
with open("journal_id_map.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=["display_name_input","display_name_matched","source_id","issn","works_count","host_org"])
    w.writeheader()
    w.writerows(sorted(hits, key=lambda r: r["display_name_input"].upper()))

found_inputs = {h["display_name_input"] for h in hits}
unmatched = sorted(set(targets) - found_inputs)
with open("journal_id_unmatched.txt", "w", encoding="utf-8") as f:
    for name in unmatched:
        f.write(name + "\n")

print(f"Matched {len(hits)} / {len(targets)} journals. See journal_id_map.csv and journal_id_unmatched.txt")
