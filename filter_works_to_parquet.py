import s3fs, gzip, json, os
import pyarrow as pa, pyarrow.parquet as pq
import csv

# 读取白名单 source_id
allow = set()
with open("journal_id_map.csv", newline="", encoding="utf-8") as f:
    rd = csv.DictReader(f)
    for r in rd:
        if r["source_id"]:
            allow.add(r["source_id"])

if not allow:
    raise SystemExit("journal_id_map.csv 没有有效的 source_id")

src = s3fs.S3FileSystem(anon=True)    # 公共桶读
dst = s3fs.S3FileSystem()              # 你自己的桶写（走实例角色）

BUCKET = "bucket-openalex"
PREFIX = "openalex/filtered_parquet"
ONE_UD = os.environ.get("TEST_UD")     # 若设置，只处理这个 updated_date=YYYY-MM-DD

if not BUCKET:
    raise SystemExit("环境变量 MY_BUCKET 未设置")

def source_ids_of_work(w):
    sids = []
    pl = (w.get("primary_location") or {}).get("source") or {}
    if pl.get("id"): sids.append(pl["id"])
    for loc in (w.get("locations") or []):
        so = (loc.get("source") or {})
        if so.get("id"): sids.append(so["id"])
    return sids

def write_batch(batch, updated_date):
    if not batch: return
    table = pa.Table.from_pylist(batch)
    key = f"s3://{BUCKET}/{PREFIX}/updated_date={updated_date}/part-{os.urandom(4).hex()}.parquet"
    with dst.open(key, "wb") as f:
        pq.write_table(table, f, compression="snappy")
    batch.clear()

# 遍历公开桶 works 分片
pattern = f"openalex/data/works/{ONE_UD}/*.gz" if ONE_UD else "openalex/data/works/updated_date=*/**/*.gz"
paths = src.glob(pattern)

BATCH = 20000
current_ud = None
buf = []

for p in paths:
    ud = next((seg.split("=")[1] for seg in p.split("/") if seg.startswith("updated_date=")), "unknown")
    if current_ud is None: current_ud = ud
    elif ud != current_ud:
        write_batch(buf, current_ud)
        current_ud = ud

    with src.open(p, "rb") as fin, gzip.open(fin, "rt", encoding="utf-8", errors="ignore") as gz:
        for line in gz:
            try:
                w = json.loads(line)
            except:
                continue
            if any(sid in allow for sid in source_ids_of_work(w)):
                buf.append({
                    "id": w.get("id"),
                    "doi": w.get("doi"),
                    "title": w.get("title"),
                    "publication_year": w.get("publication_year"),
                    "publication_date": w.get("publication_date"),
                    "is_corr_author": any(a.get("is_corresponding") for a in (w.get("authorships") or [])),
                    "journal_id": ((w.get("primary_location") or {}).get("source") or {}).get("id"),
                    "cited_by_count": w.get("cited_by_count"),
                })
                if len(buf) >= BATCH:
                    write_batch(buf, ud)

write_batch(buf, current_ud)
print("DONE")
