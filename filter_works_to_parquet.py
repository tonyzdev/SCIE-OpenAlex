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
PREFIX = "openalex/filtered_parquet_full"
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
    # 提取 updated_date
    parts = p.split("/")
    ud = next((seg.split("=")[1] for seg in parts if seg.startswith("updated_date=")), "unknown")

    if current_ud is None:
        current_ud = ud
    elif ud != current_ud:
        write_batch(buf, current_ud)
        current_ud = ud

    # 读取 gzip JSONL
    with src.open(p, "rb") as fin, gzip.open(fin, "rt", encoding="utf-8", errors="ignore") as gz:
        for line in gz:
            try:
                w = json.loads(line)
            except:
                continue

            # 命中判断：primary_location 或 locations 里的 source.id
            if any(s in allow for s in source_ids_of_work(w)):
                # 关键：整条原始 JSON 直接写入 parquet
                buf.append(w)

                if len(buf) >= BATCH:
                    write_batch(buf, ud)

# flush 最后一批
write_batch(buf, current_ud)
print("DONE")
