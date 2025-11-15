import os
import csv
import json
import gzip
import boto3
import botocore
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool
import time

# ========== 配置 ==========
BUCKET = "bucket-openalex"
PREFIX = "openalex/filtered_parquet_full"
ONE_UD = os.environ.get("TEST_UD")  # 若设置，只处理这个 updated_date=YYYY-MM-DD
BATCH = 16000
WORKERS = 5

s3 = boto3.client("s3")  # 主进程也需要列 key

# ========== 读白名单 ==========
def load_allow():
    allow = set()
    with open("journal_id_map.csv", newline="", encoding="utf-8") as f:
        rd = csv.DictReader(f)
        for r in rd:
            if r["source_id"]:
                allow.add(r["source_id"])
    if not allow:
        raise SystemExit("journal_id_map.csv 没有有效的 source_id")
    return allow

ALLOW = load_allow()

# ========== 工具函数 ==========
def source_ids_of_work(w):
    sids = []
    pl = (w.get("primary_location") or {}).get("source") or {}
    if pl.get("id"):
        sids.append(pl["id"])
    for loc in (w.get("locations") or []):
        so = (loc.get("source") or {})
        if so.get("id"):
            sids.append(so["id"])
    return sids

def extract_updated_date_from_path(p: str) -> str:
    parts = p.split("/")
    return next(
        (seg.split("=", 1)[1] for seg in parts if seg.startswith("updated_date=")),
        "unknown",
    )

# ========== worker：处理一个 .gz ==========
def process_one_gz(p: str):
    """
    单个分片处理（超稳定版）：
    - 不使用 s3fs
    - 用 boto3.get_object 流式读取
    - gzip streaming
    - 强韧性 retry（3 次）
    """
    ud = extract_updated_date_from_path(p)
    buf = []

    s3_worker = boto3.client("s3")  # 每个 worker 自己的客户端，互不影响

    def flush():
        if not buf:
            return
        table = pa.Table.from_pylist(buf)
        key = f"{PREFIX}/updated_date={ud}/part-{os.urandom(4).hex()}.parquet"
        s3_worker.put_object(
            Bucket=BUCKET,
            Key=key,
            Body=pq.write_table(table, compression="snappy").read()  # in-memory
        )
        buf.clear()

    for attempt in range(3):
        try:
            # boto3 get_object（最稳定的 S3 streaming 方式）
            resp = s3_worker.get_object(Bucket="openalex", Key=p)
            body = resp["Body"]

            # gzip streaming：不会积累 socket，不会占高内存
            gz = gzip.GzipFile(fileobj=body, mode="rb")

            for raw in gz:
                try:
                    line = raw.decode("utf-8", errors="ignore")
                    w = json.loads(line)
                except Exception:
                    continue

                if any(s in ALLOW for s in source_ids_of_work(w)):
                    buf.append(w)
                    if len(buf) >= BATCH:
                        flush()

            flush()
            return p

        except botocore.exceptions.ClientError as e:
            # 404 / 403 之类的
            if e.response["Error"]["Code"] == "NoSuchKey":
                print(f"[WARN] S3 key 不存在，跳过: {p}")
                return p
            print(f"[WARN] boto3 错误: {repr(e)} (第 {attempt+1}/3 次)")
            time.sleep(3 * (attempt + 1))

        except Exception as e:
            print(f"[WARN] 处理 {p} 出错: {repr(e)} (第 {attempt+1}/3 次)")
            time.sleep(3 * (attempt + 1))

    print(f"[WARN] 放弃分片 {p}，连续失败 3 次")
    return p


# ========== 主程序 ==========
def main():
    # 列出 openalex bucket 的全部 works 分片
    print("正在列出 openalex/data/works/...")
    paginator = s3.get_paginator("list_objects_v2")

    prefix = (
        f"openalex/data/works/{ONE_UD}/"
        if ONE_UD
        else "openalex/data/works/updated_date="
    )

    paths = []
    for page in paginator.paginate(Bucket="openalex", Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".gz"):
                paths.append(obj["Key"])

    print(f"共发现 {len(paths)} 个 gzip 分片，使用 {WORKERS} 个并行进程")

    if not paths:
        print("没有匹配到任何 .gz 文件，退出。")
        return

    with Pool(processes=WORKERS) as pool:
        for i, p in enumerate(pool.imap_unordered(process_one_gz, paths, chunksize=1), 1):
            if i % 100 == 0 or i == len(paths):
                print(f"[PROGRESS] 已完成 {i}/{len(paths)} 个分片")

    print("DONE")


if __name__ == "__main__":
    main()