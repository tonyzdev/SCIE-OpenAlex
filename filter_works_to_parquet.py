import os
import csv
import json
import gzip
import io
import time
import random
import multiprocessing
from multiprocessing import Pool

import boto3
import botocore
from botocore.client import Config
from botocore import UNSIGNED

import pyarrow as pa
import pyarrow.parquet as pq

# ========== 配置 ==========
BUCKET = "bucket-openalex"  # 你自己的桶，用来写过滤结果
PREFIX = "openalex/filtered_parquet_full"
ONE_UD = os.environ.get("TEST_UD")  # 若设置，只处理这个 updated_date=YYYY-MM-DD

BATCH = 8000            # 每个 parquet 里最多记录数（先保守一点）
WORKERS = 3             # 多进程 worker 数（先保守一点）

PUBLIC_BUCKET = "openalex"   # OpenAlex 公共桶名称


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
            sids.append(so.get("id"))
    return sids


def extract_updated_date_from_path(p: str) -> str:
    parts = p.split("/")
    return next(
        (seg.split("=", 1)[1] for seg in parts if seg.startswith("updated_date=")),
        "unknown",
    )


# ========== worker：处理一个 .gz ==========
def process_one_gz(task):
    """
    task: (global_index, key)
    """
    idx, key = task
    ud = extract_updated_date_from_path(key)
    buf = []

    # 每个 worker 自己的 client：读用匿名，写用默认角色
    s3_read = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    s3_write = boto3.client("s3")

    def flush():
        if not buf:
            return
        table = pa.Table.from_pylist(buf)

        out_buf = io.BytesIO()
        pq.write_table(table, out_buf, compression="snappy")
        out_buf.seek(0)

        out_key = f"{PREFIX}/updated_date={ud}/part-{os.urandom(4).hex()}.parquet"
        s3_write.put_object(
            Bucket=BUCKET,
            Key=out_key,
            Body=out_buf.getvalue()
        )
        buf.clear()

        del table
        del out_buf

    for attempt in range(3):
        try:
            # 这里标记一下开始处理哪个分片，方便之后排查
            if attempt == 0:
                print(f"[INFO] 开始处理分片 #{idx}: {key}")

            resp = s3_read.get_object(Bucket=PUBLIC_BUCKET, Key=key)
            with resp["Body"] as body:
                with gzip.GzipFile(fileobj=body, mode="rb") as gz:
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
            print(f"[INFO] 完成分片 #{idx}: {key}")
            return idx  # 返回 index，方便主进程统计

        except botocore.exceptions.ClientError as e:
            code = e.response["Error"].get("Code")
            if code == "NoSuchKey":
                print(f"[WARN] S3 key 不存在，跳过: {key}")
                return idx
            print(f"[WARN] boto3 ClientError 处理 #{idx} {key} 出错 (第 {attempt+1}/3 次): {e}")
            time.sleep(3 * (attempt + 1))

        except Exception as e:
            print(f"[WARN] 处理 #{idx} {key} 出错 (第 {attempt+1}/3 次): {repr(e)}")
            time.sleep(3 * (attempt + 1))

    print(f"[WARN] 放弃分片 #{idx}: {key}，连续失败 3 次")
    return idx


# ========== 主程序 ==========
def main():
    print("正在列出 openalex/data/works/ ...")

    s3_public = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    paginator = s3_public.get_paginator("list_objects_v2")

    if ONE_UD:
        prefix = f"data/works/{ONE_UD}/"
    else:
        prefix = "data/works/updated_date="

    keys = []
    for page in paginator.paginate(Bucket=PUBLIC_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".gz"):
                keys.append(obj["Key"])

    total = len(keys)
    print(f"共发现 {total} 个 gzip 分片")

    if not keys:
        print("没有匹配到任何 .gz 文件，退出。")
        return

    # 🔁 关键：把顺序随机打乱，不再每次都按同样顺序处理
    random.shuffle(keys)

    # 为了以后排查问题，把 (index, key) 一起传进 worker
    tasks = list(enumerate(keys))  # idx 从 0 开始

    print(f"使用 {WORKERS} 个进程并行处理（顺序已随机打乱）")

    done = 0
    with Pool(processes=WORKERS) as pool:
        for _ in pool.imap_unordered(process_one_gz, tasks, chunksize=1):
            done += 1
            if done % 100 == 0 or done == total:
                print(f"[PROGRESS] 已完成 {done}/{total} 个分片")

    print("DONE")


if __name__ == "__main__":
    multiprocessing.set_start_method("spawn")
    main()