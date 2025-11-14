import os
import csv
import json
import gzip
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count

# ========== 配置 ==========
# 目标桶：优先环境变量 MY_BUCKET，没有就用默认
BUCKET = "bucket-openalex"
PREFIX = "openalex/filtered_parquet_full"
ONE_UD = os.environ.get("TEST_UD")  # 若设置，只处理这个 updated_date=YYYY-MM-DD
BATCH = 20000                      # 每个 parquet 里最多多少条记录
WORKERS = 6                     # 多进程 worker 数（m7i.4xlarge 建议 4~6）


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


ALLOW = load_allow()  # 全局，用于 worker


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
    单个进程处理一个 .gz 文件：
    - 从公共 openalex 桶读
    - 过滤命中的 works
    - 按 BATCH 大小写到自己 S3 桶对应 updated_date 分区下
    """
    src = s3fs.S3FileSystem(anon=True)   # 每个进程自己建一套 client，避免跨进程共享
    dst = s3fs.S3FileSystem()            # 用实例角色写

    ud = extract_updated_date_from_path(p)
    buf = []

    def flush():
        if not buf:
            return
        table = pa.Table.from_pylist(buf)
        key = (
            f"s3://{BUCKET}/{PREFIX}/updated_date={ud}/"
            f"part-{os.urandom(4).hex()}.parquet"
        )
        with dst.open(key, "wb") as f:
            pq.write_table(table, f, compression="snappy")
        buf.clear()

    try:
        with src.open(p, "rb") as fin, gzip.open(
            fin, "rt", encoding="utf-8", errors="ignore"
        ) as gz:
            for line in gz:
                try:
                    w = json.loads(line)
                except Exception:
                    # 单行坏掉就丢掉
                    continue

                if any(s in ALLOW for s in source_ids_of_work(w)):
                    buf.append(w)
                    if len(buf) >= BATCH:
                        flush()

    except FileNotFoundError:
        # 对应之前的 NoSuchKey：列到 key 了，但读的时候发现没了
        print(f"[WARN] S3 key 不存在，跳过: {p}")
    except Exception as e:
        print(f"[WARN] 处理 {p} 出错: {e}")

    # 最后一批
    flush()
    return p  # 返回一下方便调试 / 打日志


# ========== 主程序 ==========
def main():
    # 先在主进程里列出所有路径
    src = s3fs.S3FileSystem(anon=True)

    pattern = (
        f"openalex/data/works/{ONE_UD}/*.gz"
        if ONE_UD
        else "openalex/data/works/updated_date=*/**/*.gz"
    )
    paths = list(src.glob(pattern))
    print(f"共发现 {len(paths)} 个 gzip 分片，使用 {WORKERS} 个进程并行处理")

    if not paths:
        print("没有匹配到任何 .gz 文件，退出。")
        return

    # 多进程：每个 worker 处理一部分 paths
    # chunksize=1 保守一点，不追求极限吞吐
    with Pool(processes=WORKERS) as pool:
        for i, p in enumerate(pool.imap_unordered(process_one_gz, paths, chunksize=1), 1):
            if i % 100 == 0 or i == len(paths):
                print(f"[PROGRESS] 已完成 {i}/{len(paths)} 个分片")

    print("DONE")


if __name__ == "__main__":
    main()