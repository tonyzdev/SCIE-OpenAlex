import os
import csv
import json
import gzip
import s3fs
import pyarrow as pa
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count
import time

# ========== 配置 ==========
# 目标桶：优先环境变量 MY_BUCKET，没有就用默认
BUCKET = "bucket-openalex"
PREFIX = "openalex/filtered_parquet_full"
ONE_UD = os.environ.get("TEST_UD")  # 若设置，只处理这个 updated_date=YYYY-MM-DD
BATCH = 16000                      # 每个 parquet 里最多多少条记录
WORKERS = 5                     # 多进程 worker 数（m7i.4xlarge 建议 4~6）


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
    单个分片的健壮处理：
    - 对 S3 连接问题做 3 次重试（带退避）
    - 所有异常都在本函数内部吞掉，只打印 WARN，绝不让异常冒到主进程
    """
    ud = extract_updated_date_from_path(p)
    buf = []

    def flush(dst_fs):
        if not buf:
            return
        table = pa.Table.from_pylist(buf)
        key = (
            f"s3://{BUCKET}/{PREFIX}/updated_date={ud}/"
            f"part-{os.urandom(4).hex()}.parquet"
        )
        with dst_fs.open(key, "wb") as f:
            pq.write_table(table, f, compression="snappy")
        buf.clear()

    # 最多重试 3 次
    for attempt in range(3):
        try:
            src = s3fs.S3FileSystem(anon=True)
            dst = s3fs.S3FileSystem()

            with src.open(p, "rb") as fin, gzip.open(
                fin, "rt", encoding="utf-8", errors="ignore"
            ) as gz:
                for line in gz:
                    try:
                        w = json.loads(line)
                    except Exception:
                        continue

                    if any(s in ALLOW for s in source_ids_of_work(w)):
                        buf.append(w)
                        if len(buf) >= BATCH:
                            flush(dst)

            # 正常读完就 flush 一下
            flush(dst)
            return p  # 成功，直接返回

        except FileNotFoundError:
            print(f"[WARN] S3 key 不存在，跳过: {p}")
            return p  # 不用重试了，真的没有这个 key

        except Exception as e:
            # 其他异常（包含 ConnectionError 等），打印后重试几次
            print(
                f"[WARN] 读取 {p} 出错，第 {attempt+1}/3 次尝试: {repr(e)}"
            )
            # 如果已经是最后一次尝试，就放弃这个分片
            if attempt == 2:
                print(f"[WARN] 放弃分片 {p}，连续失败 3 次")
                return p
            # 简单退避一下，避免短时间内疯狂重试
            time.sleep(5 * (attempt + 1))

    # 理论上不会跑到这里
    return p


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