import s3fs, gzip, json, os
import pyarrow as pa, pyarrow.parquet as pq
import csv
import logging
import gc
from datetime import datetime, timezone, timedelta

# 配置日志：强制使用东八区时间
class BeijingFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        # 强制转换为东八区时间（UTC+8）
        dt = datetime.fromtimestamp(record.created, tz=timezone(timedelta(hours=8)))
        if datefmt:
            return dt.strftime(datefmt)
        else:
            return dt.strftime('%Y-%m-%d %H:%M:%S')

# 配置日志格式和处理器
handler = logging.StreamHandler()
handler.setFormatter(BeijingFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

# 读取白名单 source_id
allow = set()
with open("journal_id_map.csv", newline="", encoding="utf-8") as f:
    rd = csv.DictReader(f)
    for r in rd:
        if r["source_id"]:
            allow.add(r["source_id"])

if not allow:
    logger.error("journal_id_map.csv 没有有效的 source_id")
    raise SystemExit("journal_id_map.csv 没有有效的 source_id")

logger.info(f"成功加载 {len(allow)} 个白名单 source_id")

src = s3fs.S3FileSystem(anon=True)    # 公共桶读
dst = s3fs.S3FileSystem()              # 你自己的桶写（走实例角色）

BUCKET = "bucket-openalex"
PREFIX = "openalex/filtered_parquet_full_3"
ONE_UD = os.environ.get("TEST_UD")     # 若设置，只处理这个 updated_date=YYYY-MM-DD

if not BUCKET:
    logger.error("环境变量 MY_BUCKET 未设置")
    raise SystemExit("环境变量 MY_BUCKET 未设置")

logger.info(f"目标桶: {BUCKET}/{PREFIX}")

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
    try:
        logger.info(f"[准备写入] {len(batch)} 条记录")
        table = pa.Table.from_pylist(batch)
        logger.info(f"[已构建Table] 内存大小约 {table.nbytes / 1024 / 1024:.2f} MB")
        
        key = f"s3://{BUCKET}/{PREFIX}/updated_date={updated_date}/part-{os.urandom(4).hex()}.parquet"
        logger.info(f"[开始上传] -> {key}")
        
        with dst.open(key, "wb") as f:
            pq.write_table(table, f, compression="snappy")
        
        logger.info(f"[写入成功] {len(batch)} 条记录")
        batch.clear()
    except Exception as e:
        logger.error(f"[写入失败] 错误: {e}")
        raise

# 遍历公开桶 works 分片
pattern = f"openalex/data/works/{ONE_UD}/*.gz" if ONE_UD else "openalex/data/works/updated_date=*/**/*.gz"
paths = src.glob(pattern)

logger.info(f"开始扫描文件，模式: {pattern}")
paths = list(paths)
logger.info(f"找到 {len(paths)} 个文件待处理")

BATCH = 8000
current_ud = None
buf = []
processed_files = 0

for p in paths:
    processed_files += 1
    if processed_files % 10 == 0:
        logger.info(f"处理进度: {processed_files}/{len(paths)} 文件")
    
    ud = next((seg.split("=")[1] for seg in p.split("/") if seg.startswith("updated_date=")), "unknown")
    if current_ud is None: current_ud = ud
    elif ud != current_ud:
        write_batch(buf, current_ud)
        current_ud = ud
    
    # 开始处理文件的心跳日志
    logger.info(f"[开始] 文件 {processed_files}/{len(paths)}: {p}")
    
    line_count = 0
    matched_count = 0
    
    try:
        with src.open(p, "rb") as fin, gzip.open(fin, "rt", encoding="utf-8", errors="ignore") as gz:
            for line in gz:
                line_count += 1
                # 每处理10000行输出一次心跳日志
                if line_count % 10000 == 0:
                    logger.info(f"  [心跳] 文件 {processed_files}: 已读取 {line_count} 行，匹配 {matched_count} 条，缓冲区 {len(buf)} 条")
                
                try:
                    w = json.loads(line)
                except:
                    continue
                if any(sid in allow for sid in source_ids_of_work(w)):
                    matched_count += 1
                    buf.append({
                        "id": w.get("id"),
                        "doi": w.get("doi"),
                        "title": w.get("title"),
                        "publication_year": w.get("publication_year"),
                        "publication_date": w.get("publication_date"),
                        "type": w.get("type"),
                        "abstract_inverted_index": w.get("abstract_inverted_index"),
                        "is_corr_author": any(a.get("is_corresponding") for a in (w.get("authorships") or [])),
                        "journal_id": ((w.get("primary_location") or {}).get("source") or {}).get("id"),
                        "cited_by_count": w.get("cited_by_count"),
                        "referenced_works_count": w.get("referenced_works_count"),
                        "open_access": w.get("open_access"),
                        "authorships": w.get("authorships"),
                        "primary_location": w.get("primary_location"),
                        "referenced_works": w.get("referenced_works"),
                        "topics": w.get("topics"),
                    })
                    if len(buf) >= BATCH:
                        write_batch(buf, ud)
                        gc.collect()  # 强制垃圾回收
    except Exception as e:
        logger.error(f"[文件处理异常] 文件 {p}: {e}")
        # 如果有未写入的数据，尝试写入
        if buf:
            try:
                write_batch(buf, ud)
            except:
                pass
        raise
    
    # 文件处理完成的日志
    logger.info(f"[完成] 文件 {processed_files}: 共读取 {line_count} 行，匹配 {matched_count} 条，当前缓冲区 {len(buf)} 条")
    
    # 每个文件处理完后清理内存
    gc.collect()

# 写入剩余数据
logger.info(f"[最终写入] 剩余缓冲区 {len(buf)} 条")
write_batch(buf, current_ud)
logger.info(f"处理完成！共处理 {processed_files} 个文件")
logger.info("DONE")