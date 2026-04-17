"""
Script : pipeline_bronze.py
Tầng   : BRONZE LAYER  — Extract & Load (KHÔNG transform)
Luồng  : Source PostgreSQL (erp_source)  ──►  Bronze Schema (bronze.*)

Nguyên tắc Bronze:
  - Giữ 100% dữ liệu gốc, kể cả data bẩn
  - Chỉ thêm 3 cột metadata: _ingested_at, _source_table, _batch_id
  - Mỗi lần chạy = 1 batch mới (append, KHÔNG overwrite)
  - Có log rõ ràng để trace lại sau này

Cài thư viện:
  pip install psycopg2-binary python-dotenv tabulate
"""

import os
import uuid
import logging
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from tabulate import tabulate

# ─── LOGGING ─────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bronze_pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("bronze")

# ─── CONFIG ──────────────────────────────────────────────────────────────────

load_dotenv()

# Source = ERP PostgreSQL (container Docker)
SOURCE_CFG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5433")),
    "dbname":   os.getenv("PG_DB",       "erp_source"),
    "user":     os.getenv("PG_USER",     "erp_user"),
    "password": os.getenv("PG_PASSWORD", "erp_password"),
}

# Target = cùng container, nhưng schema "bronze"
# (Nếu muốn tách DB riêng thì đổi dbname thành "erp_warehouse")
TARGET_CFG = SOURCE_CFG.copy()

# Danh sách bảng cần ingest: (source_table, bronze_table)
TABLES = [
    ("public.users",        "bronze.users"),
    ("public.products",     "bronze.products"),
    ("public.orders",       "bronze.orders"),
    ("public.order_items",  "bronze.order_items"),
    ("public.transactions", "bronze.transactions"),
]

BATCH_SIZE = 1_000   # số row fetch mỗi lần (tránh OOM với bảng lớn)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def get_conn(cfg: dict, label: str) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False
    log.info(f"[{label}] Kết nối thành công → {cfg['host']}:{cfg['port']}/{cfg['dbname']}")
    return conn


def get_source_columns(src_cur, source_table: str) -> list[str]:
    """Lấy danh sách cột của bảng nguồn."""
    schema, table = source_table.split(".")
    src_cur.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
    """, (schema, table))
    return [r[0] for r in src_cur.fetchall()]


def create_bronze_schema(tgt_cur):
    """Tạo schema bronze nếu chưa có."""
    tgt_cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    log.info("Schema 'bronze' đã sẵn sàng.")


def create_bronze_table(tgt_cur, source_table: str, bronze_table: str, src_columns: list[str]):
    """
    Tạo bảng bronze với:
      - Tất cả cột gốc kiểu TEXT (giữ nguyên, không ép kiểu)
      - 3 cột metadata
    Lý do dùng TEXT: Bronze không được transform → giữ nguyên mọi giá trị gốc,
    kể cả ngày sai format, số âm, email lỗi.
    """
    col_defs = ",\n    ".join([f'"{col}" TEXT' for col in src_columns])
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {bronze_table} (
        _bronze_id      BIGSERIAL PRIMARY KEY,
        _ingested_at    TIMESTAMP WITH TIME ZONE NOT NULL,
        _source_table   TEXT NOT NULL,
        _batch_id       TEXT NOT NULL,
        {col_defs}
    )
    """
    tgt_cur.execute(ddl)
    log.info(f"  Bảng {bronze_table} sẵn sàng ({len(src_columns)} cột gốc + 3 metadata).")


def extract_and_load(
    src_conn, tgt_conn,
    source_table: str, bronze_table: str,
    src_columns: list[str],
    batch_id: str,
    ingested_at: datetime,
) -> int:
    """
    Extract từng chunk từ source → Load vào bronze.
    Trả về tổng số row đã load.
    """
    src_cur = src_conn.cursor(name=f"cur_{source_table.replace('.','_')}")  # server-side cursor
    tgt_cur = tgt_conn.cursor()

    # Build INSERT sql
    meta_cols    = ["_ingested_at", "_source_table", "_batch_id"]
    all_cols     = meta_cols + [f'"{c}"' for c in src_columns]
    placeholders = ", ".join(["%s"] * len(all_cols))
    insert_sql   = f"INSERT INTO {bronze_table} ({', '.join(all_cols)}) VALUES ({placeholders})"

    # Extract
    src_cur.execute(f"SELECT * FROM {source_table}")

    total = 0
    while True:
        rows = src_cur.fetchmany(BATCH_SIZE)
        if not rows:
            break

        # Prepend metadata vào mỗi row, ép toàn bộ giá trị sang str (giữ nguyên bẩn)
        bronze_rows = [
            (ingested_at, source_table, batch_id) + tuple(
                str(v) if v is not None else None for v in row
            )
            for row in rows
        ]

        psycopg2.extras.execute_batch(tgt_cur, insert_sql, bronze_rows, page_size=500)
        total += len(rows)

    tgt_conn.commit()
    src_cur.close()
    tgt_cur.close()
    return total


def log_batch_summary(bronze_table: str, row_count: int, duration_s: float):
    log.info(f"  ✓ {bronze_table:<30}  {row_count:>7,} rows  ({duration_s:.2f}s)")


def get_row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    return cur.fetchone()[0]

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    run_start   = datetime.now()
    batch_id    = str(uuid.uuid4())[:8].upper()   # VD: "A3F9C12B"
    ingested_at = datetime.now(tz=timezone.utc)

    log.info("=" * 60)
    log.info("  BRONZE PIPELINE — BẮT ĐẦU")
    log.info("=" * 60)
    log.info(f"  Batch ID     : {batch_id}")
    log.info(f"  Ingested at  : {ingested_at.isoformat()}")
    log.info(f"  Tables       : {len(TABLES)}")
    log.info("")

    src_conn = get_conn(SOURCE_CFG, "SOURCE")
    tgt_conn = get_conn(TARGET_CFG, "TARGET")

    src_cur = src_conn.cursor()
    tgt_cur = tgt_conn.cursor()

    # Tạo schema bronze
    create_bronze_schema(tgt_cur)
    tgt_conn.commit()

    summary = []

    for source_table, bronze_table in TABLES:
        log.info(f"[EXTRACT] {source_table}  →  {bronze_table}")
        t0 = datetime.now()

        # Lấy cột nguồn
        src_columns = get_source_columns(src_cur, source_table)

        # Tạo bảng bronze (nếu chưa có)
        create_bronze_table(tgt_cur, source_table, bronze_table, src_columns)
        tgt_conn.commit()

        # Extract & Load
        row_count = extract_and_load(
            src_conn, tgt_conn,
            source_table, bronze_table,
            src_columns, batch_id, ingested_at,
        )

        duration = (datetime.now() - t0).total_seconds()
        log_batch_summary(bronze_table, row_count, duration)

        # Đếm tổng trong bronze (bao gồm các batch trước)
        total_in_bronze = get_row_count(tgt_cur, bronze_table)

        summary.append({
            "Source Table"  : source_table,
            "Bronze Table"  : bronze_table,
            "Batch Rows"    : f"{row_count:,}",
            "Total in Bronze": f"{total_in_bronze:,}",
            "Duration (s)"  : f"{duration:.2f}",
        })

    src_cur.close()
    tgt_cur.close()
    src_conn.close()
    tgt_conn.close()

    total_duration = (datetime.now() - run_start).total_seconds()

    # ── Summary Table ──────────────────────────────────────
    log.info("")
    log.info("=" * 60)
    log.info("  BRONZE PIPELINE — KẾT QUẢ")
    log.info("=" * 60)
    log.info(f"  Batch ID   : {batch_id}")
    log.info(f"  Total time : {total_duration:.2f}s")
    log.info("")

    headers = list(summary[0].keys())
    rows    = [list(r.values()) for r in summary]
    print(tabulate(rows, headers=headers, tablefmt="rounded_outline"))

    log.info("")
    log.info("  Nguyên tắc Bronze đã giữ:")
    log.info("    ✓ 100% data gốc — kể cả duplicate, null, ngày sai format")
    log.info("    ✓ Tất cả cột ép sang TEXT (không transform kiểu dữ liệu)")
    log.info("    ✓ Mỗi row có _batch_id để trace nguồn gốc")
    log.info("    ✓ _ingested_at ghi giờ UTC chuẩn xác")
    log.info("")
    log.info("  → Chạy silver_transform.sql để bắt đầu làm sạch dữ liệu.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()