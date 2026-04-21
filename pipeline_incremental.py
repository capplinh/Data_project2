import psycopg2
import os
import uuid
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB", "erp_source"),
    "user":     os.getenv("PG_USER", "erp_user"),
    "password": os.getenv("PG_PASSWORD", "123456"),
}

TABLES = [
    "public.users",
    "public.orders",
    "public.products",
    "public.order_items",
    "public.transactions",
]

def get_watermark(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("SELECT last_loaded_at FROM pipeline_watermark WHERE table_name = %s", (table_name,))
        row = cur.fetchone()
        return row[0] if row else None

def update_watermark(conn, table_name, batch_id, row_count):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_watermark
            SET last_loaded_at = NOW(),
                last_batch_id  = %s,
                last_row_count = %s,
                updated_at     = NOW()
            WHERE table_name = %s
        """, (batch_id, row_count, table_name))
    conn.commit()
def log_run_start(conn, batch_id, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO pipeline_run_log (batch_id, table_name, status)
            VALUES (%s, %s, 'RUNNING')
            RETURNING log_id
        """, (batch_id, table_name))
        log_id = cur.fetchone()[0]
    conn.commit()
    return log_id

def log_run_finish(conn, log_id, status, rows_loaded=0, error_message=None):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE pipeline_run_log
            SET status        = %s,
                rows_loaded   = %s,
                error_message = %s,
                finished_at   = NOW(),
                duration_sec  = EXTRACT(EPOCH FROM (NOW() - started_at))
            WHERE log_id = %s
        """, (status, rows_loaded, error_message, log_id))
    conn.commit()

def rollback_batch(conn, table_name, batch_id):
    schema, tbl = table_name.split(".")
    bronze_table = f"bronze.{tbl}"
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {bronze_table} WHERE _batch_id = %s", (batch_id,))
        deleted = cur.rowcount
    conn.commit()
    log.warning(f"  ROLLBACK: đã xóa {deleted} rows của batch {batch_id} từ {bronze_table}")
    return deleted

def incremental_load_table(conn, table_name, watermark, batch_id, ingested_at):
    schema, tbl = table_name.split(".")
    bronze_table = f"bronze.{tbl}"

    # Kiểm tra bảng có cột updated_at không
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            AND column_name IN ('updated_at', 'created_at')
            ORDER BY column_name DESC LIMIT 1
        """, (schema, tbl))
        col = cur.fetchone()

    if not col:
        log.warning(f"  {table_name}: không có updated_at/created_at — skip incremental, dùng full load")
        ts_col = None
    else:
        ts_col = col[0]

    with conn.cursor() as cur:
        if ts_col:
            query = f"""
                SELECT * FROM {table_name}
                WHERE {ts_col} > %s::timestamptz
                ORDER BY {ts_col}
            """
            cur.execute(query, (watermark,))
        else:
            cur.execute(f"SELECT * FROM {table_name}")

        # Fetch 1 batch trước để có description
        batch = cur.fetchmany(1000)
        if not batch:
            log.info(f"  ✓ {table_name}: 0 rows mới (đã up to date)")
            return 0

        cols = [desc[0] for desc in cur.description]
        bronze_cols = cols + ["_ingested_at", "_source_table", "_batch_id"]
        placeholders = ", ".join(["%s"] * len(bronze_cols))
        insert_sql = f"""
            INSERT INTO {bronze_table} ({", ".join(bronze_cols)})
            VALUES ({placeholders})
        """

        rows_loaded = 0
        batch = cur.fetchmany(1000)
        with conn.cursor() as ins:
            while batch:
                data = [
                    tuple(row) + (ingested_at, table_name, batch_id)
                    for row in batch
                ]
                ins.executemany(insert_sql, data)
                rows_loaded += len(batch)
                batch = cur.fetchmany(1000)

        conn.commit()
        log.info(f"  ✓ {table_name}: {rows_loaded} rows mới loaded vào {bronze_table}")
        return rows_loaded

def main():
    batch_id    = uuid.uuid4().hex[:8].upper()
    ingested_at = datetime.now(timezone.utc)

    log.info("=" * 50)
    log.info("INCREMENTAL BRONZE PIPELINE")
    log.info(f"Batch ID    : {batch_id}")
    log.info(f"Ingested at : {ingested_at}")
    log.info("=" * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    total = 0

    for table in TABLES:
        watermark = get_watermark(conn, table)
        log_id    = log_run_start(conn, batch_id, table)

        retry     = 0
        max_retry = 3
        success   = False

        while retry < max_retry and not success:
            try:
                rows    = incremental_load_table(conn, table, watermark, batch_id, ingested_at)
                update_watermark(conn, table, batch_id, rows)
                log_run_finish(conn, log_id, "SUCCESS", rows)
                total  += rows
                success = True

            except Exception as e:
                retry += 1
                log.error(f"  {table} — lỗi lần {retry}/{max_retry}: {e}")
                try:
                    rollback_batch(conn, table, batch_id)
                    log_run_finish(conn, log_id, "ROLLED_BACK", error_message=str(e))
                except Exception as rb_err:
                    log.error(f"  Rollback thất bại: {rb_err}")
                    log_run_finish(conn, log_id, "FAILED", error_message=str(e))

                if retry < max_retry:
                    log.info(f"  Retry sau 2 giây...")
                    import time; time.sleep(2)

        if not success:
            log.error(f"   {table} FAILED sau {max_retry} lần retry — bỏ qua, tiếp tục bảng khác")

if __name__ == "__main__":
    main()
