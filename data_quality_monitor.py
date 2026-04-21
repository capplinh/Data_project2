import psycopg2
import os
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

CHECKS = [
    {
        "layer": "bronze",
        "table": "bronze.users",
        "checks": {
            "min_rows":        1000,
            "null_col":        "email",
            "max_null_pct":    5.0,
            "duplicate_col":   "user_id",
            "max_dup_pct":     10.0,
        }
    },
    {
        "layer": "bronze",
        "table": "bronze.orders",
        "checks": {
            "min_rows":        5000,
            "null_col":        "order_date",
            "max_null_pct":    5.0,
            "duplicate_col":   "order_id",
            "max_dup_pct":     10.0,
        }
    },
    {
        "layer": "silver",
        "table": "silver.users",
        "checks": {
            "min_rows":        500,
            "null_col":        "email",
            "max_null_pct":    3.0,
            "duplicate_col":   "user_id",
            "max_dup_pct":     0.0,
        }
    },
    {
        "layer": "silver",
        "table": "silver.orders",
        "checks": {
            "min_rows":        4000,
            "null_col":        "order_date",
            "max_null_pct":    2.0,
            "duplicate_col":   "order_id",
            "max_dup_pct":     0.0,
        }
    },
    {
        "layer": "gold",
        "table": "gold.fact_orders",
        "checks": {
            "min_rows":        10000,
            "null_col":        "order_date_key",
            "max_null_pct":    0.0,
            "duplicate_col":   "order_item_sk",
            "max_dup_pct":     0.0,
        }
    },
]

def run_checks(conn, config):
    table   = config["table"]
    checks  = config["checks"]
    results = []
    passed  = 0
    failed  = 0

    with conn.cursor() as cur:
        # 1. Row count
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cur.fetchone()[0]
        min_rows  = checks.get("min_rows", 0)
        ok = row_count >= min_rows
        status = " PASS" if ok else " FAIL"
        results.append(f"  {status}  row_count={row_count:,} (min={min_rows:,})")
        passed += ok; failed += not ok

        # 2. Null check
        null_col = checks.get("null_col")
        if null_col:
            cur.execute(f"""
                SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE {null_col} IS NULL) / COUNT(*), 2)
                FROM {table}
            """)
            null_pct     = float(cur.fetchone()[0] or 0)
            max_null_pct = checks.get("max_null_pct", 5.0)
            ok = null_pct <= max_null_pct
            status = " PASS" if ok else " FAIL"
            results.append(f"  {status}  null_{null_col}={null_pct}% (max={max_null_pct}%)")
            passed += ok; failed += not ok

        # 3. Duplicate check
        dup_col = checks.get("duplicate_col")
        if dup_col:
            cur.execute(f"""
                SELECT ROUND(100.0 * (COUNT(*) - COUNT(DISTINCT {dup_col})) / NULLIF(COUNT(*), 0), 2)
                FROM {table}
            """)
            dup_pct     = float(cur.fetchone()[0] or 0)
            max_dup_pct = checks.get("max_dup_pct", 5.0)
            ok = dup_pct <= max_dup_pct
            status = " PASS" if ok else " FAIL"
            results.append(f"  {status}  dup_{dup_col}={dup_pct}% (max={max_dup_pct}%)")
            passed += ok; failed += not ok

    return results, passed, failed

def main():
    log.info("=" * 50)
    log.info("DATA QUALITY MONITOR")
    log.info(f"Run time: {datetime.now(timezone.utc)}")
    log.info("=" * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    total_passed = 0
    total_failed = 0

    for config in CHECKS:
        log.info(f"\n [{config['layer'].upper()}] {config['table']}")
        results, passed, failed = run_checks(conn, config)
        for r in results:
            log.info(r)
        total_passed += passed
        total_failed += failed

    conn.close()

    log.info("\n" + "=" * 50)
    log.info(f"TỔNG KẾT: {total_passed} PASS  |  {total_failed} FAIL")
    if total_failed > 0:
        log.warning(" CÓ CHECK THẤT BẠI — cần kiểm tra pipeline!")
    else:
        log.info(" TẤT CẢ CHECK PASS — data quality OK!")
    log.info("=" * 50)

if __name__ == "__main__":
    main()