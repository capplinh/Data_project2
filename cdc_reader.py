import os
import json
import logging
from datetime import datetime
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s"
)
log = logging.getLogger(__name__)

MYSQL_CONFIG = {
    "host":   "127.0.0.1",
    "port":   3306,
    "user":   "root",
    "passwd": os.getenv("MYSQL_PASSWORD", ""),
}

WATCH_TABLES = {
    "bronze": ["crm_cust_info", "crm_sales_details", "cdc_test"],
}

def format_event(event_type, schema, table, row):
    return {
        "event_type":  event_type,
        "schema":      schema,
        "table":       table,
        "timestamp":   datetime.now().isoformat(),
        "data":        row,
    }

def process_event(event):
    schema = event.schema
    table  = event.table

    if schema not in WATCH_TABLES:
        return
    if table not in WATCH_TABLES[schema]:
        return

    if isinstance(event, WriteRowsEvent):
        for row in event.rows:
            e = format_event("INSERT", schema, table, row["values"])
            log.info(f"  INSERT  {schema}.{table}: {json.dumps(e['data'], default=str)}")

    elif isinstance(event, UpdateRowsEvent):
        for row in event.rows:
            e = format_event("UPDATE", schema, table, {
                "before": row["before_values"],
                "after":  row["after_values"],
            })
            log.info(f"   UPDATE  {schema}.{table}:")
            log.info(f"      BEFORE: {json.dumps(row['before_values'], default=str)}")
            log.info(f"      AFTER : {json.dumps(row['after_values'], default=str)}")

    elif isinstance(event, DeleteRowsEvent):
        for row in event.rows:
            e = format_event("DELETE", schema, table, row["values"])
            log.info(f"   DELETE  {schema}.{table}: {json.dumps(e['data'], default=str)}")

def main():
    log.info("=" * 50)
    log.info("CDC READER — MySQL Binlog")
    log.info(f"Watching: {WATCH_TABLES}")
    log.info("=" * 50)

    stream = BinLogStreamReader(
    connection_settings=MYSQL_CONFIG,
    server_id=100,
    only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
    log_file="binlog.378104",
    log_pos=656747,
    resume_stream=True,
    blocking=True,
    only_schemas=["bronze"],      # ← thêm dòng này
)

    log.info("Đang lắng nghe binlog — thực hiện thay đổi trên MySQL để thấy CDC events...")
    log.info("Ctrl+C để dừng\n")

    try:
        for event in stream:
            process_event(event)
    except KeyboardInterrupt:
        log.info("\nCDC Reader dừng.")
    finally:
        stream.close()

if __name__ == "__main__":
    main()