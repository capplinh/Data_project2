import os
import json
import logging
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s"
)
log = logging.getLogger(__name__)

SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB", "erp_source"),
    "user":     os.getenv("PG_USER", "erp_user"),
    "password": os.getenv("PG_PASSWORD", "123456"),
}

# ── Alert rules ──────────────────────────────────────────────
RULES = [
    {
        "name":      "bronze_users_volume",
        "table":     "bronze.users",
        "min_rows":  1000,
        "severity":  "WARNING",
    },
    {
        "name":      "silver_users_null_email",
        "table":     "silver.users",
        "null_col":  "email",
        "max_null_pct": 5.0,
        "severity":  "CRITICAL",
    },
    {
        "name":      "gold_fact_orders_volume",
        "table":     "gold.fact_orders",
        "min_rows":  10000,
        "severity":  "CRITICAL",
    },
    {
        "name":      "pipeline_recent_failure",
        "check_type": "pipeline_log",
        "severity":  "CRITICAL",
    },
]

def send_slack(message, severity="INFO"):
    if not SLACK_WEBHOOK:
        log.warning("SLACK_WEBHOOK_URL chưa set — skip gửi alert")
        return

    emoji = {
        "INFO":     "ℹ️",
        "WARNING":  "⚠️",
        "CRITICAL": "🚨",
    }.get(severity, )

    color = {
        "INFO":     "#36a64f",
        "WARNING":  "#ffcc00",
        "CRITICAL": "#ff0000",
    }.get(severity, "#36a64f")

    payload = {
        "attachments": [{
            "color":  color,
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{emoji} *[{severity}] ERP Pipeline Alert*\n{message}"
                    }
                },
                {
                    "type": "context",
                    "elements": [{
                        "type": "mrkdwn",
                        "text": f" {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Project: Data_project2"
                    }]
                }
            ]
        }]
    }

    try:
        resp = requests.post(SLACK_WEBHOOK, json=payload, timeout=5)
        if resp.status_code == 200:
            log.info(f"   Slack alert sent: [{severity}]")
        else:
            log.error(f"   Slack error: {resp.status_code} — {resp.text}")
    except Exception as e:
        log.error(f"   Slack exception: {e}")

def check_volume(conn, rule):
    table    = rule["table"]
    min_rows = rule.get("min_rows", 0)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
    if count < min_rows:
        msg = f"*Volume Alert* — `{table}`\nRow count = *{count:,}* (min = {min_rows:,})"
        log.warning(f"   {rule['name']}: {count} < {min_rows}")
        send_slack(msg, rule["severity"])
        return False
    log.info(f"   {rule['name']}: {count:,} rows OK")
    return True

def check_null(conn, rule):
    table       = rule["table"]
    null_col    = rule["null_col"]
    max_null_pct = rule.get("max_null_pct", 5.0)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE {null_col} IS NULL) / COUNT(*), 2)
            FROM {table}
        """)
        null_pct = float(cur.fetchone()[0] or 0)
    if null_pct > max_null_pct:
        msg = f"*Null Rate Alert* — `{table}.{null_col}`\nNull = *{null_pct}%* (max = {max_null_pct}%)"
        log.warning(f"   {rule['name']}: null={null_pct}% > {max_null_pct}%")
        send_slack(msg, rule["severity"])
        return False
    log.info(f"   {rule['name']}: null={null_pct}% OK")
    return True

def check_pipeline_log(conn, rule):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*) FROM pipeline_run_log
            WHERE status = 'FAILED'
            AND started_at > NOW() - INTERVAL '24 hours'
        """)
        failed = cur.fetchone()[0]
    if failed > 0:
        msg = f"*Pipeline Failure Alert*\n*{failed}* task(s) FAILED trong 24 giờ qua\nKiểm tra `pipeline_run_log` để debug"
        log.warning(f"   {rule['name']}: {failed} failures trong 24h")
        send_slack(msg, rule["severity"])
        return False
    log.info(f"   {rule['name']}: không có failure trong 24h")
    return True

def run_all_rules():
    log.info("=" * 50)
    log.info("ALERTING MONITOR")
    log.info(f"Run time: {datetime.now(timezone.utc)}")
    log.info("=" * 50)

    conn = psycopg2.connect(**DB_CONFIG)
    passed = failed = 0

    for rule in RULES:
        log.info(f"\n🔍 Checking: {rule['name']}")
        check_type = rule.get("check_type", "")

        try:
            if check_type == "pipeline_log":
                ok = check_pipeline_log(conn, rule)
            elif "null_col" in rule:
                ok = check_null(conn, rule)
            else:
                ok = check_volume(conn, rule)

            passed += ok
            failed += not ok

        except Exception as e:
            log.error(f"   Error checking {rule['name']}: {e}")
            failed += 1

    conn.close()

    log.info("\n" + "=" * 50)
    log.info(f"TỔNG KẾT: {passed} PASS  |  {failed} FAIL")

    if failed > 0:
        summary = f"*Alert Summary*\n{passed} checks passed, *{failed} checks FAILED*\nCần kiểm tra pipeline ngay!"
        send_slack(summary, "CRITICAL")
    else:
        send_slack(" Tất cả checks PASS — pipeline healthy!", "INFO")

    log.info("=" * 50)

if __name__ == "__main__":
    run_all_rules()