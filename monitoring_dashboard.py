import os
import json
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5433)),
    "dbname":   os.getenv("PG_DB", "erp_source"),
    "user":     os.getenv("PG_USER", "erp_user"),
    "password": os.getenv("PG_PASSWORD", "123456"),
}

def get_metrics(conn):
    metrics = {}
    with conn.cursor() as cur:

        # Layer row counts
        cur.execute("""
            SELECT schemaname, tablename,
                (xpath('/row/cnt/text()',
                    query_to_xml('SELECT COUNT(*) as cnt FROM '
                    ||schemaname||'.'||tablename, false, true, ''))
                )[1]::text::int AS row_count
            FROM pg_tables
            WHERE schemaname IN ('bronze','silver','gold')
            ORDER BY schemaname, tablename
        """)
        metrics["tables"] = cur.fetchall()

        # Pipeline run log — last 10 runs
        cur.execute("""
            SELECT batch_id, table_name, status,
                   rows_loaded, duration_sec,
                   started_at
            FROM pipeline_run_log
            ORDER BY log_id DESC
            LIMIT 10
        """)
        metrics["runs"] = cur.fetchall()

        # Watermark
        cur.execute("""
            SELECT table_name, last_loaded_at,
                   last_row_count
            FROM pipeline_watermark
            ORDER BY table_name
        """)
        metrics["watermarks"] = cur.fetchall()

        # Success rate 24h
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'SUCCESS') as success,
                COUNT(*) FILTER (WHERE status IN ('FAILED','ROLLED_BACK')) as failed,
                COUNT(*) as total
            FROM pipeline_run_log
            WHERE started_at > NOW() - INTERVAL '24 hours'
        """)
        metrics["success_rate"] = cur.fetchone()

    return metrics

def generate_html(metrics):
    tables   = metrics["tables"]
    runs     = metrics["runs"]
    wmarks   = metrics["watermarks"]
    sr       = metrics["success_rate"]

    success  = sr[0] or 0
    failed   = sr[1] or 0
    total    = sr[2] or 0
    rate     = round(100 * success / total, 1) if total > 0 else 0

    rate_color = "#27ae60" if rate >= 90 else "#f39c12" if rate >= 70 else "#e74c3c"

    # Table rows
    table_rows = ""
    for schema, tbl, cnt in tables:
        color = {"bronze": "#e67e22", "silver": "#7f8c8d", "gold": "#f1c40f"}.get(schema, "#333")
        table_rows += f"""
        <tr>
            <td><span style="color:{color};font-weight:600">{schema}</span></td>
            <td>{tbl}</td>
            <td style="text-align:right;font-weight:600">{cnt:,}</td>
        </tr>"""

    # Run log rows
    run_rows = ""
    for batch, tbl, status, rows, dur, ts in runs:
        status_color = {
            "SUCCESS":     "#27ae60",
            "FAILED":      "#e74c3c",
            "ROLLED_BACK": "#e67e22",
            "RUNNING":     "#3498db",
        }.get(status, "#333")
        run_rows += f"""
        <tr>
            <td style="font-family:monospace;font-size:12px">{batch}</td>
            <td>{tbl}</td>
            <td><span style="color:{status_color};font-weight:600">{status}</span></td>
            <td style="text-align:right">{rows or 0:,}</td>
            <td style="text-align:right">{dur or 0}s</td>
            <td style="font-size:12px">{str(ts)[:19]}</td>
        </tr>"""

    # Watermark rows
    wmark_rows = ""
    for tbl, last_loaded, last_cnt in wmarks:
        wmark_rows += f"""
        <tr>
            <td>{tbl}</td>
            <td style="font-size:12px">{str(last_loaded)[:19]}</td>
            <td style="text-align:right">{last_cnt or 0:,}</td>
        </tr>"""

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    html = f"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="30">
<title>ERP Pipeline Dashboard</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family: -apple-system, sans-serif; background:#f0f2f5; color:#333; }}
  .header {{ background:#1a1a2e; color:white; padding:20px 32px; display:flex; justify-content:space-between; align-items:center; }}
  .header h1 {{ font-size:20px; font-weight:600; }}
  .header span {{ font-size:13px; opacity:0.7; }}
  .container {{ max-width:1200px; margin:24px auto; padding:0 24px; }}
  .cards {{ display:grid; grid-template-columns:repeat(4,1fr); gap:16px; margin-bottom:24px; }}
  .card {{ background:white; border-radius:12px; padding:20px; box-shadow:0 2px 8px rgba(0,0,0,0.08); }}
  .card .label {{ font-size:12px; color:#888; text-transform:uppercase; letter-spacing:0.5px; }}
  .card .value {{ font-size:28px; font-weight:700; margin:8px 0 4px; }}
  .card .sub {{ font-size:12px; color:#888; }}
  .section {{ background:white; border-radius:12px; padding:24px; margin-bottom:20px; box-shadow:0 2px 8px rgba(0,0,0,0.08); }}
  .section h2 {{ font-size:15px; font-weight:600; margin-bottom:16px; color:#444; }}
  table {{ width:100%; border-collapse:collapse; font-size:13px; }}
  th {{ text-align:left; padding:10px 12px; background:#f8f9fa; color:#666; font-weight:500; border-bottom:2px solid #eee; }}
  td {{ padding:10px 12px; border-bottom:1px solid #f0f0f0; }}
  tr:last-child td {{ border-bottom:none; }}
  tr:hover td {{ background:#f8f9fa; }}
  .badge {{ display:inline-block; padding:2px 8px; border-radius:12px; font-size:11px; font-weight:600; }}
  .refresh {{ font-size:11px; color:#aaa; text-align:right; margin-top:8px; }}
</style>
</head>
<body>
<div class="header">
  <h1> ERP Pipeline Monitoring Dashboard</h1>
  <span>Auto-refresh: 30s | Last updated: {now}</span>
</div>
<div class="container">

  <div class="cards">
    <div class="card">
      <div class="label">Success Rate (24h)</div>
      <div class="value" style="color:{rate_color}">{rate}%</div>
      <div class="sub">{success} success / {total} total runs</div>
    </div>
    <div class="card">
      <div class="label">Failed Runs (24h)</div>
      <div class="value" style="color:{'#e74c3c' if failed>0 else '#27ae60'}">{failed}</div>
      <div class="sub">tasks failed hoặc rolled back</div>
    </div>
    <div class="card">
      <div class="label">Bronze Tables</div>
      <div class="value" style="color:#e67e22">{sum(cnt for s,_,cnt in tables if s=='bronze'):,}</div>
      <div class="sub">total rows across all tables</div>
    </div>
    <div class="card">
      <div class="label">Gold Facts</div>
      <div class="value" style="color:#f1c40f">{sum(cnt for s,t,cnt in tables if s=='gold' and 'fact' in t):,}</div>
      <div class="sub">rows in fact tables</div>
    </div>
  </div>

  <div class="section">
    <h2> Row Count by Layer</h2>
    <table>
      <tr><th>Layer</th><th>Table</th><th style="text-align:right">Row Count</th></tr>
      {table_rows}
    </table>
  </div>

  <div class="section">
    <h2> Pipeline Run Log (last 10)</h2>
    <table>
      <tr><th>Batch ID</th><th>Table</th><th>Status</th><th style="text-align:right">Rows</th><th style="text-align:right">Duration</th><th>Started At</th></tr>
      {run_rows}
    </table>
  </div>

  <div class="section">
    <h2> Incremental Load Watermarks</h2>
    <table>
      <tr><th>Table</th><th>Last Loaded At</th><th style="text-align:right">Last Row Count</th></tr>
      {wmark_rows}
    </table>
  </div>

</div>
</body>
</html>"""
    return html

def main():
    conn = psycopg2.connect(**DB_CONFIG)
    metrics = get_metrics(conn)
    conn.close()

    html = generate_html(metrics)

    output_path = "pipeline_dashboard.html"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f" Dashboard generated: {output_path}")
    print(f"   Mở file trong browser để xem!")

if __name__ == "__main__":
    main()