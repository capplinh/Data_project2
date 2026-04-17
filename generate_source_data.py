"""
Script: generate_source_data.py
Mục đích: Tạo dữ liệu giả lập cho Source PostgreSQL (đóng vai ERP system).
Bảng: users, products, orders, order_items, transactions

Đặc điểm DỮ LIỆU BẨN có chủ đích (để test Silver layer):
  - ~5%  bản ghi TRÙNG (Duplicate)
  - ~8%  giá trị NULL ở các cột không bắt buộc
  - ~3%  sai định dạng ngày tháng (date format mismatch)
  - ~2%  giá trị âm / bất hợp lý ở amount/price
  - ~1%  email sai format

Yêu cầu:
  pip install faker psycopg2-binary python-dotenv
"""

import os
import random
import string
from datetime import datetime, timedelta, date

import psycopg2
from faker import Faker
from dotenv import load_dotenv

# ─── CONFIG ─────────────────────────────────────────────────────────────────
load_dotenv()

DB_CONFIG = {
    "host":     os.getenv("PG_HOST",     "localhost"),
    "port":     int(os.getenv("PG_PORT", "5433")),
    "dbname":   os.getenv("PG_DB",       "erp_source"),
    "user":     os.getenv("PG_USER",     "erp_user"),
    "password": os.getenv("PG_PASSWORD", "erp_password"),   
}

NUM_USERS        = 2_000
NUM_PRODUCTS     = 500
NUM_ORDERS       = 8_000
NUM_ORDER_ITEMS  = 20_000   # sẽ tạo thêm tự động theo orders
NUM_TRANSACTIONS = 9_000

DUPLICATE_RATE   = 0.05   # 5%  bản ghi bị duplicate
NULL_RATE        = 0.08   # 8%  NULL ở cột optional
BAD_DATE_RATE    = 0.03   # 3%  sai format ngày
NEG_AMOUNT_RATE  = 0.02   # 2%  số âm / bất hợp lý
BAD_EMAIL_RATE   = 0.01   # 1%  email sai format

fake = Faker("vi_VN")
Faker.seed(42)
random.seed(42)

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def maybe_null(value, rate=NULL_RATE):
    """Trả về None với xác suất = rate, ngược lại trả value gốc."""
    return None if random.random() < rate else value


def bad_or_good_date(dt: date, rate=BAD_DATE_RATE):
    """
    Trả về chuỗi ngày: đúng định dạng ISO hoặc cố tình sai format.
    Lý do giữ là string: để minh hoạ lỗi khi nạp vào Postgres TEXT rồi cast.
    """
    if random.random() < rate:
        formats = [
            dt.strftime("%d/%m/%Y"),          # dd/mm/yyyy
            dt.strftime("%m-%d-%Y"),          # mm-dd-yyyy
            dt.strftime("%Y%m%d"),            # yyyymmdd không dấu
            dt.strftime("%d %b %Y"),          # 12 Jan 2024
            "N/A",                            # hoàn toàn rác
        ]
        return random.choice(formats)
    return dt.isoformat()                     # chuẩn ISO


def rand_date(start_year=2022, end_year=2025) -> date:
    start = date(start_year, 1, 1)
    end   = date(end_year, 12, 31)
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def bad_email(email: str) -> str:
    """Biến đổi email thành format sai."""
    mutations = [
        lambda e: e.replace("@", ""),          # bỏ @
        lambda e: e.replace("@", "@@"),         # hai @
        lambda e: e + "..",                     # dấu chấm thừa
        lambda e: e.split("@")[0],              # chỉ local part
        lambda e: "invalid-email",              # rác hoàn toàn
    ]
    return random.choice(mutations)(email)


# ─── DDL ────────────────────────────────────────────────────────────────────

DDL = """
CREATE TABLE IF NOT EXISTS users (
    user_id       SERIAL PRIMARY KEY,
    full_name     TEXT,
    email         TEXT,
    phone         TEXT,
    city          TEXT,
    country       TEXT,
    created_at    TEXT,          -- lưu TEXT để giữ bad-date
    updated_at    TIMESTAMP,
    is_active     BOOLEAN
);

CREATE TABLE IF NOT EXISTS products (
    product_id    SERIAL PRIMARY KEY,
    product_name  TEXT NOT NULL,
    category      TEXT,
    subcategory   TEXT,
    unit_price    NUMERIC(12,2),
    cost_price    NUMERIC(12,2),
    stock_qty     INTEGER,
    supplier      TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    order_id      SERIAL PRIMARY KEY,
    user_id       INTEGER,        -- FK users (không enforce để giữ dirty data)
    order_date    TEXT,           -- TEXT để giữ bad-date
    status        TEXT,
    shipping_city TEXT,
    total_amount  NUMERIC(14,2),
    discount      NUMERIC(5,2),
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS order_items (
    item_id       SERIAL PRIMARY KEY,
    order_id      INTEGER,
    product_id    INTEGER,
    quantity      INTEGER,
    unit_price    NUMERIC(12,2),
    subtotal      NUMERIC(14,2)
);

CREATE TABLE IF NOT EXISTS transactions (
    txn_id        SERIAL PRIMARY KEY,
    order_id      INTEGER,
    user_id       INTEGER,
    txn_date      TEXT,           -- TEXT để giữ bad-date
    amount        NUMERIC(14,2),
    payment_method TEXT,
    status        TEXT,
    currency      TEXT,
    created_at    TIMESTAMP DEFAULT NOW()
);
"""

# ─── DATA GENERATORS ────────────────────────────────────────────────────────

CATEGORIES = {
    "Electronics":   ["Smartphone", "Laptop", "Tablet", "Headphone", "Smartwatch"],
    "Fashion":       ["T-Shirt", "Jeans", "Dress", "Jacket", "Shoes"],
    "Home & Living": ["Chair", "Table", "Lamp", "Curtain", "Pillow"],
    "Beauty":        ["Skincare", "Makeup", "Perfume", "Hair Care", "Nail"],
    "Sports":        ["Gym Equipment", "Running Shoes", "Yoga Mat", "Bicycle", "Racket"],
    "Food & Drink":  ["Coffee", "Tea", "Supplement", "Snack", "Juice"],
}
ORDER_STATUSES    = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
PAYMENT_METHODS   = ["credit_card", "debit_card", "bank_transfer", "e_wallet", "COD"]
TXN_STATUSES      = ["success", "failed", "pending", "refunded"]
CURRENCIES        = ["VND", "USD", "EUR"]
VIETNAM_CITIES    = ["Hồ Chí Minh", "Hà Nội", "Đà Nẵng", "Cần Thơ", "Hải Phòng",
                     "Biên Hoà", "Huế", "Nha Trang", "Buôn Ma Thuột", "Vũng Tàu"]


def gen_users(n: int) -> list[tuple]:
    rows = []
    for _ in range(n):
        email = fake.email()
        if random.random() < BAD_EMAIL_RATE:
            email = bad_email(email)

        created = rand_date(2020, 2023)
        rows.append((
            fake.name(),
            email,
            maybe_null(fake.phone_number()),
            maybe_null(random.choice(VIETNAM_CITIES)),
            maybe_null("Việt Nam"),
            bad_or_good_date(created),
            datetime.now() - timedelta(days=random.randint(0, 365)),
            random.choice([True, False]),
        ))

    # Thêm bản ghi DUPLICATE (~5%)
    dup_count = int(n * DUPLICATE_RATE)
    rows += random.choices(rows, k=dup_count)
    return rows


def gen_products(n: int) -> list[tuple]:
    rows = []
    for _ in range(n):
        cat   = random.choice(list(CATEGORIES.keys()))
        subcat = random.choice(CATEGORIES[cat])
        price  = round(random.uniform(50_000, 50_000_000), 2)
        cost   = round(price * random.uniform(0.4, 0.8), 2)
        # Đôi khi giá âm (dirty)
        if random.random() < NEG_AMOUNT_RATE:
            price = -abs(price)
        rows.append((
            f"{subcat} {fake.word().title()} {random.randint(100,999)}",
            cat,
            maybe_null(subcat),
            price,
            maybe_null(cost),
            random.randint(0, 5000),
            maybe_null(fake.company()),
        ))
    return rows


def gen_orders(n: int, user_ids: list[int]) -> list[tuple]:
    rows = []
    for _ in range(n):
        uid    = random.choice(user_ids)
        odate  = rand_date(2022, 2025)
        amount = round(random.uniform(50_000, 100_000_000), 2)
        if random.random() < NEG_AMOUNT_RATE:
            amount = -abs(amount)
        rows.append((
            maybe_null(uid),
            bad_or_good_date(odate),
            random.choice(ORDER_STATUSES),
            maybe_null(random.choice(VIETNAM_CITIES)),
            amount,
            maybe_null(round(random.uniform(0, 30), 2)),
        ))

    dup_count = int(n * DUPLICATE_RATE)
    rows += random.choices(rows, k=dup_count)
    return rows


def gen_order_items(order_ids: list[int], product_ids: list[int]) -> list[tuple]:
    rows = []
    for oid in order_ids:
        n_items = random.randint(1, 5)
        for _ in range(n_items):
            pid   = random.choice(product_ids)
            qty   = random.randint(1, 20)
            price = round(random.uniform(50_000, 10_000_000), 2)
            sub   = round(qty * price, 2)
            rows.append((oid, pid, qty, price, sub))
    return rows


def gen_transactions(n: int, order_ids: list[int], user_ids: list[int]) -> list[tuple]:
    rows = []
    for _ in range(n):
        oid    = maybe_null(random.choice(order_ids))
        uid    = maybe_null(random.choice(user_ids))
        tdate  = rand_date(2022, 2025)
        amount = round(random.uniform(10_000, 100_000_000), 2)
        if random.random() < NEG_AMOUNT_RATE:
            amount = -abs(amount)
        rows.append((
            oid,
            uid,
            bad_or_good_date(tdate),
            amount,
            random.choice(PAYMENT_METHODS),
            random.choice(TXN_STATUSES),
            random.choice(CURRENCIES),
        ))

    dup_count = int(n * DUPLICATE_RATE)
    rows += random.choices(rows, k=dup_count)
    return rows


# ─── MAIN ────────────────────────────────────────────────────────────────────

def bulk_insert(cur, table: str, columns: list[str], rows: list[tuple]):
    placeholders = ", ".join(["%s"] * len(columns))
    cols         = ", ".join(columns)
    sql          = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
    cur.executemany(sql, rows)
    print(f"  ✓ {table:<15} → {len(rows):>6} bản ghi")


def main():
    print("=" * 55)
    print("  ERP Source Database — Data Generator")
    print("=" * 55)
    print(f"  Host   : {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  DB     : {DB_CONFIG['dbname']}")
    print()

    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    cur  = conn.cursor()

    # ── Tạo schema ──────────────────────────────────────
    print("[1/6] Tạo bảng (nếu chưa có)...")
    cur.execute(DDL)
    conn.commit()

    # ── Truncate để chạy lại clean ───────────────────────
    print("[2/6] Xoá dữ liệu cũ...")
    for tbl in ["transactions", "order_items", "orders", "products", "users"]:
        cur.execute(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE")
    conn.commit()

    # ── Users ────────────────────────────────────────────
    print("[3/6] Nạp USERS...")
    user_rows = gen_users(NUM_USERS)
    bulk_insert(cur, "users",
                ["full_name","email","phone","city","country",
                 "created_at","updated_at","is_active"],
                user_rows)
    conn.commit()

    cur.execute("SELECT user_id FROM users")
    user_ids = [r[0] for r in cur.fetchall()]

    # ── Products ─────────────────────────────────────────
    print("[4/6] Nạp PRODUCTS...")
    prod_rows = gen_products(NUM_PRODUCTS)
    bulk_insert(cur, "products",
                ["product_name","category","subcategory","unit_price",
                 "cost_price","stock_qty","supplier"],
                prod_rows)
    conn.commit()

    cur.execute("SELECT product_id FROM products")
    product_ids = [r[0] for r in cur.fetchall()]

    # ── Orders ───────────────────────────────────────────
    print("[5/6] Nạp ORDERS + ORDER_ITEMS...")
    order_rows = gen_orders(NUM_ORDERS, user_ids)
    bulk_insert(cur, "orders",
                ["user_id","order_date","status","shipping_city",
                 "total_amount","discount"],
                order_rows)
    conn.commit()

    cur.execute("SELECT order_id FROM orders")
    order_ids = [r[0] for r in cur.fetchall()]

    item_rows = gen_order_items(order_ids, product_ids)
    bulk_insert(cur, "order_items",
                ["order_id","product_id","quantity","unit_price","subtotal"],
                item_rows)
    conn.commit()

    # ── Transactions ─────────────────────────────────────
    print("[6/6] Nạp TRANSACTIONS...")
    txn_rows = gen_transactions(NUM_TRANSACTIONS, order_ids, user_ids)
    bulk_insert(cur, "transactions",
                ["order_id","user_id","txn_date","amount",
                 "payment_method","status","currency"],
                txn_rows)
    conn.commit()

    # ── Summary ──────────────────────────────────────────
    print()
    print("─" * 55)
    print("  TỔNG KẾT")
    print("─" * 55)
    for tbl in ["users","products","orders","order_items","transactions"]:
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        count = cur.fetchone()[0]
        print(f"  {tbl:<20} : {count:>7,} bản ghi")

    print()
    print("  DỮ LIỆU BẨN đã nhúng (để test Silver layer):")
    print(f"    • Duplicate        : ~{DUPLICATE_RATE*100:.0f}%")
    print(f"    • NULL values      : ~{NULL_RATE*100:.0f}%  (cột optional)")
    print(f"    • Sai format ngày  : ~{BAD_DATE_RATE*100:.0f}%  (txn_date, order_date, created_at)")
    print(f"    • Giá trị âm       : ~{NEG_AMOUNT_RATE*100:.0f}%  (amount, unit_price)")
    print(f"    • Email sai format : ~{BAD_EMAIL_RATE*100:.0f}%  (users.email)")
    print()
    print("  Xong! Kết nối pipeline vào PostgreSQL để bắt đầu.")
    print("─" * 55)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()