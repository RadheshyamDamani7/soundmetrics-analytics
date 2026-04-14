"""
SoundMetrics — MySQL Loader (FINAL)
Root cause found: float('nan') values in tuples confuse MySQL connector.
Fix: convert every NaN/nan/NA to Python None before insertion.

Run from sql/ folder:
    python 02_load_FINAL.py
"""
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import os, time, math

# ── UPDATE THESE ──────────────────────────────────────────
DB_CONFIG = {
    "host"    : "localhost",
    "port"    : 3306,
    "user"    : "root",    # ← your MySQL username
    "password": "1234",    # ← your MySQL password
    "database": "soundmetrics",
}
CLEAN_DIR = "../data/cleaned"
# ─────────────────────────────────────────────────────────

# Only the 3 tables still missing from MySQL
TABLES = [
    ("subscriptions_clean.csv",   "subscriptions",   ["start_date","end_date"]),
    ("payments_clean.csv",        "payments",        ["payment_date"]),
    ("support_tickets_clean.csv", "support_tickets", ["created_date","resolved_date"]),
]

# Exact columns per table — we only insert these, nothing extra
TABLE_COLUMNS = {
    "subscriptions": [
        "sub_id","user_id","plan_type","previous_plan","status",
        "start_date","end_date","monthly_price","currency","auto_renew"
    ],
    "payments": [
        "payment_id","user_id","amount","currency","payment_date",
        "payment_method","status","promo_code","refund_reason"
    ],
    "support_tickets": [
        "ticket_id","user_id","issue_type","support_channel",
        "created_date","resolved_date","resolution_days",
        "satisfaction_score","is_repeat_contact"
    ],
}

def safe_val(v):
    """
    Convert any value to a MySQL-safe Python type.
    The key fix: float('nan') → None  (not the string 'nan')
    
    MySQL connector handles:
        None        → NULL
        int         → INT
        float       → DECIMAL
        str         → VARCHAR
    It does NOT handle:
        float('nan') → this gets passed as string 'nan' → breaks column lookup
        np.int64     → sometimes causes issues
        np.float64   → sometimes causes issues
    """
    if v is None:
        return None
    # Catch float nan (the root cause of our bug)
    if isinstance(v, float) and math.isnan(v):
        return None
    # Catch numpy nan
    if isinstance(v, float) and not math.isfinite(v):
        return None
    # Convert numpy integer types to plain Python int
    if isinstance(v, (np.integer,)):
        return int(v)
    # Convert numpy float types to plain Python float
    if isinstance(v, (np.floating,)):
        return float(v)
    # pandas NA / NaT
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return v


def connect():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print(f"✅ Connected to MySQL — soundmetrics")
            return conn
    except Error as e:
        print(f"❌ {e}")
        print("   Check your username/password in DB_CONFIG")
    return None


def load_table(conn, csv_file, table_name, date_cols, chunk_size=5000):
    path = os.path.join(CLEAN_DIR, csv_file)
    if not os.path.exists(path):
        print(f"  ⚠️  Not found: {path}")
        return

    expected_cols = TABLE_COLUMNS[table_name]
    total = sum(1 for _ in open(path)) - 1
    print(f"\n  {csv_file} → {table_name}  ({total:,} rows)")

    cursor = conn.cursor()
    done   = 0
    t0     = time.time()

    for chunk in pd.read_csv(path, chunksize=chunk_size,
                              low_memory=False, dtype_backend='numpy_nullable'):

        # Keep only the exact columns the MySQL table expects
        chunk = chunk[[c for c in expected_cols if c in chunk.columns]]

        # Convert date columns to 'YYYY-MM-DD' strings or None
        for col in date_cols:
            if col in chunk.columns:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce')
                chunk[col] = chunk[col].apply(
                    lambda x: x.strftime('%Y-%m-%d')
                    if pd.notna(x) and x is not pd.NaT else None
                )

        # Build rows — applying safe_val to every single cell
        # This is the definitive fix: every value goes through safe_val()
        cols = chunk.columns.tolist()
        data = [
            tuple(safe_val(v) for v in row)
            for row in chunk.itertuples(index=False, name=None)
        ]

        ph  = ', '.join(['%s'] * len(cols))
        cn  = ', '.join([f'`{c}`' for c in cols])
        sql = f"INSERT IGNORE INTO `{table_name}` ({cn}) VALUES ({ph})"

        try:
            cursor.executemany(sql, data)
            conn.commit()
            done += len(chunk)
            pct  = done / total * 100
            print(f"    {done:>8,} / {total:,}  ({pct:.1f}%)"
                  f"  [{time.time()-t0:.1f}s]", end='\r')
        except Error as e:
            print(f"\n  ❌ MySQL Error: {e}")
            # Show first row for debugging
            if data:
                print(f"  First row sample: {data[0]}")
            conn.rollback()
            break

    cursor.close()
    print(f"\n  ✅ {done:,} rows loaded in {time.time()-t0:.1f}s")


def verify(conn):
    print("\n" + "="*52)
    print("  FINAL VERIFICATION")
    print("="*52)
    cur   = conn.cursor()
    total = 0
    all_ok = True
    for t in ["users","subscriptions","listening_events",
              "payments","support_tickets"]:
        cur.execute(f"SELECT COUNT(*) FROM `{t}`")
        n = cur.fetchone()[0]
        total += n
        ok = n > 0
        if not ok:
            all_ok = False
        icon = "✅" if ok else "❌"
        print(f"  {icon} {t:<25} {n:>10,} rows")
    print("-"*52)
    print(f"     {'TOTAL':<25} {total:>10,} rows")
    print("="*52)
    if all_ok:
        print("\n  🎉 All 5 tables loaded! Phase 4 SQL analysis ready.")
        print("  Next → open 03_business_queries.sql in MySQL Workbench")
    else:
        print("\n  ⚠️  Some tables still empty — check errors above")
    cur.close()


if __name__ == "__main__":
    print("="*52)
    print("  SoundMetrics — MySQL Loader FINAL")
    print("="*52)
    conn = connect()
    if not conn:
        exit(1)
    t0 = time.time()
    for csv, tbl, dates in TABLES:
        load_table(conn, csv, tbl, dates)
    verify(conn)
    conn.close()
    print(f"\n  Total time: {time.time()-t0:.1f}s")
