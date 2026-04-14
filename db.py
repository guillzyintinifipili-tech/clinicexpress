import sqlite3
import os
import pandas as pd
from datetime import date

DB_PATH     = os.path.join(os.path.dirname(__file__), "vetclinic.db")
UPLOADS_DIR = os.path.join(os.path.dirname(__file__), "uploads")


def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    con = get_connection()
    cur = con.cursor()

    # ── Stock Items ─────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id TEXT, stock_barcode TEXT, stock_name TEXT NOT NULL,
            type_name TEXT, drug_type TEXT, qty REAL, unit TEXT,
            qty_cc REAL, cost_price REAL, avg_cost REAL, sell_price REAL,
            warehouse TEXT, vat TEXT, supplier TEXT,
            alert_qty REAL, alert_cc REAL, alert_expire_days REAL, imported_at TEXT
        )
    """)

    # ── Stock Incoming ──────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_incoming (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            receive_date TEXT, po_number TEXT, doc_number TEXT,
            stock_id TEXT, stock_name TEXT, supplier TEXT,
            qty REAL, unit TEXT, unit_price REAL, discount REAL,
            total_amount REAL, lot_no TEXT, manufacture_date TEXT,
            expire_date TEXT, operator TEXT, imported_at TEXT
        )
    """)

    # ── Import Log ──────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS import_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_name TEXT NOT NULL,
            file_type TEXT NOT NULL,
            period_label TEXT,
            period_start TEXT,
            period_end TEXT,
            record_count INTEGER DEFAULT 0,
            imported_at TEXT NOT NULL
        )
    """)

    # ── Financial Periods (one row per PDF) ──────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS financial_periods (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            period_label TEXT,
            total_revenue REAL DEFAULT 0,
            cash_revenue REAL DEFAULT 0,
            transfer_revenue REAL DEFAULT 0,
            total_cost REAL DEFAULT 0,
            gross_profit REAL DEFAULT 0,
            receipts_count INTEGER DEFAULT 0,
            cancelled_count INTEGER DEFAULT 0,
            discount_total REAL DEFAULT 0,
            imported_at TEXT
        )
    """)

    # ── Revenue by Category per Period ──────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS revenue_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            category TEXT NOT NULL,
            amount REAL DEFAULT 0
        )
    """)

    # ── Itemized Sales per Period ────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sales_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER,
            period_start TEXT NOT NULL,
            period_end TEXT NOT NULL,
            category TEXT,
            item_name TEXT,
            qty REAL DEFAULT 0,
            unit TEXT,
            total REAL DEFAULT 0
        )
    """)

    # ── Per-case Transactions (from CSV) ─────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS case_transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            import_id INTEGER,
            tx_date TEXT,
            receipt_no TEXT,
            client_name TEXT,
            pet_name TEXT,
            pet_type TEXT,
            category TEXT,
            item_name TEXT,
            qty REAL DEFAULT 0,
            unit TEXT,
            amount REAL DEFAULT 0,
            discount REAL DEFAULT 0,
            net_amount REAL DEFAULT 0,
            payment_method TEXT
        )
    """)

    con.commit()
    con.close()

    _auto_import_xls()


def _auto_import_xls():
    try:
        import xlrd
        base = os.path.dirname(__file__)
        items_xls    = os.path.join(base, "รายการสินค้าทั้งหมด.xls")
        incoming_xls = os.path.join(base, "การรับสินค้าเข้า Stock.xls")
        con = get_connection()
        cur = con.cursor()
        if os.path.exists(items_xls):
            if cur.execute("SELECT COUNT(*) FROM stock_items").fetchone()[0] == 0:
                import_stock_items(_read_xls_file(items_xls))
        if os.path.exists(incoming_xls):
            if cur.execute("SELECT COUNT(*) FROM stock_incoming").fetchone()[0] == 0:
                import_stock_incoming(_read_xls_file(incoming_xls))
        con.close()
    except Exception:
        pass


def _read_xls_file(path: str) -> pd.DataFrame:
    import xlrd
    wb = xlrd.open_workbook(path)
    sh = wb.sheet_by_index(0)
    rows = [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    hi = 0
    for i, row in enumerate(rows):
        f = str(row[0]).strip()
        if "Stock Id" in f or "วันที่รับ" in f:
            hi = i; break
        if f and not any(f.startswith(x) for x in ["ราย", "ช่วง"]) and f != "":
            if any(str(c).strip() for c in row[1:]):
                hi = i; break
    headers = [str(c).strip() for c in rows[hi]]
    return pd.DataFrame(rows[hi + 1:], columns=headers)


# ── Import Log ────────────────────────────────────────────────────────────────
def log_import(file_name: str, file_type: str, period_label: str,
               period_start: str, period_end: str, record_count: int = 0) -> int:
    con = get_connection()
    cur = con.execute(
        """INSERT INTO import_log
           (file_name,file_type,period_label,period_start,period_end,record_count,imported_at)
           VALUES(?,?,?,?,?,?,?)""",
        (file_name, file_type, period_label, period_start, period_end,
         record_count, date.today().isoformat())
    )
    import_id = cur.lastrowid
    con.commit(); con.close()
    return import_id


def fetch_import_log() -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM import_log ORDER BY imported_at DESC", con)
    con.close()
    return df


# ── Financial Periods ─────────────────────────────────────────────────────────
def save_financial_period(import_id: int, period_start: str, period_end: str,
                           period_label: str, info: dict,
                           summary_df: pd.DataFrame, items_df: pd.DataFrame):
    now = date.today().isoformat()
    con = get_connection()
    # Remove old data for same period
    con.execute("DELETE FROM financial_periods WHERE period_start=? AND period_end=?",
                (period_start, period_end))
    con.execute("DELETE FROM revenue_categories WHERE period_start=? AND period_end=?",
                (period_start, period_end))
    con.execute("DELETE FROM sales_items WHERE period_start=? AND period_end=?",
                (period_start, period_end))

    con.execute("""
        INSERT INTO financial_periods
        (import_id,period_start,period_end,period_label,total_revenue,cash_revenue,
         transfer_revenue,total_cost,gross_profit,receipts_count,cancelled_count,imported_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (import_id, period_start, period_end, period_label,
         info.get("total", 0), info.get("cash", 0), info.get("transfer", 0),
         info.get("cost", 0), info.get("gross_profit", 0),
         info.get("receipts", 0), info.get("cancelled", 0), now))

    for _, row in summary_df.iterrows():
        con.execute(
            "INSERT INTO revenue_categories(import_id,period_start,period_end,category,amount) VALUES(?,?,?,?,?)",
            (import_id, period_start, period_end, row["category"], row["amount"]))

    for _, row in items_df.iterrows():
        con.execute(
            "INSERT INTO sales_items(import_id,period_start,period_end,category,item_name,qty,unit,total) VALUES(?,?,?,?,?,?,?,?)",
            (import_id, period_start, period_end,
             row.get("category", ""), row.get("item_name", ""),
             row.get("qty", 0), row.get("unit", ""), row.get("total", 0)))

    con.commit(); con.close()


def fetch_financial_periods(start: str = None, end: str = None) -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM financial_periods ORDER BY period_start", con)
    con.close()
    if start: df = df[df["period_start"] >= start]
    if end:   df = df[df["period_end"]   <= end]
    return df


def fetch_revenue_categories(start: str = None, end: str = None) -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM revenue_categories", con)
    con.close()
    if not df.empty:
        if start: df = df[df["period_start"] >= start]
        if end:   df = df[df["period_end"]   <= end]
    return df


def fetch_sales_items(start: str = None, end: str = None) -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM sales_items", con)
    con.close()
    if not df.empty:
        if start: df = df[df["period_start"] >= start]
        if end:   df = df[df["period_end"]   <= end]
    return df


def delete_financial_period(import_id: int):
    con = get_connection()
    fp = pd.read_sql_query(
        "SELECT period_start,period_end FROM financial_periods WHERE import_id=?",
        con, params=(import_id,))
    if not fp.empty:
        ps, pe = fp.iloc[0]["period_start"], fp.iloc[0]["period_end"]
        con.execute("DELETE FROM financial_periods WHERE import_id=?", (import_id,))
        con.execute("DELETE FROM revenue_categories WHERE period_start=? AND period_end=?", (ps, pe))
        con.execute("DELETE FROM sales_items WHERE period_start=? AND period_end=?", (ps, pe))
    con.execute("DELETE FROM import_log WHERE id=?", (import_id,))
    con.commit(); con.close()


# ── Case Transactions ─────────────────────────────────────────────────────────
def fetch_case_transactions(start: str = None, end: str = None) -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM case_transactions ORDER BY tx_date DESC", con)
    con.close()
    if not df.empty:
        if start: df = df[df["tx_date"] >= start]
        if end:   df = df[df["tx_date"] <= end]
    return df


def import_case_transactions(df: pd.DataFrame, import_id: int):
    col_map = {
        "วันที่": "tx_date", "เลขใบเสร็จ": "receipt_no",
        "ชื่อเจ้าของ": "client_name", "ชื่อสัตว์": "pet_name",
        "ประเภทสัตว์": "pet_type", "หมวดหมู่": "category",
        "บริการ/สินค้า": "item_name", "จำนวน": "qty", "หน่วย": "unit",
        "จำนวนเงิน": "amount", "ส่วนลด": "discount",
        "ยอดสุทธิ": "net_amount", "วิธีชำระ": "payment_method",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "import_id":      import_id,
            "tx_date":        str(r.get("tx_date", "") or ""),
            "receipt_no":     str(r.get("receipt_no", "") or ""),
            "client_name":    str(r.get("client_name", "") or ""),
            "pet_name":       str(r.get("pet_name", "") or ""),
            "pet_type":       str(r.get("pet_type", "") or ""),
            "category":       str(r.get("category", "") or ""),
            "item_name":      str(r.get("item_name", "") or ""),
            "qty":            _to_float(r.get("qty", 1)),
            "unit":           str(r.get("unit", "") or ""),
            "amount":         _to_float(r.get("amount", 0)),
            "discount":       _to_float(r.get("discount", 0)),
            "net_amount":     _to_float(r.get("net_amount", 0)),
            "payment_method": str(r.get("payment_method", "") or ""),
        })
    con = get_connection()
    pd.DataFrame(rows).to_sql("case_transactions", con, if_exists="append", index=False)
    con.commit(); con.close()


# ── Stock Items ───────────────────────────────────────────────────────────────
def import_stock_items(df: pd.DataFrame):
    now = date.today().isoformat()
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "stock_id":          str(r.get("Stock Id", "") or ""),
            "stock_barcode":     str(r.get("Stock Barcode", "") or ""),
            "stock_name":        str(r.get("Stock Name", "") or ""),
            "type_name":         str(r.get("Type Name", "") or ""),
            "drug_type":         str(r.get("Drug Type", "") or ""),
            "qty":               _to_float(r.get("QTY")),
            "unit":              str(r.get("หน่วย", "") or ""),
            "qty_cc":            _to_float(r.get("จำนวน cc")),
            "cost_price":        _to_float(r.get("ราคาทุน")),
            "avg_cost":          _to_float(r.get("ราคาทุนเฉลี่ย")),
            "sell_price":        _to_float(r.get("ราคาขาย")),
            "warehouse":         str(r.get("คลัง", "") or ""),
            "vat":               str(r.get("VAT", "") or ""),
            "supplier":          str(r.get("ซื้อกับใคร(บ่อยสุด)", "") or ""),
            "alert_qty":         _to_float(r.get("แจ้งเตือนเมื่อเหลือ")),
            "alert_cc":          _to_float(r.get("แจ้งเตือนเมื่อเหลือกี่ cc")),
            "alert_expire_days": _to_float(r.get("แจ้งเตือนก่อนหมดอายุ(ต่อล็อต)")),
            "imported_at":       now,
        })
    con = get_connection()
    con.execute("DELETE FROM stock_items")
    pd.DataFrame(rows).to_sql("stock_items", con, if_exists="append", index=False)
    con.commit(); con.close()


def fetch_stock_items() -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM stock_items ORDER BY stock_name", con)
    con.close()
    return df


# ── Stock Incoming ────────────────────────────────────────────────────────────
def import_stock_incoming(df: pd.DataFrame):
    now = date.today().isoformat()
    col_map = {
        "วันที่รับสินค้าเข้า": "receive_date", "เลขที่ใบสั่งซื้อ": "po_number",
        "เลขที่เอกสาร": "doc_number",          "รหัสสินค้า": "stock_id",
        "ชื่อสินค้า": "stock_name",            "ตัวแทนจำหน่าย": "supplier",
        "จำนวน": "qty",                        "หน่วย": "unit",
        "ราคา/หน่วย": "unit_price",            "ส่วนลด": "discount",
        "จำนวนเงิน": "total_amount",           "Lot No.": "lot_no",
        "วันที่ผลิต": "manufacture_date",      "วันหมดอายุ": "expire_date",
        "ผู้ทำรายการ": "operator",
    }
    df = df.rename(columns=col_map)
    rows = []
    for _, r in df.iterrows():
        rows.append({
            "receive_date":     str(r.get("receive_date", "") or ""),
            "po_number":        str(r.get("po_number", "") or ""),
            "doc_number":       str(r.get("doc_number", "") or ""),
            "stock_id":         str(r.get("stock_id", "") or ""),
            "stock_name":       str(r.get("stock_name", "") or ""),
            "supplier":         str(r.get("supplier", "") or ""),
            "qty":              _to_float(r.get("qty")),
            "unit":             str(r.get("unit", "") or ""),
            "unit_price":       _to_float(r.get("unit_price")),
            "discount":         _to_float(r.get("discount")),
            "total_amount":     _to_float(r.get("total_amount")),
            "lot_no":           str(r.get("lot_no", "") or ""),
            "manufacture_date": str(r.get("manufacture_date", "") or ""),
            "expire_date":      str(r.get("expire_date", "") or ""),
            "operator":         str(r.get("operator", "") or ""),
            "imported_at":      now,
        })
    con = get_connection()
    con.execute("DELETE FROM stock_incoming")
    pd.DataFrame(rows).to_sql("stock_incoming", con, if_exists="append", index=False)
    con.commit(); con.close()


def fetch_stock_incoming() -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM stock_incoming ORDER BY receive_date DESC", con)
    con.close()
    return df


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0
