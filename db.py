import sqlite3
import os
import pandas as pd
from datetime import date, timedelta
import random

DB_PATH    = os.path.join(os.path.dirname(__file__), "vetclinic.db")
UPLOADS_DIR = os.path.join(os.path.dirname(__file__), "uploads")


def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    os.makedirs(UPLOADS_DIR, exist_ok=True)
    con = get_connection()
    cur = con.cursor()

    # ── Transactions ────────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_date TEXT NOT NULL,
            transaction_type TEXT NOT NULL CHECK(transaction_type IN ('รายรับ','รายจ่าย')),
            category TEXT NOT NULL,
            client_name TEXT NOT NULL,
            pet_name TEXT,
            amount REAL NOT NULL,
            tax_deduction REAL NOT NULL DEFAULT 0,
            net_amount REAL NOT NULL,
            payment_status TEXT NOT NULL CHECK(payment_status IN ('ชำระแล้ว','รอชำระ','เกินกำหนด','ผ่อนชำระ')),
            note TEXT,
            receipt_file_path TEXT
        )
    """)

    # ── Stock Items (full inventory) ────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_id TEXT,
            stock_barcode TEXT,
            stock_name TEXT NOT NULL,
            type_name TEXT,
            drug_type TEXT,
            qty REAL,
            unit TEXT,
            qty_cc REAL,
            cost_price REAL,
            avg_cost REAL,
            sell_price REAL,
            warehouse TEXT,
            vat TEXT,
            supplier TEXT,
            alert_qty REAL,
            alert_cc REAL,
            alert_expire_days REAL,
            imported_at TEXT
        )
    """)

    # ── Stock Incoming ──────────────────────────────────────────────────────
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stock_incoming (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            receive_date TEXT,
            po_number TEXT,
            doc_number TEXT,
            stock_id TEXT,
            stock_name TEXT,
            supplier TEXT,
            qty REAL,
            unit TEXT,
            unit_price REAL,
            discount REAL,
            total_amount REAL,
            lot_no TEXT,
            manufacture_date TEXT,
            expire_date TEXT,
            operator TEXT,
            imported_at TEXT
        )
    """)

    con.commit()

    # Seed transactions if empty
    count = cur.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    if count == 0:
        _seed_dummy_data(cur)
        con.commit()
    con.close()

    # Auto-import XLS files if they exist (runs once when DB is fresh)
    _auto_import_xls()


def _auto_import_xls():
    """Auto-import XLS data files found in project directory on first run."""
    try:
        import xlrd
        base = os.path.dirname(__file__)

        items_xls    = os.path.join(base, "รายการสินค้าทั้งหมด.xls")
        incoming_xls = os.path.join(base, "การรับสินค้าเข้า Stock.xls")

        con = get_connection()
        cur = con.cursor()

        if os.path.exists(items_xls):
            cnt = cur.execute("SELECT COUNT(*) FROM stock_items").fetchone()[0]
            if cnt == 0:
                df = _read_xls_file(items_xls)
                import_stock_items(df)

        if os.path.exists(incoming_xls):
            cnt = cur.execute("SELECT COUNT(*) FROM stock_incoming").fetchone()[0]
            if cnt == 0:
                df = _read_xls_file(incoming_xls)
                import_stock_incoming(df)

        con.close()
    except Exception:
        pass  # fail silently if xlrd not installed or file corrupt


def _read_xls_file(path: str) -> "pd.DataFrame":
    import xlrd
    wb = xlrd.open_workbook(path)
    sh = wb.sheet_by_index(0)
    rows = [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    hi = 0
    for i, row in enumerate(rows):
        f = str(row[0]).strip()
        if f and not any(f.startswith(x) for x in ["ราย", "ช่วง"]) and f != "":
            if any(str(c).strip() for c in row[1:]):
                hi = i
                break
    headers = [str(c).strip() for c in rows[hi]]
    return pd.DataFrame(rows[hi + 1:], columns=headers)


def _seed_dummy_data(cur):
    today = date.today()
    income_cats = ["ตรวจโรคทั่วไป", "ผ่าตัด", "ฉีดวัคซีน",
                   "อาบน้ำ-ตัดขน", "รับฝากสัตว์", "เอกซเรย์ / Lab",
                   "ทันตกรรม", "จำหน่ายยา-อาหาร"]
    expense_cats = ["ยาและเวชภัณฑ์", "ค่าเช่าสถานที่", "เงินเดือนพนักงาน",
                    "ค่าสาธารณูปโภค", "อุปกรณ์การแพทย์",
                    "อาหารสัตว์-วัสดุสิ้นเปลือง", "การตลาด"]
    statuses = ["ชำระแล้ว", "ชำระแล้ว", "ชำระแล้ว", "รอชำระ", "เกินกำหนด", "ผ่อนชำระ"]
    owners = [
        ("คุณสมชาย ใจดี", "ดาวเรือง"),
        ("คุณนภา รักสัตว์", "มะหมา"),
        ("คุณวิชัย สุขสันต์", "เหมียว"),
        ("คุณปรียา มีทรัพย์", "บัตเตอร์"),
        ("คุณอานนท์ แจ่มใส", "โกลดี้"),
        ("คุณมาลี ชื่นบาน", "ช็อกโกแลต"),
        ("คุณธนา ยิ้มแย้ม", "แพนด้า"),
        ("คุณสุดา พรมดี", "น้องแมว"),
        ("คุณกิตติ วงศ์สุข", "รัสตี้"),
        ("คุณอรอุมา ทองงาม", "มิ้ว"),
    ]
    expense_vendors = ["บริษัท ท็อปส์เวท จำกัด", "ค่าเช่าอาคาร",
                       "การไฟฟ้า / ประปา", "บริษัท เมดิเทค จำกัด",
                       "Payroll พนักงาน", "Facebook / Google Ads"]
    rows = []
    for _ in range(50):
        d = today - timedelta(days=random.randint(0, 59))
        t_type = "รายรับ" if random.random() > 0.40 else "รายจ่าย"
        if t_type == "รายรับ":
            cat = random.choice(income_cats)
            owner, pet = random.choice(owners)
            amt = round(random.uniform(300, 25000), 2)
            status = random.choice(statuses)
        else:
            cat = random.choice(expense_cats)
            owner = random.choice(expense_vendors)
            pet = None
            amt = round(random.uniform(500, 30000), 2)
            status = random.choice(["ชำระแล้ว", "ชำระแล้ว", "รอชำระ"])
        tax = round(amt * random.choice([0, 0, 0.03, 0.07]), 2)
        net = round(amt - tax, 2)
        rows.append((d.isoformat(), t_type, cat, owner, pet, amt, tax, net, status, None, None))

    cur.executemany("""
        INSERT INTO transactions
            (transaction_date, transaction_type, category, client_name, pet_name,
             amount, tax_deduction, net_amount, payment_status, note, receipt_file_path)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, rows)


# ── Transactions CRUD ─────────────────────────────────────────────────────────
def insert_transaction(row: dict):
    con = get_connection()
    con.execute("""
        INSERT INTO transactions
            (transaction_date, transaction_type, category, client_name, pet_name,
             amount, tax_deduction, net_amount, payment_status, note, receipt_file_path)
        VALUES (:transaction_date,:transaction_type,:category,:client_name,:pet_name,
                :amount,:tax_deduction,:net_amount,:payment_status,:note,:receipt_file_path)
    """, row)
    con.commit()
    con.close()


def fetch_all() -> pd.DataFrame:
    con = get_connection()
    df = pd.read_sql_query("SELECT * FROM transactions ORDER BY transaction_date DESC", con)
    con.close()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    return df


def bulk_insert_from_df(df: pd.DataFrame):
    required = {"transaction_date", "transaction_type", "category", "client_name",
                "amount", "tax_deduction", "net_amount", "payment_status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    for col in ["pet_name", "note", "receipt_file_path"]:
        if col not in df.columns:
            df = df.copy(); df[col] = None
    con = get_connection()
    df[list(required) + ["pet_name", "note", "receipt_file_path"]].to_sql(
        "transactions", con, if_exists="append", index=False
    )
    con.commit(); con.close()


# ── Stock Items ───────────────────────────────────────────────────────────────
def import_stock_items(df: pd.DataFrame):
    """Import from รายการสินค้าทั้งหมด.xls (skip 2 header rows)."""
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
    """Import from การรับสินค้าเข้า Stock.xls (skip 3 header rows)."""
    now = date.today().isoformat()
    rows = []
    col_map = {
        "วันที่รับสินค้าเข้า": "receive_date",
        "เลขที่ใบสั่งซื้อ":    "po_number",
        "เลขที่เอกสาร":        "doc_number",
        "รหัสสินค้า":          "stock_id",
        "ชื่อสินค้า":          "stock_name",
        "ตัวแทนจำหน่าย":       "supplier",
        "จำนวน":               "qty",
        "หน่วย":               "unit",
        "ราคา/หน่วย":          "unit_price",
        "ส่วนลด":              "discount",
        "จำนวนเงิน":           "total_amount",
        "Lot No.":             "lot_no",
        "วันที่ผลิต":          "manufacture_date",
        "วันหมดอายุ":          "expire_date",
        "ผู้ทำรายการ":         "operator",
    }
    df = df.rename(columns=col_map)
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
