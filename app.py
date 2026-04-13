import os, io, re
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta
import pdfplumber
import xlrd

from db import (
    init_db, fetch_all, insert_transaction, bulk_insert_from_df,
    fetch_stock_items, import_stock_items,
    fetch_stock_incoming, import_stock_incoming,
    UPLOADS_DIR,
)

# ─── Config ───────────────────────────────────────────────────────────────────
st.set_page_config(page_title="เอสพี รักษาสัตว์", page_icon="🐾",
                   layout="wide", initial_sidebar_state="expanded")

# ─── CSS (Light theme) ────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

/* App background */
.stApp { background: #F1F5F9; }
.main .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: #FFFFFF;
    border-right: 1px solid #E2E8F0;
    box-shadow: 2px 0 8px rgba(0,0,0,0.06);
}
[data-testid="stSidebar"] * { color: #334155 !important; }
[data-testid="stSidebar"] hr { border-color: #E2E8F0 !important; }
[data-testid="stSidebarNav"] { display: none; }

/* Radio nav pills */
[data-testid="stSidebar"] div[role="radiogroup"] label {
    border-radius: 10px !important;
    padding: 10px 14px !important;
    margin-bottom: 4px !important;
    transition: all 0.15s;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    border: 1px solid #E2E8F0 !important;
    background: #F8FAFC !important;
    color: #475569 !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {
    background: #EFF6FF !important;
    border-color: #3B82F6 !important;
    color: #1D4ED8 !important;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 16px;
    padding: 18px 22px 14px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
[data-testid="stMetricLabel"] { color: #64748B !important; font-size: 0.78rem !important; letter-spacing: .06em; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #0F172A !important; font-size: 1.8rem !important; font-weight: 700 !important; }

/* Headings */
h1 { color: #0369A1 !important; font-size: 1.6rem !important; font-weight: 700 !important; letter-spacing: -.03em; }
h2 { color: #0F766E !important; font-size: 1.1rem !important; font-weight: 600 !important; }
h3 { color: #1D4ED8 !important; font-size: 0.95rem !important; font-weight: 600 !important; }

/* Tabs */
[data-baseweb="tab-list"] { gap: 6px; border-bottom: 1px solid #E2E8F0; background: transparent; }
[data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important;
    padding: 8px 18px !important;
    color: #64748B !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    border: 1px solid transparent !important;
    border-bottom: none !important;
}
[data-baseweb="tab"][aria-selected="true"] {
    background: #FFFFFF !important;
    color: #0369A1 !important;
    border-color: #E2E8F0 !important;
    border-bottom: 2px solid #0369A1 !important;
}

/* Expander */
[data-testid="stExpander"] {
    border: 1px solid #E2E8F0 !important;
    border-radius: 12px !important;
    background: #FFFFFF !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
[data-testid="stExpanderToggleIcon"] { color: #64748B !important; }

/* Buttons */
.stButton > button {
    background: #F0F9FF !important; color: #0369A1 !important;
    border: 1px solid #BAE6FD !important; border-radius: 8px !important;
    font-size: 0.85rem !important; font-weight: 500 !important;
    transition: all 0.15s;
}
.stButton > button:hover {
    background: #0369A1 !important; color: #FFFFFF !important;
    border-color: #0369A1 !important;
}

/* Download button */
[data-testid="stDownloadButton"] > button {
    background: #F0FDF4 !important; color: #15803D !important;
    border: 1px solid #BBF7D0 !important; border-radius: 8px !important;
}

/* Input fields */
[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input { background: #FFFFFF !important; color: #0F172A !important; border-color: #CBD5E1 !important; border-radius: 8px !important; }
[data-baseweb="select"] { background: #FFFFFF !important; border-color: #CBD5E1 !important; border-radius: 8px !important; }
label { color: #475569 !important; font-size: 0.82rem !important; }

/* Dataframe */
[data-testid="stDataFrame"] {
    border: 1px solid #E2E8F0;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

/* Divider */
hr { border-color: #E2E8F0 !important; margin: 1.2rem 0 !important; }

/* Hide footer / menu */
#MainMenu, footer { visibility: hidden; }

/* Drilldown active badge */
.drill-badge {
    display: inline-flex; align-items: center; gap: 8px;
    background: #EFF6FF; border: 1px solid #3B82F6;
    border-radius: 20px; padding: 4px 14px;
    color: #1D4ED8; font-size: 0.82rem; font-weight: 600;
    margin-bottom: 12px;
}

/* Section card wrapper */
.section-card {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 16px;
    padding: 20px 24px;
    margin-bottom: 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}
</style>
""", unsafe_allow_html=True)

init_db()

# ─── Color palette (consistent across ALL charts) ──────────────────────────────
CAT_COLORS = {
    # Income services
    "ตรวจโรคทั่วไป":              "#38BDF8",
    "ผ่าตัด":                     "#F472B6",
    "ฉีดวัคซีน":                  "#4ADE80",
    "อาบน้ำ-ตัดขน":               "#FBBF24",
    "รับฝากสัตว์":                "#A78BFA",
    "เอกซเรย์ / Lab":             "#22D3EE",
    "ทันตกรรม":                   "#FB923C",
    "จำหน่ายยา-อาหาร":            "#E879F9",
    # Expense categories
    "ยาและเวชภัณฑ์":               "#60A5FA",
    "ค่าเช่าสถานที่":              "#F87171",
    "เงินเดือนพนักงาน":            "#34D399",
    "ค่าสาธารณูปโภค":             "#FDE68A",
    "อุปกรณ์การแพทย์":            "#C084FC",
    "อาหารสัตว์-วัสดุสิ้นเปลือง": "#67E8F9",
    "การตลาด":                    "#FCA5A5",
    # PDF report categories
    "รายการยา":                    "#38BDF8",
    "อุปกรณ์และเวชภัณฑ์":         "#4ADE80",
    "ค่าบริการทางการแพทย์":       "#FBBF24",
    "ค่าผ่าตัด":                   "#F472B6",
    "อุปกรณ์ตรวจ LAB":            "#22D3EE",
    "ค่าตรวจรักษา":               "#A78BFA",
    "น้ำเกลือ":                    "#67E8F9",
    "ค่าบริการอื่นๆ":              "#FB923C",
    "สินค้า Pet Shop":             "#E879F9",
}

INCOME_CATS  = ["ตรวจโรคทั่วไป","ผ่าตัด","ฉีดวัคซีน","อาบน้ำ-ตัดขน",
                "รับฝากสัตว์","เอกซเรย์ / Lab","ทันตกรรม","จำหน่ายยา-อาหาร"]
EXPENSE_CATS = ["ยาและเวชภัณฑ์","ค่าเช่าสถานที่","เงินเดือนพนักงาน",
                "ค่าสาธารณูปโภค","อุปกรณ์การแพทย์","อาหารสัตว์-วัสดุสิ้นเปลือง","การตลาด"]
STATUSES     = ["ชำระแล้ว","รอชำระ","เกินกำหนด","ผ่อนชำระ"]
STATUS_ICON  = {"ชำระแล้ว":"✅","รอชำระ":"🕐","เกินกำหนด":"🔴","ผ่อนชำระ":"🔵"}
STATUS_COLOR = {"ชำระแล้ว":"#4ADE80","รอชำระ":"#FBBF24","เกินกำหนด":"#F87171","ผ่อนชำระ":"#A78BFA"}

CHART_BG = dict(plot_bgcolor="rgba(0,0,0,0)", paper_bgcolor="rgba(0,0,0,0)",
                font_color="#374151",
                xaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B"),
                yaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B"),
                margin=dict(t=16, b=16, l=8, r=8))
LEGEND_STYLE = dict(bgcolor="rgba(255,255,255,0.8)", font_size=11, font_color="#374151",
                    bordercolor="#E2E8F0", borderwidth=1)


# ─── Session state ────────────────────────────────────────────────────────────
for _k, _v in [("drill_date", None), ("drill_cat", None), ("drill_pdf_cat", None),
                ("drill_stock", None)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v


# ─── Helpers ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def load_data() -> pd.DataFrame:
    df = fetch_all()
    df["transaction_date"] = pd.to_datetime(df["transaction_date"])
    return df

@st.cache_data(ttl=60)
def load_stock_items():   return fetch_stock_items()

@st.cache_data(ttl=60)
def load_stock_incoming(): return fetch_stock_incoming()


def fmt_thb(v: float) -> str:
    return f"฿{abs(v):,.0f}"

def delta_str(cur: float, prv: float):
    if prv == 0: return None, "off"
    d = cur - prv; p = d / prv * 100
    s = "▲" if d >= 0 else "▼"
    return f"{s} {abs(p):.1f}%  ({fmt_thb(d)})", "normal" if d >= 0 else "inverse"

def month_range(y, m):
    s = date(y, m, 1)
    e = (date(y, m+1, 1) if m < 12 else date(y+1,1,1)) - timedelta(days=1)
    return pd.Timestamp(s), pd.Timestamp(e)

def period_kpis(df, s, e):
    sub = df[(df["transaction_date"] >= s) & (df["transaction_date"] <= e)]
    rev = sub.loc[sub["transaction_type"]=="รายรับ",  "net_amount"].sum()
    exp = sub.loc[sub["transaction_type"]=="รายจ่าย", "net_amount"].sum()
    return rev, exp, rev - exp

def color_for(cat: str) -> str:
    return CAT_COLORS.get(cat, "#8ECDB0")

def read_xls_bytes(file_bytes) -> pd.DataFrame:
    wb = xlrd.open_workbook(file_contents=file_bytes)
    sh = wb.sheet_by_index(0)
    rows = [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    hi = 0
    for i, row in enumerate(rows):
        f = str(row[0]).strip()
        if f and not any(f.startswith(x) for x in ["ราย","ช่วง","Stock Id"]) and f != "":
            if any(str(c).strip() for c in row[1:]):
                hi = i; break
        if "Stock Id" in f or "วันที่รับ" in f:
            hi = i; break
    headers = [str(c).strip() for c in rows[hi]]
    return pd.DataFrame(rows[hi+1:], columns=headers)


@st.cache_data(ttl=300, show_spinner=False)
def parse_pdf(pdf_bytes: bytes):
    """Returns (info, summary_df, items_df) from financial report PDF."""
    info = {"cash": 0, "transfer": 0, "total": 0, "receipts": 0, "cancelled": 0,
            "clinic": "", "period": ""}
    summary_rows = []
    item_rows = []

    CAT_CLEAN = {
        "รายการยา":           "รายการยา",
        "อปุ กรณแ์ ละเวชภณั ฑ์": "อุปกรณ์และเวชภัณฑ์",
        "คา่ บรกิ ารทางการแพทย์": "ค่าบริการทางการแพทย์",
        "คา่ ผา่ ตดั":         "ค่าผ่าตัด",
        "อปุ กรณต์ รวจ LAB":   "อุปกรณ์ตรวจ LAB",
        "คา่ ตรวจรกั ษา":      "ค่าตรวจรักษา",
        "นํา\x00 เกลอื":       "น้ำเกลือ",
        "นํา เกลอื":           "น้ำเกลือ",
        "นํ าเกลอื":           "น้ำเกลือ",
        "คา่ บรกิ ารอนื\x00 ๆ": "ค่าบริการอื่นๆ",
        "คา่ บรกิ ารอนื ๆ":    "ค่าบริการอื่นๆ",
        "สนิ คา้ Pet Shop":    "สินค้า Pet Shop",
    }

    def clean_cat(raw):
        for k, v in CAT_CLEAN.items():
            if k.replace("\x00","") in raw.replace("\x00",""):
                return v
        return raw.strip()

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            # ── Page 1: header info ────────────────────────────────────────
            t1 = " ".join((pdf.pages[0].extract_text() or "").split())
            m = re.search(r"รวมใบเสร็จถ[^\d]*([\d]+)\s*รายการ\s*รวมใบเสร็จทง[^\d]*([\d]+)", t1)
            if m:
                info["cancelled"]  = int(m.group(1))
                info["receipts"]   = int(m.group(2))
            m = re.search(r"เงนิ สด[^\d]*([\d,]+)", t1)
            if m: info["cash"] = float(m.group(1).replace(",",""))
            m = re.search(r"โอนเงนิ[^\d]*([\d,]+)", t1)
            if m: info["transfer"] = float(m.group(1).replace(",",""))
            m = re.search(r"รวมเป.*?นเงนิ\s+([\d,]+)", t1)
            if m: info["total"] = float(m.group(1).replace(",",""))

            # ── Page 2: summary by category ───────────────────────────────
            words2 = pdf.pages[1].extract_words()
            amounts, labels = [], []
            for w in words2:
                txt = w["text"]
                if re.match(r"[\d,]+\.\d{2}", txt):
                    amounts.append(float(txt.replace(",","")))
                elif txt == "บาท" and amounts and len(amounts) > len(labels):
                    pass  # skip "บาท" word
            # Better: reconstruct lines from word positions
            lines_pg2 = {}
            for w in words2:
                y = round(w["top"] / 3) * 3
                lines_pg2.setdefault(y, []).append(w)
            for y in sorted(lines_pg2):
                words_in_line = sorted(lines_pg2[y], key=lambda w: w["x0"])
                line_text = " ".join(w["text"] for w in words_in_line)
                m = re.search(r"([\d,]+\.\d{2})\s*บาท", line_text)
                if m:
                    amt = float(m.group(1).replace(",",""))
                    cat_raw = line_text[:line_text.index(m.group(0))].strip()
                    if amt > 0 and cat_raw and "ยอดรวม" not in cat_raw:
                        summary_rows.append({"category": clean_cat(cat_raw), "amount": amt})

            # ── Pages 3+: detailed item tables ────────────────────────────
            for page in pdf.pages[2:]:
                tbl = page.extract_table()
                if not tbl:
                    continue
                for row in tbl:
                    if not row or len(row) < 4:
                        continue
                    cat_raw, item_name, qty_unit, total_str = (
                        str(row[0] or "").strip(),
                        str(row[1] or "").strip(),
                        str(row[2] or "").strip(),
                        str(row[3] or "").strip(),
                    )
                    if not item_name or not total_str:
                        continue
                    # Skip header rows
                    if "รายการ" in item_name and "จํ" in qty_unit:
                        continue
                    try:
                        total = float(re.sub(r"[^\d\.]", "", total_str))
                        qty_m = re.match(r"([\d,\.]+)", qty_unit)
                        qty   = float(qty_m.group(1).replace(",","")) if qty_m else 0.0
                        unit  = re.sub(r"^[\d,\.\s]+", "", qty_unit).strip()
                        if total > 0 and item_name:
                            item_rows.append({
                                "category":  clean_cat(cat_raw) if cat_raw else "—",
                                "item_name": item_name,
                                "qty":       qty,
                                "unit":      unit,
                                "total":     total,
                            })
                    except (ValueError, AttributeError):
                        pass

    except Exception as e:
        st.warning(f"PDF parse warning: {e}")

    summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(columns=["category","amount"])
    items_df   = pd.DataFrame(item_rows)    if item_rows    else pd.DataFrame(columns=["category","item_name","qty","unit","total"])
    return info, summary_df, items_df


# ─── Custom metric card ────────────────────────────────────────────────────────
def kpi(col, icon, label, value, delta=None, delta_up=None, note=None):
    delta_html = ""
    if delta:
        clr = "#16A34A" if delta_up else "#DC2626"
        arr = "▲" if delta_up else "▼"
        delta_html = f'<p style="margin:4px 0 0;color:{clr};font-size:.78rem;font-weight:600">{arr} {delta}</p>'
    note_html = f'<p style="margin:4px 0 0;color:#94A3B8;font-size:.72rem">{note}</p>' if note else ""
    col.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:16px;
                padding:18px 22px 14px;height:100%;min-height:105px;
                box-shadow:0 1px 4px rgba(0,0,0,0.06)">
      <p style="margin:0;color:#64748B;font-size:.72rem;font-weight:600;
                letter-spacing:.07em;text-transform:uppercase">{icon}&nbsp; {label}</p>
      <p style="margin:8px 0 0;color:#0F172A;font-size:1.75rem;font-weight:700;line-height:1.1">{value}</p>
      {delta_html}{note_html}
    </div>""", unsafe_allow_html=True)


def drill_badge(label: str, key: str):
    c1, c2 = st.columns([6, 1])
    c1.markdown(f'<div class="drill-badge">🔍 Drilldown: <strong>{label}</strong></div>',
                unsafe_allow_html=True)
    if c2.button("✕ ล้าง", key=f"clr_{key}"):
        for k in ["drill_date","drill_cat","drill_pdf_cat","drill_stock"]:
            st.session_state[k] = None
        st.rerun()


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 0 8px">
      <div style="font-size:1.4rem;font-weight:700;color:#0369A1;letter-spacing:-.02em">
        🐾 เอสพี รักษาสัตว์
      </div>
      <div style="font-size:.75rem;color:#94A3B8;margin-top:2px">ระบบจัดการคลินิกสัตว์</div>
    </div>""", unsafe_allow_html=True)
    st.divider()

    page = st.radio("", [
        "📊  ภาพรวมธุรกิจ",
        "📒  รายรับ-รายจ่าย",
        "💊  คลังยา & สินค้า",
        "📄  รายงานการเงิน",
    ], label_visibility="collapsed")
    st.divider()

    df_s = load_data()
    today_d = date.today()
    m_td = df_s["transaction_date"].dt.date == today_d
    rt = df_s.loc[m_td & (df_s["transaction_type"]=="รายรับ"),  "net_amount"].sum()
    et = df_s.loc[m_td & (df_s["transaction_type"]=="รายจ่าย"), "net_amount"].sum()
    pend = (df_s["payment_status"]=="รอชำระ").sum()
    over = (df_s["payment_status"]=="เกินกำหนด").sum()

    st.markdown('<p style="color:#94A3B8;font-size:.72rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase">วันนี้</p>', unsafe_allow_html=True)
    st.markdown(f'<p style="margin:2px 0;color:#334155;font-size:.85rem">💰 รายรับ &nbsp;<strong style="color:#16A34A">{fmt_thb(rt)}</strong></p>', unsafe_allow_html=True)
    st.markdown(f'<p style="margin:2px 0;color:#334155;font-size:.85rem">📤 รายจ่าย <strong style="color:#DC2626">{fmt_thb(et)}</strong></p>', unsafe_allow_html=True)
    if pend: st.markdown(f'<p style="margin:4px 0;color:#D97706;font-size:.82rem">🕐 รอชำระ <strong>{pend}</strong> รายการ</p>', unsafe_allow_html=True)
    if over: st.markdown(f'<p style="margin:4px 0;color:#DC2626;font-size:.82rem">🔴 เกินกำหนด <strong>{over}</strong> รายการ</p>', unsafe_allow_html=True)

    df_si = load_stock_items()
    if not df_si.empty:
        low = ((df_si["qty"] <= df_si["alert_qty"]) & (df_si["qty"] > 0)).sum()
        out = (df_si["qty"] <= 0).sum()
        if low: st.markdown(f'<p style="margin:4px 0;color:#D97706;font-size:.82rem">⚠️ ใกล้หมด <strong>{low}</strong> รายการ</p>', unsafe_allow_html=True)
        if out: st.markdown(f'<p style="margin:4px 0;color:#DC2626;font-size:.82rem">🚨 หมดสต๊อก <strong>{out}</strong> รายการ</p>', unsafe_allow_html=True)
    st.divider()
    st.caption("SQLite Local  •  v2.1")

df_all = load_data()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊  ภาพรวมธุรกิจ":
    st.title("📊 ภาพรวมธุรกิจ")

    today = date.today()
    cy, cm = today.year, today.month
    py, pm = (cy, cm-1) if cm > 1 else (cy-1, 12)
    cs, ce = month_range(cy, cm)
    ps, pe = month_range(py, pm)
    cr, ce_, cn = period_kpis(df_all, cs, ce)
    pr, pe_, pn = period_kpis(df_all, ps, pe)

    rd, rc = delta_str(cr, pr);  ed, ec = delta_str(ce_, pe_);  nd, nc = delta_str(cn, pn)
    mask_m = (df_all["transaction_date"] >= cs) & (df_all["transaction_date"] <= ce)
    cases  = int((df_all[mask_m]["transaction_type"]=="รายรับ").sum())

    k1,k2,k3,k4 = st.columns(4)
    kpi(k1, "💰", "รายรับเดือนนี้",  fmt_thb(cr), rd,  rc=="normal",   f"เทียบเดือนก่อน {fmt_thb(pr)}")
    kpi(k2, "📤", "รายจ่ายเดือนนี้", fmt_thb(ce_), ed, ec!="normal",   f"เทียบเดือนก่อน {fmt_thb(pe_)}")
    kpi(k3, "📈", "กำไรสุทธิ",       fmt_thb(cn), nd,  nc=="normal",   f"เทียบเดือนก่อน {fmt_thb(pn)}")
    kpi(k4, "🐾", "เคสเดือนนี้",     f"{cases} เคส", None, None, "รายการรายรับทั้งหมด")
    st.divider()

    # Period filter
    min_d = df_all["transaction_date"].min().date() if not df_all.empty else date(2024,1,1)
    max_d = max(df_all["transaction_date"].max().date() if not df_all.empty else today, today)
    fc1,fc2,_fc = st.columns([1,1,3])
    d_from = fc1.date_input("ตั้งแต่", value=cs.date(), min_value=min_d, max_value=max_d)
    d_to   = fc2.date_input("ถึง",     value=today,     min_value=min_d, max_value=max_d)
    df_p = df_all[
        (df_all["transaction_date"] >= pd.Timestamp(d_from)) &
        (df_all["transaction_date"] <= pd.Timestamp(d_to))
    ].copy()
    st.divider()

    # ── Row 1: Daily bar + Service donut ─────────────────────────────────────
    cc1, cc2 = st.columns([3, 2])

    with cc1:
        st.markdown("## 📅 รายรับ vs รายจ่าย รายวัน")
        st.caption("คลิกที่แท่งเพื่อ drilldown รายการในวันนั้น")
        if not df_p.empty:
            daily = df_p.groupby(["transaction_date","transaction_type"])["net_amount"].sum().reset_index()
            daily["ds"] = daily["transaction_date"].dt.strftime("%Y-%m-%d")
            fig_bar = go.Figure()
            for tt, clr in [("รายรับ","#00C26B"),("รายจ่าย","#F87171")]:
                d_ = daily[daily["transaction_type"]==tt]
                fig_bar.add_trace(go.Bar(
                    x=d_["ds"], y=d_["net_amount"], name=tt,
                    marker_color=clr, marker_line_width=0,
                    hovertemplate=f"<b>%{{x}}</b><br>{tt}: ฿%{{y:,.0f}}<extra></extra>",
                ))
            fig_bar.update_layout(**CHART_BG, barmode="group", height=300,
                                   bargap=0.25, bargroupgap=0.08,
                                   legend=LEGEND_STYLE)
            ev1 = st.plotly_chart(fig_bar, key="bar_daily", on_select="rerun",
                                  use_container_width=True)
            pts1 = ev1.selection.points if ev1.selection else []
            if pts1:
                xv = pts1[0].get("x") if isinstance(pts1[0], dict) else getattr(pts1[0],"x",None)
                if xv:
                    st.session_state.drill_date = str(xv)[:10]
                    st.session_state.drill_cat  = None
        else:
            st.info("ไม่มีข้อมูลในช่วงที่เลือก")

    with cc2:
        st.markdown("## 🥧 รายรับตามบริการ")
        st.caption("คลิกที่ slice เพื่อ drilldown")
        df_inc = df_p[df_p["transaction_type"]=="รายรับ"]
        if not df_inc.empty:
            cat_i = df_inc.groupby("category")["net_amount"].sum().reset_index()
            cat_i["color"] = cat_i["category"].map(lambda c: color_for(c))
            fig_pie = go.Figure(go.Pie(
                labels=cat_i["category"], values=cat_i["net_amount"],
                hole=0.52,
                marker_colors=cat_i["color"].tolist(),
                textposition="inside", textinfo="percent",
                hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<br>%{percent}<extra></extra>",
            ))
            fig_pie.update_layout(**{**CHART_BG, "margin": dict(t=16,b=16,l=16,r=16)},
                                   height=300, showlegend=True, legend=LEGEND_STYLE)
            ev2 = st.plotly_chart(fig_pie, key="pie_service", on_select="rerun",
                                  use_container_width=True)
            pts2 = ev2.selection.points if ev2.selection else []
            if pts2:
                pt2 = pts2[0]
                lbl = pt2.get("label") if isinstance(pt2,dict) else getattr(pt2,"label",None)
                if not lbl:
                    lbl = pt2.get("x") if isinstance(pt2,dict) else getattr(pt2,"x",None)
                if lbl:
                    st.session_state.drill_cat  = lbl
                    st.session_state.drill_date = None
        else:
            st.info("ไม่มีข้อมูลรายรับ")

    # ── Row 2: Expense bar + Pending ─────────────────────────────────────────
    ec1, ec2 = st.columns([2, 3])

    with ec1:
        st.markdown("## 📊 รายจ่ายตามหมวด")
        st.caption("คลิกที่แท่งเพื่อ drilldown")
        df_exp = df_p[df_p["transaction_type"]=="รายจ่าย"]
        if not df_exp.empty:
            cat_e = df_exp.groupby("category")["net_amount"].sum().reset_index().sort_values("net_amount")
            cat_e["color"] = cat_e["category"].map(lambda c: color_for(c))
            fig_hbar = go.Figure(go.Bar(
                x=cat_e["net_amount"], y=cat_e["category"],
                orientation="h",
                marker_color=cat_e["color"].tolist(),
                marker_line_width=0,
                hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>",
            ))
            fig_hbar.update_layout(**CHART_BG, height=280)
            ev3 = st.plotly_chart(fig_hbar, key="bar_expense", on_select="rerun",
                                  use_container_width=True)
            pts3 = ev3.selection.points if ev3.selection else []
            if pts3:
                pt3 = pts3[0]
                yv  = pt3.get("y") if isinstance(pt3,dict) else getattr(pt3,"y",None)
                if yv:
                    st.session_state.drill_cat  = yv
                    st.session_state.drill_date = None
        else:
            st.info("ไม่มีข้อมูลรายจ่าย")

    with ec2:
        st.markdown("## ⚠️ รายการค้างชำระ")
        df_alert = df_all[df_all["payment_status"].isin(["รอชำระ","เกินกำหนด"])].copy()
        if not df_alert.empty:
            df_alert = df_alert.sort_values("payment_status", ascending=False)
            df_alert["transaction_date"] = df_alert["transaction_date"].dt.strftime("%Y-%m-%d")
            sh = df_alert[["transaction_date","client_name","pet_name","category","net_amount","payment_status"]].copy()
            sh.columns = ["วันที่","เจ้าของ","สัตว์","บริการ","ยอด (฿)","สถานะ"]
            sh["สถานะ"] = sh["สถานะ"].map(lambda v: f"{STATUS_ICON.get(v,'')} {v}")
            st.dataframe(sh, use_container_width=True, height=280, hide_index=True)
        else:
            st.success("ไม่มีรายการค้างชำระ ✅")

    st.divider()

    # ── Drilldown transaction table ───────────────────────────────────────────
    df_drill = df_p.copy()
    drill_label = None

    if st.session_state.drill_date:
        df_drill   = df_drill[df_drill["transaction_date"].dt.strftime("%Y-%m-%d") == st.session_state.drill_date]
        drill_label = st.session_state.drill_date
    elif st.session_state.drill_cat:
        df_drill   = df_drill[df_drill["category"] == st.session_state.drill_cat]
        drill_label = st.session_state.drill_cat

    if drill_label:
        drill_badge(drill_label, "dash")

    title_sfx = f" — {drill_label}" if drill_label else f" ({d_from} → {d_to})"
    st.markdown(f"## 📋 รายการธุรกรรม{title_sfx}")

    if not df_drill.empty:
        sh2 = df_drill[["transaction_date","transaction_type","category",
                        "client_name","pet_name","net_amount","payment_status"]].copy()
        sh2["transaction_date"] = sh2["transaction_date"].dt.strftime("%Y-%m-%d")
        sh2["payment_status"]   = sh2["payment_status"].map(lambda v: f"{STATUS_ICON.get(v,'')} {v}")
        sh2.columns = ["วันที่","ประเภท","บริการ","เจ้าของสัตว์","ชื่อสัตว์","ยอดสุทธิ (฿)","สถานะ"]
        st.dataframe(sh2, use_container_width=True, height=360, hide_index=True)
        t_in  = df_drill[df_drill["transaction_type"]=="รายรับ"]["net_amount"].sum()
        t_out = df_drill[df_drill["transaction_type"]=="รายจ่าย"]["net_amount"].sum()
        m1,m2,m3 = st.columns(3)
        m1.metric("รายรับ", fmt_thb(t_in))
        m2.metric("รายจ่าย", fmt_thb(t_out))
        m3.metric("กำไรสุทธิ", fmt_thb(t_in-t_out))
        st.caption(f"แสดง {len(sh2):,} รายการ")
    else:
        st.info("ไม่มีรายการในช่วงที่เลือก")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — LEDGER
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📒  รายรับ-รายจ่าย":
    st.title("📒 บันทึกรายรับ-รายจ่าย")

    with st.expander("➕  เพิ่มรายการใหม่", expanded=True):
        with st.form("add_tx", clear_on_submit=True):
            fa1,fa2,fa3 = st.columns(3)
            tx_date  = fa1.date_input("วันที่", value=date.today())
            tx_type  = fa2.radio("ประเภท", ["รายรับ","รายจ่าย"], horizontal=True)
            tx_cat   = fa3.selectbox("หมวดหมู่", INCOME_CATS if tx_type=="รายรับ" else EXPENSE_CATS)

            fb1,fb2,fb3 = st.columns(3)
            client  = fb1.text_input("ชื่อเจ้าของสัตว์ / ผู้จำหน่าย")
            pet     = fb2.text_input("ชื่อสัตว์เลี้ยง", placeholder="ถ้ามี")
            amount  = fb3.number_input("จำนวนเงิน (฿)", min_value=0.0, step=100.0, format="%.2f")

            fc1_,fc2_,fc3_ = st.columns(3)
            tx_status = fc1_.selectbox("สถานะการชำระ", STATUSES)
            use_tax   = fc2_.toggle("หักภาษี?", value=False)
            tax_rate  = fc3_.selectbox("อัตราภาษี", ["3% WHT","7% VAT"], disabled=not use_tax)

            note    = st.text_input("หมายเหตุ", placeholder="อาการ / วิธีรักษา / ฯลฯ (ไม่บังคับ)")
            receipt = st.file_uploader("แนบใบเสร็จ", type=["pdf","png","jpg","jpeg"])

            if st.form_submit_button("💾  บันทึกรายการ", use_container_width=True):
                if not client.strip():
                    st.error("กรุณากรอกชื่อเจ้าของสัตว์ / ผู้จำหน่าย")
                elif amount <= 0:
                    st.error("กรุณากรอกจำนวนเงินให้ถูกต้อง")
                else:
                    rate = (0.03 if "3%" in tax_rate else 0.07) if use_tax else 0
                    tax  = round(amount * rate, 2)
                    fp   = None
                    if receipt:
                        os.makedirs(UPLOADS_DIR, exist_ok=True)
                        fp = os.path.join(UPLOADS_DIR, f"{tx_date}_{client.strip().replace(' ','_')}_{receipt.name}")
                        with open(fp,"wb") as f: f.write(receipt.getbuffer())
                    insert_transaction({"transaction_date":tx_date.isoformat(), "transaction_type":tx_type,
                                        "category":tx_cat, "client_name":client.strip(),
                                        "pet_name":pet.strip() or None, "amount":amount,
                                        "tax_deduction":tax, "net_amount":round(amount-tax,2),
                                        "payment_status":tx_status, "note":note.strip() or None,
                                        "receipt_file_path":fp})
                    st.success("✅ บันทึกเรียบร้อยแล้ว!")
                    st.cache_data.clear(); st.rerun()

    st.divider()
    st.markdown("## 📋 ตารางรายการ")
    df_all = load_data()

    # ── Global filters ──────────────────────────────────────────────────────
    gf1, gf2, gf3 = st.columns(3)
    gs = gf1.multiselect("สถานะ", STATUSES, default=STATUSES)
    sr = gf2.text_input("🔍 ค้นหาชื่อ")
    date_range = gf3.date_input("ช่วงวันที่", value=[], help="เลือก 1 วัน หรือ 2 วันเพื่อกำหนดช่วง")

    dv = df_all[df_all["payment_status"].isin(gs)]
    if sr:
        dv = dv[dv["client_name"].str.contains(sr,case=False,na=False) |
                dv["pet_name"].fillna("").str.contains(sr,case=False,na=False)]
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        dv = dv[(dv["transaction_date"].dt.date >= date_range[0]) &
                (dv["transaction_date"].dt.date <= date_range[1])]

    def render_table(df_sub, type_label, cat_list):
        f1, f2 = st.columns(2)
        sel_cat = f1.multiselect(f"หมวดหมู่{type_label}", cat_list, key=f"cat_{type_label}")
        if sel_cat: df_sub = df_sub[df_sub["category"].isin(sel_cat)]

        sh = df_sub[["id","transaction_date","category","client_name",
                     "pet_name","amount","tax_deduction","net_amount",
                     "payment_status","note"]].copy()
        sh["transaction_date"] = sh["transaction_date"].dt.strftime("%Y-%m-%d")
        sh["payment_status"]   = sh["payment_status"].map(lambda v: f"{STATUS_ICON.get(v,'')} {v}")
        sh.columns = ["#","วันที่","หมวดหมู่","เจ้าของสัตว์","ชื่อสัตว์",
                      "ยอดเต็ม (฿)","ภาษี (฿)","ยอดสุทธิ (฿)","สถานะ","หมายเหตุ"]
        st.dataframe(sh, use_container_width=True, height=400, hide_index=True)
        st.caption(f"แสดง {len(sh):,} รายการ  •  รวม {fmt_thb(df_sub['net_amount'].sum())}")
        return df_sub

    tab_in, tab_out, tab_all = st.tabs(["💰 รายรับ", "📤 รายจ่าย", "📊 รวม"])

    with tab_in:
        df_in = dv[dv["transaction_type"]=="รายรับ"].copy()
        kpi1,kpi2,kpi3 = st.columns(3)
        kpi(kpi1,"💰","รายรับรวม", fmt_thb(df_in["net_amount"].sum()))
        kpi(kpi2,"🧾","จำนวนรายการ", f"{len(df_in):,} รายการ")
        kpi(kpi3,"📅","เฉลี่ย/รายการ",
            fmt_thb(df_in["net_amount"].mean()) if len(df_in) else "฿0")
        st.divider()
        render_table(df_in, "รายรับ", INCOME_CATS)

    with tab_out:
        df_out = dv[dv["transaction_type"]=="รายจ่าย"].copy()
        kpi1,kpi2,kpi3 = st.columns(3)
        kpi(kpi1,"📤","รายจ่ายรวม", fmt_thb(df_out["net_amount"].sum()))
        kpi(kpi2,"🧾","จำนวนรายการ", f"{len(df_out):,} รายการ")
        kpi(kpi3,"📅","เฉลี่ย/รายการ",
            fmt_thb(df_out["net_amount"].mean()) if len(df_out) else "฿0")
        st.divider()
        render_table(df_out, "รายจ่าย", EXPENSE_CATS)

    with tab_all:
        ti = dv[dv["transaction_type"]=="รายรับ"]["net_amount"].sum()
        to = dv[dv["transaction_type"]=="รายจ่าย"]["net_amount"].sum()
        s1,s2,s3 = st.columns(3)
        kpi(s1,"💰","รายรับ", fmt_thb(ti))
        kpi(s2,"📤","รายจ่าย", fmt_thb(to))
        kpi(s3,"📈","กำไรสุทธิ", fmt_thb(ti-to), delta_up=(ti-to)>=0)
        st.divider()
        sh_all = dv[["id","transaction_date","transaction_type","category",
                     "client_name","pet_name","net_amount","payment_status","note"]].copy()
        sh_all["transaction_date"] = sh_all["transaction_date"].dt.strftime("%Y-%m-%d")
        sh_all["payment_status"]   = sh_all["payment_status"].map(lambda v: f"{STATUS_ICON.get(v,'')} {v}")
        sh_all.columns = ["#","วันที่","ประเภท","หมวดหมู่","เจ้าของสัตว์","ชื่อสัตว์","ยอดสุทธิ (฿)","สถานะ","หมายเหตุ"]
        st.dataframe(sh_all, use_container_width=True, height=400, hide_index=True)
        st.caption(f"แสดง {len(sh_all):,} จาก {len(df_all):,} รายการ")

    st.divider()
    ec_,ic_ = st.columns(2)
    with ec_:
        st.markdown("**⬇️ Export CSV**")
        st.download_button("ดาวน์โหลดทั้งหมด", data=df_all.to_csv(index=False).encode("utf-8-sig"),
                           file_name="vetclinic_transactions.csv", mime="text/csv", use_container_width=True)
    with ic_:
        st.markdown("**⬆️ Import CSV**")
        cu = st.file_uploader("อัปโหลด CSV", type=["csv"], key="csv_imp")
        if cu:
            try:
                di = pd.read_csv(cu)
                st.dataframe(di.head(5))
                if st.button("✅ ยืนยันนำเข้า", use_container_width=True):
                    bulk_insert_from_df(di); st.success(f"นำเข้า {len(di)} รายการ")
                    st.cache_data.clear(); st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — STOCK
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💊  คลังยา & สินค้า":
    st.title("💊 คลังยา & สินค้า")

    tab1, tab2, tab3 = st.tabs(["📦  รายการสินค้า", "🚚  ประวัติรับสินค้า", "⬆️  นำเข้าข้อมูล"])

    with tab1:
        df_si = load_stock_items()
        if df_si.empty:
            st.info("ยังไม่มีข้อมูล กรุณานำเข้าที่แท็บ 'นำเข้าข้อมูล'")
        else:
            low = ((df_si["qty"] <= df_si["alert_qty"]) & (df_si["qty"] > 0)).sum()
            out = (df_si["qty"] <= 0).sum()
            inv_val = (df_si["qty"] * df_si["avg_cost"].fillna(0)).sum()
            ok  = len(df_si) - low - out

            k1,k2,k3,k4 = st.columns(4)
            kpi(k1,"📦","รายการทั้งหมด", f"{len(df_si):,} รายการ")
            kpi(k2,"✅","ปกติ",         f"{ok:,} รายการ")
            kpi(k3,"⚠️","ใกล้หมด",     f"{low} รายการ",   note="QTY ≤ alert")
            kpi(k4,"🚨","หมดสต๊อก",    f"{out} รายการ",   note="QTY ≤ 0")
            st.divider()

            fa,fb,fc_,fd = st.columns(4)
            drug_types = ["ทั้งหมด"] + sorted(df_si["drug_type"].dropna().unique().tolist())
            sel_d  = fa.selectbox("ประเภทยา", drug_types)
            sel_al = fb.selectbox("กรองสถานะ", ["ทั้งหมด","✅ ปกติ","⚠️ ใกล้หมด","🚨 หมดสต๊อก"])
            srch_s = fc_.text_input("🔍 ค้นหาชื่อสินค้า")
            sel_sup = fd.selectbox("ผู้จำหน่าย", ["ทั้งหมด"] + sorted(df_si["supplier"].dropna().replace("","ไม่ระบุ").unique().tolist()))

            dsi = df_si.copy()
            if sel_d  != "ทั้งหมด": dsi = dsi[dsi["drug_type"]==sel_d]
            if sel_al == "⚠️ ใกล้หมด":   dsi = dsi[(dsi["qty"] <= dsi["alert_qty"]) & (dsi["qty"] > 0)]
            if sel_al == "🚨 หมดสต๊อก":  dsi = dsi[dsi["qty"] <= 0]
            if sel_al == "✅ ปกติ":       dsi = dsi[dsi["qty"] > dsi["alert_qty"]]
            if srch_s: dsi = dsi[dsi["stock_name"].str.contains(srch_s,case=False,na=False)]
            if sel_sup != "ทั้งหมด": dsi = dsi[dsi["supplier"].fillna("ไม่ระบุ")==sel_sup]

            # Add status column
            def stock_status(row):
                if row["qty"] <= 0:            return "🚨 หมด"
                if row["qty"] <= row["alert_qty"]: return "⚠️ ใกล้หมด"
                return "✅ ปกติ"

            disp = dsi[["stock_id","stock_name","drug_type","qty","unit","avg_cost","sell_price","alert_qty","supplier"]].copy()
            disp["สถานะสต๊อก"] = dsi.apply(stock_status, axis=1)
            disp.columns = ["รหัส","ชื่อสินค้า","ประเภท","คงเหลือ","หน่วย","ราคาทุนเฉลี่ย","ราคาขาย","แจ้งเตือนเมื่อเหลือ","ผู้จำหน่าย","สถานะ"]
            st.dataframe(disp, use_container_width=True, height=460, hide_index=True)
            st.caption(f"แสดง {len(dsi):,} จาก {len(df_si):,} รายการ")

            st.divider()
            st.markdown("## 📊 Top 10 มูลค่าคลังสินค้า")
            st.caption("คลิกที่แท่งเพื่อดูประวัติรับสินค้า")
            df_val = df_si.copy()
            df_val["มูลค่า"] = df_val["qty"] * df_val["avg_cost"].fillna(0)
            top10 = df_val[df_val["มูลค่า"] > 0].nlargest(10,"มูลค่า")
            fig_stk = go.Figure(go.Bar(
                x=top10["มูลค่า"], y=top10["stock_name"], orientation="h",
                marker=dict(color=top10["มูลค่า"], colorscale=[[0,"#DBEAFE"],[1,"#1D4ED8"]],
                             line_width=0),
                hovertemplate="<b>%{y}</b><br>มูลค่า: ฿%{x:,.0f}<extra></extra>",
            ))
            fig_stk.update_layout(**CHART_BG, height=360, coloraxis_showscale=False)
            ev_stk = st.plotly_chart(fig_stk, key="bar_stock", on_select="rerun",
                                     use_container_width=True)
            pts_stk = ev_stk.selection.points if ev_stk.selection else []
            if pts_stk:
                pt_s = pts_stk[0]
                yv   = pt_s.get("y") if isinstance(pt_s,dict) else getattr(pt_s,"y",None)
                if yv: st.session_state.drill_stock = yv

            if st.session_state.drill_stock:
                drill_badge(st.session_state.drill_stock, "stock")

    with tab2:
        df_inc = load_stock_incoming()
        if df_inc.empty:
            st.info("ยังไม่มีข้อมูล กรุณานำเข้าที่แท็บ 'นำเข้าข้อมูล'")
        else:
            i1,i2,i3 = st.columns(3)
            kpi(i1,"📋","รายการรับทั้งหมด", f"{len(df_inc):,}")
            kpi(i2,"💰","มูลค่ารับรวม", fmt_thb(df_inc["total_amount"].sum()))
            kpi(i3,"🏪","ผู้จำหน่าย", f"{df_inc['supplier'].nunique()} ราย")
            st.divider()

            si1,si2 = st.columns(2)
            srch_i  = si1.text_input("🔍 ค้นหาสินค้า / เลขที่เอกสาร",
                                      value=st.session_state.drill_stock or "")
            sel_sup2 = si2.selectbox("ผู้จำหน่าย",
                                      ["ทั้งหมด"] + sorted(df_inc["supplier"].dropna().unique().tolist()))

            di2 = df_inc.copy()
            if srch_i:  di2 = di2[di2["stock_name"].str.contains(srch_i,case=False,na=False) |
                                   di2["doc_number"].str.contains(srch_i,case=False,na=False)]
            if sel_sup2 != "ทั้งหมด": di2 = di2[di2["supplier"]==sel_sup2]

            show_i = di2[["receive_date","doc_number","stock_id","stock_name",
                           "supplier","qty","unit","unit_price","total_amount","expire_date","operator"]].copy()
            show_i.columns = ["วันที่รับ","เลขที่เอกสาร","รหัสสินค้า","ชื่อสินค้า",
                               "ผู้จำหน่าย","จำนวน","หน่วย","ราคา/หน่วย","รวมเงิน","วันหมดอายุ","ผู้ทำรายการ"]
            st.dataframe(show_i, use_container_width=True, height=420, hide_index=True)
            st.caption(f"แสดง {len(di2):,} จาก {len(df_inc):,} รายการ")

            # Incoming by supplier chart
            st.markdown("## 🏪 มูลค่ารับตามผู้จำหน่าย")
            supp_val = df_inc.groupby("supplier")["total_amount"].sum().reset_index()
            supp_val = supp_val.sort_values("total_amount",ascending=False).head(8)
            fig_sup = go.Figure(go.Bar(
                x=supp_val["supplier"], y=supp_val["total_amount"],
                marker=dict(color=supp_val["total_amount"],
                             colorscale=[[0,"#DBEAFE"],[1,"#1D4ED8"]], line_width=0),
                hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
            ))
            fig_sup.update_layout(**CHART_BG, height=260, coloraxis_showscale=False)
            st.plotly_chart(fig_sup, use_container_width=True)

    with tab3:
        st.markdown("## ⬆️ นำเข้าข้อมูลจากไฟล์ XLS")
        col_a, col_b = st.columns(2)

        with col_a:
            st.markdown("#### 📋 รายการสินค้าทั้งหมด")
            st.caption("`รายการสินค้าทั้งหมด.xls`")
            up_items = st.file_uploader("เลือกไฟล์", type=["xls","xlsx"], key="up_items")
            if up_items:
                try:
                    df_pv = read_xls_bytes(up_items.read())
                    st.success(f"พบ {len(df_pv):,} รายการ")
                    st.dataframe(df_pv.head(5), use_container_width=True)
                    if st.button("✅ นำเข้ารายการสินค้า", use_container_width=True):
                        import_stock_items(df_pv)
                        st.success("นำเข้าสำเร็จ!")
                        st.cache_data.clear(); st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

        with col_b:
            st.markdown("#### 🚚 ประวัติรับสินค้าเข้า Stock")
            st.caption("`การรับสินค้าเข้า Stock.xls`")
            up_inc = st.file_uploader("เลือกไฟล์", type=["xls","xlsx"], key="up_incoming")
            if up_inc:
                try:
                    df_pv2 = read_xls_bytes(up_inc.read())
                    st.success(f"พบ {len(df_pv2):,} รายการ")
                    st.dataframe(df_pv2.head(5), use_container_width=True)
                    if st.button("✅ นำเข้าประวัติรับสินค้า", use_container_width=True):
                        import_stock_incoming(df_pv2)
                        st.success("นำเข้าสำเร็จ!")
                        st.cache_data.clear(); st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")

        st.divider()
        df_si2 = load_stock_items()
        if not df_si2.empty:
            st.download_button("⬇️ Export สต๊อก CSV",
                               data=df_si2.to_csv(index=False).encode("utf-8-sig"),
                               file_name="stock_items.csv", mime="text/csv")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — FINANCIAL REPORT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📄  รายงานการเงิน":
    st.title("📄 รายงานทางการเงิน")

    default_pdf = os.path.join(os.path.dirname(__file__), "รายงานทางการเงิน.pdf")
    pdf_bytes, pdf_name = None, None

    up_pdf = st.file_uploader("📎 อัปโหลด PDF รายงาน (หรือใช้ไฟล์เดิมในโฟลเดอร์อัตโนมัติ)", type=["pdf"])
    if up_pdf:
        pdf_bytes = up_pdf.read(); pdf_name = up_pdf.name
    elif os.path.exists(default_pdf):
        with open(default_pdf,"rb") as f: pdf_bytes = f.read()
        pdf_name = "รายงานทางการเงิน.pdf"
        st.info(f"📂 โหลดไฟล์อัตโนมัติ: **{pdf_name}**")

    if not pdf_bytes:
        st.warning("กรุณาอัปโหลด PDF รายงานทางการเงิน")
        st.stop()

    with st.spinner("กำลังวิเคราะห์ PDF..."):
        info, summary_df, items_df = parse_pdf(pdf_bytes)

    # ── KPI row from PDF ───────────────────────────────────────────────────────
    k1,k2,k3,k4,k5 = st.columns(5)
    kpi(k1,"💰","รายรับรวม (ไม่รวมมัดจำ)", fmt_thb(summary_df["amount"].sum() if not summary_df.empty else 0))
    kpi(k2,"💵","ชำระด้วยเงินสด",          fmt_thb(info.get("cash",0)))
    kpi(k3,"📲","ชำระด้วยการโอน",           fmt_thb(info.get("transfer",0)))
    kpi(k4,"🧾","ใบเสร็จทั้งหมด",           f"{info.get('receipts',0):,} ใบ")
    kpi(k5,"❌","ใบเสร็จยกเลิก",           f"{info.get('cancelled',0):,} ใบ")
    st.divider()

    view_tab, raw_tab = st.tabs(["📊 วิเคราะห์ข้อมูล", "📃 ดู PDF ต้นฉบับ"])

    with view_tab:
        if summary_df.empty:
            st.warning("ไม่พบข้อมูลสรุปรายได้ในไฟล์นี้")
        else:
            # Assign colors
            summary_df["color"] = summary_df["category"].map(lambda c: color_for(c))
            summary_df = summary_df.sort_values("amount", ascending=False)

            ch1, ch2 = st.columns([2,1])
            with ch1:
                st.markdown("## 💰 รายรับแยกตามหมวดหมู่")
                st.caption("คลิกที่แท่งเพื่อ drilldown รายการสินค้า")
                fig_sum = go.Figure(go.Bar(
                    x=summary_df["category"], y=summary_df["amount"],
                    marker=dict(color=summary_df["color"].tolist(), line_width=0),
                    hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
                    text=summary_df["amount"].map(lambda v: fmt_thb(v)),
                    textposition="outside", textfont=dict(size=11, color="#A8D5BF"),
                ))
                fig_sum.update_layout(**CHART_BG, height=340)
                ev_sum = st.plotly_chart(fig_sum, key="bar_pdf_cat", on_select="rerun",
                                         use_container_width=True)
                pts_sum = ev_sum.selection.points if ev_sum.selection else []
                if pts_sum:
                    pt_s2 = pts_sum[0]
                    xv    = pt_s2.get("x") if isinstance(pt_s2,dict) else getattr(pt_s2,"x",None)
                    if xv: st.session_state.drill_pdf_cat = xv

            with ch2:
                st.markdown("## 🥧 สัดส่วนรายรับ")
                fig_p = go.Figure(go.Pie(
                    labels=summary_df["category"], values=summary_df["amount"],
                    hole=0.5,
                    marker_colors=summary_df["color"].tolist(),
                    textposition="inside", textinfo="percent",
                    hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<br>%{percent}<extra></extra>",
                ))
                fig_p.update_layout(**{**CHART_BG, "margin": dict(t=10,b=10,l=10,r=10)},
                                     height=340, showlegend=True, legend=LEGEND_STYLE)
                ev_p2 = st.plotly_chart(fig_p, key="pie_pdf", on_select="rerun",
                                        use_container_width=True)
                pts_p2 = ev_p2.selection.points if ev_p2.selection else []
                if pts_p2:
                    pt_p2 = pts_p2[0]
                    lbl2  = pt_p2.get("label") if isinstance(pt_p2,dict) else getattr(pt_p2,"label",None)
                    if not lbl2:
                        lbl2 = pt_p2.get("x") if isinstance(pt_p2,dict) else getattr(pt_p2,"x",None)
                    if lbl2: st.session_state.drill_pdf_cat = lbl2

            # Payment method chart
            st.divider()
            st.markdown("## 💳 ช่องทางการชำระเงิน")
            pay_df = pd.DataFrame([
                {"method":"เงินสด",             "amount": info.get("cash",0),     "color":"#4ADE80"},
                {"method":"โอนเงินผ่านบัญชี",    "amount": info.get("transfer",0), "color":"#38BDF8"},
            ])
            fig_pay = go.Figure(go.Bar(
                x=pay_df["method"], y=pay_df["amount"],
                marker=dict(color=pay_df["color"].tolist(), line_width=0),
                text=pay_df["amount"].map(lambda v: fmt_thb(v)),
                textposition="outside", textfont=dict(size=13, color="#E2F5EB"),
                hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
                width=[0.4, 0.4],
            ))
            fig_pay.update_layout(**CHART_BG, height=260)
            st.plotly_chart(fig_pay, use_container_width=True)

            # ── Drilldown items table ────────────────────────────────────────
            if not items_df.empty:
                st.divider()
                items_filtered = items_df.copy()
                drill_lbl = None

                if st.session_state.drill_pdf_cat:
                    items_filtered = items_filtered[
                        items_filtered["category"] == st.session_state.drill_pdf_cat
                    ]
                    drill_lbl = st.session_state.drill_pdf_cat

                if drill_lbl:
                    drill_badge(drill_lbl, "pdf")

                title = f"## 🗃️ รายการสินค้า{' — '+drill_lbl if drill_lbl else ' ทั้งหมด'}"
                st.markdown(title)

                if not items_filtered.empty:
                    # Top items chart
                    top_items = items_filtered.groupby("item_name")["total"].sum().reset_index()
                    top_items = top_items.sort_values("total",ascending=False).head(15)

                    fig_ti = go.Figure(go.Bar(
                        x=top_items["total"], y=top_items["item_name"],
                        orientation="h",
                        marker=dict(
                            color=top_items["total"],
                            colorscale=[[0,"#DBEAFE"],[0.5,"#3B82F6"],[1,"#1D4ED8"]],
                            line_width=0
                        ),
                        hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>",
                    ))
                    fig_ti.update_layout(**CHART_BG, height=max(300, len(top_items)*28),
                                          coloraxis_showscale=False)
                    st.plotly_chart(fig_ti, use_container_width=True)

                    # Full table
                    disp_items = items_filtered[["category","item_name","qty","unit","total"]].copy()
                    disp_items = disp_items.sort_values("total",ascending=False)
                    disp_items.columns = ["หมวด","ชื่อสินค้า/บริการ","จำนวนขาย","หน่วย","ยอดรวม (฿)"]
                    st.dataframe(disp_items, use_container_width=True, height=400, hide_index=True)
                    st.caption(f"แสดง {len(items_filtered):,} รายการ  •  รวม ฿{items_filtered['total'].sum():,.0f}")
                else:
                    st.info(f"ไม่มีรายการในหมวด '{drill_lbl}'")

    with raw_tab:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            total_pg = len(pdf.pages)
            st.markdown(f"**{pdf_name}** &nbsp;•&nbsp; {total_pg} หน้า")
            pc1,pc2,_ = st.columns([1,1,4])
            pg_from = pc1.number_input("หน้าเริ่ม", 1, total_pg, 1)
            pg_to   = pc2.number_input("หน้าสุดท้าย", 1, total_pg, min(5, total_pg))

            for i in range(int(pg_from)-1, int(pg_to)):
                pg   = pdf.pages[i]
                text = pg.extract_text() or ""
                lines = [" ".join(l.split()) for l in text.split("\n") if l.strip()]
                st.markdown(f"**— หน้า {i+1} —**")
                st.markdown(
                    "<div style='background:#F8FAFC;border:1px solid #E2E8F0;"
                    "border-radius:10px;padding:14px 18px;font-size:.83rem;"
                    "color:#334155;white-space:pre-wrap;line-height:1.8;"
                    "font-family:monospace'>" + "\n".join(lines) + "</div>",
                    unsafe_allow_html=True,
                )
                st.markdown("")

        st.download_button("⬇️ ดาวน์โหลด PDF",
                           data=pdf_bytes, file_name=pdf_name, mime="application/pdf")
