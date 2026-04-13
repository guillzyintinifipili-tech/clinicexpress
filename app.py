import os, io, re
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import pdfplumber
import xlrd

from db import (
    init_db, fetch_all, bulk_insert_from_df,
    fetch_stock_items, import_stock_items,
    fetch_stock_incoming, import_stock_incoming,
    UPLOADS_DIR,
)

# ─── Config ───────────────────────────────────────────────────────────────────
st.set_page_config(page_title="เอสพี รักษาสัตว์", page_icon="🐾",
                   layout="wide", initial_sidebar_state="expanded")

# ─── CSS (Light theme, purple accent #7C3AED) ─────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.stApp { background: #F1F5F9; }
.main .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }

[data-testid="stSidebar"] {
    background: #FFFFFF;
    border-right: 1px solid #E2E8F0;
    box-shadow: 2px 0 8px rgba(0,0,0,0.06);
}
[data-testid="stSidebar"] * { color: #334155 !important; }
[data-testid="stSidebar"] hr { border-color: #E2E8F0 !important; }
[data-testid="stSidebarNav"] { display: none; }

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
    background: #EDE9FE !important;
    border-color: #7C3AED !important;
    color: #5B21B6 !important;
}

[data-testid="stMetric"] {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 16px;
    padding: 18px 22px 14px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
[data-testid="stMetricLabel"] { color: #64748B !important; font-size: 0.78rem !important; letter-spacing: .06em; text-transform: uppercase; }
[data-testid="stMetricValue"] { color: #0F172A !important; font-size: 1.8rem !important; font-weight: 700 !important; }

h1 { color: #7C3AED !important; font-size: 1.6rem !important; font-weight: 700 !important; letter-spacing: -.03em; }
h2 { color: #0F766E !important; font-size: 1.1rem !important; font-weight: 600 !important; }
h3 { color: #1D4ED8 !important; font-size: 0.95rem !important; font-weight: 600 !important; }

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
    color: #7C3AED !important;
    border-color: #E2E8F0 !important;
    border-bottom: 2px solid #7C3AED !important;
}

[data-testid="stExpander"] {
    border: 1px solid #E2E8F0 !important;
    border-radius: 12px !important;
    background: #FFFFFF !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
[data-testid="stExpanderToggleIcon"] { color: #64748B !important; }

.stButton > button {
    background: #F5F3FF !important; color: #7C3AED !important;
    border: 1px solid #DDD6FE !important; border-radius: 8px !important;
    font-size: 0.85rem !important; font-weight: 500 !important;
    transition: all 0.15s;
}
.stButton > button:hover {
    background: #7C3AED !important; color: #FFFFFF !important;
    border-color: #7C3AED !important;
}

[data-testid="stDownloadButton"] > button {
    background: #F0FDF4 !important; color: #15803D !important;
    border: 1px solid #BBF7D0 !important; border-radius: 8px !important;
}

[data-testid="stTextInput"] input,
[data-testid="stNumberInput"] input { background: #FFFFFF !important; color: #0F172A !important; border-color: #CBD5E1 !important; border-radius: 8px !important; }
[data-baseweb="select"] { background: #FFFFFF !important; border-color: #CBD5E1 !important; border-radius: 8px !important; }
label { color: #475569 !important; font-size: 0.82rem !important; }

[data-testid="stDataFrame"] {
    border: 1px solid #E2E8F0;
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}

hr { border-color: #E2E8F0 !important; margin: 1.2rem 0 !important; }

#MainMenu, footer { visibility: hidden; }

.drill-badge {
    display: inline-flex; align-items: center; gap: 8px;
    background: #EDE9FE; border: 1px solid #7C3AED;
    border-radius: 20px; padding: 4px 14px;
    color: #5B21B6; font-size: 0.82rem; font-weight: 600;
    margin-bottom: 12px;
}

.section-card {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 16px;
    padding: 20px 24px;
    margin-bottom: 16px;
    box-shadow: 0 1px 4px rgba(0,0,0,0.05);
}

.dot { display:inline-block; width:10px; height:10px; border-radius:50%; margin-right:6px; }
</style>
""", unsafe_allow_html=True)

init_db()

# ─── Chart constants ───────────────────────────────────────────────────────────
CHART_BG = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font_color="#374151",
    xaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B"),
    yaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B"),
)

def ch(**overrides):
    """Merge CHART_BG with overrides (margin safe)."""
    d = {**CHART_BG}
    d.update(overrides)
    if "margin" not in d:
        d["margin"] = dict(t=16, b=16, l=8, r=8)
    return d

LEGEND_STYLE = dict(
    bgcolor="rgba(255,255,255,0.8)",
    font_size=11,
    font_color="#374151",
    bordercolor="#E2E8F0",
    borderwidth=1,
)

CAT_COLORS = {
    "รายการยา":               "#38BDF8",
    "อุปกรณ์และเวชภัณฑ์":     "#4ADE80",
    "ค่าบริการทางการแพทย์":   "#FBBF24",
    "ค่าผ่าตัด":               "#F472B6",
    "อุปกรณ์ตรวจ LAB":        "#22D3EE",
    "ค่าตรวจรักษา":           "#A78BFA",
    "น้ำเกลือ":                "#67E8F9",
    "ค่าบริการอื่นๆ":          "#FB923C",
    "สินค้า Pet Shop":         "#E879F9",
    "ยาและเวชภัณฑ์":           "#EC4899",
    "บริการตรวจรักษา":         "#F97316",
    "เอกซเรย์ / Lab":          "#3B82F6",
    "ผ่าตัดและหัตถการ":        "#8B5CF6",
    "วัคซีน":                  "#10B981",
    "อาบน้ำ-ตัดขน":           "#F59E0B",
    "รับฝากสัตว์":             "#06B6D4",
    "อาหาร-อุปกรณ์":           "#84CC16",
}

def color_for(cat: str) -> str:
    return CAT_COLORS.get(cat, "#94A3B8")

# ─── Session state ─────────────────────────────────────────────────────────────
for _k, _v in [("drill_pdf_cat", None)]:
    if _k not in st.session_state:
        st.session_state[_k] = _v

# ─── Helper functions ──────────────────────────────────────────────────────────
def fmt_thb(v: float) -> str:
    return f"฿{v:,.0f}"

def stock_status(row):
    if row["qty"] <= 0:
        return "🔴 หมดสต๊อก", "#FEE2E2", "#991B1B"
    if row["qty"] <= row["alert_qty"]:
        return "🟡 ใกล้หมด", "#FEF3C7", "#92400E"
    return "🟢 ปกติ", "#D1FAE5", "#065F46"

def read_xls_bytes(file_bytes) -> pd.DataFrame:
    wb = xlrd.open_workbook(file_contents=file_bytes)
    sh = wb.sheet_by_index(0)
    rows = [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    hi = 0
    for i, row in enumerate(rows):
        f = str(row[0]).strip()
        if "Stock Id" in f or "วันที่รับ" in f:
            hi = i
            break
        if f and not any(f.startswith(x) for x in ["ราย", "ช่วง"]) and f != "":
            if any(str(c).strip() for c in row[1:]):
                hi = i
                break
    headers = [str(c).strip() for c in rows[hi]]
    return pd.DataFrame(rows[hi + 1:], columns=headers)


# ─── PDF parsing ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=300, show_spinner=False)
def parse_pdf(pdf_bytes: bytes):
    """Returns (info, summary_df, items_df) from financial report PDF."""
    info = {"cash": 0, "transfer": 0, "total": 0, "receipts": 0, "cancelled": 0,
            "cost": 0, "gross_profit": 0, "clinic": "", "period": ""}
    summary_rows = []
    item_rows = []

    CAT_CLEAN = {
        "รายการยา":                "รายการยา",
        "อปุ กรณแ์ ละเวชภณั ฑ์":   "อุปกรณ์และเวชภัณฑ์",
        "คา่ บรกิ ารทางการแพทย์":   "ค่าบริการทางการแพทย์",
        "คา่ ผา่ ตดั":              "ค่าผ่าตัด",
        "อปุ กรณต์ รวจ LAB":        "อุปกรณ์ตรวจ LAB",
        "คา่ ตรวจรกั ษา":           "ค่าตรวจรักษา",
        "นํา\x00 เกลอื":            "น้ำเกลือ",
        "นํา เกลอื":                "น้ำเกลือ",
        "นํ าเกลอื":                "น้ำเกลือ",
        "คา่ บรกิ ารอนื\x00 ๆ":     "ค่าบริการอื่นๆ",
        "คา่ บรกิ ารอนื ๆ":         "ค่าบริการอื่นๆ",
        "สนิ คา้ Pet Shop":         "สินค้า Pet Shop",
    }

    def clean_cat(raw):
        for k, v in CAT_CLEAN.items():
            if k.replace("\x00", "") in raw.replace("\x00", ""):
                return v
        return raw.strip()

    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            t1 = " ".join((pdf.pages[0].extract_text() or "").split())
            m = re.search(r"รวมใบเสร็จถ[^\d]*([\d]+)\s*รายการ\s*รวมใบเสร็จทง[^\d]*([\d]+)", t1)
            if m:
                info["cancelled"] = int(m.group(1))
                info["receipts"]  = int(m.group(2))
            m = re.search(r"เงนิ สด[^\d]*([\d,]+)", t1)
            if m: info["cash"] = float(m.group(1).replace(",", ""))
            m = re.search(r"โอนเงนิ[^\d]*([\d,]+)", t1)
            if m: info["transfer"] = float(m.group(1).replace(",", ""))
            m = re.search(r"รวมเป.*?นเงนิ\s+([\d,]+)", t1)
            if m: info["total"] = float(m.group(1).replace(",", ""))
            m = re.search(r"ต้นทุน[^\d]*([\d,]+)", t1)
            if m: info["cost"] = float(m.group(1).replace(",", ""))
            m = re.search(r"กำไรขั้นต้น[^\d]*([\d,]+)", t1)
            if m: info["gross_profit"] = float(m.group(1).replace(",", ""))

            if len(pdf.pages) > 1:
                words2 = pdf.pages[1].extract_words()
                lines_pg2 = {}
                for w in words2:
                    y = round(w["top"] / 3) * 3
                    lines_pg2.setdefault(y, []).append(w)
                for y in sorted(lines_pg2):
                    words_in_line = sorted(lines_pg2[y], key=lambda w: w["x0"])
                    line_text = " ".join(w["text"] for w in words_in_line)
                    m = re.search(r"([\d,]+\.\d{2})\s*บาท", line_text)
                    if m:
                        amt = float(m.group(1).replace(",", ""))
                        cat_raw = line_text[:line_text.index(m.group(0))].strip()
                        if amt > 0 and cat_raw and "ยอดรวม" not in cat_raw:
                            summary_rows.append({"category": clean_cat(cat_raw), "amount": amt})

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
                    if "รายการ" in item_name and "จํ" in qty_unit:
                        continue
                    try:
                        total = float(re.sub(r"[^\d\.]", "", total_str))
                        qty_m = re.match(r"([\d,\.]+)", qty_unit)
                        qty   = float(qty_m.group(1).replace(",", "")) if qty_m else 0.0
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

    summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(columns=["category", "amount"])
    items_df   = pd.DataFrame(item_rows)    if item_rows    else pd.DataFrame(columns=["category", "item_name", "qty", "unit", "total"])
    return info, summary_df, items_df


# ─── Data loaders ──────────────────────────────────────────────────────────────
DEFAULT_PDF = os.path.join(os.path.dirname(__file__), "รายงานทางการเงิน.pdf")

@st.cache_data(ttl=60)
def load_stock_items() -> pd.DataFrame:
    return fetch_stock_items()

@st.cache_data(ttl=60)
def load_stock_incoming() -> pd.DataFrame:
    return fetch_stock_incoming()

def load_pdf_data():
    """Load PDF bytes from session state upload or default file."""
    if "pdf_bytes" in st.session_state and st.session_state["pdf_bytes"]:
        return st.session_state["pdf_bytes"]
    if os.path.exists(DEFAULT_PDF):
        with open(DEFAULT_PDF, "rb") as f:
            return f.read()
    return None


# ─── KPI card ─────────────────────────────────────────────────────────────────
def kpi(col, icon, label, value, note=None, accent="#7C3AED"):
    note_html = f'<p style="margin:4px 0 0;color:#94A3B8;font-size:.72rem">{note}</p>' if note else ""
    col.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-top:3px solid {accent};
                border-radius:12px;padding:16px 20px 12px;height:100%;min-height:100px;
                box-shadow:0 1px 4px rgba(0,0,0,0.06)">
      <p style="margin:0;color:#94A3B8;font-size:.70rem;font-weight:600;
                letter-spacing:.07em;text-transform:uppercase">{label}</p>
      <p style="margin:6px 0 0;color:#0F172A;font-size:1.65rem;font-weight:700;line-height:1.1">{value}</p>
      {note_html}
    </div>""", unsafe_allow_html=True)


def drill_badge(label: str, key: str):
    c1, c2 = st.columns([6, 1])
    c1.markdown(f'<div class="drill-badge">🔍 Drilldown: <strong>{label}</strong></div>',
                unsafe_allow_html=True)
    if c2.button("✕ ล้าง", key=f"clr_{key}"):
        st.session_state["drill_pdf_cat"] = None
        st.rerun()


# ─── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:16px 0 8px">
      <div style="font-size:1.4rem;font-weight:700;color:#7C3AED;letter-spacing:-.02em">
        🐾 เอสพี รักษาสัตว์
      </div>
      <div style="font-size:.75rem;color:#94A3B8;margin-top:2px">ระบบจัดการคลินิกสัตว์</div>
    </div>""", unsafe_allow_html=True)
    st.divider()

    page = st.radio("เมนู", [
        "📊 ภาพรวมธุรกิจ",
        "📄 รายงานการเงิน",
        "💊 คลังยา & สินค้า",
        "⬆️ นำเข้าไฟล์",
    ], label_visibility="collapsed")
    st.divider()

    # Quick stats from loaded data
    pdf_bytes_sb = load_pdf_data()
    if pdf_bytes_sb:
        try:
            info_sb, _, _ = parse_pdf(pdf_bytes_sb)
            total_sb = info_sb.get("total", 0)
            st.markdown(f'<p style="margin:2px 0;color:#334155;font-size:.85rem">💰 รายรับรวม &nbsp;<strong style="color:#10B981">{fmt_thb(total_sb)}</strong></p>', unsafe_allow_html=True)
        except Exception:
            pass

    df_si_sb = load_stock_items()
    if not df_si_sb.empty:
        low_sb = int(((df_si_sb["qty"] <= df_si_sb["alert_qty"]) & (df_si_sb["qty"] > 0)).sum())
        out_sb = int((df_si_sb["qty"] <= 0).sum())
        val_sb = (df_si_sb["qty"] * df_si_sb["avg_cost"]).sum()
        if low_sb:
            st.markdown(f'<p style="margin:4px 0;color:#D97706;font-size:.82rem">⚠️ ใกล้หมด <strong>{low_sb}</strong> รายการ</p>', unsafe_allow_html=True)
        if out_sb:
            st.markdown(f'<p style="margin:4px 0;color:#DC2626;font-size:.82rem">🚨 หมดสต๊อก <strong>{out_sb}</strong> รายการ</p>', unsafe_allow_html=True)
        st.markdown(f'<p style="margin:4px 0;color:#334155;font-size:.82rem">📦 มูลค่าคลัง <strong style="color:#7C3AED">{fmt_thb(val_sb)}</strong></p>', unsafe_allow_html=True)

    st.divider()
    st.caption("SQLite Local  •  v4.0")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW DASHBOARD
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 ภาพรวมธุรกิจ":
    st.title("📊 ภาพรวมธุรกิจ")

    pdf_bytes = load_pdf_data()
    df_stock  = load_stock_items()

    # Load PDF data
    info, summary_df, items_df = ({}, pd.DataFrame(), pd.DataFrame())
    if pdf_bytes:
        info, summary_df, items_df = parse_pdf(pdf_bytes)
    else:
        st.info("ยังไม่มีข้อมูล PDF — กรุณาไปที่หน้า ⬆️ นำเข้าไฟล์ เพื่ออัปโหลดรายงานการเงิน")

    total_rev   = info.get("total", 0)
    cash_val    = info.get("cash", 0)
    transfer_val= info.get("transfer", 0)
    receipts    = info.get("receipts", 0)
    cancelled   = info.get("cancelled", 0)
    cost_val    = info.get("cost", 0)
    gross_profit = info.get("gross_profit", 0)
    if gross_profit == 0 and total_rev > 0:
        gross_profit = total_rev - cost_val
    margin_pct  = (gross_profit / total_rev * 100) if total_rev > 0 else 0

    # Row 1: KPI from PDF — รายรับรวม, กำไรสุทธิ, ต้นทุน, อัตรากำไร%
    st.markdown("### ข้อมูลจากรายงานการเงิน (PDF)")
    r1c1, r1c2, r1c3, r1c4 = st.columns(4)
    kpi(r1c1, "💰", "รายรับรวม",    fmt_thb(total_rev),    accent="#10B981")
    kpi(r1c2, "📈", "กำไรสุทธิ",    fmt_thb(gross_profit), accent="#7C3AED")
    kpi(r1c3, "📤", "ต้นทุน",        fmt_thb(cost_val),     accent="#EF4444")
    kpi(r1c4, "📊", "อัตรากำไร",    f"{margin_pct:.1f}%",  accent="#3B82F6")
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Row 2: KPI from PDF — เงินสด, โอนเงิน, จำนวนใบเสร็จ, ยกเลิก
    r2c1, r2c2, r2c3, r2c4 = st.columns(4)
    kpi(r2c1, "💵", "เงินสด",         fmt_thb(cash_val),     accent="#F59E0B")
    kpi(r2c2, "🏦", "โอนเงิน",        fmt_thb(transfer_val), accent="#06B6D4")
    kpi(r2c3, "🧾", "จำนวนใบเสร็จ",  f"{receipts} ใบ",      accent="#84CC16")
    kpi(r2c4, "❌", "ยกเลิก",         f"{cancelled} ใบ",     accent="#94A3B8")
    st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

    # Row 3: KPI from XLS stock
    st.markdown("### ข้อมูลจากคลังสินค้า (XLS)")
    if not df_stock.empty:
        total_items  = len(df_stock)
        stock_value  = (df_stock["qty"] * df_stock["avg_cost"]).sum()
        near_empty   = int(((df_stock["qty"] <= df_stock["alert_qty"]) & (df_stock["qty"] > 0)).sum())
        out_of_stock = int((df_stock["qty"] <= 0).sum())
        r3c1, r3c2, r3c3, r3c4 = st.columns(4)
        kpi(r3c1, "📦", "รายการสินค้า",   f"{total_items:,} รายการ", accent="#7C3AED")
        kpi(r3c2, "💰", "มูลค่าคลัง",     fmt_thb(stock_value),      accent="#10B981")
        kpi(r3c3, "⚠️", "ใกล้หมด",        f"{near_empty} รายการ",    accent="#F59E0B")
        kpi(r3c4, "🚨", "หมดสต๊อก",       f"{out_of_stock} รายการ",  accent="#EF4444")
    else:
        st.info("ยังไม่มีข้อมูลคลังสินค้า — กรุณาไปที่หน้า ⬆️ นำเข้าไฟล์ เพื่ออัปโหลดไฟล์สินค้า")

    st.divider()

    # Charts row
    ch_col1, ch_col2 = st.columns([55, 45])

    # Revenue by category bar chart (from PDF page 2)
    with ch_col1:
        st.markdown("## รายได้แยกตามหมวดหมู่")
        if not summary_df.empty:
            sum_sorted = summary_df.sort_values("amount", ascending=False)
            colors = [color_for(c) for c in sum_sorted["category"]]
            fig_bar = go.Figure(go.Bar(
                x=sum_sorted["category"],
                y=sum_sorted["amount"],
                marker_color=colors,
                marker_line_width=0,
                hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
            ))
            fig_bar.update_layout(**ch(height=300, showlegend=False,
                                       margin=dict(t=16, b=60, l=8, r=8)))
            fig_bar.update_xaxes(tickangle=-30)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("ไม่มีข้อมูลหมวดหมู่รายได้จาก PDF")

    # Stock status donut
    with ch_col2:
        st.markdown("## สถานะคลังสินค้า")
        if not df_stock.empty:
            ok_cnt   = int(((df_stock["qty"] > df_stock["alert_qty"])).sum())
            low_cnt  = int(((df_stock["qty"] <= df_stock["alert_qty"]) & (df_stock["qty"] > 0)).sum())
            out_cnt  = int((df_stock["qty"] <= 0).sum())
            fig_donut = go.Figure(go.Pie(
                labels=["🟢 ปกติ", "🟡 ใกล้หมด", "🔴 หมดสต๊อก"],
                values=[ok_cnt, low_cnt, out_cnt],
                hole=0.55,
                marker_colors=["#10B981", "#F59E0B", "#EF4444"],
                textposition="inside",
                textinfo="percent+value",
                hovertemplate="<b>%{label}</b><br>%{value} รายการ (%{percent})<extra></extra>",
            ))
            fig_donut.update_layout(**ch(
                height=300, showlegend=True,
                legend=dict(orientation="h", yanchor="top", y=-0.1,
                            xanchor="center", x=0.5, **LEGEND_STYLE),
                margin=dict(t=10, b=60, l=10, r=10),
                annotations=[dict(text=f"<b>{len(df_stock)}</b><br>รายการ",
                                  x=0.5, y=0.5, font_size=14,
                                  font_color="#374151", showarrow=False)],
            ))
            st.plotly_chart(fig_donut, use_container_width=True)
        else:
            st.info("ยังไม่มีข้อมูลคลังสินค้า")

    st.divider()

    # Top 15 items sold (from PDF pages 3+)
    if not items_df.empty:
        st.markdown("## Top 15 สินค้าขายดี (จาก PDF)")
        top15 = items_df.groupby("item_name")["total"].sum().nlargest(15).reset_index()
        top15 = top15.sort_values("total", ascending=True)
        fig_top = go.Figure(go.Bar(
            y=top15["item_name"],
            x=top15["total"],
            orientation="h",
            marker_color="#7C3AED",
            marker_line_width=0,
            hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>",
        ))
        fig_top.update_layout(**ch(height=420, showlegend=False,
                                   margin=dict(t=16, b=16, l=8, r=8)))
        st.plotly_chart(fig_top, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — FINANCIAL REPORT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📄 รายงานการเงิน":
    st.title("📄 รายงานการเงิน")

    pdf_bytes = load_pdf_data()
    if not pdf_bytes:
        st.warning("ยังไม่มีไฟล์รายงานการเงิน PDF — กรุณาไปที่หน้า ⬆️ นำเข้าไฟล์ เพื่ออัปโหลด")
        st.stop()

    info, summary_df, items_df = parse_pdf(pdf_bytes)

    total_rev    = info.get("total", 0)
    cash_val     = info.get("cash", 0)
    transfer_val = info.get("transfer", 0)
    receipts     = info.get("receipts", 0)
    cancelled    = info.get("cancelled", 0)

    # KPI row
    kc1, kc2, kc3, kc4, kc5 = st.columns(5)
    kpi(kc1, "💰", "รายรับรวม",     fmt_thb(total_rev),    accent="#10B981")
    kpi(kc2, "💵", "เงินสด",         fmt_thb(cash_val),     accent="#F59E0B")
    kpi(kc3, "🏦", "โอนเงิน",        fmt_thb(transfer_val), accent="#06B6D4")
    kpi(kc4, "🧾", "จำนวนใบเสร็จ",  f"{receipts} ใบ",      accent="#7C3AED")
    kpi(kc5, "❌", "ยกเลิก",         f"{cancelled} ใบ",     accent="#94A3B8")
    st.divider()

    # Tabs
    tab_chart, tab_table, tab_raw = st.tabs(["📊 กราฟ", "📋 ตารางรายการ", "📄 ข้อความ PDF"])

    with tab_chart:
        if summary_df.empty:
            st.info("ไม่พบข้อมูลหมวดหมู่ในรายงาน PDF")
        else:
            col_bar, col_pie = st.columns(2)
            sum_sorted = summary_df.sort_values("amount", ascending=False)

            # Bar chart with drilldown
            with col_bar:
                st.markdown("## รายได้แยกตามหมวดหมู่")
                if st.session_state.get("drill_pdf_cat"):
                    drill_badge(st.session_state["drill_pdf_cat"], "pdf_cat")
                colors = [color_for(c) for c in sum_sorted["category"]]
                fig_catbar = go.Figure(go.Bar(
                    x=sum_sorted["category"],
                    y=sum_sorted["amount"],
                    marker_color=colors,
                    marker_line_width=0,
                    hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
                ))
                fig_catbar.update_layout(**ch(height=320, showlegend=False,
                                              margin=dict(t=16, b=70, l=8, r=8)))
                fig_catbar.update_xaxes(tickangle=-30)
                ev_catbar = st.plotly_chart(fig_catbar, key="pdf_catbar",
                                            on_select="rerun", use_container_width=True)
                pts = ev_catbar.selection.points if ev_catbar.selection else []
                if pts:
                    pt = pts[0]
                    lbl = pt.get("x") if isinstance(pt, dict) else getattr(pt, "x", None)
                    if lbl:
                        st.session_state["drill_pdf_cat"] = lbl
                        st.rerun()

            # Donut / Pie
            with col_pie:
                st.markdown("## สัดส่วนรายได้ตามหมวดหมู่")
                total_sum = sum_sorted["amount"].sum()
                colors_pie = [color_for(c) for c in sum_sorted["category"]]
                fig_pie = go.Figure(go.Pie(
                    labels=sum_sorted["category"],
                    values=sum_sorted["amount"],
                    hole=0.5,
                    marker_colors=colors_pie,
                    textposition="inside",
                    textinfo="percent",
                    hovertemplate="<b>%{label}</b><br>฿%{value:,.0f} (%{percent})<extra></extra>",
                ))
                fig_pie.update_layout(**ch(
                    height=320, showlegend=True,
                    legend=dict(orientation="v", yanchor="middle", y=0.5,
                                xanchor="left", x=1.02, **LEGEND_STYLE),
                    margin=dict(t=10, b=10, l=10, r=140),
                    annotations=[dict(text=f"<b>฿{total_sum:,.0f}</b>",
                                      x=0.5, y=0.5, font_size=12,
                                      font_color="#374151", showarrow=False)],
                ))
                st.plotly_chart(fig_pie, use_container_width=True)

            # Payment method bar
            st.markdown("## วิธีการชำระเงิน")
            fig_pay = go.Figure(go.Bar(
                x=["เงินสด", "โอนเงิน"],
                y=[cash_val, transfer_val],
                marker_color=["#F59E0B", "#06B6D4"],
                marker_line_width=0,
                text=[fmt_thb(cash_val), fmt_thb(transfer_val)],
                textposition="outside",
                hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
            ))
            fig_pay.update_layout(**ch(height=220, showlegend=False))
            st.plotly_chart(fig_pay, use_container_width=True)

            # Drilldown items table
            drill_cat = st.session_state.get("drill_pdf_cat")
            if drill_cat and not items_df.empty:
                st.divider()
                st.markdown(f"## รายการสินค้าในหมวด: {drill_cat}")
                drill_items = items_df[items_df["category"] == drill_cat].copy()
                drill_items = drill_items.sort_values("total", ascending=False)
                st.dataframe(
                    drill_items[["item_name", "qty", "unit", "total"]].rename(columns={
                        "item_name": "ชื่อสินค้า", "qty": "จำนวน",
                        "unit": "หน่วย", "total": "ยอดรวม (฿)"
                    }),
                    use_container_width=True, hide_index=True
                )

    with tab_table:
        st.markdown("## ตารางรายการทั้งหมด")
        if not items_df.empty:
            # Category filter
            cats = ["ทั้งหมด"] + sorted(items_df["category"].unique().tolist())
            sel_cat = st.selectbox("กรองตามหมวดหมู่", cats)
            show_df = items_df if sel_cat == "ทั้งหมด" else items_df[items_df["category"] == sel_cat]
            show_df = show_df.sort_values("total", ascending=False)
            st.dataframe(
                show_df[["category", "item_name", "qty", "unit", "total"]].rename(columns={
                    "category": "หมวดหมู่", "item_name": "ชื่อสินค้า",
                    "qty": "จำนวน", "unit": "หน่วย", "total": "ยอดรวม (฿)"
                }),
                use_container_width=True, hide_index=True
            )
            st.caption(f"แสดง {len(show_df):,} รายการ")
        else:
            st.info("ไม่พบตารางรายการจาก PDF")

    with tab_raw:
        st.markdown("## ข้อความดิบจาก PDF")
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for i, pg in enumerate(pdf.pages):
                    with st.expander(f"หน้า {i+1}", expanded=(i == 0)):
                        txt = pg.extract_text() or "(ไม่มีข้อความ)"
                        st.text(txt)
        except Exception as e:
            st.error(f"ไม่สามารถอ่าน PDF: {e}")

        st.divider()
        st.download_button(
            label="⬇️ ดาวน์โหลด PDF",
            data=pdf_bytes,
            file_name="รายงานทางการเงิน.pdf",
            mime="application/pdf",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — STOCK MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💊 คลังยา & สินค้า":
    st.title("💊 คลังยา & สินค้า")

    tab_items, tab_incoming, tab_import = st.tabs([
        "📦 รายการสินค้า", "📥 ประวัติรับสินค้า", "⬆️ นำเข้าข้อมูล"
    ])

    # ── Tab 1: Stock Items ──────────────────────────────────────────────────
    with tab_items:
        df_stock = load_stock_items()
        if df_stock.empty:
            st.info("ยังไม่มีข้อมูลสินค้า — กรุณานำเข้าไฟล์ รายการสินค้าทั้งหมด.xls")
        else:
            # Ensure numeric
            for col in ["qty", "alert_qty", "avg_cost", "sell_price", "cost_price"]:
                df_stock[col] = pd.to_numeric(df_stock[col], errors="coerce").fillna(0)

            total_items  = len(df_stock)
            stock_value  = (df_stock["qty"] * df_stock["avg_cost"]).sum()
            ok_cnt       = int(((df_stock["qty"] > df_stock["alert_qty"])).sum())
            low_cnt      = int(((df_stock["qty"] <= df_stock["alert_qty"]) & (df_stock["qty"] > 0)).sum())
            out_cnt      = int((df_stock["qty"] <= 0).sum())

            kk1, kk2, kk3, kk4, kk5 = st.columns(5)
            kpi(kk1, "📦", "รายการทั้งหมด", f"{total_items:,}", accent="#7C3AED")
            kpi(kk2, "🟢", "ปกติ",           f"{ok_cnt:,}",     accent="#10B981")
            kpi(kk3, "🟡", "ใกล้หมด",        f"{low_cnt:,}",    accent="#F59E0B")
            kpi(kk4, "🔴", "หมดสต๊อก",       f"{out_cnt:,}",    accent="#EF4444")
            kpi(kk5, "💰", "มูลค่าคลัง",     fmt_thb(stock_value), accent="#06B6D4")
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

            # Filters
            fc1, fc2 = st.columns([2, 3])
            drug_types  = ["ทั้งหมด"] + sorted(df_stock["drug_type"].dropna().unique().tolist())
            sel_drug    = fc1.selectbox("ประเภทยา", drug_types)
            search_term = fc2.text_input("ค้นหาชื่อสินค้า", placeholder="พิมพ์ชื่อสินค้า...")

            filtered = df_stock.copy()
            if sel_drug != "ทั้งหมด":
                filtered = filtered[filtered["drug_type"] == sel_drug]
            if search_term:
                filtered = filtered[filtered["stock_name"].str.contains(search_term, case=False, na=False)]

            # Top 20 by value chart
            st.markdown("## Top 20 สินค้าโดยมูลค่า (qty × ราคาขาย)")
            df_val = df_stock.copy()
            df_val["value"] = df_val["qty"] * df_val["sell_price"]
            top20 = df_val.nlargest(20, "value")[["stock_name", "value"]].sort_values("value", ascending=True)
            fig_top20 = go.Figure(go.Bar(
                y=top20["stock_name"],
                x=top20["value"],
                orientation="h",
                marker_color="#7C3AED",
                marker_line_width=0,
                hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<extra></extra>",
            ))
            fig_top20.update_layout(**ch(height=420, showlegend=False,
                                         margin=dict(t=16, b=16, l=8, r=8)))
            st.plotly_chart(fig_top20, use_container_width=True)

            # Table with status badges
            st.markdown(f"## รายการสินค้า ({len(filtered):,} รายการ)")
            rows_html = ""
            for _, row in filtered.iterrows():
                status_label, bg_color, text_color = stock_status(row)
                rows_html += f"""
                <tr style="border-bottom:1px solid #F1F5F9">
                  <td style="padding:7px 8px;font-size:.8rem;color:#64748B">{row.get('stock_id','')}</td>
                  <td style="padding:7px 8px;font-size:.83rem;font-weight:500">{row.get('stock_name','')}</td>
                  <td style="padding:7px 8px;font-size:.8rem">{row.get('type_name','')}</td>
                  <td style="padding:7px 8px;font-size:.8rem">{row.get('drug_type','')}</td>
                  <td style="padding:7px 8px;text-align:right;font-size:.83rem;font-weight:600">{row.get('qty',0):,.1f}</td>
                  <td style="padding:7px 8px;font-size:.8rem">{row.get('unit','')}</td>
                  <td style="padding:7px 8px;text-align:right;font-size:.83rem">฿{row.get('avg_cost',0):,.2f}</td>
                  <td style="padding:7px 8px;text-align:right;font-size:.83rem">฿{row.get('sell_price',0):,.2f}</td>
                  <td style="padding:7px 8px;font-size:.8rem">{row.get('supplier','')}</td>
                  <td style="padding:7px 8px">
                    <span style="background:{bg_color};color:{text_color};padding:2px 10px;
                                 border-radius:12px;font-size:.75rem;font-weight:600">{status_label}</span>
                  </td>
                </tr>"""
            st.markdown(f"""
            <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;
                        overflow-y:auto;max-height:500px;box-shadow:0 1px 3px rgba(0,0,0,0.05)">
              <table style="width:100%;border-collapse:collapse">
                <thead style="position:sticky;top:0;background:#F8FAFC;z-index:1">
                  <tr style="border-bottom:2px solid #E2E8F0">
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">รหัส</th>
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">ชื่อสินค้า</th>
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">ประเภท</th>
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">ประเภทยา</th>
                    <th style="padding:8px;text-align:right;font-size:.75rem;color:#64748B">จำนวน</th>
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">หน่วย</th>
                    <th style="padding:8px;text-align:right;font-size:.75rem;color:#64748B">ราคาทุนเฉลี่ย</th>
                    <th style="padding:8px;text-align:right;font-size:.75rem;color:#64748B">ราคาขาย</th>
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">ผู้จัดจำหน่าย</th>
                    <th style="padding:8px;text-align:left;font-size:.75rem;color:#64748B">สถานะ</th>
                  </tr>
                </thead>
                <tbody>{rows_html}</tbody>
              </table>
            </div>""", unsafe_allow_html=True)

    # ── Tab 2: Stock Incoming ───────────────────────────────────────────────
    with tab_incoming:
        df_inc = load_stock_incoming()
        if df_inc.empty:
            st.info("ยังไม่มีข้อมูลการรับสินค้า — กรุณานำเข้าไฟล์ การรับสินค้าเข้า Stock.xls")
        else:
            for col in ["qty", "unit_price", "discount", "total_amount"]:
                df_inc[col] = pd.to_numeric(df_inc[col], errors="coerce").fillna(0)

            total_records   = len(df_inc)
            total_value     = df_inc["total_amount"].sum()
            num_suppliers   = df_inc["supplier"].nunique()

            ik1, ik2, ik3 = st.columns(3)
            kpi(ik1, "📋", "จำนวนรายการ",   f"{total_records:,}",    accent="#7C3AED")
            kpi(ik2, "💰", "มูลค่ารวม",     fmt_thb(total_value),    accent="#10B981")
            kpi(ik3, "🏭", "จำนวน Suppliers", f"{num_suppliers:,}",  accent="#F59E0B")
            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

            # Timeline bar chart
            st.markdown("## ยอดรับสินค้าตามวันที่")
            df_inc_dates = df_inc.copy()
            df_inc_dates["receive_date"] = pd.to_datetime(df_inc_dates["receive_date"], errors="coerce")
            df_inc_dates = df_inc_dates.dropna(subset=["receive_date"])
            if not df_inc_dates.empty:
                daily_inc = df_inc_dates.groupby(df_inc_dates["receive_date"].dt.date)["total_amount"].sum().reset_index()
                daily_inc.columns = ["date", "amount"]
                daily_inc = daily_inc.sort_values("date")
                fig_timeline = go.Figure(go.Bar(
                    x=daily_inc["date"].astype(str),
                    y=daily_inc["amount"],
                    marker_color="#7C3AED",
                    marker_line_width=0,
                    hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
                ))
                fig_timeline.update_layout(**ch(height=260, showlegend=False,
                                                 margin=dict(t=16, b=60, l=8, r=8)))
                fig_timeline.update_xaxes(tickangle=-30)
                st.plotly_chart(fig_timeline, use_container_width=True)

            # Table
            st.markdown("## รายการรับสินค้าทั้งหมด")
            show_cols = ["receive_date", "po_number", "doc_number", "stock_name",
                         "supplier", "qty", "unit", "unit_price", "discount",
                         "total_amount", "lot_no", "expire_date", "operator"]
            show_cols = [c for c in show_cols if c in df_inc.columns]
            col_rename = {
                "receive_date": "วันที่รับ", "po_number": "เลขใบสั่งซื้อ",
                "doc_number": "เลขเอกสาร", "stock_name": "ชื่อสินค้า",
                "supplier": "ตัวแทนจำหน่าย", "qty": "จำนวน", "unit": "หน่วย",
                "unit_price": "ราคา/หน่วย", "discount": "ส่วนลด",
                "total_amount": "จำนวนเงิน", "lot_no": "Lot No.",
                "expire_date": "วันหมดอายุ", "operator": "ผู้ทำรายการ",
            }
            st.dataframe(
                df_inc[show_cols].rename(columns=col_rename),
                use_container_width=True, hide_index=True
            )

    # ── Tab 3: Import ───────────────────────────────────────────────────────
    with tab_import:
        st.markdown("## ⬆️ นำเข้าข้อมูลคลังสินค้า")

        st.markdown("### อัปโหลด รายการสินค้าทั้งหมด.xls")
        items_file = st.file_uploader("เลือกไฟล์ รายการสินค้าทั้งหมด.xls",
                                       type=["xls", "xlsx"], key="stock_items_upload_tab3")
        if items_file:
            if st.button("นำเข้าสินค้า", key="import_items_tab3"):
                with st.spinner("กำลังนำเข้าข้อมูลสินค้า..."):
                    df_new = read_xls_bytes(items_file.read())
                    import_stock_items(df_new)
                    load_stock_items.clear()
                st.success(f"นำเข้าสำเร็จ {len(df_new):,} รายการ")
                st.rerun()

        st.divider()

        st.markdown("### อัปโหลด การรับสินค้าเข้า Stock.xls")
        incoming_file = st.file_uploader("เลือกไฟล์ การรับสินค้าเข้า Stock.xls",
                                          type=["xls", "xlsx"], key="stock_incoming_upload_tab3")
        if incoming_file:
            if st.button("นำเข้าประวัติรับสินค้า", key="import_incoming_tab3"):
                with st.spinner("กำลังนำเข้าข้อมูลประวัติรับสินค้า..."):
                    df_new = read_xls_bytes(incoming_file.read())
                    import_stock_incoming(df_new)
                    load_stock_incoming.clear()
                st.success(f"นำเข้าสำเร็จ {len(df_new):,} รายการ")
                st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — IMPORT HUB
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "⬆️ นำเข้าไฟล์":
    st.title("⬆️ นำเข้าไฟล์")
    st.caption("อัปโหลดไฟล์ข้อมูลเพื่อแทนที่ข้อมูลปัจจุบันในระบบ")

    # ── Section 1: PDF Financial Report ──────────────────────────────────────
    st.markdown("## 📄 อัปโหลด PDF รายงานการเงิน")
    pdf_bytes_cur = load_pdf_data()
    if pdf_bytes_cur:
        try:
            _info_cur, _sum_cur, _items_cur = parse_pdf(pdf_bytes_cur)
            cats_cur = len(_sum_cur)
            items_cur = len(_items_cur)
            pdf_source = "ไฟล์ที่อัปโหลด" if "pdf_bytes" in st.session_state and st.session_state.get("pdf_bytes") else "ไฟล์เริ่มต้น (รายงานทางการเงิน.pdf)"
            st.markdown(f"""
            <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:12px;padding:14px 18px;margin-bottom:12px">
              <p style="margin:0;font-size:.85rem;font-weight:600;color:#065F46">✅ ไฟล์ปัจจุบัน: {pdf_source}</p>
              <p style="margin:4px 0 0;font-size:.8rem;color:#047857">หมวดหมู่: {cats_cur} หมวด | รายการ: {items_cur:,} รายการ</p>
            </div>""", unsafe_allow_html=True)
        except Exception:
            pass
    else:
        st.markdown("""
        <div style="background:#FEF3C7;border:1px solid #FDE68A;border-radius:12px;padding:14px 18px;margin-bottom:12px">
          <p style="margin:0;font-size:.85rem;font-weight:600;color:#92400E">⚠️ ยังไม่มีไฟล์ PDF</p>
        </div>""", unsafe_allow_html=True)

    pdf_file = st.file_uploader("เลือกไฟล์ PDF รายงานการเงิน", type=["pdf"], key="pdf_upload")
    if pdf_file:
        pdf_data = pdf_file.read()
        col_prev, col_save = st.columns([2, 1])
        with col_prev:
            st.markdown(f"ไฟล์: **{pdf_file.name}** ({len(pdf_data)/1024:.1f} KB)")
        with col_save:
            if st.button("บันทึก PDF นี้", key="save_pdf"):
                with st.spinner("กำลังบันทึกและแยกวิเคราะห์ PDF..."):
                    # Save to default path
                    with open(DEFAULT_PDF, "wb") as f_out:
                        f_out.write(pdf_data)
                    st.session_state["pdf_bytes"] = pdf_data
                    parse_pdf.clear()
                st.success("บันทึก PDF สำเร็จ!")
                st.rerun()

    st.divider()

    # ── Section 2: Stock Items XLS ────────────────────────────────────────────
    st.markdown("## 📦 อัปโหลด รายการสินค้าทั้งหมด.xls")
    df_si_cur = load_stock_items()
    if not df_si_cur.empty:
        imp_at = df_si_cur["imported_at"].iloc[0] if "imported_at" in df_si_cur.columns else "ไม่ทราบ"
        st.markdown(f"""
        <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:12px;padding:14px 18px;margin-bottom:12px">
          <p style="margin:0;font-size:.85rem;font-weight:600;color:#065F46">✅ ข้อมูลปัจจุบัน: {len(df_si_cur):,} รายการสินค้า</p>
          <p style="margin:4px 0 0;font-size:.8rem;color:#047857">นำเข้าล่าสุด: {imp_at}</p>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background:#FEF3C7;border:1px solid #FDE68A;border-radius:12px;padding:14px 18px;margin-bottom:12px">
          <p style="margin:0;font-size:.85rem;font-weight:600;color:#92400E">⚠️ ยังไม่มีข้อมูลสินค้า</p>
        </div>""", unsafe_allow_html=True)

    items_xls = st.file_uploader("เลือกไฟล์ รายการสินค้าทั้งหมด.xls",
                                  type=["xls", "xlsx"], key="items_xls_upload")
    if items_xls:
        if st.button("นำเข้าและแทนที่ข้อมูลสินค้า", key="import_items_p4"):
            with st.spinner("กำลังนำเข้าข้อมูลสินค้า..."):
                df_new = read_xls_bytes(items_xls.read())
                import_stock_items(df_new)
                load_stock_items.clear()
            st.success(f"นำเข้าสำเร็จ {len(df_new):,} รายการ")
            st.rerun()

    if not df_si_cur.empty:
        csv_items = df_si_cur.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="⬇️ Export สินค้าเป็น CSV",
            data=csv_items,
            file_name="รายการสินค้า_export.csv",
            mime="text/csv",
            key="export_items",
        )

    st.divider()

    # ── Section 3: Stock Incoming XLS ─────────────────────────────────────────
    st.markdown("## 📥 อัปโหลด การรับสินค้าเข้า Stock.xls")
    df_inc_cur = load_stock_incoming()
    if not df_inc_cur.empty:
        imp_at2 = df_inc_cur["imported_at"].iloc[0] if "imported_at" in df_inc_cur.columns else "ไม่ทราบ"
        st.markdown(f"""
        <div style="background:#F0FDF4;border:1px solid #BBF7D0;border-radius:12px;padding:14px 18px;margin-bottom:12px">
          <p style="margin:0;font-size:.85rem;font-weight:600;color:#065F46">✅ ข้อมูลปัจจุบัน: {len(df_inc_cur):,} รายการรับสินค้า</p>
          <p style="margin:4px 0 0;font-size:.8rem;color:#047857">นำเข้าล่าสุด: {imp_at2}</p>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown("""
        <div style="background:#FEF3C7;border:1px solid #FDE68A;border-radius:12px;padding:14px 18px;margin-bottom:12px">
          <p style="margin:0;font-size:.85rem;font-weight:600;color:#92400E">⚠️ ยังไม่มีข้อมูลการรับสินค้า</p>
        </div>""", unsafe_allow_html=True)

    incoming_xls = st.file_uploader("เลือกไฟล์ การรับสินค้าเข้า Stock.xls",
                                     type=["xls", "xlsx"], key="incoming_xls_upload")
    if incoming_xls:
        if st.button("นำเข้าและแทนที่ประวัติรับสินค้า", key="import_incoming_p4"):
            with st.spinner("กำลังนำเข้าข้อมูลการรับสินค้า..."):
                df_new = read_xls_bytes(incoming_xls.read())
                import_stock_incoming(df_new)
                load_stock_incoming.clear()
            st.success(f"นำเข้าสำเร็จ {len(df_new):,} รายการ")
            st.rerun()

    if not df_inc_cur.empty:
        csv_inc = df_inc_cur.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="⬇️ Export ประวัติรับสินค้าเป็น CSV",
            data=csv_inc,
            file_name="ประวัติรับสินค้า_export.csv",
            mime="text/csv",
            key="export_incoming",
        )
