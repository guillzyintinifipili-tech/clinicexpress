import os, io, re
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import date
import pdfplumber
import xlrd

from db import (
    init_db, fetch_stock_items, import_stock_items,
    fetch_stock_incoming, import_stock_incoming,
    fetch_financial_periods, fetch_revenue_categories, fetch_sales_items,
    fetch_case_transactions, import_case_transactions,
    save_financial_period, log_import, fetch_import_log,
    delete_financial_period, UPLOADS_DIR,
)

st.set_page_config(page_title="เอสพี รักษาสัตว์", page_icon="🐾",
                   layout="wide", initial_sidebar_state="expanded")

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
/* ── Sidebar nav: hide radio circle, style as nav items ── */
[data-testid="stSidebar"] div[role="radiogroup"] { gap: 2px; }
[data-testid="stSidebar"] div[role="radiogroup"] label > div:first-child { display: none !important; }
[data-testid="stSidebar"] div[role="radiogroup"] label {
    display: flex !important; align-items: center !important; cursor: pointer !important;
    border-radius: 8px !important; padding: 11px 16px !important;
    margin-bottom: 2px !important; transition: all 0.15s ease !important;
    font-size: 0.9rem !important; font-weight: 500 !important;
    color: #475569 !important; background: transparent !important;
    border: none !important; border-left: 3px solid transparent !important;
    width: 100% !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] label:hover {
    background: #F8FAFC !important; color: #334155 !important;
    border-left-color: #CBD5E1 !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {
    background: #EDE9FE !important; border-left-color: #7C3AED !important;
    color: #5B21B6 !important; font-weight: 700 !important;
}
[data-baseweb="tab-list"] { gap: 6px; border-bottom: 1px solid #E2E8F0; background: transparent; }
[data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important; padding: 8px 18px !important;
    color: #64748B !important; font-weight: 500 !important; font-size: 0.85rem !important;
    border: 1px solid transparent !important; border-bottom: none !important;
}
[data-baseweb="tab"][aria-selected="true"] {
    background: #FFFFFF !important; color: #7C3AED !important;
    border-color: #E2E8F0 !important; border-bottom: 2px solid #7C3AED !important;
}
[data-testid="stExpander"] {
    border: 1px solid #E2E8F0 !important; border-radius: 12px !important;
    background: #FFFFFF !important; box-shadow: 0 1px 3px rgba(0,0,0,0.05);
}
[data-testid="stVerticalBlockBorderWrapper"] {
    background: #FFFFFF !important;
    border-radius: 16px !important;
    border: 1px solid #E2E8F0 !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06) !important;
    padding: 4px 8px 8px !important;
}
[data-testid="stVerticalBlockBorderWrapper"] h4 {
    color: #0F172A !important; font-size: 0.95rem !important; font-weight: 700 !important; margin-bottom: 2px !important;
}
.stButton > button {
    background: #F5F3FF !important; color: #7C3AED !important;
    border: 1px solid #DDD6FE !important; border-radius: 8px !important;
    font-size: 0.85rem !important; font-weight: 500 !important;
}
.stButton > button:hover { background: #7C3AED !important; color: #FFFFFF !important; border-color: #7C3AED !important; }
[data-testid="stDownloadButton"] > button {
    background: #F0FDF4 !important; color: #15803D !important; border: 1px solid #BBF7D0 !important; border-radius: 8px !important;
}
[data-testid="stTextInput"] input, [data-testid="stNumberInput"] input {
    background: #FFFFFF !important; color: #0F172A !important; border-color: #CBD5E1 !important; border-radius: 8px !important;
}
[data-baseweb="select"] { background: #FFFFFF !important; border-color: #CBD5E1 !important; border-radius: 8px !important; }
label { color: #475569 !important; font-size: 0.82rem !important; }
[data-testid="stDataFrame"] { border: 1px solid #E2E8F0; border-radius: 12px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
hr { border-color: #E2E8F0 !important; margin: 1.2rem 0 !important; }
#MainMenu, footer { visibility: hidden; }
h1 { color: #7C3AED !important; font-size: 1.5rem !important; font-weight: 700 !important; letter-spacing:-.03em; }
h2 { color: #0F172A !important; font-size: 1.05rem !important; font-weight: 600 !important; }
h3 { color: #1D4ED8 !important; font-size: 0.95rem !important; font-weight: 600 !important; }
.drill-badge {
    display:inline-flex; align-items:center; gap:8px;
    background:#EDE9FE; border:1px solid #7C3AED; border-radius:20px;
    padding:4px 14px; color:#5B21B6; font-size:0.82rem; font-weight:600; margin-bottom:12px;
}
</style>
""", unsafe_allow_html=True)

init_db()

# ─── Constants ────────────────────────────────────────────────────────────────
DEFAULT_PDF = os.path.join(os.path.dirname(__file__), "รายงานทางการเงิน.pdf")
PALETTE = ["#7C3AED","#3B82F6","#10B981","#F59E0B","#EF4444",
           "#EC4899","#8B5CF6","#06B6D4","#84CC16","#F97316"]
CAT_COLORS = {
    "รายการยา":             "#7C3AED",
    "อุปกรณ์และเวชภัณฑ์":  "#3B82F6",
    "ค่าบริการทางการแพทย์": "#10B981",
    "ค่าผ่าตัด":            "#F59E0B",
    "อุปกรณ์ตรวจ LAB":     "#EF4444",
    "ค่าตรวจรักษา":         "#EC4899",
    "น้ำเกลือ":             "#8B5CF6",
    "ค่าบริการอื่นๆ":       "#06B6D4",
    "สินค้า Pet Shop":      "#84CC16",
}
PET_EMOJI = {"สุนัข":"🐶","แมว":"🐱","กระต่าย":"🐰","นก":"🦜","ปลา":"🐟","แรคคูน":"🦝"}

CHART_BG = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font_color="#374151",
    font_family="Inter, sans-serif",
    xaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B", tickfont=dict(size=11)),
    yaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B", tickfont=dict(size=11)),
)
LEGEND_STYLE = dict(bgcolor="rgba(255,255,255,0.9)", font_size=11, font_color="#374151",
                    bordercolor="#E2E8F0", borderwidth=1)

def ch(**kw):
    d = {**CHART_BG}
    d.update(kw)
    if "margin" not in d:
        d["margin"] = dict(t=24, b=24, l=16, r=16)
    return d

def color_for(cat: str) -> str:
    return CAT_COLORS.get(cat, PALETTE[hash(cat) % len(PALETTE)])

def fmt_thb(v: float) -> str:
    return f"฿{v:,.0f}"

def stock_status(row):
    if row["qty"] <= 0:         return "🔴 หมดสต๊อก", "#FEE2E2", "#991B1B"
    if row["qty"] <= row["alert_qty"]: return "🟡 ใกล้หมด", "#FEF3C7", "#92400E"
    return "🟢 ปกติ", "#D1FAE5", "#065F46"

def read_xls_bytes(file_bytes) -> pd.DataFrame:
    wb = xlrd.open_workbook(file_contents=file_bytes)
    sh = wb.sheet_by_index(0)
    rows = [[sh.cell_value(r, c) for c in range(sh.ncols)] for r in range(sh.nrows)]
    hi = 0
    for i, row in enumerate(rows):
        f = str(row[0]).strip()
        if "Stock Id" in f or "วันที่รับ" in f: hi = i; break
        if f and not any(f.startswith(x) for x in ["ราย","ช่วง"]) and f != "":
            if any(str(c).strip() for c in row[1:]): hi = i; break
    headers = [str(c).strip() for c in rows[hi]]
    return pd.DataFrame(rows[hi+1:], columns=headers)


# ─── PDF helpers ──────────────────────────────────────────────────────────────
THAI_MONTHS = {
    "มกราคม":"01","กุมภาพันธ์":"02","มีนาคม":"03","เมษายน":"04",
    "พฤษภาคม":"05","มิถุนายน":"06","กรกฎาคม":"07","สิงหาคม":"08",
    "กันยายน":"09","ตุลาคม":"10","พฤศจิกายน":"11","ธันวาคม":"12",
    "มกราคม":"01","กมุ ภาพนั ธ":"02","มนี าคม":"03",
    "มถิ นุ ายน":"06","กรกฎาคม":"07","สงิ หาคม":"08",
    "กนั ยายน":"09","ตลุ าคม":"10","พฤศจกิ ายน":"11","ธนั วาคม":"12",
}
MONTH_TH = {"01":"ม.ค.","02":"ก.พ.","03":"มี.ค.","04":"เม.ย.",
            "05":"พ.ค.","06":"มิ.ย.","07":"ก.ค.","08":"ส.ค.",
            "09":"ก.ย.","10":"ต.ค.","11":"พ.ย.","12":"ธ.ค."}

def extract_period_from_pdf(pdf_bytes: bytes):
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            t = " ".join((pdf.pages[0].extract_text() or "").split())
        m = re.search(r"(\d{1,2})\s+(\S+)\s+(\d{4})\s*[-–]\s*(\d{1,2})\s+(\S+)\s+(\d{4})", t)
        if m:
            d1, mo1, y1, d2, mo2, y2 = m.groups()
            y1_ad, y2_ad = int(y1)-543, int(y2)-543
            m1 = THAI_MONTHS.get(mo1, "01")
            m2 = THAI_MONTHS.get(mo2, "12")
            start = f"{y1_ad}-{m1}-{int(d1):02d}"
            end   = f"{y2_ad}-{m2}-{int(d2):02d}"
            lbl   = f"{MONTH_TH.get(m1,m1)} {y1} – {MONTH_TH.get(m2,m2)} {y2}"
            return lbl, start, end
    except Exception:
        pass
    today = date.today()
    lbl = f"{MONTH_TH.get(f'{today.month:02d}','?')} {today.year+543}"
    return lbl, today.replace(day=1).isoformat(), today.isoformat()


@st.cache_data(ttl=300, show_spinner=False)
def parse_pdf(pdf_bytes: bytes):
    info = {"cash":0,"transfer":0,"total":0,"receipts":0,"cancelled":0,"cost":0,"gross_profit":0}
    summary_rows, item_rows = [], []
    CAT_CLEAN = {
        "รายการยา":"รายการยา",
        "อปุ กรณแ์ ละเวชภณั ฑ์":"อุปกรณ์และเวชภัณฑ์",
        "คา่ บรกิ ารทางการแพทย์":"ค่าบริการทางการแพทย์",
        "คา่ ผา่ ตดั":"ค่าผ่าตัด",
        "อปุ กรณต์ รวจ LAB":"อุปกรณ์ตรวจ LAB",
        "คา่ ตรวจรกั ษา":"ค่าตรวจรักษา",
        "นํา\x00 เกลอื":"น้ำเกลือ","นํา เกลอื":"น้ำเกลือ","นํ าเกลอื":"น้ำเกลือ",
        "คา่ บรกิ ารอนื\x00 ๆ":"ค่าบริการอื่นๆ","คา่ บรกิ ารอนื ๆ":"ค่าบริการอื่นๆ",
        "สนิ คา้ Pet Shop":"สินค้า Pet Shop",
    }
    def clean_cat(raw):
        for k, v in CAT_CLEAN.items():
            if k.replace("\x00","") in raw.replace("\x00",""): return v
        return raw.strip()
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            t1 = " ".join((pdf.pages[0].extract_text() or "").split())
            m = re.search(r"รวมใบเสร็จถ[^\d]*([\d]+)\s*รายการ\s*รวมใบเสร็จทง[^\d]*([\d]+)", t1)
            if m: info["cancelled"]=int(m.group(1)); info["receipts"]=int(m.group(2))
            m = re.search(r"เงนิ สด[^\d]*([\d,]+)", t1)
            if m: info["cash"] = float(m.group(1).replace(",",""))
            m = re.search(r"โอนเงนิ[^\d]*([\d,]+)", t1)
            if m: info["transfer"] = float(m.group(1).replace(",",""))
            m = re.search(r"รวมเป.*?นเงนิ\s+([\d,]+)", t1)
            if m: info["total"] = float(m.group(1).replace(",",""))
            m = re.search(r"ตน้ ทนุ[^\d]*([\d,]+\.[\d]+)", t1)
            if m: info["cost"] = float(m.group(1).replace(",",""))
            m = re.search(r"กาํ ไรสทุ ธ[^\d]*([\d,]+\.[\d]+)", t1)
            if m: info["gross_profit"] = float(m.group(1).replace(",",""))
            if info["gross_profit"] == 0 and info["total"] > 0 and info["cost"] > 0:
                info["gross_profit"] = info["total"] - info["cost"]

            if len(pdf.pages) > 1:
                words2 = pdf.pages[1].extract_words()
                lines2 = {}
                for w in words2:
                    y = round(w["top"]/3)*3
                    lines2.setdefault(y,[]).append(w)
                for y in sorted(lines2):
                    ln = " ".join(w["text"] for w in sorted(lines2[y], key=lambda w: w["x0"]))
                    m = re.search(r"([\d,]+\.\d{2})\s*บาท", ln)
                    if m:
                        amt = float(m.group(1).replace(",",""))
                        cat_raw = ln[:ln.index(m.group(0))].strip()
                        if amt > 0 and cat_raw and "ยอดรวม" not in cat_raw:
                            summary_rows.append({"category": clean_cat(cat_raw), "amount": amt})

            for page in pdf.pages[2:]:
                tbl = page.extract_table()
                if not tbl: continue
                for row in tbl:
                    if not row or len(row) < 4: continue
                    cat_raw = str(row[0] or "").strip()
                    item_name = str(row[1] or "").strip()
                    qty_unit  = str(row[2] or "").strip()
                    total_str = str(row[3] or "").strip()
                    if not item_name or not total_str: continue
                    if "รายการ" in item_name and "จํ" in qty_unit: continue
                    try:
                        total = float(re.sub(r"[^\d\.]","",total_str))
                        qty_m = re.match(r"([\d,\.]+)", qty_unit)
                        qty   = float(qty_m.group(1).replace(",","")) if qty_m else 0.0
                        unit  = re.sub(r"^[\d,\.\s]+","",qty_unit).strip()
                        if total > 0 and item_name:
                            item_rows.append({"category": clean_cat(cat_raw) if cat_raw else "—",
                                              "item_name": item_name, "qty": qty, "unit": unit, "total": total})
                    except (ValueError, AttributeError):
                        pass
    except Exception as e:
        st.warning(f"PDF parse warning: {e}")

    summary_df = pd.DataFrame(summary_rows) if summary_rows else pd.DataFrame(columns=["category","amount"])
    items_df   = pd.DataFrame(item_rows)    if item_rows    else pd.DataFrame(columns=["category","item_name","qty","unit","total"])
    return info, summary_df, items_df


# ─── Cached loaders ───────────────────────────────────────────────────────────
@st.cache_data(ttl=60)
def load_stock_items(): return fetch_stock_items()

@st.cache_data(ttl=60)
def load_stock_incoming(): return fetch_stock_incoming()

@st.cache_data(ttl=30)
def load_fp(start, end):   return fetch_financial_periods(start, end)

@st.cache_data(ttl=30)
def load_cats(start, end): return fetch_revenue_categories(start, end)

@st.cache_data(ttl=30)
def load_items(start, end): return fetch_sales_items(start, end)

@st.cache_data(ttl=30)
def load_cases(start, end): return fetch_case_transactions(start, end)


# ─── Auto-import default PDF on first run ─────────────────────────────────────
@st.cache_data(show_spinner=False)
def _auto_import_pdf_once():
    fp = fetch_financial_periods()
    if fp.empty and os.path.exists(DEFAULT_PDF):
        with open(DEFAULT_PDF, "rb") as f:
            pb = f.read()
        info, sdf, idf = parse_pdf(pb)
        lbl, ps, pe = extract_period_from_pdf(pb)
        imp_id = log_import("รายงานทางการเงิน.pdf", "pdf_financial", lbl, ps, pe, len(idf))
        save_financial_period(imp_id, ps, pe, lbl, info, sdf, idf)
    return True

_auto_import_pdf_once()


# ─── UI helpers ───────────────────────────────────────────────────────────────
def kpi(col, label, value, note=None, accent="#7C3AED", delta=None, delta_up=True):
    dhtml = ""
    if delta:
        clr = "#16A34A" if delta_up else "#DC2626"
        arr = "▲" if delta_up else "▼"
        dhtml = f'<p style="margin:4px 0 0;color:{clr};font-size:.75rem;font-weight:600">{arr} {delta}</p>'
    nhtml = f'<p style="margin:3px 0 0;color:#94A3B8;font-size:.70rem">{note}</p>' if note else ""
    col.markdown(f"""
    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-top:3px solid {accent};
                border-radius:12px;padding:16px 18px 12px;min-height:96px;
                box-shadow:0 1px 4px rgba(0,0,0,0.05)">
      <p style="margin:0;color:#94A3B8;font-size:.68rem;font-weight:600;letter-spacing:.07em;text-transform:uppercase">{label}</p>
      <p style="margin:6px 0 0;color:#0F172A;font-size:1.55rem;font-weight:700;line-height:1.1">{value}</p>
      {dhtml}{nhtml}
    </div>""", unsafe_allow_html=True)

def section(title, caption=""):
    st.markdown(f"## {title}")
    if caption: st.caption(caption)

def no_data(msg="ยังไม่มีข้อมูลในช่วงที่เลือก"):
    st.info(f"📭 {msg}")

def hbar(df, x_col, y_col, title="", colors=None, height=None, fmt=True, total=None, key=None):
    if df.empty: return no_data()
    n = len(df)
    h = height or max(280, n * 40)
    clrs = colors or [PALETTE[i % len(PALETTE)] for i in range(n)]
    total_val = total if total else df[x_col].sum()
    pct_vals  = [(v / total_val * 100) if total_val > 0 else 0 for v in df[x_col]]
    if fmt:
        text = [f"{fmt_thb(v)}  ({p:.1f}%)" for v, p in zip(df[x_col], pct_vals)]
    else:
        text = df[x_col].map(str).tolist()
    fig = go.Figure(go.Bar(
        x=df[x_col], y=df[y_col], orientation="h",
        marker=dict(color=clrs, line_width=0),
        text=text, textposition="outside",
        textfont=dict(size=10, color="#374151"),
        customdata=pct_vals,
        hovertemplate="<b>%{y}</b><br>฿%{x:,.0f}<br>สัดส่วน: <b>%{customdata:.1f}%</b><extra></extra>",
    ))
    fig.update_layout(**ch(
        height=h,
        yaxis=dict(gridcolor="rgba(0,0,0,0)", zeroline=False, color="#374151",
                   tickfont=dict(size=11), categoryorder="total ascending"),
        xaxis=dict(gridcolor="#E2E8F0", zeroline=False, color="#64748B",
                   tickfont=dict(size=10)),
        margin=dict(t=8, b=8, l=8, r=120),
        showlegend=False,
    ))
    if title: section(title)
    ev = st.plotly_chart(fig, use_container_width=True, on_select="rerun",
                         selection_mode="points", key=key or f"hbar_{y_col}_{title}")
    return ev

def donut(labels, values, title="", colors=None, center_text=""):
    clrs = colors or [color_for(l) for l in labels]
    total = sum(values)
    fig = go.Figure(go.Pie(
        labels=labels, values=values, hole=0.58,
        marker=dict(colors=clrs, line=dict(color="#FFFFFF", width=2)),
        textposition="inside", textinfo="percent",
        hovertemplate="<b>%{label}</b><br>฿%{value:,.0f}<br>สัดส่วน: <b>%{percent}</b><extra></extra>",
        pull=[0]*len(labels),
    ))
    ann_text = center_text or fmt_thb(total)
    fig.update_layout(**ch(
        height=320,
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.12,
                    xanchor="center", x=0.5, **LEGEND_STYLE),
        margin=dict(t=8, b=60, l=8, r=8),
        annotations=[dict(text=f"<b>{ann_text}</b>", x=0.5, y=0.5,
                          font_size=14, font_color="#374151", showarrow=False)],
    ))
    if title: section(title)
    st.plotly_chart(fig, use_container_width=True)

def drill_badge(label, key):
    c1, c2 = st.columns([7,1])
    c1.markdown(f'<div class="drill-badge">🔍 Drilldown: <strong>{label}</strong></div>', unsafe_allow_html=True)
    if c2.button("✕", key=f"clr_{key}"):
        st.session_state["drill_cat"] = None
        st.rerun()


# ─── Session state ─────────────────────────────────────────────────────────────
if "drill_cat" not in st.session_state: st.session_state["drill_cat"] = None


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div style="padding:14px 0 8px">
      <div style="font-size:1.35rem;font-weight:700;color:#7C3AED">🐾 เอสพี รักษาสัตว์</div>
      <div style="font-size:.74rem;color:#94A3B8;margin-top:2px">ระบบจัดการคลินิกสัตว์</div>
    </div>""", unsafe_allow_html=True)
    st.divider()

    page = st.radio("เมนู", [
        "📊 ภาพรวมธุรกิจ",
        "💰 กำไร-ขาดทุน",
        "🐾 เคสผู้ป่วย",
        "💊 คลังยา & สินค้า",
        "⬆️ นำเข้าไฟล์",
    ], label_visibility="collapsed")

    st.divider()
    st.markdown("**📅 ช่วงเวลา**")
    fp_all = fetch_financial_periods()
    if not fp_all.empty:
        min_d = pd.to_datetime(fp_all["period_start"].min()).date()
        max_d = pd.to_datetime(fp_all["period_end"].max()).date()
    else:
        min_d = date(2025, 1, 1)
        max_d = date.today()

    fs_date = st.date_input("ตั้งแต่", value=min_d, min_value=min_d, max_value=max_d, key="sb_start")
    fe_date = st.date_input("ถึง",     value=max_d, min_value=min_d, max_value=max_d, key="sb_end")
    fs, fe = fs_date.isoformat(), fe_date.isoformat()

    st.divider()
    # Quick stats
    _fp_sb = load_fp(fs, fe)
    if not _fp_sb.empty:
        _rev = _fp_sb["total_revenue"].sum()
        _gp  = _fp_sb["gross_profit"].sum()
        _mg  = (_gp/_rev*100) if _rev > 0 else 0
        st.markdown(f'<p style="margin:2px 0;font-size:.83rem">💰 รายรับ <strong style="color:#10B981">{fmt_thb(_rev)}</strong></p>', unsafe_allow_html=True)
        st.markdown(f'<p style="margin:2px 0;font-size:.83rem">📈 กำไร <strong style="color:#7C3AED">{fmt_thb(_gp)}</strong> ({_mg:.1f}%)</p>', unsafe_allow_html=True)

    _si_sb = load_stock_items()
    if not _si_sb.empty:
        _low = int((((_si_sb["qty"] <= _si_sb["alert_qty"]) & (_si_sb["qty"] > 0))).sum())
        _out = int((_si_sb["qty"] <= 0).sum())
        if _low: st.markdown(f'<p style="margin:2px 0;color:#D97706;font-size:.82rem">⚠️ ใกล้หมด <strong>{_low}</strong></p>', unsafe_allow_html=True)
        if _out: st.markdown(f'<p style="margin:2px 0;color:#DC2626;font-size:.82rem">🚨 หมดสต๊อก <strong>{_out}</strong></p>', unsafe_allow_html=True)

    st.divider()
    st.caption("SQLite Local  •  v5.0")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════
if page == "📊 ภาพรวมธุรกิจ":
    st.title("📊 ภาพรวมธุรกิจ")

    fp   = load_fp(fs, fe)
    cats = load_cats(fs, fe)
    itms = load_items(fs, fe)
    stk  = load_stock_items()

    if fp.empty:
        st.info("📭 ยังไม่มีข้อมูลรายงานการเงิน — ไปที่ **⬆️ นำเข้าไฟล์** เพื่ออัปโหลด PDF")
    else:
        total_rev  = fp["total_revenue"].sum()
        total_cost = fp["total_cost"].sum()
        gross_p    = fp["gross_profit"].sum()
        margin_pct = (gross_p / total_rev * 100) if total_rev > 0 else 0
        cash_v     = fp["cash_revenue"].sum()
        transfer_v = fp["transfer_revenue"].sum()
        receipts   = int(fp["receipts_count"].sum())
        cancelled  = int(fp["cancelled_count"].sum())

        # ── KPI Row – 5 essential metrics ─────────────────────────────────────
        k1,k2,k3,k4,k5 = st.columns(5)
        kpi(k1, "รายรับรวม", fmt_thb(total_rev), accent="#7C3AED")
        kpi(k2, "กำไรขั้นต้น", fmt_thb(gross_p), accent="#10B981")
        kpi(k3, "อัตรากำไร", f"{margin_pct:.1f}%", note="เป้าหมาย ≥ 50%", accent="#F59E0B")
        kpi(k4, "ใบเสร็จทั้งหมด", f"{receipts:,} ใบ",
            note=f"ยกเลิก {cancelled:,} ใบ", accent="#3B82F6")
        if not stk.empty:
            inv_val_kpi = (stk["qty"] * stk["avg_cost"].fillna(0)).sum()
            kpi(k5, "มูลค่าคลังสินค้า", fmt_thb(inv_val_kpi), accent="#84CC16")
        else:
            kpi(k5, "ต้นทุนรวม", fmt_thb(total_cost), accent="#EF4444")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # ── Row 1: Revenue by category + Donut ────────────────────────────────
        c1, c2 = st.columns([6, 4])
        with c1:
            with st.container(border=True):
                st.markdown("#### 📊 รายรับแยกตามหมวดหมู่")
                st.caption("คลิกที่แถบเพื่อ Drilldown รายละเอียด")
                if not cats.empty:
                    cat_agg = cats.groupby("category")["amount"].sum().reset_index()
                    cat_agg = cat_agg.sort_values("amount", ascending=True)
                    clrs = [color_for(c) for c in cat_agg["category"]]
                    ev_cat = hbar(cat_agg, "amount", "category", colors=clrs,
                                  total=cat_agg["amount"].sum(), key="overview_cat_bar")
                    # Drilldown handler
                    sel_cat = None
                    if ev_cat and ev_cat.selection and ev_cat.selection.get("points"):
                        sel_cat = ev_cat.selection["points"][0].get("y")
                    if sel_cat and not itms.empty:
                        sub = itms[itms["category"] == sel_cat]
                        if not sub.empty:
                            st.markdown(f'<div class="drill-badge">🔍 Drilldown: <strong>{sel_cat}</strong></div>', unsafe_allow_html=True)
                            sub_agg = sub.groupby("item_name")["total"].sum().reset_index()
                            sub_agg = sub_agg.sort_values("total", ascending=True)
                            sub_total = sub_agg["total"].sum()
                            vals_sub = sub_agg["total"].tolist()
                            mx_sub = max(vals_sub) if vals_sub else 1
                            c_sub = [color_for(sel_cat)] * len(sub_agg)
                            hbar(sub_agg, "total", "item_name", colors=c_sub,
                                 total=sub_total, key=f"drill_{sel_cat}")
                else:
                    no_data()

        with c2:
            with st.container(border=True):
                st.markdown("#### 🍩 สัดส่วนรายรับ")
                if not cats.empty:
                    cat_agg2 = cats.groupby("category")["amount"].sum().reset_index()
                    cat_agg2 = cat_agg2.sort_values("amount", ascending=False)
                    donut(cat_agg2["category"].tolist(), cat_agg2["amount"].tolist())
                else:
                    no_data()

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── Row 2: Payment method + Stock status ──────────────────────────────
        c3, c4 = st.columns(2)
        with c3:
            with st.container(border=True):
                st.markdown("#### 💳 ช่องทางการชำระเงิน")
                if cash_v + transfer_v > 0:
                    pay_pct_cash  = cash_v  / (cash_v + transfer_v) * 100
                    pay_pct_trans = transfer_v / (cash_v + transfer_v) * 100
                    pc1, pc2 = st.columns(2)
                    pc1.markdown(f"""<div style="text-align:center;padding:12px;background:#F0FDF4;border-radius:10px;margin-bottom:8px">
                        <div style="font-size:.70rem;color:#065F46;font-weight:700;text-transform:uppercase;letter-spacing:.05em">💵 เงินสด</div>
                        <div style="font-size:1.2rem;font-weight:800;color:#10B981;margin:4px 0">{fmt_thb(cash_v)}</div>
                        <div style="font-size:.8rem;color:#16A34A;font-weight:600">{pay_pct_cash:.1f}%</div>
                    </div>""", unsafe_allow_html=True)
                    pc2.markdown(f"""<div style="text-align:center;padding:12px;background:#EFF6FF;border-radius:10px;margin-bottom:8px">
                        <div style="font-size:.70rem;color:#1E40AF;font-weight:700;text-transform:uppercase;letter-spacing:.05em">📱 โอนเงิน</div>
                        <div style="font-size:1.2rem;font-weight:800;color:#3B82F6;margin:4px 0">{fmt_thb(transfer_v)}</div>
                        <div style="font-size:.8rem;color:#2563EB;font-weight:600">{pay_pct_trans:.1f}%</div>
                    </div>""", unsafe_allow_html=True)
                    donut(["เงินสด","โอนเงิน"], [cash_v, transfer_v],
                          colors=["#10B981","#3B82F6"])
                else:
                    no_data()

        with c4:
            with st.container(border=True):
                st.markdown("#### 📦 สถานะสต๊อกสินค้า")
                if not stk.empty:
                    low_n = int(((stk["qty"] <= stk["alert_qty"]) & (stk["qty"] > 0)).sum())
                    out_n = int((stk["qty"] <= 0).sum())
                    ok_n  = len(stk) - low_n - out_n
                    ss1, ss2, ss3 = st.columns(3)
                    ss1.markdown(f"""<div style="text-align:center;padding:12px 6px;background:#D1FAE5;border-radius:10px;margin-bottom:8px">
                        <div style="font-size:.68rem;color:#065F46;font-weight:700">✅ ปกติ</div>
                        <div style="font-size:1.6rem;font-weight:800;color:#059669;line-height:1.1">{ok_n}</div>
                        <div style="font-size:.68rem;color:#065F46">รายการ</div>
                    </div>""", unsafe_allow_html=True)
                    ss2.markdown(f"""<div style="text-align:center;padding:12px 6px;background:#FEF3C7;border-radius:10px;margin-bottom:8px">
                        <div style="font-size:.68rem;color:#92400E;font-weight:700">⚠️ ใกล้หมด</div>
                        <div style="font-size:1.6rem;font-weight:800;color:#D97706;line-height:1.1">{low_n}</div>
                        <div style="font-size:.68rem;color:#92400E">รายการ</div>
                    </div>""", unsafe_allow_html=True)
                    ss3.markdown(f"""<div style="text-align:center;padding:12px 6px;background:#FEE2E2;border-radius:10px;margin-bottom:8px">
                        <div style="font-size:.68rem;color:#991B1B;font-weight:700">🔴 หมดสต๊อก</div>
                        <div style="font-size:1.6rem;font-weight:800;color:#DC2626;line-height:1.1">{out_n}</div>
                        <div style="font-size:.68rem;color:#991B1B">รายการ</div>
                    </div>""", unsafe_allow_html=True)
                    donut(["ปกติ","ใกล้หมด","หมดสต๊อก"], [ok_n, low_n, out_n],
                          colors=["#10B981","#F59E0B","#EF4444"],
                          center_text=f"{len(stk)} รายการ")
                else:
                    no_data("ยังไม่มีข้อมูลสต๊อก — นำเข้าที่หน้า 💊 คลังยา")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── Trend chart (only if multiple periods) ────────────────────────────
        if len(fp) > 1:
            with st.container(border=True):
                st.markdown("#### 📈 แนวโน้มรายรับแต่ละรอบ")
                st.caption("เปรียบเทียบระหว่างรอบรายงาน")
                fig_tr = go.Figure()
                fig_tr.add_trace(go.Bar(
                    x=fp["period_label"], y=fp["total_revenue"], name="รายรับ",
                    marker=dict(color="#7C3AED", line_width=0, opacity=0.85),
                    hovertemplate="<b>%{x}</b><br>รายรับ: ฿%{y:,.0f}<extra></extra>",
                ))
                fig_tr.add_trace(go.Scatter(
                    x=fp["period_label"], y=fp["gross_profit"], name="กำไรขั้นต้น",
                    mode="lines+markers+text",
                    line=dict(color="#10B981", width=2.5),
                    marker=dict(size=8, color="#10B981"),
                    text=fp["gross_profit"].map(fmt_thb), textposition="top center",
                    textfont=dict(size=10, color="#065F46"),
                    hovertemplate="<b>%{x}</b><br>กำไร: ฿%{y:,.0f}<extra></extra>",
                ))
                fig_tr.update_layout(**ch(barmode="group", height=320, legend=LEGEND_STYLE,
                                          margin=dict(t=24,b=24,l=16,r=16)))
                st.plotly_chart(fig_tr, use_container_width=True)

            st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── Top 15 items ───────────────────────────────────────────────────────
        with st.container(border=True):
            st.markdown("#### 🏆 Top 15 สินค้า/บริการขายดีที่สุด")
            st.caption("จากรายงานการเงินในช่วงที่เลือก")
            if not itms.empty:
                top15_all = itms.groupby("item_name")["total"].sum().reset_index()
                top15 = top15_all.sort_values("total", ascending=True).tail(15)
                vals  = top15["total"].tolist()
                mx    = max(vals) if vals else 1
                clrs  = [f"rgba(124,58,237,{0.35 + 0.65*(v/mx):.2f})" for v in vals]
                hbar(top15, "total", "item_name", colors=clrs,
                     total=top15_all["total"].sum(), key="overview_top15")
            else:
                no_data()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — P&L ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💰 กำไร-ขาดทุน":
    st.title("💰 วิเคราะห์กำไร-ขาดทุน")

    fp   = load_fp(fs, fe)
    cats = load_cats(fs, fe)
    itms = load_items(fs, fe)

    if fp.empty:
        st.info("📭 ยังไม่มีข้อมูล — กรุณานำเข้า PDF รายงานการเงินก่อน")
        st.stop()

    total_rev  = fp["total_revenue"].sum()
    total_cost = fp["total_cost"].sum()
    gross_p    = fp["gross_profit"].sum()
    margin_pct = (gross_p / total_rev * 100) if total_rev > 0 else 0

    # Summary KPIs
    k1,k2,k3,k4 = st.columns(4)
    kpi(k1,"รายรับรวม",    fmt_thb(total_rev),  accent="#7C3AED")
    kpi(k2,"ต้นทุนรวม",    fmt_thb(total_cost), accent="#EF4444",
        note=f"{total_cost/total_rev*100:.1f}% ของรายรับ" if total_rev else "")
    kpi(k3,"กำไรขั้นต้น",  fmt_thb(gross_p),    accent="#10B981")
    kpi(k4,"อัตรากำไร %",  f"{margin_pct:.1f}%",
        note="เป้าหมาย ≥ 50%", accent="#F59E0B",
        delta=None, delta_up=margin_pct >= 50)

    st.divider()

    # Revenue vs Cost vs Profit per period
    if len(fp) >= 1:
        with st.container(border=True):
            st.markdown("#### 📊 รายรับ vs ต้นทุน vs กำไร แต่ละรอบ")
            fig_pnl = go.Figure()
            fig_pnl.add_trace(go.Bar(name="รายรับ", x=fp["period_label"], y=fp["total_revenue"],
                                      marker=dict(color="#7C3AED", line_width=0)))
            fig_pnl.add_trace(go.Bar(name="ต้นทุน", x=fp["period_label"], y=fp["total_cost"],
                                      marker=dict(color="#EF4444", line_width=0)))
            fig_pnl.add_trace(go.Bar(name="กำไร",   x=fp["period_label"], y=fp["gross_profit"],
                                      marker=dict(color="#10B981", line_width=0)))
            fig_pnl.update_layout(**ch(barmode="group", height=360, legend=LEGEND_STYLE,
                                        margin=dict(t=24,b=24,l=16,r=16)))
            st.plotly_chart(fig_pnl, use_container_width=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Margin trend
    if len(fp) > 1:
        with st.container(border=True):
            st.markdown("#### 📉 อัตรากำไรขั้นต้น (%) แต่ละรอบ")
            st.caption("เส้นสีแดงคือค่าเฉลี่ย")
            fp["margin_pct"] = (fp["gross_profit"] / fp["total_revenue"] * 100).round(1)
            avg_mg = fp["margin_pct"].mean()
            fig_mg = go.Figure()
            fig_mg.add_trace(go.Scatter(
                x=fp["period_label"], y=fp["margin_pct"],
                mode="lines+markers+text", name="อัตรากำไร %",
                fill="tozeroy", fillcolor="rgba(16,185,129,0.12)",
                line=dict(color="#10B981", width=2.5, shape="spline"),
                marker=dict(size=9, color="#10B981"),
                text=fp["margin_pct"].map(lambda v: f"{v:.1f}%"),
                textposition="top center", textfont=dict(size=11),
            ))
            fig_mg.add_hline(y=avg_mg, line_dash="dash", line_color="#EF4444",
                              annotation_text=f"ค่าเฉลี่ย {avg_mg:.1f}%",
                              annotation_position="bottom right")
            fig_mg.update_layout(**ch(height=300, yaxis=dict(ticksuffix="%", gridcolor="#E2E8F0")))
            st.plotly_chart(fig_mg, use_container_width=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Categories: best & worst
    if not cats.empty:
        with st.container(border=True):
            st.markdown("#### 🏆 หมวดหมู่ที่ทำรายได้สูงสุด")
            st.caption("สัดส่วนเทียบกับรายรับรวม")
            cat_agg = cats.groupby("category")["amount"].sum().reset_index().sort_values("amount", ascending=False)
            total_cat = cat_agg["amount"].sum()
            cat_agg["pct"] = (cat_agg["amount"] / total_cat * 100).round(1)
            top5 = cat_agg.head(5)
            cols = st.columns(min(5, len(top5)))
            for i, (_, row) in enumerate(top5.iterrows()):
                with cols[i]:
                    clr = color_for(row["category"])
                    st.markdown(f"""
                    <div style="background:#FFFFFF;border:1px solid #E2E8F0;border-left:4px solid {clr};
                                border-radius:10px;padding:14px 16px;text-align:center;
                                box-shadow:0 1px 3px rgba(0,0,0,0.05)">
                      <p style="margin:0;font-size:.72rem;color:#64748B;font-weight:600">{row['category']}</p>
                      <p style="margin:6px 0 2px;font-size:1.2rem;font-weight:700;color:#0F172A">{fmt_thb(row['amount'])}</p>
                      <p style="margin:0;font-size:.78rem;color:{clr};font-weight:600">{row['pct']}%</p>
                    </div>""", unsafe_allow_html=True)

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Top 10 best & bottom 10 worst items
    if not itms.empty:
        item_agg = itms.groupby("item_name")["total"].sum().reset_index()

        c_best, c_worst = st.columns(2)
        item_total = item_agg["total"].sum()
        with c_best:
            with st.container(border=True):
                st.markdown("#### 📈 Top 10 สินค้าขายดีที่สุด")
                top10 = item_agg.sort_values("total", ascending=True).tail(10)
                vals = top10["total"].tolist()
                mx = max(vals) if vals else 1
                clrs = [f"rgba(16,185,129,{0.4 + 0.6*(v/mx):.2f})" for v in vals]
                hbar(top10, "total", "item_name", colors=clrs,
                     total=item_total, key="pnl_top10")

        with c_worst:
            with st.container(border=True):
                st.markdown("#### 📉 Bottom 10 สินค้าขายน้อยที่สุด")
                st.caption("สินค้าที่มียอดต่ำที่สุดในช่วงนี้")
                bot10 = item_agg[item_agg["total"] > 0].sort_values("total", ascending=True).head(10)
                vals2 = bot10["total"].tolist()
                mx2 = max(vals2) if vals2 else 1
                clrs2 = [f"rgba(239,68,68,{0.35 + 0.65*(v/mx2):.2f})" for v in vals2]
                hbar(bot10, "total", "item_name", colors=clrs2,
                     total=item_total, key="pnl_bot10")

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Payment split
    cash_v     = fp["cash_revenue"].sum()
    transfer_v = fp["transfer_revenue"].sum()
    if cash_v + transfer_v > 0:
        with st.container(border=True):
            st.markdown("#### 💳 สัดส่วนการชำระเงิน")
            c_pay1, c_pay2 = st.columns([4,6])
            with c_pay1:
                donut(["เงินสด","โอนเงิน"], [cash_v, transfer_v],
                      colors=["#10B981","#3B82F6"])
            with c_pay2:
                fig_pay = go.Figure()
                fig_pay.add_trace(go.Bar(
                    x=["เงินสด","โอนเงิน"], y=[cash_v, transfer_v],
                    marker=dict(color=["#10B981","#3B82F6"], line_width=0),
                    text=[fmt_thb(cash_v), fmt_thb(transfer_v)],
                    textposition="outside", textfont=dict(size=13),
                    hovertemplate="<b>%{x}</b><br>฿%{y:,.0f}<extra></extra>",
                    width=[0.4, 0.4],
                ))
                fig_pay.update_layout(**ch(height=280, showlegend=False,
                                            xaxis=dict(tickfont=dict(size=13)),
                                            margin=dict(t=24,b=24,l=16,r=16)))
                st.plotly_chart(fig_pay, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — CASES
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "🐾 เคสผู้ป่วย":
    st.title("🐾 เคสผู้ป่วย")

    cases = load_cases(fs, fe)

    if cases.empty:
        st.info("📭 ยังไม่มีข้อมูลเคสผู้ป่วย")
        st.markdown("""
        **วิธีนำเข้าข้อมูลเคส:**
        ไปที่หน้า **⬆️ นำเข้าไฟล์** → อัปโหลด CSV เคสผู้ป่วย

        ไฟล์ CSV ต้องมีคอลัมน์เหล่านี้:
        `วันที่, เลขใบเสร็จ, ชื่อเจ้าของ, ชื่อสัตว์, ประเภทสัตว์, หมวดหมู่, บริการ/สินค้า, จำนวน, หน่วย, จำนวนเงิน, ส่วนลด, ยอดสุทธิ, วิธีชำระ`
        """)

        # Download template
        template_cols = ["วันที่","เลขใบเสร็จ","ชื่อเจ้าของ","ชื่อสัตว์","ประเภทสัตว์",
                         "หมวดหมู่","บริการ/สินค้า","จำนวน","หน่วย",
                         "จำนวนเงิน","ส่วนลด","ยอดสุทธิ","วิธีชำระ"]
        tpl = pd.DataFrame(columns=template_cols)
        st.download_button("⬇️ ดาวน์โหลด Template CSV",
                           data=tpl.to_csv(index=False).encode("utf-8-sig"),
                           file_name="case_template.csv", mime="text/csv")
        st.stop()

    # KPIs
    total_case_rev = cases["net_amount"].sum()
    unique_clients = cases["client_name"].nunique()
    unique_pets    = cases["pet_name"].nunique()
    k1,k2,k3,k4 = st.columns(4)
    kpi(k1,"จำนวนเคสทั้งหมด",f"{len(cases):,} เคส", accent="#7C3AED")
    kpi(k2,"รายรับรวม",fmt_thb(total_case_rev), accent="#10B981")
    kpi(k3,"จำนวนเจ้าของสัตว์",f"{unique_clients:,} คน", accent="#3B82F6")
    kpi(k4,"จำนวนสัตว์เลี้ยง",f"{unique_pets:,} ตัว", accent="#F59E0B")

    st.divider()

    # Charts
    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            st.markdown("#### 🐶 สัดส่วนตามประเภทสัตว์")
            pt_agg = cases.groupby("pet_type")["id"].count().reset_index()
            pt_agg.columns = ["ประเภท","จำนวน"]
            pt_agg = pt_agg.sort_values("จำนวน", ascending=False)
            labels_pt = [f"{PET_EMOJI.get(t,'🐾')} {t}" for t in pt_agg["ประเภท"]]
            donut(labels_pt, pt_agg["จำนวน"].tolist(),
                  colors=PALETTE[:len(pt_agg)], center_text=f"{len(cases)} เคส")

    with c2:
        with st.container(border=True):
            st.markdown("#### 🏥 บริการที่ใช้บ่อยที่สุด (Top 10)")
            svc_agg = cases.groupby("item_name")["id"].count().reset_index()
            svc_agg.columns = ["บริการ","จำนวนครั้ง"]
            svc_agg = svc_agg.sort_values("จำนวนครั้ง", ascending=True).tail(10)
            clrs_svc = [PALETTE[i % len(PALETTE)] for i in range(len(svc_agg))]
            fig_svc = go.Figure(go.Bar(
                x=svc_agg["จำนวนครั้ง"], y=svc_agg["บริการ"], orientation="h",
                marker=dict(color=clrs_svc, line_width=0),
                text=svc_agg["จำนวนครั้ง"], textposition="outside",
                hovertemplate="<b>%{y}</b><br>%{x} ครั้ง<extra></extra>",
            ))
            fig_svc.update_layout(**ch(height=320, showlegend=False,
                                        yaxis=dict(showgrid=False, tickfont=dict(size=10)),
                                        margin=dict(t=8,b=8,l=8,r=60)))
            st.plotly_chart(fig_svc, use_container_width=True)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Top clients
    with st.container(border=True):
        st.markdown("#### 👑 Top 10 ลูกค้าที่ใช้บริการสูงสุด")
        st.caption("ยอดสุทธิสะสม")
        top_cli = cases.groupby("client_name")["net_amount"].sum().reset_index()
        top_cli = top_cli.sort_values("net_amount", ascending=True).tail(10)
        vals_cli = top_cli["net_amount"].tolist()
        mx_cli = max(vals_cli) if vals_cli else 1
        clrs_cli = [f"rgba(124,58,237,{0.35+0.65*(v/mx_cli):.2f})" for v in vals_cli]
        hbar(top_cli, "net_amount", "client_name", colors=clrs_cli)

    st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

    # Search & filter
    with st.container(border=True):
        st.markdown("#### 🔍 ค้นหาและกรองเคส")
        s1, s2, s3 = st.columns(3)
        search_owner = s1.text_input("ชื่อเจ้าของ")
        search_pet   = s2.text_input("ชื่อสัตว์")
        pet_types_all = sorted(cases["pet_type"].dropna().unique().tolist())
        pet_type_flt  = s3.multiselect("ประเภทสัตว์", pet_types_all)

        disp = cases.copy()
        if search_owner: disp = disp[disp["client_name"].str.contains(search_owner, case=False, na=False)]
        if search_pet:   disp = disp[disp["pet_name"].str.contains(search_pet, case=False, na=False)]
        if pet_type_flt: disp = disp[disp["pet_type"].isin(pet_type_flt)]

        disp["ประเภทสัตว์"] = disp["pet_type"].map(lambda t: f"{PET_EMOJI.get(t,'🐾')} {t}")
        show_cols = {
            "tx_date":"วันที่","receipt_no":"เลขใบเสร็จ","client_name":"ชื่อเจ้าของ",
            "pet_name":"ชื่อสัตว์","ประเภทสัตว์":"ประเภทสัตว์","category":"หมวดหมู่",
            "item_name":"บริการ/สินค้า","qty":"จำนวน","net_amount":"ยอดสุทธิ (฿)","payment_method":"วิธีชำระ",
        }
        disp_show = disp.rename(columns=show_cols)[[v for v in show_cols.values() if v in disp.rename(columns=show_cols).columns]]
        st.dataframe(disp_show, use_container_width=True, height=400, hide_index=True)
        st.caption(f"แสดง {len(disp_show):,} เคส  •  ยอดรวม {fmt_thb(disp['net_amount'].sum())}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — STOCK
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💊 คลังยา & สินค้า":
    st.title("💊 คลังยา & สินค้า")

    tab1, tab2, tab3 = st.tabs(["📦 รายการสินค้า","🚚 ประวัติรับสินค้า","⬆️ นำเข้า XLS"])

    with tab1:
        df_si = load_stock_items()
        if df_si.empty:
            st.info("ยังไม่มีข้อมูล — นำเข้าที่แท็บ ⬆️ นำเข้า XLS")
        else:
            inv_val = (df_si["qty"] * df_si["avg_cost"].fillna(0)).sum()
            low_n = int(((df_si["qty"] <= df_si["alert_qty"]) & (df_si["qty"] > 0)).sum())
            out_n = int((df_si["qty"] <= 0).sum())
            ok_n  = len(df_si) - low_n - out_n

            k1,k2,k3,k4 = st.columns(4)
            kpi(k1,"รายการทั้งหมด",f"{len(df_si):,} รายการ", accent="#7C3AED")
            kpi(k2,"มูลค่าคลัง",fmt_thb(inv_val), accent="#10B981")
            kpi(k3,"ใกล้หมดสต๊อก",f"{low_n} รายการ", accent="#F59E0B")
            kpi(k4,"หมดสต๊อก",f"{out_n} รายการ", accent="#EF4444")
            st.divider()

            fa, fb = st.columns(2)
            drug_types = ["ทั้งหมด"] + sorted(df_si["drug_type"].dropna().unique().tolist())
            sel_type = fa.selectbox("ประเภทยา", drug_types)
            search_s = fb.text_input("🔍 ค้นหาชื่อสินค้า")

            df_f = df_si.copy()
            if sel_type != "ทั้งหมด": df_f = df_f[df_f["drug_type"] == sel_type]
            if search_s: df_f = df_f[df_f["stock_name"].str.contains(search_s, case=False, na=False)]

            # Top 20 by value chart
            with st.container(border=True):
                st.markdown("#### 💎 Top 20 สินค้าตามมูลค่าคลัง")
                df_f["inv_value"] = df_f["qty"] * df_f["sell_price"].fillna(0)
                top20 = df_f.nlargest(20, "inv_value")[["stock_name","inv_value"]].sort_values("inv_value")
                vals_s = top20["inv_value"].tolist()
                mx_s = max(vals_s) if vals_s else 1
                clrs_s = [f"rgba(59,130,246,{0.35+0.65*(v/mx_s):.2f})" for v in vals_s]
                hbar(top20, "inv_value", "stock_name", colors=clrs_s,
                     total=df_f["inv_value"].sum(), key="stock_top20")

            # Table
            st.divider()
            def row_status(r):
                lbl,_,clr = stock_status(r)
                return lbl
            df_f["สถานะ"] = df_f.apply(row_status, axis=1)
            show_si = df_f[["stock_id","stock_name","drug_type","qty","unit",
                             "avg_cost","sell_price","alert_qty","สถานะ"]].copy()
            show_si.columns = ["รหัส","ชื่อสินค้า","ประเภท","QTY","หน่วย",
                                "ทุนเฉลี่ย","ราคาขาย","แจ้งเตือน","สถานะ"]
            st.dataframe(show_si, use_container_width=True, height=420, hide_index=True)
            st.caption(f"แสดง {len(show_si):,} รายการ  •  มูลค่าคลัง {fmt_thb(inv_val)}")

    with tab2:
        df_inc = load_stock_incoming()
        if df_inc.empty:
            st.info("ยังไม่มีข้อมูลการรับสินค้า")
        else:
            total_incoming = df_inc["total_amount"].sum()
            n_suppliers = df_inc["supplier"].nunique()
            k1,k2,k3 = st.columns(3)
            kpi(k1,"รายการรับสินค้า",f"{len(df_inc):,} รายการ", accent="#7C3AED")
            kpi(k2,"มูลค่ารวม",fmt_thb(total_incoming), accent="#EF4444")
            kpi(k3,"จำนวน Supplier",f"{n_suppliers} ราย", accent="#3B82F6")
            st.divider()

            c1,c2 = st.columns([6,4])
            with c1:
                with st.container(border=True):
                    st.markdown("#### 📅 มูลค่าการรับสินค้าตามเดือน")
                    df_inc2 = df_inc.copy()
                    df_inc2["month"] = df_inc2["receive_date"].str[:7]
                    monthly = df_inc2.groupby("month")["total_amount"].sum().reset_index()
                    monthly = monthly.sort_values("month")
                    fig_m = go.Figure(go.Bar(
                        x=monthly["month"], y=monthly["total_amount"],
                        marker=dict(color="#EF4444", line_width=0, opacity=0.85),
                        text=monthly["total_amount"].map(fmt_thb),
                        textposition="outside", textfont=dict(size=10),
                    ))
                    fig_m.update_layout(**ch(height=280, showlegend=False,
                                              margin=dict(t=24,b=24,l=16,r=16)))
                    st.plotly_chart(fig_m, use_container_width=True)

            with c2:
                with st.container(border=True):
                    st.markdown("#### 🏭 แยกตาม Supplier")
                    sup_agg = df_inc.groupby("supplier")["total_amount"].sum().reset_index()
                    sup_agg = sup_agg[sup_agg["supplier"].str.strip() != ""].sort_values("total_amount", ascending=False)
                    if not sup_agg.empty:
                        donut(sup_agg["supplier"].tolist(), sup_agg["total_amount"].tolist(),
                              colors=PALETTE[:len(sup_agg)])

            st.divider()
            show_inc = df_inc[["receive_date","po_number","stock_name","supplier",
                                "qty","unit","unit_price","total_amount","expire_date"]].copy()
            show_inc.columns = ["วันที่รับ","เลขPO","ชื่อสินค้า","Supplier","จำนวน","หน่วย","ราคา/หน่วย","รวม (฿)","วันหมดอายุ"]
            st.dataframe(show_inc, use_container_width=True, height=380, hide_index=True)

    with tab3:
        col_a, col_b = st.columns(2)
        with col_a:
            st.markdown("#### 📋 รายการสินค้าทั้งหมด")
            st.caption("ไฟล์: `รายการสินค้าทั้งหมด.xls`")
            up_items = st.file_uploader("เลือกไฟล์ XLS", type=["xls","xlsx"], key="up_items")
            if up_items:
                try:
                    df_pv = read_xls_bytes(up_items.read())
                    st.success(f"พบ {len(df_pv):,} รายการ")
                    st.dataframe(df_pv.head(5), use_container_width=True)
                    if st.button("✅ นำเข้ารายการสินค้า", use_container_width=True):
                        import_stock_items(df_pv)
                        st.cache_data.clear(); st.success("นำเข้าสำเร็จ!"); st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
        with col_b:
            st.markdown("#### 🚚 ประวัติรับสินค้าเข้า Stock")
            st.caption("ไฟล์: `การรับสินค้าเข้า Stock.xls`")
            up_inc = st.file_uploader("เลือกไฟล์ XLS", type=["xls","xlsx"], key="up_incoming")
            if up_inc:
                try:
                    df_pv2 = read_xls_bytes(up_inc.read())
                    st.success(f"พบ {len(df_pv2):,} รายการ")
                    st.dataframe(df_pv2.head(5), use_container_width=True)
                    if st.button("✅ นำเข้าประวัติรับสินค้า", use_container_width=True):
                        import_stock_incoming(df_pv2)
                        st.cache_data.clear(); st.success("นำเข้าสำเร็จ!"); st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — IMPORT FILES
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "⬆️ นำเข้าไฟล์":
    st.title("⬆️ นำเข้าไฟล์ข้อมูล")

    # ── Section 1: PDF Financial Reports ──────────────────────────────────────
    st.markdown("## 📄 รายงานการเงิน (PDF)")
    st.caption("ระบบจะจดจำทุกรายงานที่นำเข้า และสามารถกรองตามช่วงเวลาได้")

    # Show existing imports
    pdf_log = fetch_import_log()
    pdf_log = pdf_log[pdf_log["file_type"] == "pdf_financial"] if not pdf_log.empty else pdf_log
    if not pdf_log.empty:
        st.markdown("**รายงานที่นำเข้าแล้ว:**")
        disp_log = pdf_log[["period_label","period_start","period_end","record_count","imported_at","file_name","id"]].copy()
        disp_log.columns = ["งวดเวลา","วันเริ่มต้น","วันสิ้นสุด","รายการสินค้า","นำเข้าเมื่อ","ชื่อไฟล์","import_id"]
        st.dataframe(disp_log.drop(columns=["import_id"]), use_container_width=True, hide_index=True)

        # Delete option
        del_ids = pdf_log["id"].tolist()
        del_labels = pdf_log["period_label"].tolist()
        del_map = dict(zip(del_labels, del_ids))
        with st.expander("🗑️ ลบรายงาน"):
            del_sel = st.selectbox("เลือกรายงานที่ต้องการลบ", del_labels)
            if st.button("❌ ยืนยันลบ", key="del_pdf"):
                delete_financial_period(del_map[del_sel])
                st.cache_data.clear(); st.success(f"ลบรายงาน '{del_sel}' แล้ว"); st.rerun()
    else:
        st.info("ยังไม่มีรายงานการเงิน")

    st.divider()
    up_pdf = st.file_uploader("📎 อัปโหลด PDF รายงานการเงินใหม่", type=["pdf"], key="up_pdf_main")
    if up_pdf:
        pdf_bytes = up_pdf.read()
        with st.spinner("กำลังวิเคราะห์ PDF..."):
            info, sdf, idf = parse_pdf(pdf_bytes)
            lbl, ps, pe = extract_period_from_pdf(pdf_bytes)

        st.markdown(f"**งวดเวลาที่ตรวจพบ:** `{lbl}` ({ps} → {pe})")
        st.caption("หากวันที่ไม่ถูกต้อง สามารถแก้ไขได้ก่อนบันทึก")
        ec1,ec2 = st.columns(2)
        ps_edit = ec1.date_input("วันเริ่มต้น", value=pd.to_datetime(ps).date(), key="ps_edit")
        pe_edit = ec2.date_input("วันสิ้นสุด",  value=pd.to_datetime(pe).date(), key="pe_edit")
        lbl_edit = st.text_input("ชื่องวด", value=lbl, key="lbl_edit")

        # Preview KPIs
        pv1,pv2,pv3,pv4,pv5 = st.columns(5)
        kpi(pv1,"รายรับรวม",fmt_thb(info.get("total",0)))
        kpi(pv2,"เงินสด",fmt_thb(info.get("cash",0)))
        kpi(pv3,"โอนเงิน",fmt_thb(info.get("transfer",0)))
        kpi(pv4,"ใบเสร็จ",f"{info.get('receipts',0):,} ใบ")
        kpi(pv5,"ต้นทุน",fmt_thb(info.get("cost",0)))

        if not sdf.empty:
            st.markdown("**ยอดแยกหมวด:**")
            st.dataframe(sdf, use_container_width=True, hide_index=True, height=200)

        if st.button("✅ บันทึกรายงานนี้เข้าระบบ", use_container_width=True, key="save_pdf"):
            ps_f = ps_edit.isoformat()
            pe_f = pe_edit.isoformat()
            imp_id = log_import(up_pdf.name, "pdf_financial", lbl_edit, ps_f, pe_f, len(idf))
            save_financial_period(imp_id, ps_f, pe_f, lbl_edit, info, sdf, idf)
            with open(DEFAULT_PDF, "wb") as f:
                f.write(pdf_bytes)
            st.cache_data.clear()
            st.success(f"✅ บันทึกรายงาน '{lbl_edit}' เรียบร้อยแล้ว!")
            st.rerun()

    st.divider()

    # ── Section 2: CSV Cases ──────────────────────────────────────────────────
    st.markdown("## 🐾 เคสผู้ป่วย (CSV)")
    st.caption("ข้อมูลรายเคส: เจ้าของ, ชื่อสัตว์, ประเภทสัตว์, บริการ, ยอดชำระ")

    case_log = fetch_import_log()
    case_log = case_log[case_log["file_type"] == "csv_cases"] if not case_log.empty else pd.DataFrame()
    if not case_log.empty:
        st.dataframe(case_log[["file_name","period_label","record_count","imported_at"]].rename(
            columns={"file_name":"ไฟล์","period_label":"งวด","record_count":"จำนวนเคส","imported_at":"นำเข้าเมื่อ"}),
            use_container_width=True, hide_index=True)

    # Template download
    tpl_cols = ["วันที่","เลขใบเสร็จ","ชื่อเจ้าของ","ชื่อสัตว์","ประเภทสัตว์",
                "หมวดหมู่","บริการ/สินค้า","จำนวน","หน่วย",
                "จำนวนเงิน","ส่วนลด","ยอดสุทธิ","วิธีชำระ"]
    tpl_df = pd.DataFrame(columns=tpl_cols)
    st.download_button("⬇️ ดาวน์โหลด Template CSV",
                       data=tpl_df.to_csv(index=False).encode("utf-8-sig"),
                       file_name="case_template.csv", mime="text/csv")

    up_csv = st.file_uploader("📎 อัปโหลด CSV เคสผู้ป่วย", type=["csv"], key="up_csv")
    if up_csv:
        try:
            csv_df = pd.read_csv(up_csv)
            st.success(f"พบ {len(csv_df):,} รายการ")
            st.dataframe(csv_df.head(5), use_container_width=True)
            lbl_csv = st.text_input("ชื่องวด (เช่น มกราคม 2569)", key="csv_lbl")
            if st.button("✅ บันทึกข้อมูลเคสเข้าระบบ", key="save_csv"):
                imp_id = log_import(up_csv.name, "csv_cases", lbl_csv, "", "", len(csv_df))
                import_case_transactions(csv_df, imp_id)
                st.cache_data.clear(); st.success("บันทึกเรียบร้อย!"); st.rerun()
        except Exception as e:
            st.error(f"Error: {e}")

    st.divider()

    # ── Section 3: Import History ─────────────────────────────────────────────
    st.markdown("## 📋 ประวัติการนำเข้าทั้งหมด")
    all_log = fetch_import_log()
    if not all_log.empty:
        type_map = {"pdf_financial":"📄 PDF รายงาน","csv_cases":"🐾 CSV เคส",
                    "xls_stock":"📦 XLS สต๊อก","xls_incoming":"🚚 XLS รับสินค้า"}
        all_log["ประเภทไฟล์"] = all_log["file_type"].map(lambda t: type_map.get(t, t))
        st.dataframe(all_log[["ประเภทไฟล์","file_name","period_label","record_count","imported_at"]].rename(
            columns={"file_name":"ชื่อไฟล์","period_label":"งวดเวลา","record_count":"จำนวนรายการ","imported_at":"นำเข้าเมื่อ"}),
            use_container_width=True, hide_index=True)
    else:
        st.info("ยังไม่มีประวัติการนำเข้า")
