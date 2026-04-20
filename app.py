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
    insert_expense, fetch_expenses, delete_expense,
)

st.set_page_config(page_title="เอสพี รักษาสัตว์", page_icon="🐾",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
.stApp { background: #FFFFFF !important; }
.main .block-container { padding-top: 1.5rem; padding-bottom: 2rem; background: #FFFFFF !important; }

[data-testid="stSidebar"] {
    background: #FFFFFF;
    border-right: 1px solid #F1F5F9;
    box-shadow: none;
}
[data-testid="stSidebar"] * { color: #334155 !important; }
[data-testid="stSidebarNav"] { display: none; }
/* ── Sidebar nav: style as nav items ── */
[data-testid="stSidebar"] div[role="radiogroup"] { gap: 2px; }
[data-testid="stSidebar"] div[role="radiogroup"] label > div:first-child { display: none !important; }
[data-testid="stSidebar"] div[role="radiogroup"] label {
    display: flex !important; align-items: center !important; cursor: pointer !important;
    border-radius: 8px !important; padding: 10px 16px !important;
    margin-bottom: 2px !important; transition: all 0.15s ease !important;
    font-size: 0.88rem !important; font-weight: 500 !important;
    color: #475569 !important; background: transparent !important;
    border: none !important; border-left: 3px solid transparent !important;
    width: 100% !important;
}
[data-testid="stSidebar"] div[role="radiogroup"] label:has(input:checked) {
    background: #F5F3FF !important; border-left-color: #7C3AED !important;
    color: #7C3AED !important; font-weight: 700 !important;
}
[data-baseweb="tab-list"] { gap: 8px; border-bottom: 1px solid #F1F5F9; background: transparent; }
[data-baseweb="tab"] {
    border-radius: 8px 8px 0 0 !important; padding: 10px 20px !important;
    color: #64748B !important; font-weight: 500 !important; font-size: 0.85rem !important;
    border: 1px solid transparent !important; border-bottom: none !important;
}
[data-baseweb="tab"][aria-selected="true"] {
    background: #FFFFFF !important; color: #7C3AED !important;
    border-color: #F1F5F9 !important; border-bottom: 2px solid #7C3AED !important;
}
[data-testid="stVerticalBlockBorderWrapper"] {
    background: #FFFFFF !important;
    border-radius: 12px !important;
    border: 1px solid #F1F5F9 !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.03) !important;
    padding: 1.25rem !important;
    margin-bottom: 1rem !important;
}
/* Force white background for all nested elements inside containers */
[data-testid="stVerticalBlockBorderWrapper"] div,
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stHorizontalBlock"],
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stBlock"],
[data-testid="stVerticalBlockBorderWrapper"] .element-container,
[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stMarkdownContainer"],
[data-testid="stVerticalBlockBorderWrapper"] .stMarkdown,
[data-testid="stVerticalBlockBorderWrapper"] section {
    background: #FFFFFF !important;
}
[data-testid="stVerticalBlockBorderWrapper"] h4 {
    color: #0F172A !important; font-size: 1.15rem !important; font-weight: 800 !important; 
    margin-bottom: 1.5rem !important; background: #FFFFFF !important;
    letter-spacing: -0.02em;
}
/* Fix dataframe styling */
[data-testid="stDataFrame"], [data-testid="stDataFrame"] > div {
    background: #FFFFFF !important;
}
[data-testid="stDataFrame"] {
    border: 1px solid #F1F5F9 !important;
    border-radius: 8px !important;
    overflow: hidden !important;
}
.stButton > button {
    background: #FFFFFF !important; color: #475569 !important;
    border: 1px solid #E2E8F0 !important; border-radius: 8px !important;
    font-size: 0.82rem !important; font-weight: 600 !important;
    padding: 0.4rem 1rem !important;
}
.stButton > button:hover {
    background: #F8FAFC !important; color: #7C3AED !important;
    border-color: #7C3AED !important;
}
.drill-badge {
    display:inline-flex; align-items:center; gap:8px;
    background:#F5F3FF; border:1px solid #DDD6FE; border-radius:20px;
    padding:6px 16px; color:#7C3AED; font-size:0.82rem; font-weight:600; margin-bottom:16px;
}
/* ── Clickable KPI Card Styles ── */
.kpi-wrapper {
    position: relative;
    width: 100%;
}
.kpi-card {
    background: #FFFFFF;
    border: 1px solid #F1F5F9;
    border-top: 4px solid var(--accent);
    border-radius: 16px;
    padding: 22px;
    min-height: 110px;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05);
    display: flex;
    flex-direction: column;
    justify-content: center;
    transition: all 0.2s ease;
}
.kpi-wrapper:hover .kpi-card {
    transform: translateY(-3px);
    box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.08);
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
    plot_bgcolor="#FFFFFF",
    paper_bgcolor="#FFFFFF",
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

@st.cache_data(ttl=30)
def load_expenses(start, end):
    try:
        return fetch_expenses(start, end)
    except Exception:
        return pd.DataFrame(columns=["id", "expense_date", "category", "amount", "description", "created_at"])


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
def kpi(col, label, value, note=None, accent="#7C3AED", delta=None, delta_up=True, click_key=None):
    dhtml = ""
    if delta:
        clr = "#16A34A" if delta_up else "#DC2626"
        arr = "▲" if delta_up else "▼"
        dhtml = f'<p style="margin:4px 0 0;color:{clr};font-size:.75rem;font-weight:600">{arr} {delta}</p>'
    nhtml = f'<p style="margin:4px 0 0;color:#94A3B8;font-size:.72rem;line-height:1.2">{note}</p>' if note else ""
    col.markdown(
        f'<div style="background:#FFFFFF;border:1px solid #E2E8F0;border-top:4px solid {accent};border-radius:16px;padding:20px 22px 16px;min-height:108px;box-shadow:0 2px 6px rgba(0,0,0,0.04)">'
        f'<p style="margin:0;color:#64748B;font-size:.68rem;font-weight:700;letter-spacing:.08em;text-transform:uppercase">{label}</p>'
        f'<p style="margin:6px 0 0;color:#0F172A;font-size:1.6rem;font-weight:800;line-height:1.1">{value}</p>'
        f'{dhtml}{nhtml}</div>',
        unsafe_allow_html=True
    )
    return False

@st.dialog("📋 รายละเอียดข้อมูล")
def show_drilldown_modal(title, df, columns=None, download_name="data.csv"):
    st.markdown(f"#### {title}")
    if df.empty:
        st.info("ไม่พบข้อมูลที่เกี่ยวข้อง")
    else:
        if columns:
            disp_df = df[columns].copy()
        else:
            disp_df = df.copy()
        
        st.dataframe(disp_df, use_container_width=True, hide_index=True)
        
        csv = disp_df.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="📥 ดาวน์โหลดข้อมูล (CSV)",
            data=csv,
            file_name=download_name,
            mime='text/csv',
            key=f"dl_{hash(title)}"
        )

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

def donut(labels, values, title="", colors=None, center_text="", height=380):
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
        height=height,
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.05,
                    xanchor="center", x=0.5, **LEGEND_STYLE),
        margin=dict(t=8, b=80, l=8, r=8),
        annotations=[dict(text=f"<b>{ann_text}</b>", x=0.5, y=0.5,
                          font_size=14, font_color="#374151", showarrow=False)],
    ))
    if title: section(title)
    st.plotly_chart(fig, use_container_width=True)

def clean_txt(t):
    if not t: return ""
    # Remove replacement characters and other odd non-printable stuff
    return str(t).replace("", "").strip()

def render_revenue_table(df, label_col, value_col, total_val, colors=None):
    html = f"""<div style="background:#FFFFFF; border:1px solid #E2E8F0; border-radius:12px; overflow:hidden; margin-top:5px;">
<table style="width:100%; font-size:0.82rem; border-collapse:collapse; font-family:'Inter',sans-serif; table-layout:fixed;">
<thead>
<tr style="background:#F8FAFC; border-bottom:1px solid #E2E8F0;">
<th style="text-align:left; padding:12px 16px; font-weight:600; color:#64748B; width:55%; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.05em;">รายการ</th>
<th style="text-align:right; padding:12px 16px; font-weight:600; color:#64748B; width:25%; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.05em;">ยอดรวม (฿)</th>
<th style="text-align:right; padding:12px 16px; font-weight:600; color:#64748B; width:20%; font-size:0.72rem; text-transform:uppercase; letter-spacing:0.05em;">สัดส่วน</th>
</tr>
</thead>
<tbody>"""
    for i, row in df.iterrows():
        color = colors[i] if colors else color_for(row[label_col])
        share = (row[value_col] / total_val * 100) if total_val > 0 else 0
        txt = clean_txt(row[label_col])
        html += f"""<tr style="border-bottom:1px solid #F1F5F9;">
<td style="padding:10px 16px; vertical-align:middle;">
<div style="display:flex; align-items:center; gap:10px;">
<div style="min-width:10px; height:10px; border-radius:2px; background:{color};"></div>
<div style="color:#334155; font-weight:500; white-space:nowrap; overflow:hidden; text-overflow:ellipsis;" title="{txt}">{txt}</div>
</div>
</td>
<td style="text-align:right; padding:10px 16px; color:#0F172A; font-weight:700;">{fmt_thb(row[value_col])}</td>
<td style="text-align:right; padding:10px 16px;">
<span style="background:#F5F3FF; color:#7C3AED; padding:2px 8px; border-radius:10px; font-size:0.75rem; font-weight:700;">{share:.1f}%</span>
</td>
</tr>"""
    html += "</tbody></table></div>"
    st.markdown(html, unsafe_allow_html=True)

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
        "📊 แดชบอร์ดภาพรวม",
        "📈 วิเคราะห์เชิงลึก",
        "💰 รายงานกำไร-ขาดทุน",
        "🐾 บริหารจัดการเคส",
        "💊 สต๊อกสินค้าและยา",
        "💸 บันทึกค่าใช้จ่าย",
        "⬆️ นำเข้าข้อมูลระบบ",
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
if page == "📊 แดชบอร์ดภาพรวม":
    st.title("📊 แดชบอร์ดภาพรวม")

    # ── Global Drilldown Handler ──────────────────────────────────────────
    if st.session_state.get("drill_cat"):
        drill_type = st.session_state["drill_cat"]
        cats_all = load_cats(fs, fe)
        itms_all = load_items(fs, fe)

        if drill_type == "ALL_CATS":
            st.markdown("### 🔍 รายละเอียดรายได้แยกตามประเภทสินค้า")
            if not cats_all.empty:
                cat_sum = cats_all.groupby("category")["amount"].sum().reset_index().sort_values("amount", ascending=False)
                render_revenue_table(cat_sum, "category", "amount", cat_sum["amount"].sum())
            if st.button("⬅️ กลับหน้าหลัก"):
                st.session_state["drill_cat"] = None
                st.rerun()
            st.divider()
        elif drill_type == "ALL_ITEMS":
            st.markdown("### 🔍 รายละเอียดรายได้แยกตามรายการ")
            if not itms_all.empty:
                itm_sum = itms_all.groupby(["category", "item_name"])["total"].sum().reset_index().sort_values("total", ascending=False)
                st.dataframe(itm_sum, use_container_width=True, hide_index=True)
            if st.button("⬅️ กลับหน้าหลัก"):
                st.session_state["drill_cat"] = None
                st.rerun()
            st.divider()

    fp   = load_fp(fs, fe)
    cats = load_cats(fs, fe)
    itms = load_items(fs, fe)
    stk  = load_stock_items()
    opex_df = load_expenses(fs, fe)

    if fp.empty:
        st.info("📭 ยังไม่มีข้อมูลรายงานการเงิน — ไปที่ **⬆️ นำเข้าไฟล์** เพื่ออัปโหลด PDF")
    else:
        total_rev  = fp["total_revenue"].sum()
        total_cost = fp["total_cost"].sum()
        gross_p    = fp["gross_profit"].sum()
        total_opex = opex_df["amount"].sum() if not opex_df.empty else 0
        net_profit = gross_p - total_opex
        
        margin_pct = (gross_p / total_rev * 100) if total_rev > 0 else 0
        cash_v     = fp["cash_revenue"].sum()
        transfer_v = fp["transfer_revenue"].sum()
        receipts   = int(fp["receipts_count"].sum())
        cancelled  = int(fp["cancelled_count"].sum())

        # ── KPI Row ───────────────────────────────────────────────────────────
        k1,k2,k3,k4,k5,k6 = st.columns(6)
        
        if kpi(k1, "รายรับรวม", fmt_thb(total_rev), accent="#7C3AED", click_key="rev_all"):
            show_drilldown_modal("💰 รายละเอียดรายรับตามหมวดหมู่", load_cats(fs, fe), columns=["category", "amount"])
            
        kpi(k2, "กำไรขั้นต้น", fmt_thb(gross_p), accent="#10B981")
        
        if kpi(k3, "กำไรสุทธิ (Net)", fmt_thb(net_profit), accent="#059669", click_key="net_all"):
            show_drilldown_modal("💸 รายละเอียดค่าใช้จ่าย (OPEX)", load_expenses(fs, fe), columns=["expense_date", "category", "amount", "description"])

        kpi(k4, "อัตรากำไร", f"{margin_pct:.1f}%", note="เป้าหมาย ≥ 50%", accent="#F59E0B")
        
        if kpi(k5, "ใบเสร็จทั้งหมด", f"{receipts:,} ใบ", note=f"ยกเลิก {cancelled:,} ใบ", accent="#3B82F6", click_key="rec_all"):
            show_drilldown_modal("📄 รายละเอียดรอบรายงานการเงิน", load_fp(fs, fe), columns=["period_label", "period_start", "total_revenue", "receipts_count"])

        if not stk.empty:
            inv_val_kpi = (stk["qty"] * stk["avg_cost"].fillna(0)).sum()
            if kpi(k6, "มูลค่าคลังสินค้า", fmt_thb(inv_val_kpi), accent="#84CC16", click_key="stk_all"):
                show_drilldown_modal("📦 รายการสินค้าที่มีมูลค่าสูงสุด", stk.nlargest(20, 'qty'), columns=["stock_name", "qty", "unit", "avg_cost"])
        else:
            kpi(k6, "ต้นทุนรวม", fmt_thb(total_cost), accent="#EF4444")

        st.markdown("<div style='height:12px'></div>", unsafe_allow_html=True)

        # ── Row 1: Revenue by category + Table ────────────────────────────────
        with st.container(border=True):
            cols = st.columns([10, 2])
            cols[0].markdown("#### 🛍️ รายได้แยกตามประเภทสินค้า")
            if cols[1].button("🔍 ดูแบบเต็ม", key="btn_cat_full"):
                st.session_state["drill_cat"] = "ALL_CATS"
            
            if not cats.empty:
                cat_agg = cats.groupby("category")["amount"].sum().reset_index().sort_values("amount", ascending=False)
                total_cat = cat_agg["amount"].sum()
                
                c1, c2 = st.columns([5, 5])
                with c1:
                    donut(cat_agg["category"].tolist(), cat_agg["amount"].tolist(), height=400)
                with c2:
                    render_revenue_table(cat_agg, "category", "amount", total_cat)
            else:
                no_data()

        st.markdown("<div style='height:8px'></div>", unsafe_allow_html=True)

        # ── Row 2: Revenue by Services/Items + Table ──────────────────────────
        with st.container(border=True):
            cols = st.columns([10, 2])
            cols[0].markdown("#### 🩺 รายได้แยกตามรายการบริการ/สินค้า")
            if cols[1].button("🔍 ดูแบบเต็ม", key="btn_itm_full"):
                st.session_state["drill_cat"] = "ALL_ITEMS"

            if not itms.empty:
                itm_agg = itms.groupby("item_name")["total"].sum().reset_index().sort_values("total", ascending=False).head(10)
                total_itm = itms["total"].sum()
                
                c1, c2 = st.columns([5, 5])
                with c1:
                    # For items, we might have many, so just show top 10 in donut
                    donut(itm_agg["item_name"].tolist(), itm_agg["total"].tolist(), height=420)
                with c2:
                    render_revenue_table(itm_agg, "item_name", "total", total_itm)
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
# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — ANALYTICS (CONSOLIDATED)
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "📈 วิเคราะห์เชิงลึก":
    st.title("📈 วิเคราะห์ข้อมูลเชิงลึก")

    with st.container(border=True):
        st.markdown("#### 📉 แนวโน้มรายรับและกำไร (รายเดือน)")
        fp_all = load_fp(None, None)
        if not fp_all.empty:
            fp_all["month"] = pd.to_datetime(fp_all["period_start"]).dt.strftime('%Y-%m')
            trend = fp_all.groupby("month")[["total_revenue", "gross_profit"]].sum().reset_index()
            trend = trend.sort_values("month")
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=trend["month"], y=trend["total_revenue"], name="รายรับรวม",
                                     line=dict(color="#7C3AED", width=3), mode='lines+markers'))
            fig.add_trace(go.Scatter(x=trend["month"], y=trend["gross_profit"], name="กำไรขั้นต้น",
                                     line=dict(color="#10B981", width=3), mode='lines+markers'))
            fig.update_layout(ch(height=350), hovermode="x unified", legend=LEGEND_STYLE)
            st.plotly_chart(fig, use_container_width=True)
        else:
            no_data()

    c1, c2 = st.columns(2)
    with c1:
        with st.container(border=True):
            st.markdown("#### 👥 ข้อมูลลูกค้าเชิงลึก (Top Spenders)")
            df_c = load_cases(fs, fe)
            if not df_c.empty:
                top_spend = df_c.groupby("client_name")["net_amount"].sum().reset_index()
                top_spend = top_spend.sort_values("net_amount", ascending=False).head(10)
                st.dataframe(top_spend.rename(columns={"client_name":"ชื่อลูกค้า","net_amount":"ยอดรวม"}),
                             use_container_width=True, hide_index=True)
            else:
                no_data()

    with c2:
        with st.container(border=True):
            st.markdown("#### ⚙️ ประสิทธิภาพร้าน (Operational)")
            fp = load_fp(fs, fe)
            if not fp.empty:
                t_rev = fp["total_revenue"].sum()
                t_rec = fp["receipts_count"].sum()
                atv = t_rev / t_rec if t_rec > 0 else 0
                k1, k2 = st.columns(2)
                kpi(k1, "เฉลี่ยต่อบิล", fmt_thb(atv), note="ATV Value", accent="#3B82F6")
                kpi(k2, "จำนวนบิล", f"{t_rec:,.0f} ใบ", accent="#7C3AED")

                st.divider()
                st.markdown("#### 📊 รายรับตามช่องทาง")
                cash = fp["cash_revenue"].sum()
                tran = fp["transfer_revenue"].sum()
                fig_p = go.Figure(data=[go.Pie(labels=["เงินสด", "โอนเงิน"], values=[cash, tran], hole=.4,
                                               marker=dict(colors=["#F59E0B", "#3B82F6"]))])
                fig_p.update_layout(ch(height=250), showlegend=True, legend=LEGEND_STYLE)
                st.plotly_chart(fig_p, use_container_width=True)
            else:
                no_data()
# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — P&L WITH ITEM ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💰 รายงานกำไร-ขาดทุน":
    st.title("💰 รายงานกำไร-ขาดทุนรายรายการ")

    fp = load_fp(fs, fe)
    itms = load_items(fs, fe)
    stk = load_stock_items()

    if fp.empty:
        st.info("📭 ยังไม่มีข้อมูลรายงานการเงิน")
    else:
        total_rev  = fp["total_revenue"].sum()
        total_cost = fp["total_cost"].sum()
        gross_p    = fp["gross_profit"].sum()
        margin_avg = (gross_p / total_rev * 100) if total_rev > 0 else 0

        k1,k2,k3,k4 = st.columns(4)
        kpi(k1,"รายรับรวม", fmt_thb(total_rev), accent="#7C3AED")
        kpi(k2,"ต้นทุนรวม", fmt_thb(total_cost), accent="#EF4444")
        kpi(k3,"กำไรขั้นต้น", fmt_thb(gross_p), accent="#10B981")
        kpi(k4,"Margin เฉลี่ย", f"{margin_avg:.1f}%", accent="#F59E0B")

        st.divider()
        
        st.markdown("### 📊 วิเคราะห์กำไรรายรายการสินค้า/บริการ")
        if not itms.empty:
            # Aggregate sales items
            itm_agg = itms.groupby("item_name").agg({"qty":"sum", "total":"sum"}).reset_index()
            
            # Map cost from stock_items if available
            cost_map = {}
            if not stk.empty:
                cost_map = dict(zip(stk["stock_name"], stk["avg_cost"]))
            
            def analyze_pnl(row):
                name = row["item_name"]
                rev = row["total"]
                qty = row["qty"] if row["qty"] > 0 else 1
                price_per = rev / qty
                cost_per = cost_map.get(name, 0)
                profit_per = price_per - cost_per
                margin = (profit_per / price_per * 100) if price_per > 0 else 0
                
                symbol = "🟢"
                if margin < 15: symbol = "🔴"
                elif margin < 30: symbol = "⚠️"
                
                return pd.Series([cost_per, price_per, profit_per, margin, symbol])

            itm_agg[["ทุน/หน่วย", "ขาย/หน่วย", "กำไร/หน่วย", "Margin%", "คุ้มค่า"]] = itm_agg.apply(analyze_pnl, axis=1)
            itm_agg = itm_agg.sort_values("Margin%", ascending=True)
            
            show_pnl = itm_agg[["คุ้มค่า", "item_name", "ทุน/หน่วย", "ขาย/หน่วย", "กำไร/หน่วย", "Margin%"]].copy()
            show_pnl.columns = ["สถานะ", "ชื่อรายการ", "ต้นทุน", "ราคาขาย", "กำไรต่อหน่วย", "% กำไร"]
            
            st.dataframe(show_pnl.style.format({
                "ต้นทุน": "{:,.2f}", "ราคาขาย": "{:,.2f}", "กำไรต่อหน่วย": "{:,.2f}", "% กำไร": "{:.1f}%"
            }), use_container_width=True, hide_index=True)
            
            st.caption("🔴 กำไร < 15% | ⚠️ กำไร < 30% | 🟢 กำไรปกติ")
        else:
            no_data("ยังไม่มีข้อมูลรายการสินค้าที่ขาย")


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
# PAGE 5 — STOCK (UPGRADED WITH DIALOGS)
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💊 สต๊อกสินค้าและยา":
    st.title("💊 สต๊อกสินค้าและบริหารจัดการคลัง")

    df_si = load_stock_items()
    if df_si.empty:
        st.info("ยังไม่มีข้อมูล — กรุณานำเข้า XLS รายการสินค้าก่อน")
    else:
        # Data calculations
        inv_val = (df_si["qty"] * df_si["avg_cost"].fillna(0)).sum()
        low_stock_df = df_si[(df_si["qty"] <= df_si["alert_qty"]) & (df_si["qty"] > 0)]
        out_stock_df = df_si[df_si["qty"] <= 0]
        
        low_n = len(low_stock_df)
        out_n = len(out_stock_df)

        # Standard KPI Cards
        k1,k2,k3,k4 = st.columns(4)
        kpi(k1, "มูลค่าคลังรวม", fmt_thb(inv_val), accent="#7C3AED")
        kpi(k2, "รายการทั้งหมด", f"{len(df_si):,} รายการ", accent="#3B82F6")
        
        # Interactive KPI Cards using the click_key mechanism
        if kpi(k3, "ใกล้หมดสต๊อก", f"{low_n} รายการ", accent="#F59E0B", click_key="kpi_low"):
            cols_to_show = ["stock_id", "stock_name", "qty", "unit", "alert_qty", "supplier"]
            proc_df = low_stock_df.copy()
            proc_df["ควรสั่งเพิ่ม"] = (proc_df["alert_qty"] * 2 - proc_df["qty"]).clip(lower=1)
            show_drilldown_modal("⚠️ รายการสินค้าใกล้หมด", proc_df, 
                                columns=cols_to_show + ["ควรสั่งเพิ่ม"], 
                                download_name=f"low_stock_{date.today()}.csv")
            
        if kpi(k4, "หมดสต๊อก", f"{out_n} รายการ", accent="#EF4444", click_key="kpi_out"):
            cols_to_show = ["stock_id", "stock_name", "qty", "unit", "alert_qty", "supplier"]
            proc_df = out_stock_df.copy()
            proc_df["ควรสั่งเพิ่ม"] = proc_df["alert_qty"].clip(lower=1)
            show_drilldown_modal("🚨 รายการสินค้าที่หมดแล้ว", proc_df, 
                                columns=cols_to_show + ["ควรสั่งเพิ่ม"], 
                                download_name=f"out_of_stock_{date.today()}.csv")
        
        st.divider()

        # ── Main Content Tabs ──
        tab1, tab2, tab3 = st.tabs(["🔍 ค้นหาสต๊อก", "📊 เคลื่อนไหว & ขาดสต๊อก", "⬆️ นำเข้าข้อมูล"])

        with tab1:
            st.markdown("#### 📦 ค้นหารายการสินค้าในคลัง")
            drug_types = ["ทั้งหมด"] + sorted(df_si["drug_type"].dropna().unique().tolist())
            fa, fb = st.columns([4, 6])
            sel_type = fa.selectbox("กรองประเภทสินค้า", drug_types)
            search_s = fb.text_input("🔍 พิมพ์ชื่อสินค้าเพื่อค้นหา...")
            
            df_f = df_si.copy()
            if sel_type != "ทั้งหมด": df_f = df_f[df_f["drug_type"] == sel_type]
            if search_s: df_f = df_f[df_f["stock_name"].str.contains(search_s, case=False, na=False)]
            
            # Show a more comprehensive table for searching
            st.dataframe(df_f[["stock_id", "stock_name", "drug_type", "qty", "unit", "sell_price", "warehouse", "supplier"]].rename(
                columns={"stock_id":"รหัส", "stock_name":"ชื่อสินค้า", "drug_type":"ประเภท", "qty":"จำนวน", "unit":"หน่วย", "sell_price":"ราคาขาย", "warehouse":"คลัง", "supplier":"ผู้ผลิต"}), 
                use_container_width=True, hide_index=True)

        with tab2:
            st.markdown("#### 📊 ความเคลื่อนไหวสต๊อกรายเดือน")
            df_inc = load_stock_incoming()
            
            col_m1, col_m2 = st.columns([6, 4])
            
            with col_m1:
                if not df_inc.empty:
                    df_inc["month"] = pd.to_datetime(df_inc["receive_date"]).dt.strftime('%Y-%m')
                    monthly_inc = df_inc.groupby("month")["total_amount"].sum().reset_index()
                    fig_inc = go.Figure(go.Bar(x=monthly_inc["month"], y=monthly_inc["total_amount"], 
                                               marker_color="#10B981", name="ยอดรับเข้า (฿)"))
                    fig_inc.update_layout(ch(height=350), title="มูลค่าการรับสินค้าเข้าคลังรายเดือน")
                    st.plotly_chart(fig_inc, use_container_width=True)
                else:
                    no_data("ยังไม่มีประวัติการรับสินค้าเข้า")

            with col_m2:
                st.markdown("#### 🚨 สรุปสถานะสต๊อกขาด")
                if low_n + out_n > 0:
                    st.error(f"ตรวจพบสินค้าที่มีปัญหา {low_n + out_n} รายการ")
                    st.write(f"- ใกล้หมด: {low_n} รายการ")
                    st.write(f"- หมดสต๊อก: {out_n} รายการ")
                    st.info("💡 คลิกที่ Card ด้านบนเพื่อดูรายละเอียดและสั่งซื้อ")
                else:
                    st.success("✅ สต๊อกสินค้าอยู่ในระดับปกติทุกรายการ")

        with tab3:
            col_a, col_b = st.columns(2)
            with col_a:
                with st.container(border=True):
                    st.markdown("#### 📋 อัปเดตรายการสินค้า (Master)")
                    up_items = st.file_uploader("ไฟล์ XLS รายการสินค้าทั้งหมด", type=["xls","xlsx"], key="up_items_v2")
                    if up_items and st.button("✅ เริ่มนำเข้า", key="btn_imp_v2_1"):
                        try:
                            df_pv = read_xls_bytes(up_items.read())
                            import_stock_items(df_pv)
                            st.cache_data.clear(); st.success("นำเข้าสำเร็จ!"); st.rerun()
                        except Exception as e: st.error(f"Error: {e}")
            with col_b:
                with st.container(border=True):
                    st.markdown("#### 🚚 อัปเดตประวัติรับเข้า (History)")
                    up_inc = st.file_uploader("ไฟล์ XLS ประวัติรับสินค้า", type=["xls","xlsx"], key="up_inc_v2")
                    if up_inc and st.button("✅ เริ่มนำเข้า", key="btn_imp_v2_2"):
                        try:
                            df_pv2 = read_xls_bytes(up_inc.read())
                            import_stock_incoming(df_pv2)
                            st.cache_data.clear(); st.success("นำเข้าสำเร็จ!"); st.rerun()
                        except Exception as e: st.error(f"Error: {e}")

# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — IMPORT FILES
# ═══════════════════════════════════════════════════════════════════════════════
elif page == "💸 ค่าใช้จ่าย":
    st.title("💸 การจัดการค่าใช้จ่าย (OPEX)")
    
    col1, col2 = st.columns([4, 6])
    
    with col1:
        with st.container(border=True):
            st.markdown("#### ➕ เพิ่มรายการค่าใช้จ่าย")
            with st.form("expense_form", clear_on_submit=True):
                ex_date = st.date_input("วันที่", date.today())
                ex_cat = st.selectbox("หมวดหมู่", ["ค่าเช่า", "เงินเดือน/ค่าจ้าง", "ค่าน้ำ-ไฟ", "ค่าอินเทอร์เน็ต", "เครื่องใช้สำนักงาน", "ค่าซ่อมแซม", "อื่นๆ"])
                ex_amount = st.number_input("จำนวนเงิน (บาท)", min_value=0.0, step=100.0)
                ex_desc = st.text_input("รายละเอียด/หมายเหตุ")
                
                if st.form_submit_button("บันทึกรายการ", use_container_width=True):
                    if ex_amount > 0:
                        insert_expense(ex_date.isoformat(), ex_cat, ex_amount, ex_desc)
                        st.success("บันทึกสำเร็จ!")
                        st.cache_data.clear()
                        st.rerun()
                    else:
                        st.error("กรุณาระบุจำนวนเงิน")
                        
    with col2:
        df_ex = load_expenses(fs, fe)
        if not df_ex.empty:
            st.markdown(f"#### 📑 รายการช่วง {fs} ถึง {fe}")
            total_ex = df_ex["amount"].sum()
            k_ex = st.columns(1)[0]
            kpi(k_ex, "รวมค่าใช้จ่าย", fmt_thb(total_ex), accent="#EF4444")
            
            ex_agg = df_ex.groupby("category")["amount"].sum().reset_index()
            fig_ex = go.Figure(go.Bar(x=ex_agg["category"], y=ex_agg["amount"], marker_color="#EF4444"))
            fig_ex.update_layout(ch(height=250))
            st.plotly_chart(fig_ex, use_container_width=True)
            
            st.markdown("---")
            df_display = df_ex.copy()
            df_display.columns = ["ID", "วันที่", "หมวดหมู่", "จำนวนเงิน", "รายละเอียด"]
            st.dataframe(df_display, use_container_width=True, hide_index=True)
            
            with st.expander("🗑️ ลบรายการ"):
                del_id = st.number_input("ระบุ ID ที่ต้องการลบ", min_value=1, step=1)
                if st.button("ยืนยันการลบ", type="primary"):
                    delete_expense(del_id)
                    st.warning(f"ลบรายการ ID {del_id} แล้ว")
                    st.cache_data.clear()
                    st.rerun()
        else:
            no_data("ยังไม่มีการบันทึกค่าใช้จ่ายในช่วงนี้")

elif page == "⬆️ นำเข้าข้อมูลระบบ":
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
