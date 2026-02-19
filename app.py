"""
MA Enrollment Intelligence Dashboard
=====================================
• Data: CMS CPSC monthly enrollment (CSV) + MA Plan Directory
• Host: Streamlit Community Cloud (public, no login)
• Refresh: Automatic monthly via GitHub Actions + manual button
• Rolling: 24-month window, always current
"""

import streamlit as st
import pandas as pd
import numpy as np
import requests
import zipfile
import io
import os
import hashlib
import json
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
import plotly.express as px
import plotly.graph_objects as go

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG  ── must be first Streamlit call
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="MA Enrollment Intelligence",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("""
<style>
/* ---------- base ---------- */
.main .block-container{padding-top:1.2rem;padding-bottom:2rem}
h1{color:#003087!important;font-size:1.75rem!important;font-weight:700}
h2{color:#003087!important;font-size:1.25rem!important}
h3{color:#005EB8!important;font-size:1.05rem!important;margin-top:.5rem}

/* ---------- KPI cards ---------- */
.kpi-wrap{display:flex;flex-direction:column;height:100%}
.kpi-card{
  background:linear-gradient(135deg,#003087,#005EB8);
  border-radius:12px;padding:1rem 1.2rem;color:#fff;
  box-shadow:0 4px 14px rgba(0,48,135,.18);flex:1
}
.kpi-label{font-size:.7rem;text-transform:uppercase;letter-spacing:.1em;opacity:.8;margin-bottom:.25rem}
.kpi-value{font-size:1.85rem;font-weight:800;line-height:1.05}
.kpi-delta{font-size:.75rem;margin-top:.3rem}
.kpi-pos{color:#7DFFB3}
.kpi-neg{color:#FF8080}

/* ---------- status banner ---------- */
.status-bar{
  background:#EBF4FF;border-left:4px solid #005EB8;
  padding:.55rem 1rem;border-radius:0 8px 8px 0;
  font-size:.82rem;margin-bottom:.8rem;color:#003087
}

/* ---------- sidebar tweaks ---------- */
section[data-testid="stSidebar"] .block-container{padding-top:1.5rem}

/* ---------- metric widget ---------- */
[data-testid="stMetric"]{
  background:#F5F8FF;border-radius:8px;padding:.7rem;
  border-left:3px solid #005EB8
}
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────
MONTH_NAMES = {
    1:"january",2:"february",3:"march",4:"april",5:"may",6:"june",
    7:"july",8:"august",9:"september",10:"october",11:"november",12:"december"
}

# Known exact column names used in CMS CPSC CSV files (multiple vintages)
# Enrollment column: CMS uses "Enrollment" in recent files
CPSC_COL_ALIASES = {
    # contract
    "contract_number":        "Contract_ID",
    "contract number":        "Contract_ID",
    "h_number":               "Contract_ID",
    "h number":               "Contract_ID",
    # plan
    "plan_id":                "Plan_ID",
    "plan id":                "Plan_ID",
    "plan_identifier":        "Plan_ID",
    # segment
    "segment_id":             "Segment_ID",
    "segment id":             "Segment_ID",
    # org / plan name
    "organization_name":      "Org_Name",
    "organization name":      "Org_Name",
    "organization_marketing_name": "Org_Name",
    "organization marketing name": "Org_Name",
    "contract_name":          "Org_Name",
    "contract name":          "Org_Name",
    # state
    "state":                  "State",
    "state_code":             "State",
    "state code":             "State",
    "ssa_state_code":         "State_SSA",
    # county
    "county":                 "County",
    "county_name":            "County",
    "county name":            "County",
    "ssa_county_code":        "County_SSA",
    # combined fips/ssa
    "ssa_state_county_code":  "SSA_Code",
    "ssa state county code":  "SSA_Code",
    "fips_state_county_code": "FIPS",
    # plan type
    "plan_type":              "Plan_Type",
    "plan type":              "Plan_Type",
    "type_of_medicare_health_plan": "Plan_Type",
    "type of medicare health plan": "Plan_Type",
    # enrollment — primary field
    "enrollment":             "Enrollment",
    "total_enrollment":       "Enrollment",
    "total enrollment":       "Enrollment",
    "enrolled":               "Enrollment",
}

# Plan directory column aliases
PLANDIR_COL_ALIASES = {
    "contract_number":        "Contract_ID",
    "contract number":        "Contract_ID",
    "h_number":               "Contract_ID",
    "h number":               "Contract_ID",
    "contract_id":            "Contract_ID",
    "organization_name":      "Org_Name",
    "organization name":      "Org_Name",
    "contract_name":          "Org_Name",
    "contract name":          "Org_Name",
    "parent_organization":    "Parent_Org",
    "parent organization":    "Parent_Org",
    "parent_org":             "Parent_Org",
    "parent org":             "Parent_Org",
    "plan_type":              "Plan_Type",
    "plan type":              "Plan_Type",
}

CHART_COLORS = px.colors.qualitative.Set2 + px.colors.qualitative.Pastel


# ─────────────────────────────────────────────────────────────────────────────
# URL BUILDERS  +  LOCAL DATA PATH HELPERS
# ─────────────────────────────────────────────────────────────────────────────
# The GitHub Action (fetch_cms_data.yml) downloads CMS ZIPs on GitHub's
# servers (which CAN reach cms.gov) and commits the extracted CSVs into
#   data/cpsc/cpsc-YYYY-MM.csv   and   data/plandir/plan-directory.csv
# The app reads those local files directly — no runtime HTTP to cms.gov needed.
# Live HTTP fetch is kept as a fallback for local dev / first-run before the
# Action has run.

DATA_DIR      = os.path.join(os.path.dirname(__file__), "data")
CPSC_DIR      = os.path.join(DATA_DIR, "cpsc")
PLANDIR_DIR   = os.path.join(DATA_DIR, "plandir")

def cpsc_url(year: int, month: int) -> str:
    return (
        f"https://www.cms.gov/files/zip/"
        f"monthly-enrollment-cpsc-{MONTH_NAMES[month]}-{year}.zip"
    )

def plandir_url(year: int, month: int) -> str:
    return (
        f"https://www.cms.gov/files/zip/"
        f"plan-directory-{MONTH_NAMES[month]}-{year}.zip"
    )

def rolling_months(n: int = 24) -> list[tuple[int, int]]:
    """Last n months (most-recent first), offset 2 months for CMS lag."""
    base = date.today().replace(day=1) - relativedelta(months=2)
    return [((base - relativedelta(months=i)).year,
              (base - relativedelta(months=i)).month)
            for i in range(n)]

def _local_cpsc_path(year: int, month: int) -> str | None:
    """Return path to pre-fetched CPSC file if it exists, else None."""
    for ext in (".csv", ".xlsx", ".xls"):
        p = os.path.join(CPSC_DIR, f"cpsc-{year}-{month:02d}{ext}")
        if os.path.exists(p):
            return p
    return None

def _local_plandir_path() -> str | None:
    """Return path to pre-fetched Plan Directory file if it exists."""
    if not os.path.isdir(PLANDIR_DIR):
        return None
    for fname in sorted(os.listdir(PLANDIR_DIR), reverse=True):
        if fname.startswith("plan-directory") and fname.endswith((".csv",".xlsx",".xls")):
            return os.path.join(PLANDIR_DIR, fname)
    return None

def _read_local_file(path: str) -> pd.DataFrame | None:
    """Read a local CSV or Excel file into a DataFrame."""
    try:
        raw = open(path, "rb").read()
        if path.lower().endswith(".csv"):
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    buf = io.BytesIO(raw)
                    df = pd.read_csv(buf, encoding=enc, low_memory=False,
                                     skiprows=_detect_skiprows(raw, enc))
                    if len(df.columns) >= 3:
                        return df
                except Exception:
                    continue
            return None
        else:
            return pd.read_excel(io.BytesIO(raw), engine="openpyxl")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# ZIP DOWNLOAD + PARSE  (live HTTP fallback — used only when local files absent)
# ─────────────────────────────────────────────────────────────────────────────
def _pick_file(names: list[str], preferred_hint: str = "") -> str | None:
    """Pick best CSV/XLSX from zip namelist."""
    for n in names:
        if preferred_hint and preferred_hint.lower() in n.lower():
            if n.lower().endswith((".csv", ".xlsx", ".xls")):
                return n
    for n in names:
        if "__MACOSX" in n or n.startswith("."):
            continue
        if n.lower().endswith(".csv"):
            return n
    for n in names:
        if "__MACOSX" in n or n.startswith("."):
            continue
        if n.lower().endswith((".xlsx", ".xls")):
            return n
    return None


@st.cache_data(ttl=21_600, show_spinner=False)
def _fetch_zip_df(url: str, hint: str = "") -> pd.DataFrame | None:
    """Download a CMS zip and return its first CSV/Excel as DataFrame."""
    try:
        r = requests.get(url, timeout=90,
                         headers={"User-Agent": "Mozilla/5.0 (compatible)"})
        if r.status_code != 200:
            return None
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            target = _pick_file(z.namelist(), hint)
            if target is None:
                return None
            with z.open(target) as f:
                raw = f.read()
        buf = io.BytesIO(raw)
        if target.lower().endswith(".csv"):
            for enc in ("utf-8", "latin-1", "cp1252"):
                try:
                    buf.seek(0)
                    df = pd.read_csv(buf, encoding=enc, low_memory=False,
                                     skiprows=_detect_skiprows(raw, enc))
                    if len(df.columns) >= 3:
                        return df
                except Exception:
                    continue
            return None
        else:
            buf.seek(0)
            return pd.read_excel(buf, engine="openpyxl")
    except Exception:
        return None


def _detect_skiprows(raw: bytes, enc: str) -> int:
    """CMS files occasionally have a 1-row note before headers."""
    try:
        lines = raw.decode(enc, errors="replace").splitlines()
        for i, line in enumerate(lines[:5]):
            lower = line.lower()
            # header rows usually contain 'contract' or 'enrollment'
            if "contract" in lower or "enrollment" in lower or "plan" in lower:
                return i
    except Exception:
        pass
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# COLUMN NORMALISATION
# ─────────────────────────────────────────────────────────────────────────────

def _dedup_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix duplicate column names in a DataFrame — the root cause of
    pandas.errors.InvalidIndexError when pd.concat tries to align frames.

    Strategy:
      1. Strip leading/trailing whitespace from all column names.
      2. For duplicate raw names, keep the FIRST occurrence and rename
         the rest to  "col_dup2", "col_dup3", … so they are unique.
      3. After the rename step, if two different raw columns mapped to the
         same normalised alias, only the first mapping survives; the rest
         are suffixed with _dup2, _dup3 so they don't collide.
    """
    # Step 1 – strip whitespace
    cols = [c.strip() if isinstance(c, str) else str(c) for c in df.columns]

    # Step 2 – make every raw name unique before we touch anything else
    seen: dict[str, int] = {}
    unique_cols = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            unique_cols.append(f"{c}_dup{seen[c]}")
        else:
            seen[c] = 1
            unique_cols.append(c)

    df = df.copy()
    df.columns = unique_cols
    return df


def _normalise_cols(df: pd.DataFrame, alias_map: dict) -> pd.DataFrame:
    """
    Rename columns using alias_map (case-insensitive).
    Deduplicates raw column names first, then resolves any
    post-rename collisions so the resulting index is always unique.
    """
    df = _dedup_columns(df)

    # Build rename dict – only the FIRST column that maps to a given alias wins
    rename: dict[str, str] = {}
    target_seen: set[str] = set()
    for c in df.columns:
        key = c.strip().lower()
        # Strip _dup2/_dup3 suffixes before alias lookup
        base_key = key
        for suffix_n in range(2, 10):
            if base_key.endswith(f"_dup{suffix_n}"):
                base_key = base_key[: -len(f"_dup{suffix_n}")]
                break
        target = alias_map.get(key) or alias_map.get(base_key)
        if target:
            if target not in target_seen:
                rename[c] = target
                target_seen.add(target)
            # duplicate mapping – leave the column under its original name;
            # it will be dropped later when we select only needed columns.

    df = df.rename(columns=rename)

    # Final safety: if somehow duplicates still exist, deduplicate again
    if df.columns.duplicated().any():
        seen2: dict[str, int] = {}
        final_cols = []
        for c in df.columns:
            if c in seen2:
                seen2[c] += 1
                final_cols.append(f"{c}_{seen2[c]}")
            else:
                seen2[c] = 1
                final_cols.append(c)
        df.columns = final_cols

    return df


def _clean_enrollment(df: pd.DataFrame) -> pd.DataFrame:
    """Parse Enrollment to int; drop rows with 0 or masked (*) enrollment."""
    if "Enrollment" not in df.columns:
        candidates = [c for c in df.columns if "enroll" in c.lower()]
        if candidates:
            df = df.rename(columns={candidates[0]: "Enrollment"})
        else:
            df["Enrollment"] = 0
    df["Enrollment"] = (
        df["Enrollment"].astype(str)
        .str.replace(",", "", regex=False)
        .str.replace("*", "0", regex=False)
        .str.strip()
    )
    df["Enrollment"] = (
        pd.to_numeric(df["Enrollment"], errors="coerce").fillna(0).astype(int)
    )
    return df[df["Enrollment"] > 0].copy()


# The canonical output columns every monthly frame must have.
CPSC_OUTPUT_COLS = [
    "Contract_ID", "Plan_ID", "Segment_ID", "Org_Name",
    "State", "County", "Plan_Type", "Enrollment",
    "Year", "Month", "Period", "Period_Label",
]


def normalise_cpsc(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    df = _normalise_cols(df, CPSC_COL_ALIASES)
    df = _clean_enrollment(df)

    # Derive State from SSA_Code if needed
    if "State" not in df.columns and "SSA_Code" in df.columns:
        df["SSA_Code"] = df["SSA_Code"].astype(str).str.zfill(5)
        df["State"] = df["SSA_Code"].str[:2]

    # Guarantee every output column exists
    for col in ["State", "County", "Contract_ID", "Plan_ID",
                "Plan_Type", "Org_Name", "Segment_ID"]:
        if col not in df.columns:
            df[col] = ""

    df["Year"]         = year
    df["Month"]        = month
    df["Period"]       = pd.Timestamp(year, month, 1)
    df["Period_Label"] = f"{MONTH_NAMES[month].title()[:3]} {year}"

    # ── KEY FIX: select only the canonical columns so every frame that
    # enters pd.concat has an identical, non-duplicate column set. ──────────
    present = [c for c in CPSC_OUTPUT_COLS if c in df.columns]
    return df[present].copy()


def normalise_plandir(df: pd.DataFrame) -> pd.DataFrame:
    df = _normalise_cols(df, PLANDIR_COL_ALIASES)
    for col in ["Contract_ID", "Org_Name", "Parent_Org", "Plan_Type"]:
        if col not in df.columns:
            df[col] = ""
    df["Parent_Org"] = df["Parent_Org"].replace("", np.nan).fillna(
        df.get("Org_Name", "Independent/Other")
    )
    return df[["Contract_ID", "Org_Name", "Parent_Org", "Plan_Type"]].drop_duplicates("Contract_ID")


# ─────────────────────────────────────────────────────────────────────────────
# LOAD FUNCTIONS  (local files first → live HTTP fallback)
# ─────────────────────────────────────────────────────────────────────────────
def _load_one_cpsc(yr: int, mo: int) -> pd.DataFrame | None:
    """
    Load one month of CPSC data.
    Priority: local pre-fetched file → live HTTP download.
    """
    # 1. Local file (committed by GitHub Action)
    local = _local_cpsc_path(yr, mo)
    if local:
        df = _read_local_file(local)
        if df is not None and len(df) > 10:
            return df

    # 2. Live HTTP fallback
    return _fetch_zip_df(cpsc_url(yr, mo), hint="CPSC")


@st.cache_data(ttl=21_600, show_spinner=False)
def load_enrollment(months: tuple) -> pd.DataFrame:
    frames: list = []
    local_count  = 0
    live_count   = 0
    prog = st.progress(0, text="Initialising data load…")

    for i, (yr, mo) in enumerate(months):
        prog.progress(
            (i + 1) / len(months),
            text=f"Loading {MONTH_NAMES[mo].title()} {yr}…",
        )
        # Determine source for status tracking
        local_path = _local_cpsc_path(yr, mo)
        raw = _load_one_cpsc(yr, mo)

        if raw is not None and len(raw) > 10:
            try:
                normed = normalise_cpsc(raw, yr, mo)
                if normed.columns.duplicated().any():
                    continue
                frames.append(normed)
                if local_path:
                    local_count += 1
                else:
                    live_count += 1
            except Exception:
                continue

    prog.empty()
    if not frames:
        return pd.DataFrame()

    # Align all frames to identical canonical column set before concat
    all_cols = list(CPSC_OUTPUT_COLS)
    aligned = []
    for f in frames:
        f = f.copy()
        for col in all_cols:
            if col not in f.columns:
                f[col] = 0 if col == "Enrollment" else ""
        aligned.append(f[all_cols])

    result = pd.concat(aligned, ignore_index=True)
    # Store source info as a DataFrame attribute for the status banner
    result.attrs["local_count"] = local_count
    result.attrs["live_count"]  = live_count
    return result


@st.cache_data(ttl=86_400, show_spinner=False)
def load_plan_directory() -> pd.DataFrame:
    """Local pre-fetched file first, then live HTTP fallback (last 6 months)."""
    # 1. Local file
    local = _local_plandir_path()
    if local:
        df = _read_local_file(local)
        if df is not None and len(df) > 5:
            return normalise_plandir(df)

    # 2. Live HTTP fallback
    for yr, mo in rolling_months(6):
        raw = _fetch_zip_df(plandir_url(yr, mo), hint="Plan_Directory")
        if raw is not None and len(raw) > 5:
            return normalise_plandir(raw)
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# DEMO / SYNTHETIC DATA  (fallback when CMS is unreachable)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def demo_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    rng = np.random.default_rng(42)
    parents = {
        "UnitedHealth Group":  ["H0001","H0002","H0003"],
        "Humana":              ["H0010","H0011"],
        "CVS / Aetna":         ["H0020","H0021"],
        "Elevance Health":     ["H0030"],
        "Kaiser Permanente":   ["H0040","H0041"],
        "Centene":             ["H0050"],
        "Molina Healthcare":   ["H0060"],
        "BCBS Plans":          ["H0070","H0071"],
    }
    geo = {
        "CA": ["Los Angeles","San Diego","Orange","Riverside"],
        "TX": ["Harris","Dallas","Tarrant","Bexar"],
        "FL": ["Miami-Dade","Broward","Palm Beach","Hillsborough"],
        "NY": ["New York","Kings","Queens","Bronx"],
        "PA": ["Philadelphia","Allegheny","Montgomery"],
        "OH": ["Franklin","Cuyahoga","Hamilton"],
        "IL": ["Cook","DuPage","Lake"],
        "NC": ["Mecklenburg","Wake","Guilford"],
        "GA": ["Fulton","Gwinnett","DeKalb"],
        "MI": ["Wayne","Oakland","Macomb"],
    }
    plan_types = ["HMO","PPO","HMO-POS","PFFS"]
    months_list = rolling_months(24)

    # seed base values
    base: dict = {}
    for st_code, counties in geo.items():
        for county in counties:
            for parent, cids in parents.items():
                for cid in cids:
                    for pt in plan_types[:2]:
                        base[(st_code,county,parent,cid,pt)] = rng.integers(300,12000)

    rows = []
    for yr, mo in reversed(months_list):
        period = pd.Timestamp(yr, mo, 1)
        for st_code, counties in geo.items():
            for county in counties:
                for parent, cids in parents.items():
                    for cid in cids:
                        for pt in plan_types[:2]:
                            k = (st_code,county,parent,cid,pt)
                            base[k] = max(50, int(base[k]*(1+rng.normal(.003,.014))))
                            rows.append({
                                "State":        st_code,
                                "County":       county,
                                "Contract_ID":  cid,
                                "Plan_ID":      "001",
                                "Plan_Type":    pt,
                                "Org_Name":     f"{parent} – {cid}",
                                "Enrollment":   base[k],
                                "Year":         yr,
                                "Month":        mo,
                                "Period":       period,
                                "Period_Label": f"{MONTH_NAMES[mo].title()[:3]} {yr}",
                            })

    enroll_df = pd.DataFrame(rows)
    plan_rows = [
        {"Contract_ID": cid, "Parent_Org": parent,
         "Org_Name": f"{parent} – {cid}", "Plan_Type": "HMO"}
        for parent, cids in parents.items() for cid in cids
    ]
    return enroll_df, pd.DataFrame(plan_rows)


# ─────────────────────────────────────────────────────────────────────────────
# MERGE HELPER
# ─────────────────────────────────────────────────────────────────────────────
def add_parent_org(enroll: pd.DataFrame, plandir: pd.DataFrame) -> pd.DataFrame:
    if plandir.empty or "Contract_ID" not in plandir.columns:
        enroll["Parent_Org"] = enroll.get("Org_Name", pd.Series("Independent/Other", index=enroll.index))
        return enroll
    merged = enroll.merge(
        plandir[["Contract_ID","Parent_Org"]].drop_duplicates("Contract_ID"),
        on="Contract_ID", how="left"
    )
    merged["Parent_Org"] = merged["Parent_Org"].fillna("Independent / Other")
    return merged


# ─────────────────────────────────────────────────────────────────────────────
# FORMAT HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def fmt(n: float | int) -> str:
    if n >= 1_000_000: return f"{n/1_000_000:.2f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return f"{int(n):,}"

def kpi(label: str, value: str, delta_pct: float | None = None,
        delta_abs: int | None = None) -> str:
    d = ""
    if delta_pct is not None:
        cls  = "kpi-pos" if delta_pct >= 0 else "kpi-neg"
        sign = "▲" if delta_pct >= 0 else "▼"
        abs_s = f" ({'+' if (delta_abs or 0)>=0 else ''}{fmt(delta_abs or 0)})" if delta_abs is not None else ""
        d = f'<div class="kpi-delta <span class="{cls}">{sign} {abs(delta_pct):.1f}%{abs_s} MoM</span>"></div>'
        d = f'<div class="kpi-delta"><span class="{cls}">{sign} {abs(delta_pct):.1f}%{abs_s} MoM</span></div>'
    return f"""
    <div class="kpi-card">
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
      {d}
    </div>"""


# ─────────────────────────────────────────────────────────────────────────────
# CHARTS
# ─────────────────────────────────────────────────────────────────────────────
_LAYOUT = dict(
    plot_bgcolor="white", paper_bgcolor="white",
    font=dict(family="Arial, sans-serif", size=12),
    hovermode="x unified",
)
_MARGIN_DEFAULT = dict(t=52, b=16, l=8,  r=8)
_MARGIN_DONUT   = dict(t=52, b=20, l=20, r=20)

def _apply(fig, height=420, legend_below=True, margin=None):
    fig.update_layout(
        **_LAYOUT,
        height=height,
        margin=margin if margin is not None else _MARGIN_DEFAULT,
    )
    if legend_below:
        fig.update_layout(legend=dict(orientation="h", y=-0.22, x=0))
    return fig


def chart_trend(df: pd.DataFrame, group: str) -> go.Figure:
    agg = (df.groupby(["Period", group])["Enrollment"]
             .sum().reset_index().sort_values("Period"))
    fig = px.line(agg, x="Period", y="Enrollment", color=group,
                  title=f"Monthly Enrollment Trend by {group.replace('_',' ')}",
                  markers=True, color_discrete_sequence=CHART_COLORS,
                  labels={"Enrollment":"Total Enrolled","Period":"Month"})
    fig.update_traces(line_width=2.5)
    return _apply(fig, 420)


def chart_donut(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby("Parent_Org")["Enrollment"].sum().reset_index()
    agg = agg.sort_values("Enrollment", ascending=False)
    top = agg.head(8)
    if len(agg) > 8:
        top = pd.concat([top,
            pd.DataFrame([{"Parent_Org":"All Other",
                           "Enrollment": agg.iloc[8:]["Enrollment"].sum()}])],
            ignore_index=True)
    fig = px.pie(top, names="Parent_Org", values="Enrollment", hole=.5,
                 title="Current Market Share by Parent Organization",
                 color_discrete_sequence=CHART_COLORS)
    fig.update_traces(textposition="outside", textinfo="percent+label",
                      textfont_size=11)
    fig.update_layout(showlegend=False)
    return _apply(fig, height=400, legend_below=False, margin=_MARGIN_DONUT)


def chart_mom_change(df: pd.DataFrame, group: str) -> go.Figure:
    top6 = (df.groupby(group)["Enrollment"].sum()
              .nlargest(6).index.tolist())
    sub = df[df[group].isin(top6)]
    agg = (sub.groupby(["Period", group])["Enrollment"]
               .sum().reset_index().sort_values(["Period", group]))
    agg["MoM"] = agg.groupby(group)["Enrollment"].diff()
    fig = px.bar(agg.dropna(subset=["MoM"]),
                 x="Period", y="MoM", color=group, barmode="group",
                 title="Month-over-Month Net Change (Top 6)",
                 color_discrete_sequence=CHART_COLORS,
                 labels={"MoM":"MoM Change","Period":"Month"})
    fig.add_hline(y=0, line_dash="dash", line_color="#aaa")
    return _apply(fig, 420)


def chart_plan_mix(df: pd.DataFrame) -> go.Figure:
    agg = (df.groupby(["Period","Plan_Type"])["Enrollment"]
             .sum().reset_index().sort_values("Period"))
    fig = px.area(agg, x="Period", y="Enrollment", color="Plan_Type",
                  title="Plan Type Mix Over Time",
                  color_discrete_sequence=CHART_COLORS,
                  labels={"Enrollment":"Total Enrolled","Period":"Month"})
    return _apply(fig, 390)


def chart_state_map(df: pd.DataFrame) -> go.Figure:
    agg = df.groupby("State")["Enrollment"].sum().reset_index()
    fig = px.choropleth(agg, locations="State", locationmode="USA-states",
                        color="Enrollment", scope="usa",
                        title="Enrollment by State (Latest Month)",
                        color_continuous_scale="Blues",
                        labels={"Enrollment":"Total Enrolled"})
    _apply(fig, height=400, legend_below=False)
    fig.update_layout(geo=dict(bgcolor="white"),
                      coloraxis_colorbar=dict(thickness=12))
    return fig


def chart_top_counties(df: pd.DataFrame, n: int = 15) -> go.Figure:
    agg = df.groupby(["State","County"])["Enrollment"].sum().reset_index()
    agg["Label"] = agg["County"] + ", " + agg["State"]
    agg = agg.nlargest(n, "Enrollment")
    fig = px.bar(agg, x="Enrollment", y="Label", orientation="h",
                 color="Enrollment", color_continuous_scale="Blues",
                 title=f"Top {n} Counties (Latest Month)",
                 labels={"Label":"","Enrollment":"Total Enrolled"})
    _apply(fig, height=420, legend_below=False)
    fig.update_layout(yaxis=dict(autorange="reversed"),
                      coloraxis_showscale=False)
    return fig


def chart_yoy(df: pd.DataFrame, group: str) -> go.Figure | None:
    periods = sorted(df["Period"].unique())
    if len(periods) < 13:
        return None
    latest = periods[-1]
    prior  = min(periods, key=lambda p: abs((p - (latest - relativedelta(years=1))).days))
    c = df[df["Period"]==latest].groupby(group)["Enrollment"].sum()
    p = df[df["Period"]==prior ].groupby(group)["Enrollment"].sum()
    chg = ((c - p) / p * 100).dropna().reset_index()
    chg.columns = [group, "YoY_Pct"]
    chg = chg.sort_values("YoY_Pct")
    fig = px.bar(chg, x="YoY_Pct", y=group, orientation="h",
                 color="YoY_Pct",
                 color_continuous_scale=["#D32F2F","#FFFFFF","#1565C0"],
                 color_continuous_midpoint=0,
                 title=f"Year-over-Year Enrollment Change (%) by {group.replace('_',' ')}",
                 labels={"YoY_Pct":"YoY % Change", group:""})
    fig.add_vline(x=0, line_dash="dash", line_color="#888")
    _apply(fig, height=max(340, len(chg)*28), legend_below=False)
    fig.update_layout(coloraxis_showscale=False)
    return fig


def chart_bubble_org(df_latest: pd.DataFrame, df_prev: pd.DataFrame) -> go.Figure:
    """Bubble: x=market share, y=YoY growth, size=enrollment."""
    curr = df_latest.groupby("Parent_Org")["Enrollment"].sum().reset_index()
    prev = df_prev.groupby("Parent_Org")["Enrollment"].sum().reset_index()
    prev.columns = ["Parent_Org","Prev"]
    merged = curr.merge(prev, on="Parent_Org", how="left")
    merged["Mkt_Share"] = merged["Enrollment"] / merged["Enrollment"].sum() * 100
    merged["Growth"]    = (merged["Enrollment"] - merged["Prev"]) / merged["Prev"] * 100
    merged = merged.dropna(subset=["Growth"])
    fig = px.scatter(merged, x="Mkt_Share", y="Growth",
                     size="Enrollment", color="Parent_Org",
                     text="Parent_Org",
                     size_max=60,
                     title="Market Share vs Growth Rate (bubble = enrollment size)",
                     labels={"Mkt_Share":"Market Share %","Growth":"YoY Growth %"},
                     color_discrete_sequence=CHART_COLORS)
    fig.update_traces(textposition="top center")
    fig.add_hline(y=0, line_dash="dot", line_color="#aaa")
    return _apply(fig, 460, legend_below=False)


# ─────────────────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────────────────
def render_sidebar(df: pd.DataFrame) -> dict:
    with st.sidebar:
        st.markdown("## 🔍 Filters")
        st.markdown("---")

        # Parent org
        orgs = sorted(df["Parent_Org"].dropna().unique()) if "Parent_Org" in df.columns else []
        sel_org = st.multiselect("Parent Organization", orgs,
                                 placeholder="All organizations")

        # State
        states = sorted(df["State"].dropna().unique()) if "State" in df.columns else []
        sel_state = st.multiselect("State", states, placeholder="All states")

        # County – dynamic
        sub = df[df["State"].isin(sel_state)] if sel_state else df
        counties = sorted(sub["County"].dropna().unique()) if "County" in sub.columns else []
        sel_county = st.multiselect("County", counties, placeholder="All counties")

        # Plan type
        pts = sorted(df["Plan_Type"].dropna().replace("",np.nan).dropna().unique()) \
              if "Plan_Type" in df.columns else []
        sel_pt = st.multiselect("Plan Type", pts, placeholder="All plan types")

        # Date range
        st.markdown("**Date Range**")
        periods = sorted(df["Period"].unique())
        labels  = [p.strftime("%b %Y") for p in periods]
        if len(periods) >= 2:
            start_i, end_i = st.select_slider(
                "Select window",
                options=list(range(len(periods))),
                value=(0, len(periods)-1),
                format_func=lambda i: labels[i],
            )
            period_range = (periods[start_i], periods[end_i])
        else:
            period_range = (periods[0], periods[-1]) if periods else (None, None)

        st.markdown("---")
        group_col = st.radio(
            "Group trend charts by",
            ["Parent_Org","Plan_Type","State"],
            format_func=lambda x: {
                "Parent_Org":"Parent Organization",
                "Plan_Type":"Plan Type",
                "State":"State"}[x],
        )

    return dict(
        Parent_Org=sel_org, State=sel_state, County=sel_county,
        Plan_Type=sel_pt, period_range=period_range, group=group_col,
    )


def apply_filters(df: pd.DataFrame, f: dict) -> pd.DataFrame:
    sub = df.copy()
    if f["Parent_Org"]: sub = sub[sub["Parent_Org"].isin(f["Parent_Org"])]
    if f["State"]:      sub = sub[sub["State"].isin(f["State"])]
    if f["County"]:     sub = sub[sub["County"].isin(f["County"])]
    if f["Plan_Type"]:  sub = sub[sub["Plan_Type"].isin(f["Plan_Type"])]
    s, e = f.get("period_range", (None, None))
    if s and e:
        sub = sub[(sub["Period"] >= s) & (sub["Period"] <= e)]
    return sub


# ─────────────────────────────────────────────────────────────────────────────
# COMPETITIVE SUMMARY TABLE
# ─────────────────────────────────────────────────────────────────────────────
def build_comp_table(df: pd.DataFrame, latest: pd.Timestamp,
                     prior_mo: pd.Timestamp,
                     prior_yr: pd.Timestamp | None) -> pd.DataFrame:
    curr = df[df["Period"]==latest].groupby("Parent_Org")["Enrollment"].sum().reset_index()
    prev = df[df["Period"]==prior_mo].groupby("Parent_Org")["Enrollment"].sum().reset_index()
    prev.columns = ["Parent_Org","Prior_Mo"]
    comp = curr.merge(prev, on="Parent_Org", how="left")
    comp["MoM_Chg"] = comp["Enrollment"] - comp["Prior_Mo"]
    comp["MoM_%"]   = (comp["MoM_Chg"] / comp["Prior_Mo"] * 100).round(1)
    if prior_yr is not None:
        yr = df[df["Period"]==prior_yr].groupby("Parent_Org")["Enrollment"].sum().reset_index()
        yr.columns = ["Parent_Org","Prior_Yr"]
        comp = comp.merge(yr, on="Parent_Org", how="left")
        comp["YoY_%"] = ((comp["Enrollment"] - comp["Prior_Yr"]) / comp["Prior_Yr"] * 100).round(1)
    total = comp["Enrollment"].sum()
    comp["Mkt_%"] = (comp["Enrollment"] / total * 100).round(2)
    comp = comp.sort_values("Enrollment", ascending=False)
    col_map = {
        "Parent_Org":  "Parent Organization",
        "Enrollment":  f"Enrolled ({latest.strftime('%b %Y')})",
        "Mkt_%":       "Mkt Share %",
        "MoM_Chg":     "MoM Change",
        "MoM_%":       "MoM %",
    }
    if "YoY_%" in comp.columns:
        col_map["YoY_%"] = "YoY %"
    return comp.rename(columns=col_map)[[v for v in col_map.values() if v in comp.rename(columns=col_map).columns]]


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    # ── Header ──
    st.markdown(
        "<h1>🏥 Medicare Advantage Enrollment Intelligence Dashboard</h1>"
        "<p style='color:#555;margin-top:-.4rem;margin-bottom:.8rem;'>"
        "Rolling 24-Month Analysis &nbsp;·&nbsp; Powered by CMS Public Data</p>",
        unsafe_allow_html=True,
    )

    # ── Top controls ──
    ctrl1, ctrl2, ctrl3, ctrl4 = st.columns([2.5, 1, 1, 1])
    with ctrl1:
        use_demo = st.checkbox(
            "📊 Use demo data (synthetic preview — uncheck for live CMS data)",
            value=False,
            help="Uses realistic synthetic data when CMS files are unavailable or "
                 "for fast exploration.",
        )
    with ctrl2:
        months_back = st.selectbox("Months to load", [12, 18, 24], index=2,
                                   disabled=use_demo)
    with ctrl3:
        refresh = st.button("🔄 Refresh Now", type="primary", disabled=use_demo)
    with ctrl4:
        st.markdown(
            "<div style='font-size:.72rem;color:#666;padding-top:.6rem'>"
            "Auto-refreshes monthly<br>via GitHub Actions</div>",
            unsafe_allow_html=True,
        )

    if refresh:
        st.cache_data.clear()
        st.rerun()

    # ── Load ──
    with st.spinner("Loading data…"):
        if use_demo:
            enroll_df, plan_df = demo_data()
            src_label = "🟡 Demo mode — synthetic data"
        else:
            month_list = tuple(rolling_months(months_back))
            enroll_df  = load_enrollment(month_list)
            plan_df    = load_plan_directory()
            if enroll_df.empty:
                st.warning(
                    "⚠️ Could not load CMS data from local files or live servers. "
                    "Go to your GitHub repo → Actions → **Fetch CMS Data** → "
                    "**Run workflow** to download the data files. "
                    "Falling back to demo data for now."
                )
                enroll_df, plan_df = demo_data()
                src_label = "🟡 Demo fallback — run the Fetch CMS Data action in GitHub"
            else:
                local_n = enroll_df.attrs.get("local_count", 0)
                live_n  = enroll_df.attrs.get("live_count",  0)
                if local_n > 0 and live_n == 0:
                    src_icon = "🟢"
                    src_note = f"local repo files ({local_n} months)"
                elif local_n > 0:
                    src_icon = "🟢"
                    src_note = f"local files ({local_n}) + live ({live_n} months)"
                else:
                    src_icon = "🟡"
                    src_note = f"live CMS download ({live_n} months)"
                src_label = (
                    f"{src_icon} CMS data via {src_note} &nbsp;·&nbsp; "
                    f"{len(enroll_df):,} rows &nbsp;·&nbsp; "
                    f"{enroll_df['Period'].nunique()} months loaded"
                )

    if enroll_df.empty:
        st.error("No data. Enable demo mode to continue.")
        return

    # Add parent org
    full_df = add_parent_org(enroll_df, plan_df)

    # Ensure core columns exist
    for c in ["Parent_Org","State","County","Plan_Type","Enrollment","Period","Contract_ID"]:
        if c not in full_df.columns:
            full_df[c] = "" if c != "Enrollment" else 0

    # Status bar
    st.markdown(
        f'<div class="status-bar">{src_label} &nbsp;·&nbsp; '
        f'Last refreshed {datetime.now().strftime("%Y-%m-%d %H:%M UTC")}</div>',
        unsafe_allow_html=True,
    )

    # ── Sidebar ──
    filters = render_sidebar(full_df)
    filtered = apply_filters(full_df, filters)

    if filtered.empty:
        st.warning("No data matches the current filters — try broadening your selection.")
        return

    group = filters["group"]
    periods = sorted(filtered["Period"].unique())
    latest    = periods[-1]
    prior_mo  = periods[-2] if len(periods) >= 2 else periods[-1]
    prior_yr  = periods[-13] if len(periods) >= 13 else None

    df_latest = filtered[filtered["Period"] == latest]
    df_prior  = filtered[filtered["Period"] == prior_mo]
    df_oldest = filtered[filtered["Period"] == periods[0]]

    tot_now   = int(df_latest["Enrollment"].sum())
    tot_prev  = int(df_prior["Enrollment"].sum())
    tot_old   = int(df_oldest["Enrollment"].sum())
    mom_abs   = tot_now - tot_prev
    mom_pct   = (mom_abs / tot_prev * 100) if tot_prev else 0.0
    g24m_pct  = ((tot_now - tot_old) / tot_old * 100) if tot_old else 0.0
    n_orgs    = df_latest["Parent_Org"].nunique()
    n_ctrcts  = df_latest["Contract_ID"].nunique()
    n_states  = df_latest["State"].nunique()

    # ── KPI Row ──
    st.markdown("### 📌 Key Metrics")
    k1, k2, k3, k4, k5 = st.columns(5)
    for col, label, value, dp, da in [
        (k1, f"Total Enrolled ({latest.strftime('%b %Y')})",
             fmt(tot_now), mom_pct, mom_abs),
        (k2, "24-Month Growth",     f"{g24m_pct:+.1f}%", None, None),
        (k3, "Parent Organizations", str(n_orgs),         None, None),
        (k4, "Active Contracts",     str(n_ctrcts),       None, None),
        (k5, "States Covered",       str(n_states),       None, None),
    ]:
        col.markdown(kpi(label, value, dp, da), unsafe_allow_html=True)

    st.markdown("---")

    # ── Row 1: Trend + Donut ──
    st.markdown("### 📈 Enrollment Trend & Market Share")
    r1a, r1b = st.columns([3, 2])
    with r1a:
        st.plotly_chart(chart_trend(filtered, group), use_container_width=True)
    with r1b:
        st.plotly_chart(chart_donut(df_latest), use_container_width=True)

    # ── Row 2: MoM Change + Plan Mix ──
    st.markdown("### 📊 Change Analysis")
    r2a, r2b = st.columns(2)
    with r2a:
        st.plotly_chart(chart_mom_change(filtered, group), use_container_width=True)
    with r2b:
        st.plotly_chart(chart_plan_mix(filtered), use_container_width=True)

    # ── Row 3: Geography ──
    st.markdown("### 🗺️ Geographic Distribution")
    r3a, r3b = st.columns(2)
    with r3a:
        st.plotly_chart(chart_state_map(df_latest), use_container_width=True)
    with r3b:
        st.plotly_chart(chart_top_counties(df_latest), use_container_width=True)

    # ── Row 4: YoY + Bubble ──
    st.markdown("### 🏆 Competitive Landscape")
    yoy = chart_yoy(filtered, group)
    if yoy:
        r4a, r4b = st.columns(2)
        with r4a:
            st.plotly_chart(yoy, use_container_width=True)
        if prior_yr is not None:
            df_prior_yr = filtered[filtered["Period"] == prior_yr]
            with r4b:
                st.plotly_chart(chart_bubble_org(df_latest, df_prior_yr),
                                use_container_width=True)
    else:
        st.info("YoY chart requires ≥13 months of data.")

    # ── Competitive Table ──
    st.markdown("#### Competitive Summary Table")
    comp = build_comp_table(filtered, latest, prior_mo, prior_yr)
    st.dataframe(comp.reset_index(drop=True), use_container_width=True, height=340)

    # ── Downloads ──
    st.markdown("### 📥 Data Downloads")
    d1, d2, d3 = st.columns(3)
    with d1:
        st.download_button(
            "⬇️ Filtered Dataset (CSV)",
            filtered.to_csv(index=False).encode(),
            f"ma_enrollment_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    with d2:
        st.download_button(
            "⬇️ Competitive Summary (CSV)",
            comp.to_csv(index=False).encode(),
            f"ma_competitive_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
    with d3:
        if not plan_df.empty:
            st.download_button(
                "⬇️ Plan Directory (CSV)",
                plan_df.to_csv(index=False).encode(),
                f"ma_plan_directory_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

    # ── Raw data explorer ──
    with st.expander("🔎 Browse full filtered dataset", expanded=False):
        st.info(f"{len(filtered):,} rows · {filtered['Period'].nunique()} months")
        search = st.text_input("Search (Parent Org / State / County)",
                               placeholder="Type to filter rows…")
        show = filtered.copy()
        if search:
            mask = show.apply(
                lambda row: row.astype(str).str.contains(search, case=False).any(),
                axis=1
            )
            show = show[mask]
        st.dataframe(show, use_container_width=True, height=380)

    # ── Footer ──
    st.markdown("---")
    st.caption(
        "Data: CMS Medicare Advantage/Part D Contract and Enrollment Data (public) · "
        "Dashboard for internal strategy use only."
    )


if __name__ == "__main__":
    main()
