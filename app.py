# app.py
# NSE Delivery Downloader — fixed last 1 month, blank row between each symbol
# Works on Python 3.9.7 + Streamlit 1.12.0

import io
import time
import random
import requests
import pandas as pd
import streamlit as st
from pathlib import Path
from datetime import date, timedelta
from typing import List, Optional

st.set_page_config(page_title="NSE Delivery Downloader", page_icon="📊", layout="wide")

# -------------------------
# Back-compat cache shim (Streamlit 1.12 doesn't have cache_data)
# -------------------------
try:
    cache_data = st.cache_data
    _CACHE_KW = dict(show_spinner=False)
except AttributeError:
    cache_data = st.cache
    _CACHE_KW = {}

# -------------------------
# ---------- CONFIG ----------
# -------------------------
SYMBOLS_TEST = ["OLAELEC"]   # used only if TEST_MODE=True
TEST_MODE = False            # set True to quickly test with SYMBOLS_TEST

# Optional local caching of the NIFTY500 CSV
LOCAL_NIFTY500_CSV = Path("ind_nifty500list.csv")
NIFTY500_URL = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"

# Politeness between NSE API calls
SLEEP_BETWEEN_SYMBOLS = (0.6, 1.3)

# API endpoints + headers
API_URL = "https://www.nseindia.com/api/historicalOR/generateSecurityWiseHistoricalData"
REPORT_URL = "https://www.nseindia.com/report-detail/eq_security"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": REPORT_URL,  # critical
}

# -------------------------
# Helpers
# -------------------------
def _download_nifty500_csv_to_local() -> None:
    if not LOCAL_NIFTY500_CSV.exists():
        r = requests.get(NIFTY500_URL, headers={"User-Agent": HEADERS["User-Agent"]}, timeout=15)
        r.raise_for_status()
        LOCAL_NIFTY500_CSV.write_bytes(r.content)

def load_symbols_from_csv() -> List[str]:
    df = pd.read_csv(LOCAL_NIFTY500_CSV)
    sym_col = next((c for c in df.columns if c.strip().lower() == "symbol"), None)
    if not sym_col:
        raise RuntimeError(f"'Symbol' column not found in {LOCAL_NIFTY500_CSV}")
    syms = df[sym_col].astype(str).str.strip().unique()
    return sorted([s for s in syms if s])

@cache_data(**_CACHE_KW)
def load_nifty500_symbols() -> List[str]:
    """Fetch NIFTY 500 list (download once to local if missing)."""
    try:
        _download_nifty500_csv_to_local()
    except Exception as e:
        # If download fails but file exists, try reading what we have
        if not LOCAL_NIFTY500_CSV.exists():
            raise RuntimeError(f"Failed to download NIFTY 500 list: {e}")
    return load_symbols_from_csv()

def get_session_cookies() -> Optional[requests.Session]:
    """Warm up a session to get NSE cookies."""
    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(REPORT_URL, timeout=10)
        return session
    except requests.RequestException:
        return None

def fetch_data_for_symbol(session: requests.Session, symbol: str) -> Optional[pd.DataFrame]:
    """Use warmed session to hit the API for one symbol (fixed 1 month window)."""
    to_date = date.today()
    from_date = to_date - timedelta(days=31)
    params = {
        "symbol": symbol,
        "from": from_date.strftime("%d-%m-%Y"),
        "to": to_date.strftime("%d-%m-%Y"),
        "series": "EQ",
        "type": "priceVolumeDeliverable",
    }
    try:
        resp = session.get(API_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if not data or "data" not in data or not data["data"]:
            return None
        return pd.DataFrame(data["data"])
    except Exception:
        return None

def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean/rename columns and types."""
    rename_map = {
        "mTIMESTAMP": "date",            # correct date column
        "CH_SYMBOL": "symbol",
        "CH_SERIES": "series",
        "CH_TOT_TRADED_QTY": "traded_qty",
        "COP_DELIV_QTY": "deliverable_qty",
        "COP_DELIV_PERC": "delivery_pct",
        "CH_CLOSING_PRICE": "close_price",
    }
    df = df.rename(columns=rename_map)
    required = ["date", "symbol", "series", "traded_qty", "deliverable_qty", "delivery_pct", "close_price"]
    if not all(c in df.columns for c in required):
        return pd.DataFrame()
    df = df[required].copy()
    df["date"] = pd.to_datetime(df["date"], format="%d-%b-%Y", errors="coerce")
    for col in ["traded_qty", "deliverable_qty", "delivery_pct", "close_price"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def build_output_excels(full_df: pd.DataFrame):
    """
    Return two workbooks as bytes:
      1) raw (delivery_1M_raw + summary_avg_delivery)
      2) with blanks between symbols (delivery_1M_raw + summary_avg_delivery)
    """
    # Prepare summary + format date for Excel
    df_save = full_df.copy()
    df_save["date"] = df_save["date"].dt.strftime("%Y-%m-%d")
    summary = (
        df_save.groupby("symbol")["delivery_pct"]
        .mean()
        .reset_index()
        .rename(columns={"delivery_pct": "avg_delivery_pct_1M"})
        .sort_values("avg_delivery_pct_1M", ascending=False)
    )

    # Raw workbook
    raw_buf = io.BytesIO()
    with pd.ExcelWriter(raw_buf, engine="openpyxl") as writer:
        df_save.to_excel(writer, sheet_name="delivery_1M_raw", index=False)
        summary.to_excel(writer, sheet_name="summary_avg_delivery", index=False)
    raw_bytes = raw_buf.getvalue()

    # With blanks workbook
    blanks_buf = io.BytesIO()
    with pd.ExcelWriter(blanks_buf, engine="openpyxl") as writer:
        start_row = 0
        first = True
        # keep original symbol order (no sort=True)
        for symbol, block in df_save.groupby("symbol", sort=False):
            if first:
                block.to_excel(writer, sheet_name="delivery_1M_raw", index=False, header=True, startrow=start_row)
                start_row += len(block) + 1
                first = False
            else:
                # one blank row between blocks
                block.to_excel(writer, sheet_name="delivery_1M_raw", index=False, header=False, startrow=start_row + 1)
                start_row += len(block) + 1
        summary.to_excel(writer, sheet_name="summary_avg_delivery", index=False)
    blanks_bytes = blanks_buf.getvalue()

    return raw_bytes, blanks_bytes, summary

def parse_custom_symbols(text: str) -> List[str]:
    """Split by commas/spaces/newlines; uppercase; unique; sorted."""
    raw = [tok.strip().upper() for tok in text.replace(",", " ").split()]
    return sorted(list({s for s in raw if s}))

# -------------------------
# UI
# -------------------------
st.title("📊 NSE Delivery Downloader — Last 1 Month (EQ)")
st.caption("All NIFTY 500 or custom symbols. Generates two Excel files: raw + summary, and a version with a blank row between each stock.")

mode = st.radio("Choose symbols source:", ("All NIFTY 500", "Custom list"))
limit = st.number_input("Limit symbols (optional, for quick tests)", min_value=0, step=1, value=0)

custom_input = ""
if mode == "Custom list":
    custom_input = st.text_area(
        "Enter symbols (comma/space/newline separated):",
        placeholder="RELIANCE, HDFCBANK, INFY\nor one per line...",
        height=100,
    )

run = st.button("🔎 Fetch last 1 month data")

# -------------------------
# Action
# -------------------------
if run:
    # Resolve symbols
    if TEST_MODE:
        symbols = SYMBOLS_TEST
    else:
        if mode == "All NIFTY 500":
            with st.spinner("Loading NIFTY 500 symbols..."):
                try:
                    symbols = load_nifty500_symbols()
                except Exception as e:
                    st.error(f"Failed to load NIFTY 500 list: {e}")
                    st.stop()
        else:
            symbols = parse_custom_symbols(custom_input)
            if not symbols:
                st.warning("Please enter at least one symbol.")
                st.stop()

    if limit and limit > 0:
        symbols = symbols[:limit]

    st.info(f"Will fetch for **{len(symbols)}** symbol(s).  (Fixed window: last 31 days)")

    session = get_session_cookies()
    if not session:
        st.error("Could not initialize NSE session (cookies). Try again.")
        st.stop()

    progress = st.progress(0)
    status = st.empty()
    frames = []
    errors = []

    for i, sym in enumerate(symbols, start=1):
        status.write(f"Fetching {sym} ({i}/{len(symbols)})")
        raw_df = fetch_data_for_symbol(session, sym)
        if raw_df is not None and not raw_df.empty:
            eq_df = raw_df[raw_df.get("CH_SERIES", "") == "EQ"].copy()
            if not eq_df.empty:
                clean = normalize_dataframe(eq_df)
                if not clean.empty:
                    frames.append(clean)
            else:
                errors.append((sym, "No EQ series data"))
        else:
            errors.append((sym, "No data returned"))

        progress.progress(i / len(symbols))
        time.sleep(random.uniform(*SLEEP_BETWEEN_SYMBOLS))

    if not frames:
        st.error("No data fetched. Try fewer symbols or try again later.")
        if errors:
            with st.expander("Errors / Skipped symbols"):
                err_df = pd.DataFrame(errors, columns=["symbol", "reason"])
                st.write(err_df)
        st.stop()

    full_df = pd.concat(frames, ignore_index=True)
    full_df = full_df.sort_values(["symbol", "date"])

    st.success(f"Fetched {len(full_df)} rows for {full_df['symbol'].nunique()} symbols.")

    with st.expander("Preview (first 100 rows)"):
        st.write(full_df.head(100))

    with st.spinner("Building Excel files..."):
        raw_bytes, blanks_bytes, summary_df = build_output_excels(full_df)

    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "⬇️ Download Raw Excel (2 sheets)",
            data=raw_bytes,
            file_name="NSE_API_Delivery.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    with c2:
        st.download_button(
            "⬇️ Download Excel with Blank Rows",
            data=blanks_bytes,
            file_name="NSE_API_Delivery_With_Blanks.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    with st.expander("Summary — Avg Delivery % (descending)"):
        st.write(summary_df)

    if errors:
        with st.expander("Errors / Skipped symbols"):
            err_df = pd.DataFrame(errors, columns=["symbol", "reason"])
            st.write(err_df)
