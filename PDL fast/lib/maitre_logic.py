"""Domain logic for Maitre Sync — pure pandas, no I/O.

Sources:
  * Forecast Bibel  (Spreadsheet "Maitre Meal Forecast")
      - Tab "2025 Verden Forecast (DE+NOR)"  → DE + NORDICS
      - Tab "2025 Bleijswijk Forecast (BNL)" → BNL  (Phase 2)
    Header row = 2.
      Key columns: Market, week, Recipe Slot Number, Recipe Code, Maitre Code,
                   Maitre SKU Name, order  (column S)
  * Inbound      (Spreadsheet "OUTPUT - [F_ x HF] Weekly Fulfillment Report")
      - Tab "Logistik - FCMS Meals"
      Key columns:
          F  Item Number
          G  Description           (contains "(FExxxx)" → recipe code)
          M  PO Expected Eaches/Weight
          O  Total Received Eaches/Weight
          P  Variance % to PO
"""

from __future__ import annotations

from dataclasses import dataclass
import re

import pandas as pd

from .sheets_client import extract_codes, read_tab_as_df, to_number


FORECAST_SPREADSHEET_ID = "1L8QtO2TgW0DbmzLSbVHwotKAn1b-1y4fQgTJsuJCiJE"
INBOUND_SPREADSHEET_ID = "1YscgiuKYVI2pGcMJ3RcJwWGQEkG46RnVnji8q8a4AeE"

FORECAST_TABS = {
    "DE_NOR": "2025 Verden Forecast (DE+NOR)",
    "BNL": "2025 Bleijswijk Forecast (BNL)",
}
INBOUND_TAB = "Logistik - FCMS Meals"


# ---------------------------------------------------------------------------
# Forecast
# ---------------------------------------------------------------------------

@dataclass
class ForecastConfig:
    spreadsheet_id: str = FORECAST_SPREADSHEET_ID
    tabs: tuple[str, ...] = (FORECAST_TABS["DE_NOR"],)
    order_col: str = "order"
    qty_cols: tuple[str, ...] = (
        "wed -4wk", "wed -3wk", "wed -2wk", "wed-1wk",
        "Fri -1wk", "Mon", "Tues", "Weds", "Thurs",
        "order", "Thur Del. PO", "Fri Del. PO",
    )


def load_forecast(config: ForecastConfig | None = None) -> pd.DataFrame:
    """Pull all configured forecast tabs and concatenate."""
    cfg = config or ForecastConfig()
    frames = []
    for tab in cfg.tabs:
        df = read_tab_as_df(cfg.spreadsheet_id, tab, header_row=2)
        if df.empty:
            continue
        df["__source_tab"] = tab
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    # numeric cleanup
    for c in cfg.qty_cols:
        if c in out.columns:
            out[c] = to_number(out[c])
    # standard market labels
    if "Market" in out.columns:
        out["Market"] = out["Market"].astype(str).str.upper().str.strip()
    if "week" in out.columns:
        out["week"] = out["week"].astype(str).str.upper().str.strip()
    if "Recipe Code" in out.columns:
        out["Recipe Code"] = out["Recipe Code"].astype(str).str.strip()
    return out


def filter_forecast(df: pd.DataFrame, week: str | None = None,
                    markets: list[str] | None = None) -> pd.DataFrame:
    """Subset by week (e.g. 'W17') and markets (['DE','NORDICS'])."""
    out = df
    if week and "week" in out.columns:
        wk = str(week).upper().strip()
        if not wk.startswith("W"):
            wk = f"W{wk}"
        if len(wk) == 2:  # 'W7' → 'W07'
            wk = f"W0{wk[1]}"
        out = out[out["week"] == wk]
    if markets and "Market" in out.columns:
        wanted = {m.upper() for m in markets}
        out = out[out["Market"].isin(wanted)]
    return out.reset_index(drop=True)


def normalize_recipe_code(value: object) -> str | None:
    """Normalize recipe codes so forecast/inbound can be joined.

    Forecast usually carries variant suffixes like FE0426C while inbound carries FE0426.
    We keep the base code only.
    """
    if pd.isna(value):
        return None
    text = str(value).upper().strip()
    match = re.search(r"((?:FE|FV)[A-Z0-9]{4,5}[A-Z]?)", text)
    if not match:
        return None
    code = match.group(1)
    if code and code[-1].isalpha():
        return code[:-1]
    return code


# ---------------------------------------------------------------------------
# Inbound
# ---------------------------------------------------------------------------

@dataclass
class InboundConfig:
    spreadsheet_id: str = INBOUND_SPREADSHEET_ID
    tab: str = INBOUND_TAB
    expected_col: str = "PO Expected Eaches/Weight"
    received_col: str = "Total Received Eaches/Weight"
    description_col: str = "Description"
    date_col: str = "Unload Date Time (Local)"
    production_week_offset: int = 1


def load_inbound(config: InboundConfig | None = None) -> pd.DataFrame:
    cfg = config or InboundConfig()
    df = read_tab_as_df(cfg.spreadsheet_id, cfg.tab, header_row=1)
    if df.empty:
        return df
    if cfg.expected_col in df.columns:
        df[cfg.expected_col] = to_number(df[cfg.expected_col])
    if cfg.received_col in df.columns:
        df[cfg.received_col] = to_number(df[cfg.received_col])
    # extract recipe code from description ("FA-DE Foo (FE0612)")
    df["Recipe Code"] = extract_codes(df.get(cfg.description_col, pd.Series(dtype=str)))
    df["join_code"] = df["Recipe Code"].map(normalize_recipe_code)
    # market hint from description prefix
    desc_str = df.get(cfg.description_col, pd.Series(dtype=str)).astype(str)
    df["Market_hint"] = (
        desc_str.str.extract(r"FA-(DE|NO|NOR|BNL|NL|BE)", expand=False)
        .replace({"NO": "NORDICS", "NOR": "NORDICS", "NL": "BNL", "BE": "BNL"})
    )
    # parse unload date and shift it to the production week anchor
    if cfg.date_col in df.columns:
        raw = df[cfg.date_col]
        # Sheets returns serial numbers (days since 1899-12-30) under UNFORMATTED_VALUE
        numeric = pd.to_numeric(raw, errors="coerce")
        if numeric.notna().any():
            dt = pd.to_datetime(numeric, unit="D", origin="1899-12-30", errors="coerce")
        else:
            dt = pd.to_datetime(raw, errors="coerce", dayfirst=False)
        unload_iso = dt.dt.isocalendar()
        production_dt = dt + pd.to_timedelta(cfg.production_week_offset * 7, unit="D")
        production_iso = production_dt.dt.isocalendar()
        df["unload_week"] = unload_iso["week"].apply(
            lambda w: f"W{int(w):02d}" if pd.notna(w) else None
        )
        df["unload_iso_year"] = unload_iso["year"]
        df["week"] = production_iso["week"].apply(
            lambda w: f"W{int(w):02d}" if pd.notna(w) else None
        )
        df["iso_year"] = production_iso["year"]
        df["unload_date"] = dt.dt.date
    else:
        df["unload_week"] = None
        df["unload_iso_year"] = None
        df["week"] = None
        df["iso_year"] = None
    return df


def filter_inbound(df: pd.DataFrame, week: str | None = None) -> pd.DataFrame:
    """Subset inbound by ISO week label (e.g. 'W17')."""
    if df.empty or not week or "week" not in df.columns:
        return df
    wk = str(week).upper().strip()
    if not wk.startswith("W"):
        wk = f"W{wk}"
    if len(wk) == 2:  # 'W7' → 'W07'
        wk = f"W0{wk[1]}"
    return df[df["week"] == wk].reset_index(drop=True)


def aggregate_inbound(df: pd.DataFrame, config: InboundConfig | None = None) -> pd.DataFrame:
    """Sum received qty per recipe code (across multiple POs / line items)."""
    cfg = config or InboundConfig()
    if df.empty or "join_code" not in df.columns:
        return pd.DataFrame(columns=["join_code", "Market_hint", "week", "expected", "received"])
    g = (
        df.dropna(subset=["join_code"])
        .groupby(["join_code", "Market_hint", "week"], dropna=False)
        .agg(
            expected=(cfg.expected_col, "sum"),
            received=(cfg.received_col, "sum"),
            line_items=("join_code", "size"),
        )
        .reset_index()
    )
    return g


# ---------------------------------------------------------------------------
# Vergleich Forecast vs Inbound
# ---------------------------------------------------------------------------

def compare_forecast_vs_inbound(
    forecast: pd.DataFrame, inbound_agg: pd.DataFrame,
    forecast_qty_col: str = "order",
) -> pd.DataFrame:
    """Per-recipe Soll/Ist comparison.

    Joins on normalized recipe base code + market + production week.
    """
    if forecast.empty:
        return pd.DataFrame()
    forecast = forecast.copy()
    forecast["join_code"] = forecast["Recipe Code"].map(normalize_recipe_code)
    f = (
        forecast.dropna(subset=["join_code"])
        .groupby(["Market", "week", "join_code"], dropna=False)
        .agg(
            forecast_qty=(forecast_qty_col, "sum"),
            slots=("Recipe Slot Number", lambda s: ", ".join(sorted(map(str, s.dropna().unique())))),
            sku_name=("Maitre SKU Name", "first"),
            maitre_code=("Maitre Code", "first"),
            recipe_codes=("Recipe Code", lambda s: ", ".join(sorted(map(str, s.dropna().unique())))),
        )
        .reset_index()
    )
    if inbound_agg.empty:
        f["received"] = 0.0
        f["expected_inbound"] = 0.0
        f["delta"] = -f["forecast_qty"].fillna(0)
        f["fulfillment_pct"] = 0.0
        return f
    merged = f.merge(
        inbound_agg.rename(columns={"expected": "expected_inbound"}),
        left_on=["Market", "week", "join_code"],
        right_on=["Market_hint", "week", "join_code"],
        how="left",
    )
    merged["received"] = merged["received"].fillna(0)
    merged["expected_inbound"] = merged["expected_inbound"].fillna(0)
    merged["delta"] = merged["received"] - merged["forecast_qty"].fillna(0)
    merged["fulfillment_pct"] = (
        (merged["received"] / merged["forecast_qty"]).where(merged["forecast_qty"] > 0) * 100
    ).round(1)
    return merged.sort_values(["Market", "delta"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Swap-Engine
# ---------------------------------------------------------------------------

@dataclass
class SwapSuggestion:
    market: str
    week: str
    short_recipe: str
    short_name: str
    short_qty: float
    donor_recipe: str
    donor_name: str
    donor_surplus: float
    swap_qty: float

    def to_dict(self) -> dict:
        return self.__dict__


def suggest_swaps(comparison: pd.DataFrame, min_shortfall: float = 50.0) -> pd.DataFrame:
    """Pair each shortage (received < forecast) with the largest surplus
    (received > forecast) within the same Market+week. Greedy allocation."""
    if comparison.empty:
        return pd.DataFrame()
    suggestions: list[SwapSuggestion] = []
    for (market, week), grp in comparison.groupby(["Market", "week"], dropna=False):
        shortages = grp[grp["delta"] < -min_shortfall].copy().sort_values("delta")
        surpluses = grp[grp["delta"] > min_shortfall].copy().sort_values("delta", ascending=False)
        if shortages.empty or surpluses.empty:
            continue
        # mutable surplus pool
        pool = surpluses[["Recipe Code", "sku_name", "delta"]].values.tolist()
        for _, sh in shortages.iterrows():
            need = -float(sh["delta"])
            for i, (donor_code, donor_name, donor_surplus) in enumerate(pool):
                if donor_surplus <= min_shortfall:
                    continue
                give = min(need, donor_surplus)
                suggestions.append(
                    SwapSuggestion(
                        market=str(market),
                        week=str(week),
                        short_recipe=sh["Recipe Code"],
                        short_name=str(sh["sku_name"]) if pd.notna(sh["sku_name"]) else "",
                        short_qty=float(sh["forecast_qty"] or 0),
                        donor_recipe=str(donor_code),
                        donor_name=str(donor_name) if pd.notna(donor_name) else "",
                        donor_surplus=float(donor_surplus),
                        swap_qty=float(give),
                    )
                )
                pool[i][2] = donor_surplus - give
                need -= give
                if need <= min_shortfall:
                    break
    if not suggestions:
        return pd.DataFrame()
    return pd.DataFrame([s.to_dict() for s in suggestions])
