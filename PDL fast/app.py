import streamlit as st
import pandas as pd
import io
import json
import re
import os
import shutil
import sys
import time
from datetime import date, datetime, timedelta
from collections import OrderedDict
from pathlib import Path

from factor_runtime import append_factor_run_history, build_factor_quality_report, build_factor_run_history_entry, build_factor_run_signature, load_factor_run_history
from lib import maitre_logic

# Optionaler Hub fuer den Unified-Platform-Blueprint.
# Beim eigenstaendigen Verteilen kann der Pfad fehlen -> No-Op-Stubs.
SHARED_DIR = Path(__file__).resolve().parents[1] / "Unified-Platform-Blueprint" / "shared"
if SHARED_DIR.exists() and str(SHARED_DIR) not in sys.path:
    sys.path.append(str(SHARED_DIR))

try:
    from hub_client import add_artifact, finish_run, start_run  # type: ignore
except Exception:
    def start_run(*_a, **_kw):  # type: ignore
        return None

    def add_artifact(*_a, **_kw):  # type: ignore
        return False

    def finish_run(*_a, **_kw):  # type: ignore
        return False

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    from googleapiclient.http import MediaInMemoryUpload
except ImportError:
    service_account = None
    build = None
    HttpError = None
    MediaInMemoryUpload = None

st.set_page_config(page_title="PDL Fast", layout="wide")
st.title("PDL Fast – Tracking & RESET Generator")

SETTINGS_PATH = Path(".pdl_fast_settings.json")
DEFAULT_SCALE_DRIVE_FOLDER_ID = "1LtFvWcBvuHzGP7KOVysOVoR1JzA0bv4p"
# Standardpfad fuer den Waagen-Sync. Auf fremden Rechnern wird er ueber
# die Settings-UI ueberschrieben; zur Vermeidung harter Personenpfade
# faellt der Default auf einen lokalen Unterordner zurueck, wenn der
# urspruengliche Pfad nicht existiert.
_LEGACY_SCALE_ROOT = Path(r"C:\Users\MarcelSpindler\Meine Ablage\Notfall\001_Weigt Calculator")
DEFAULT_SCALE_SYNC_ROOT = _LEGACY_SCALE_ROOT if _LEGACY_SCALE_ROOT.exists() else Path(__file__).resolve().parent / "Scale Sync"
DEFAULT_SCALE_RESET_TEMPLATE = DEFAULT_SCALE_SYNC_ROOT / "DELETE.DEL"
CACHE_TTL_SECONDS = 300
CENTRAL_RESULTS_DIR = Path(__file__).resolve().parents[1] / "Unified-Platform-Blueprint" / "results" / "pdl-fast"


def load_app_settings(settings_path: Path) -> dict:
    if not settings_path.exists():
        return {}
    try:
        return json.loads(settings_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def save_app_settings(settings_path: Path, settings: dict):
    settings_path.write_text(json.dumps(settings, ensure_ascii=True, indent=2), encoding="utf-8")


APP_SETTINGS = load_app_settings(SETTINGS_PATH)


def get_app_setting(key: str, default):
    return APP_SETTINGS.get(key, default)


def set_app_settings(updates: dict):
    global APP_SETTINGS
    normalized_updates = {key: value for key, value in updates.items()}
    if all(APP_SETTINGS.get(key) == value for key, value in normalized_updates.items()):
        return
    APP_SETTINGS.update(normalized_updates)
    save_app_settings(SETTINGS_PATH, APP_SETTINGS)


def get_path_mtime_ns(path: Path | None) -> int:
    if path is None:
        return 0
    try:
        return path.stat().st_mtime_ns
    except OSError:
        return 0


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def read_excel_from_path(path_str: str, modified_time_ns: int, sheet_name=0):
    del modified_time_ns
    return pd.read_excel(path_str, sheet_name=sheet_name)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def read_excel_from_bytes(file_bytes: bytes, sheet_name=0):
    return pd.read_excel(io.BytesIO(file_bytes), sheet_name=sheet_name)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def read_excel_sheet_names(file_bytes: bytes) -> list[str]:
    return pd.ExcelFile(io.BytesIO(file_bytes)).sheet_names


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_maitre_forecast_cached(tabs: tuple[str, ...]) -> pd.DataFrame:
    config = maitre_logic.ForecastConfig(tabs=tabs or (maitre_logic.FORECAST_TABS["DE_NOR"],))
    return maitre_logic.load_forecast(config)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_maitre_inbound_cached() -> pd.DataFrame:
    return maitre_logic.load_inbound()


def build_maitre_admin_snapshot(selected_week: int) -> dict:
    week_label = f"W{int(selected_week):02d}"
    forecast_raw_df = load_maitre_forecast_cached((maitre_logic.FORECAST_TABS["DE_NOR"],))
    inbound_raw_df = load_maitre_inbound_cached()
    forecast_week_df = maitre_logic.filter_forecast(forecast_raw_df, week=week_label, markets=["DE", "NORDICS"])
    inbound_week_df = maitre_logic.filter_inbound(inbound_raw_df, week=week_label)
    inbound_agg_df = maitre_logic.aggregate_inbound(inbound_week_df)
    comparison_df = maitre_logic.compare_forecast_vs_inbound(forecast_week_df, inbound_agg_df)
    shortages_df = comparison_df[comparison_df["delta"] < -50].copy() if not comparison_df.empty else pd.DataFrame()
    overages_df = comparison_df[comparison_df["delta"] > 50].copy() if not comparison_df.empty else pd.DataFrame()
    return {
        "week_label": week_label,
        "forecast_raw_df": forecast_raw_df,
        "inbound_raw_df": inbound_raw_df,
        "forecast_week_df": forecast_week_df,
        "inbound_week_df": inbound_week_df,
        "inbound_agg_df": inbound_agg_df,
        "comparison_df": comparison_df,
        "shortages_df": shortages_df,
        "overages_df": overages_df,
    }

# ──────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────

def expand_factor_meal_swap(meal_swap_str: str, bag_size: int = 1) -> str:
    """
    Factor: '601:2 602:2 603:1' → '601 601 602 602 603-1p'
    Expand recipe:count pairs, preserve original order, append -<bag_size>p.
    """
    items = []
    for pair in meal_swap_str.strip().split():
        if ":" in pair:
            recipe, count = pair.split(":")
            items.extend([recipe] * int(count))
        else:
            items.append(pair)
    suffix = f"-{bag_size}p"
    if items:
        return " ".join(items[:-1]) + " " + items[-1] + suffix if len(items) > 1 else items[0] + suffix
    return suffix


def parse_factor_cutoff_label(source_name: str) -> str:
    match = re.search(r"(CO[_-]?\d+)", str(source_name), flags=re.IGNORECASE)
    if match:
        return match.group(1).replace("-", "_").upper()
    return Path(str(source_name)).stem.upper()


def build_factor_packaging_weight_map(pkg_df: pd.DataFrame | None) -> dict[str, float]:
    if pkg_df is None or pkg_df.empty:
        return {}
    key_col = pkg_df.columns[0]
    weight_col = pkg_df.columns[1]
    mapping = {}
    for _, row in pkg_df.iterrows():
        key = str(row.get(key_col, "")).strip()
        if not key or key.lower() == "nan":
            continue
        try:
            mapping[key] = float(str(row.get(weight_col, 0)).replace(",", "."))
        except (TypeError, ValueError):
            continue
    return mapping


def build_factor_recipe_weight_map(picklist_df: pd.DataFrame | None, sku_df: pd.DataFrame | None) -> dict[str, float]:
    if picklist_df is None or picklist_df.empty or sku_df is None or sku_df.empty:
        return {}
    sku_code_col = first_existing_column(sku_df, ["SKU Code", "Culinary SKU Code*", "SKU"])
    sku_weight_col = first_existing_column(sku_df, ["Weight (g)", "Pack weight new (g)", "Gewicht"])
    recipe_col = first_existing_column(picklist_df, ["Recipe"])
    picklist_sku_col = first_existing_column(picklist_df, ["SKU"])
    quantity_col = first_existing_column(picklist_df, ["Quantity"])
    if not all([sku_code_col, sku_weight_col, recipe_col, picklist_sku_col, quantity_col]):
        return {}

    sku_weights = {}
    for _, row in sku_df.iterrows():
        sku_code = str(row.get(sku_code_col, "")).strip()
        if not sku_code:
            continue
        try:
            sku_weights[sku_code] = float(str(row.get(sku_weight_col, 0)).replace(",", "."))
        except (TypeError, ValueError):
            continue

    recipe_weights: dict[str, float] = {}
    for _, row in picklist_df.iterrows():
        recipe_key = str(row.get(recipe_col, "")).strip()
        sku_code = str(row.get(picklist_sku_col, "")).strip()
        if not recipe_key or not sku_code:
            continue
        try:
            quantity = float(str(row.get(quantity_col, 0)).replace(",", "."))
        except (TypeError, ValueError):
            quantity = 0.0
        recipe_weights[recipe_key] = recipe_weights.get(recipe_key, 0.0) + (sku_weights.get(sku_code, 0.0) * quantity)
    return recipe_weights


def build_factor_manual_weight_reference_df(fulfillment_df: pd.DataFrame | None, base_recipe_weight_map: dict[str, float] | None) -> pd.DataFrame:
    if fulfillment_df is None or fulfillment_df.empty:
        return pd.DataFrame(columns=[
            "Maitre Code",
            "Rezepte",
            "Regionen",
            "Artikel",
            "Theorie Rezeptgewicht (g)",
            "Manuelles Rezeptgewicht (g)",
        ])

    work_df = fulfillment_df.copy()
    work_df["Maitre Code"] = work_df["Maitre Code"].apply(clean_sheet_cell)
    work_df["Artikel"] = work_df["Artikel"].apply(clean_sheet_cell)
    work_df = work_df[work_df["Recipe"].notna()].copy()
    if work_df.empty:
        return pd.DataFrame()

    rows = []
    for maitre_code, group in work_df.groupby("Maitre Code", sort=False, dropna=False):
        recipes = unique_preserve_order([str(int(value)) for value in group["Recipe"].tolist() if pd.notna(value)])
        regions = unique_preserve_order(group["Region"].astype(str).tolist())
        article = next((value for value in group["Artikel"].tolist() if clean_sheet_cell(value)), "")
        meal_code = next((clean_sheet_cell(value) for value in group.get("Meal Code", pd.Series(dtype=str)).tolist() if clean_sheet_cell(value)), "")
        fallback_weights = []
        for recipe in recipes:
            recipe_key = f"{recipe}r-1p"
            fallback_weight = (base_recipe_weight_map or {}).get(recipe_key)
            if fallback_weight is not None and fallback_weight > 0:
                fallback_weights.append(float(fallback_weight))
        theory_weight = round(sum(fallback_weights) / len(fallback_weights), 1) if fallback_weights else 0.0
        rows.append({
            "Maitre Code": maitre_code,
            "Meal Code": meal_code,
            "Meal Code FE": derive_factor_meal_code_transition_fields(meal_code)[0],
            "Meal Code FV": derive_factor_meal_code_transition_fields(meal_code)[1],
            "Rezepte": ", ".join(recipes),
            "Regionen": ", ".join(regions),
            "Artikel": article,
            "Theorie Rezeptgewicht (g)": theory_weight,
            "Manuelles Rezeptgewicht (g)": theory_weight,
        })

    result_df = pd.DataFrame(rows)
    return result_df.sort_values(["Maitre Code", "Rezepte"], kind="stable").reset_index(drop=True)


def build_factor_recipe_weight_map_with_manuals(
    base_recipe_weight_map: dict[str, float] | None,
    fulfillment_df: pd.DataFrame | None,
    manual_weight_df: pd.DataFrame | None,
) -> dict[str, float]:
    combined_map = dict(base_recipe_weight_map or {})
    if fulfillment_df is None or fulfillment_df.empty or manual_weight_df is None or manual_weight_df.empty:
        return combined_map

    manual_by_maitre: dict[str, float] = {}
    for _, row in manual_weight_df.iterrows():
        maitre_code = clean_sheet_cell(row.get("Maitre Code", ""))
        try:
            manual_weight = float(str(row.get("Manuelles Rezeptgewicht (g)", "")).replace(",", "."))
        except (TypeError, ValueError):
            manual_weight = 0.0
        if maitre_code and manual_weight > 0:
            manual_by_maitre[maitre_code] = manual_weight

    if not manual_by_maitre:
        return combined_map

    mapped_df = fulfillment_df[["Recipe", "Maitre Code"]].copy()
    mapped_df["Maitre Code"] = mapped_df["Maitre Code"].apply(clean_sheet_cell)
    mapped_df = mapped_df.dropna(subset=["Recipe"]).drop_duplicates()

    for _, row in mapped_df.iterrows():
        maitre_code = row.get("Maitre Code", "")
        if maitre_code not in manual_by_maitre:
            continue
        try:
            recipe_number = int(row.get("Recipe"))
        except (TypeError, ValueError):
            continue
        combined_map[f"{recipe_number}r-1p"] = manual_by_maitre[maitre_code]

    return combined_map


def load_factor_saved_maitre_weights(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        return pd.DataFrame(columns=[
            "Maitre Code",
            "Meal Code",
            "Artikel",
            "Gespeichertes Rezeptgewicht (g)",
            "Zuletzt gesehen in KW",
            "Aktualisiert am",
        ])
    try:
        saved_df = pd.read_csv(csv_path, sep=";", dtype=str).fillna("")
    except Exception:
        return pd.DataFrame(columns=[
            "Maitre Code",
            "Meal Code",
            "Artikel",
            "Gespeichertes Rezeptgewicht (g)",
            "Zuletzt gesehen in KW",
            "Aktualisiert am",
        ])

    if "Maitre Code" not in saved_df.columns:
        return pd.DataFrame(columns=[
            "Maitre Code",
            "Meal Code",
            "Artikel",
            "Gespeichertes Rezeptgewicht (g)",
            "Zuletzt gesehen in KW",
            "Aktualisiert am",
        ])
    return saved_df


def apply_factor_saved_maitre_weights(
    reference_df: pd.DataFrame | None,
    saved_weights_df: pd.DataFrame | None,
) -> pd.DataFrame:
    if reference_df is None or reference_df.empty:
        return pd.DataFrame()

    result_df = reference_df.copy()
    if saved_weights_df is None or saved_weights_df.empty:
        result_df["Gewicht gespeichert"] = "Nein"
        return result_df

    saved_lookup = {}
    for _, row in saved_weights_df.iterrows():
        maitre_code = clean_sheet_cell(row.get("Maitre Code", ""))
        if not maitre_code:
            continue
        try:
            saved_weight = float(str(row.get("Gespeichertes Rezeptgewicht (g)", "")).replace(",", "."))
        except (TypeError, ValueError):
            continue
        if saved_weight > 0:
            saved_lookup[maitre_code] = saved_weight

    result_df["Gewicht gespeichert"] = "Nein"
    for index, row in result_df.iterrows():
        maitre_code = clean_sheet_cell(row.get("Maitre Code", ""))
        saved_weight = saved_lookup.get(maitre_code)
        if saved_weight is None:
            continue
        result_df.at[index, "Manuelles Rezeptgewicht (g)"] = saved_weight
        result_df.at[index, "Gewicht gespeichert"] = "Ja"
    return result_df


def save_factor_maitre_weights(
    csv_path: Path,
    edited_df: pd.DataFrame | None,
    week_label: str,
) -> tuple[int, pd.DataFrame]:
    saved_df = load_factor_saved_maitre_weights(csv_path)
    if edited_df is None or edited_df.empty:
        return 0, saved_df

    now_text = datetime.now().strftime("%Y-%m-%d %H:%M")
    save_rows = []
    for _, row in edited_df.iterrows():
        maitre_code = clean_sheet_cell(row.get("Maitre Code", ""))
        if not maitre_code:
            continue
        try:
            manual_weight = float(str(row.get("Manuelles Rezeptgewicht (g)", "")).replace(",", "."))
        except (TypeError, ValueError):
            continue
        if manual_weight <= 0:
            continue
        save_rows.append({
            "Maitre Code": maitre_code,
            "Meal Code": clean_sheet_cell(row.get("Meal Code", "")),
            "Artikel": clean_sheet_cell(row.get("Artikel", "")),
            "Gespeichertes Rezeptgewicht (g)": f"{manual_weight:.1f}",
            "Zuletzt gesehen in KW": week_label,
            "Aktualisiert am": now_text,
        })

    if not save_rows:
        return 0, saved_df

    update_df = pd.DataFrame(save_rows).drop_duplicates(subset=["Maitre Code"], keep="last")
    if saved_df.empty:
        merged_df = update_df
    else:
        base_df = saved_df.copy()
        if "Maitre Code" not in base_df.columns:
            base_df = pd.DataFrame(columns=update_df.columns)
        for column in update_df.columns:
            if column not in base_df.columns:
                base_df[column] = ""
        base_df = base_df.set_index("Maitre Code", drop=False)
        update_indexed_df = update_df.set_index("Maitre Code", drop=False)
        base_df.update(update_indexed_df)
        missing_codes = [code for code in update_indexed_df.index if code not in base_df.index]
        merged_df = pd.concat([base_df.reset_index(drop=True), update_indexed_df.loc[missing_codes].reset_index(drop=True)], ignore_index=True) if missing_codes else base_df.reset_index(drop=True)

    merged_df = merged_df.sort_values(["Maitre Code"], kind="stable").reset_index(drop=True)
    merged_df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
    return len(update_df), merged_df


def compute_factor_theoretical_weight_kg(row: pd.Series, recipe_weight_map: dict[str, float], packaging_weight_map: dict[str, float]) -> float:
    meal_swap_text = str(row.get("meal_swap", "")).strip()
    ingredient_weight_g = 0.0
    for token in meal_swap_text.split():
        recipe_code = token
        count = 1
        if ":" in token:
            recipe_code, count_text = token.split(":", 1)
            try:
                count = int(count_text)
            except ValueError:
                count = 1
        recipe_key = f"{recipe_code}r-1p"
        ingredient_weight_g += recipe_weight_map.get(recipe_key, 0.0) * count

    packaging_weight_g = 0.0
    for field_name in ["box_size", "cool_pouch_name"]:
        package_name = str(row.get(field_name, "")).strip()
        if package_name and package_name.lower() != "nan":
            packaging_weight_g += packaging_weight_map.get(package_name, 0.0)

    ice_count = row.get("total_ice_count", row.get("number_ice_packs", 0))
    try:
        ice_count_value = float(str(ice_count).replace(",", "."))
    except (TypeError, ValueError):
        ice_count_value = 0.0
    ice_weight_g = ice_count_value * packaging_weight_map.get("Eispack", packaging_weight_map.get("Icepack", 490.0))

    return round((ingredient_weight_g + packaging_weight_g + ice_weight_g) / 1000, 3)


def build_factor_reset_input_df(pdl_df: pd.DataFrame, recipe_weight_map: dict[str, float], packaging_weight_map: dict[str, float]) -> pd.DataFrame:
    if pdl_df is None or pdl_df.empty:
        return pd.DataFrame()
    work_df = pdl_df.copy()
    work_df["Expanded Meal Swap"] = work_df.apply(
        lambda row: expand_factor_meal_swap(str(row.get("meal_swap", "")).strip(), int(row.get("bag_size", 1) or 1)),
        axis=1,
    )
    work_df["Cutoff"] = work_df["_source"].apply(parse_factor_cutoff_label) if "_source" in work_df.columns else "UPLOAD"
    work_df["Theoretisches Gewicht (kg)"] = work_df.apply(
        lambda row: compute_factor_theoretical_weight_kg(row, recipe_weight_map, packaging_weight_map),
        axis=1,
    )

    grouped_rows = []
    for meal_swap, group in work_df.groupby("Expanded Meal Swap", sort=False):
        reference_row = group.iloc[0]
        grouped_rows.append({
            "Cutoff": reference_row.get("Cutoff", "UPLOAD"),
            "Meal Swap": meal_swap,
            "Referenz Box_ID": str(reference_row.get("boxid", "")).strip(),
            "Boxen": int(len(group)),
            "Theoretisches Gewicht (kg)": float(reference_row.get("Theoretisches Gewicht (kg)", 0.0)),
            "Manuelles Gewicht (kg)": float(reference_row.get("Theoretisches Gewicht (kg)", 0.0)),
        })
    return pd.DataFrame(grouped_rows)


def build_factor_reset_map_from_input(weight_input_df: pd.DataFrame) -> OrderedDict:
    reset_map = OrderedDict()
    if weight_input_df is None or weight_input_df.empty:
        return reset_map
    sorted_df = weight_input_df.sort_values(["Cutoff", "Meal Swap"], kind="stable")
    for _, row in sorted_df.iterrows():
        meal_swap = str(row.get("Meal Swap", "")).strip()
        if not meal_swap:
            continue
        try:
            weight = float(str(row.get("Manuelles Gewicht (kg)", 0)).replace(",", "."))
        except (TypeError, ValueError):
            try:
                weight = float(str(row.get("Theoretisches Gewicht (kg)", 0)).replace(",", "."))
            except (TypeError, ValueError):
                weight = 0.0
        reset_map[meal_swap] = weight
    return reset_map


FACTOR_SWAP_MARKET_CONFIG = {
    "FA-DE": "Swaps - DE",
    "FA-BENL": "Swaps - BENL | Nordics",
    "FA-DKSE": "Swaps - BENL | Nordics",
}


def factor_swap_tab_for_market(market: str) -> str:
    return FACTOR_SWAP_MARKET_CONFIG.get(str(market).strip(), "Swaps - DE")


def factor_swap_get_week_options(sheets_service, spreadsheet_id: str) -> list[str]:
    values = sheets_read_values(sheets_service, spreadsheet_id, "'Data Validation'!A1:A60")
    return [row[0] for row in values if row and str(row[0]).strip()]


def factor_swap_set_controls(sheets_service, spreadsheet_id: str, tab_name: str, market: str, week_label: str, meal_short: str | None = None):
    data = [
        {"range": f"'{tab_name}'!E3", "values": [[market]]},
        {"range": f"'{tab_name}'!E4", "values": [[week_label]]},
    ]
    if meal_short is not None:
        data.append({"range": f"'{tab_name}'!E5", "values": [[meal_short]]})
    sheets_service.spreadsheets().values().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"valueInputOption": "USER_ENTERED", "data": data},
    ).execute()


def factor_swap_get_meal_options(sheets_service, spreadsheet_id: str, market: str, week_label: str) -> list[str]:
    tab_name = factor_swap_tab_for_market(market)
    current_market = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!E3")
    current_week = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!E4")
    current_meal = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!E5")
    original_market = current_market[0][0] if current_market and current_market[0] else market
    original_week = current_week[0][0] if current_week and current_week[0] else week_label
    original_meal = current_meal[0][0] if current_meal and current_meal[0] else ""
    try:
        factor_swap_set_controls(sheets_service, spreadsheet_id, tab_name, market, week_label)
        for _ in range(8):
            options_values = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!G30:G47")
            options = [row[0] for row in options_values if row and str(row[0]).strip() and str(row[0]).strip() != "#REF!"]
            if options:
                return options
            time.sleep(0.35)
        return []
    finally:
        try:
            factor_swap_set_controls(sheets_service, spreadsheet_id, tab_name, original_market, original_week, original_meal)
        except Exception:
            pass


def factor_swap_get_results(sheets_service, spreadsheet_id: str, market: str, week_label: str, meal_short: str) -> tuple[dict[str, str], pd.DataFrame]:
    tab_name = factor_swap_tab_for_market(market)
    current_market = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!E3")
    current_week = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!E4")
    current_meal = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!E5")
    original_market = current_market[0][0] if current_market and current_market[0] else market
    original_week = current_week[0][0] if current_week and current_week[0] else week_label
    original_meal = current_meal[0][0] if current_meal and current_meal[0] else meal_short
    try:
        factor_swap_set_controls(sheets_service, spreadsheet_id, tab_name, market, week_label, meal_short)
        time.sleep(0.6)
        summary_values = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!D3:E6")
        result_values = sheets_read_values(sheets_service, spreadsheet_id, f"'{tab_name}'!C7:G24")
        summary = {}
        for row in summary_values:
            if len(row) >= 2 and str(row[0]).strip():
                summary[str(row[0]).strip()] = str(row[1]).strip() if len(row) > 1 else ""
            elif len(row) >= 2 and not str(row[0]).strip() and str(row[1]).strip():
                summary["Meal Name"] = str(row[1]).strip()

        result_rows = []
        for row in result_values[1:]:
            if len(row) >= 2 and str(row[0]).strip() and str(row[1]).strip() and str(row[1]).strip() != "#REF!":
                priority_text = str(row[0]).strip()
                try:
                    priority_number = int(priority_text)
                except ValueError:
                    priority_number = None
                result_rows.append({
                    "Swap Priority": priority_text,
                    "Wahl": f"{priority_text}. Wahl" if priority_number is not None else priority_text,
                    "Meal Short": str(row[1]).strip(),
                    "Meal Name": str(row[4]).strip() if len(row) > 4 else "",
                })
        return summary, pd.DataFrame(result_rows)
    finally:
        try:
            factor_swap_set_controls(sheets_service, spreadsheet_id, tab_name, original_market, original_week, original_meal)
        except Exception:
            pass


def build_tracking_line(box_id: str, bag_size, num_meals, meal_swap: str,
                        correction, num_cols: int = 21) -> str:
    """
    Build a single Tracking CSV line (semicolon-separated).
    Column positions (0-indexed):
      0: Box_ID, 4: BagSize, 6: number_of_meals, 7: Meal Swap,
      11: Correction, 16: barcode (=Box_ID)
    """
    cols = [""] * num_cols
    cols[0] = str(box_id)
    cols[4] = str(bag_size)
    cols[6] = str(num_meals)
    cols[7] = str(meal_swap)
    # Format correction with comma as decimal separator (always 3 decimals)
    if isinstance(correction, (int, float)):
        cols[11] = f"{correction:.3f}".replace(".", ",")
    else:
        try:
            cols[11] = f"{float(str(correction).replace(',', '.')):.3f}".replace(".", ",")
        except (ValueError, TypeError):
            cols[11] = "0,000"
    cols[16] = str(box_id)
    return ";".join(cols)


TRACKING_HEADER = (
    "Box_ID;placeholder;placeholder;placeholder;BagSize;placeholder;"
    "number_of_meals;Meal Swap;placeholder;placeholder;placeholder;"
    "Correction;placeholder;placeholder;placeholder;placeholder;"
    "barcode;placeholder;placeholder;placeholder;placeholder"
)


def normalize_csv_text(csv_text: str) -> str:
    normalized_lines = str(csv_text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    return "\r\n".join(normalized_lines)


def csv_text_to_bytes(csv_text: str) -> bytes:
    return normalize_csv_text(csv_text).encode("utf-8-sig")


def generate_tracking_csv(lines: list[str]) -> str:
    return TRACKING_HEADER + "\n" + "\n".join(lines)


def generate_reset_csv(combo_weight_map: OrderedDict) -> str:
    """combo_weight_map: OrderedDict { 'meal_combo-Xp': weight_kg_float }"""
    rows = []
    for combo, weight in combo_weight_map.items():
        w_str = f"{weight:.3f}".replace(".", ",")
        rows.append(f"{combo};{w_str}")
    return "\n".join(rows)


def parse_reset_csv(reset_csv_text: str) -> dict[str, float]:
    reset_map = {}
    for line in str(reset_csv_text).splitlines():
        line = line.replace("\ufeff", "").strip()
        if not line or ";" not in line:
            continue
        combo, weight = line.split(";", 1)
        try:
            reset_map[combo] = float(weight.replace(",", "."))
        except ValueError:
            continue
    return reset_map


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, sep=";", lineterminator="\r\n").encode("utf-8-sig")


def parse_correction_value(val) -> float:
    """Try to parse a correction value from PDL (may be NoData, NaN, etc.)."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if s in ("", "NoData", "nan", "NaN", "None"):
        return 0.0
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return 0.0


def get_current_year_week() -> tuple[int, int]:
    iso_now = date.today().isocalendar()
    return iso_now.year, iso_now.week


HF_WEEKDAY_CUTOFF_MAP = {
    0: "CO_3",  # Montag
    1: "CO_4",  # Dienstag
    2: "CO_5",  # Mittwoch
    3: "CO_6",  # Donnerstag
    4: "CO_1",  # Freitag
    5: "CO_2",  # Samstag
    6: "CO_2",  # Sonntag
}


def get_hf_operational_year_week(reference_date: date | None = None) -> tuple[int, int]:
    current_date = reference_date or date.today()
    operational_date = current_date + timedelta(days=4)
    iso_now = operational_date.isocalendar()
    return iso_now.year, iso_now.week


def get_hf_recommended_cutoff(reference_date: date | None = None) -> str:
    current_date = reference_date or date.today()
    return HF_WEEKDAY_CUTOFF_MAP[current_date.weekday()]


def format_week_label(year: int, week: int) -> str:
    return f"{year}-W{int(week):02d}"


def unique_preserve_order(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def make_export_folder_name(week: int, cutoffs: list[str] | None = None) -> str:
    normalized_cutoffs = [normalize_cutoff_label(cutoff) for cutoff in (cutoffs or []) if str(cutoff).strip()]
    normalized_cutoffs = unique_preserve_order(normalized_cutoffs)
    suffix = f"-{'_'.join(normalized_cutoffs)}" if normalized_cutoffs else ""
    return f"KW{int(week):02d}{suffix}"


def mirror_export_dir_to_central(export_root: Path, export_dir: Path) -> Path:
    root_label = str(export_root).lower()
    if "factor" in root_label:
        scope = "factor"
    elif "hello" in root_label:
        scope = "hellofresh"
    else:
        scope = "generic"

    target_dir = CENTRAL_RESULTS_DIR / scope / export_dir.name
    target_dir.mkdir(parents=True, exist_ok=True)
    for item in export_dir.iterdir():
        if item.is_file():
            shutil.copy2(item, target_dir / item.name)
    return target_dir


def quality_to_run_status(status_value: str) -> str:
    normalized = str(status_value or "").strip().upper()
    if normalized == "ROT":
        return "failed"
    if normalized == "GELB":
        return "warning"
    return "success"


def persist_export_files(export_root: Path, week: int, cutoffs: list[str] | None, tracking_csv: str, reset_csv: str | None) -> Path:
    export_dir = export_root / make_export_folder_name(week, cutoffs)
    export_dir.mkdir(parents=True, exist_ok=True)
    tracking_path = export_dir / "Tracking.csv"
    tracking_path.write_bytes(csv_text_to_bytes(tracking_csv))

    reset_path = export_dir / "RESET.csv"
    if reset_csv is not None:
        reset_path.write_bytes(csv_text_to_bytes(reset_csv))
    elif reset_path.exists():
        reset_path.unlink()

    mirror_export_dir_to_central(export_root, export_dir)

    return export_dir


def deploy_scale_reset_file(target_dir: Path, source_path: Path | None = None) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / "DELETE.DEL"
    template_path = source_path or DEFAULT_SCALE_RESET_TEMPLATE
    if template_path.exists():
        target_path.write_bytes(template_path.read_bytes())
    else:
        target_path.write_bytes(b"")
    return target_path


def find_picklist_file(base_dir: Path, year: int, week: int) -> Path | None:
    target = base_dir / f"picklist-{format_week_label(year, week)}.xlsx"
    if target.exists():
        return target
    matches = sorted(base_dir.glob(f"*{format_week_label(year, week)}*.xlsx"))
    return matches[0] if matches else None


def find_factor_picklist_file(base_dir: Path, year: int, week: int) -> Path | None:
    exact_names = [
        f"Marcel_picklist_neu-{year}-W{int(week):02d}.xlsx",
        f"picklist-{format_week_label(year, week)}.xlsx",
    ]
    for file_name in exact_names:
        candidate = base_dir / file_name
        if candidate.exists():
            return candidate
    matches = sorted(base_dir.glob(f"*{year}*W{int(week):02d}*.xlsx"))
    return matches[0] if matches else None


def list_cutoff_dirs(base_dir: Path, year: int, week: int) -> list[str]:
    week_dir = base_dir / format_week_label(year, week)
    if not week_dir.exists():
        return []
    return sorted(
        child.name for child in week_dir.iterdir()
        if child.is_dir() and child.name.upper().startswith("CO_")
    )


def find_pdl_file(base_dir: Path, year: int, week: int, cutoff: str) -> Path | None:
    cutoff_dir = base_dir / format_week_label(year, week) / cutoff / "DWHTAXI"
    if not cutoff_dir.exists():
        return None
    exact_name = cutoff.replace("_", "")
    exact = cutoff_dir / f"PDL-{exact_name}.xlsx"
    if exact.exists():
        return exact
    matches = sorted(
        path for path in cutoff_dir.glob("PDL-*.xlsx")
        if not path.name.startswith("~$")
    )
    return matches[0] if matches else None


@st.cache_resource(show_spinner=False)
def get_google_drive_service(credentials_path: Path, readonly: bool = True):
    if service_account is None or build is None or not credentials_path.exists():
        return None
    scopes = ["https://www.googleapis.com/auth/drive.readonly"] if readonly else ["https://www.googleapis.com/auth/drive"]
    creds = service_account.Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    return build("drive", "v3", credentials=creds)


@st.cache_resource(show_spinner=False)
def get_google_sheets_service(credentials_path: Path, readonly: bool = True):
    if service_account is None or build is None or not credentials_path.exists():
        return None
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"] if readonly else ["https://www.googleapis.com/auth/spreadsheets"]
    creds = service_account.Credentials.from_service_account_file(str(credentials_path), scopes=scopes)
    return build("sheets", "v4", credentials=creds)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def sheets_read_values(_sheets_service, spreadsheet_id: str, range_name: str) -> list[list[str]]:
    if _sheets_service is None:
        return []
    response = _sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_name,
    ).execute()
    return response.get("values", [])


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_sheet_dataframe(_sheets_service, spreadsheet_id: str, tab_name: str, header_row_index: int = 0, end_col: str = "AZ", end_row: int = 2000) -> pd.DataFrame:
    values = sheets_read_values(_sheets_service, spreadsheet_id, f"'{tab_name}'!A1:{end_col}{end_row}")
    if not values or len(values) <= header_row_index:
        return pd.DataFrame()
    header = values[header_row_index]
    width = len(header)
    rows = []
    for row in values[header_row_index + 1:]:
        normalized = list(row[:width]) + [""] * max(0, width - len(row))
        if any(str(cell).strip() for cell in normalized):
            rows.append(normalized)
    columns = []
    seen = {}
    for col in header:
        base = str(col).strip() if str(col).strip() else "col"
        count = seen.get(base, 0)
        seen[base] = count + 1
        columns.append(base if count == 0 else f"{base}_{count + 1}")
    return pd.DataFrame(rows, columns=columns)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def load_sheet_dataframe_by_header(_sheets_service, spreadsheet_id: str, tab_name: str, required_headers: list[str], end_col: str = "AZ", end_row: int = 500) -> pd.DataFrame:
    values = sheets_read_values(_sheets_service, spreadsheet_id, f"'{tab_name}'!A1:{end_col}{end_row}")
    if not values:
        return pd.DataFrame()

    header_row_index = None
    for index, row in enumerate(values[:50]):
        row_cells = {clean_sheet_cell(cell) for cell in row}
        if all(header in row_cells for header in required_headers):
            header_row_index = index
            break

    if header_row_index is None:
        return pd.DataFrame()

    header = values[header_row_index]
    width = len(header)
    rows = []
    for row in values[header_row_index + 1:]:
        normalized = list(row[:width]) + [""] * max(0, width - len(row))
        if any(clean_sheet_cell(cell) for cell in normalized):
            rows.append(normalized)

    columns = []
    seen = {}
    for col in header:
        base = clean_sheet_cell(col) or "col"
        count = seen.get(base, 0)
        seen[base] = count + 1
        columns.append(base if count == 0 else f"{base}_{count + 1}")
    return pd.DataFrame(rows, columns=columns)


def parse_sheet_weight_kg(value) -> float | None:
    text = clean_sheet_cell(value)
    if not text:
        return None
    try:
        return round(float(text.replace(",", ".")), 3)
    except ValueError:
        return None


def load_hf_buero_recipe_weights(sheets_service, spreadsheet_id: str) -> tuple[dict[str, float], set[str], str | None]:
    if sheets_service is None:
        return {}, set(), None

    values = sheets_read_values(sheets_service, spreadsheet_id, "'Büro'!A1:Q250")
    if not values:
        return {}, set(), None

    week_label = None
    if len(values) > 1 and len(values[1]) > 1:
        week_label = clean_sheet_cell(values[1][1]) or None

    header_row_index = None
    for index, row in enumerate(values):
        cells = [clean_sheet_cell(cell) for cell in row]
        if "Hide?" in cells and "Rezept Nummer" in cells and "MK" in cells:
            header_row_index = index
            break

    if header_row_index is None:
        return {}, set(), week_label

    recipe_weights: dict[str, float] = {}
    hidden_recipe_keys: set[str] = set()

    for row in values[header_row_index + 1:]:
        recipe_label = clean_sheet_cell(row[2] if len(row) > 2 else "")
        if not recipe_label or not re.fullmatch(r"\d+r", recipe_label):
            continue

        hidden = clean_sheet_cell(row[1] if len(row) > 1 else "").upper() == "TRUE"
        bag_weight_columns = {
            2: 3,
            3: 6,
            4: 9,
        }
        for bag_size, column_index in bag_weight_columns.items():
            weight = parse_sheet_weight_kg(row[column_index] if len(row) > column_index else "")
            recipe_key = f"{recipe_label}-{bag_size}p"
            if hidden:
                hidden_recipe_keys.add(recipe_key)
            if weight is not None and weight > 0:
                recipe_weights[recipe_key] = weight

    return recipe_weights, hidden_recipe_keys, week_label


def build_hf_reset_map_from_sheet(
    combos: list[str],
    recipe_weight_map: dict[str, float],
    hidden_recipe_keys: set[str] | None = None,
) -> tuple[OrderedDict, list[str], list[str]]:
    hidden_recipe_keys = hidden_recipe_keys or set()
    combo_weight_map: OrderedDict[str, float] = OrderedDict()
    hidden_combos: list[str] = []
    missing_combos: list[str] = []

    for combo in combos:
        recipe_keys = meal_swap_to_recipe_keys(combo)
        if not recipe_keys:
            missing_combos.append(combo)
            continue
        if any(recipe_key in hidden_recipe_keys for recipe_key in recipe_keys):
            hidden_combos.append(combo)
            continue
        weights = [recipe_weight_map.get(recipe_key) for recipe_key in recipe_keys]
        if any(weight is None for weight in weights):
            missing_combos.append(combo)
            continue
        combo_weight_map[combo] = round(sum(weight for weight in weights if weight is not None), 3)

    return combo_weight_map, hidden_combos, missing_combos


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def drive_list_children(_drive_service, folder_id: str) -> list[dict]:
    if _drive_service is None:
        return []
    children = []
    page_token = None
    while True:
        response = _drive_service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            pageSize=100,
            fields="nextPageToken,files(id,name,mimeType,modifiedTime)",
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            pageToken=page_token,
        ).execute()
        children.extend(response.get("files", []))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return children


def drive_find_named_child(drive_service, parent_id: str, child_name: str, mime_type: str | None = None) -> dict | None:
    for child in drive_list_children(drive_service, parent_id):
        if child.get("name") == child_name and (mime_type is None or child.get("mimeType") == mime_type):
            return child
    return None


def drive_list_cutoff_dirs(drive_service, pdl_root_id: str, year: int, week: int) -> list[str]:
    week_folder = drive_find_named_child(
        drive_service,
        pdl_root_id,
        format_week_label(year, week),
        "application/vnd.google-apps.folder",
    )
    if not week_folder:
        return []
    return sorted(
        child.get("name") for child in drive_list_children(drive_service, week_folder["id"])
        if child.get("mimeType") == "application/vnd.google-apps.folder" and child.get("name", "").upper().startswith("CO_")
    )


def drive_find_picklist_file(drive_service, picklist_root_id: str, year: int, week: int) -> dict | None:
    target_name = f"picklist-{format_week_label(year, week)}.xlsx"
    exact = drive_find_named_child(drive_service, picklist_root_id, target_name)
    if exact:
        return exact
    for child in drive_list_children(drive_service, picklist_root_id):
        if format_week_label(year, week) in child.get("name", "") and child.get("name", "").lower().endswith(".xlsx"):
            return child
    return None


def drive_find_pdl_file(drive_service, pdl_root_id: str, year: int, week: int, cutoff: str) -> dict | None:
    week_folder = drive_find_named_child(
        drive_service,
        pdl_root_id,
        format_week_label(year, week),
        "application/vnd.google-apps.folder",
    )
    if not week_folder:
        return None
    cutoff_folder = drive_find_named_child(
        drive_service,
        week_folder["id"],
        cutoff,
        "application/vnd.google-apps.folder",
    )
    if not cutoff_folder:
        return None
    dwhtaxi_folder = drive_find_named_child(
        drive_service,
        cutoff_folder["id"],
        "DWHTAXI",
        "application/vnd.google-apps.folder",
    )
    if not dwhtaxi_folder:
        return None
    exact_name = f"PDL-{cutoff.replace('_', '')}.xlsx"
    exact = drive_find_named_child(drive_service, dwhtaxi_folder["id"], exact_name)
    if exact:
        return exact
    for child in drive_list_children(drive_service, dwhtaxi_folder["id"]):
        if child.get("name", "").startswith("PDL-") and child.get("name", "").lower().endswith(".xlsx"):
            return child
    return None


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def drive_download_excel(_drive_service, file_id: str) -> pd.DataFrame:
    request = _drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    return read_excel_from_bytes(request.execute())


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def drive_download_text(_drive_service, file_id: str) -> str:
    request = _drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    return request.execute().decode("utf-8", errors="replace")


def drive_find_latest_output_folder(drive_service, outputs_root_id: str, week: int, cutoff: str) -> dict | None:
    cutoff_label = cutoff.replace("_", "")
    prefix = f"KW{int(week):02d}-{cutoff_label}"
    active_matches = [
        child for child in drive_list_children(drive_service, outputs_root_id)
        if child.get("mimeType") == "application/vnd.google-apps.folder"
        and child.get("name", "").startswith(prefix)
        and not child.get("name", "").startswith("ARCHIVED_")
    ]
    matches = active_matches or [
        child for child in drive_list_children(drive_service, outputs_root_id)
        if child.get("mimeType") == "application/vnd.google-apps.folder"
        and child.get("name", "").startswith(f"ARCHIVED_{prefix}")
    ]
    if not matches:
        return None
    matches.sort(key=lambda item: item.get("modifiedTime", ""), reverse=True)
    return matches[0]


def drive_find_child_file(drive_service, folder_id: str, file_name: str) -> dict | None:
    return drive_find_named_child(drive_service, folder_id, file_name)


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def drive_download_bytes(_drive_service, file_id: str) -> bytes:
    request = _drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    return request.execute()


@st.cache_data(show_spinner=False, ttl=CACHE_TTL_SECONDS)
def drive_get_item_metadata(_drive_service, item_id: str) -> dict | None:
    if _drive_service is None or not item_id:
        return None
    return _drive_service.files().get(
        fileId=item_id,
        supportsAllDrives=True,
        fields="id,name,mimeType,driveId,capabilities(canAddChildren,canEdit)",
    ).execute()


def describe_drive_upload_error(exc: Exception) -> str:
    if HttpError is not None and isinstance(exc, HttpError):
        reason = ""
        try:
            error_payload = exc.error_details[0] if getattr(exc, "error_details", None) else {}
            reason = str(error_payload.get("reason", "")).strip()
        except Exception:
            reason = ""
        if reason == "storageQuotaExceeded":
            return (
                "Der Zielordner liegt offenbar nicht auf einem Shared Drive. "
                "Der Service Account kann dort keine neuen Dateien anlegen. "
                "Nutze einen Shared-Drive-Ordner oder lege Tracking.csv bzw. RESET.csv einmal manuell im Zielordner an, "
                "damit die App sie danach nur noch aktualisieren muss."
            )
    return str(exc)


def drive_upsert_text_file(drive_service, folder_id: str, file_name: str, content: str, mime_type: str = "text/csv") -> dict:
    if drive_service is None:
        raise ValueError("Google-Drive-Service ist nicht verfuegbar.")
    if MediaInMemoryUpload is None:
        raise ImportError("Google-Drive-Upload ist nicht verfuegbar, weil googleapiclient.http fehlt.")

    media = MediaInMemoryUpload(content.encode("utf-8"), mimetype=mime_type, resumable=False)
    existing_file = drive_find_named_child(drive_service, folder_id, file_name)
    if existing_file is not None:
        return drive_service.files().update(
            fileId=existing_file["id"],
            media_body=media,
            supportsAllDrives=True,
            fields="id,name,webViewLink",
        ).execute()

    return drive_service.files().create(
        body={"name": file_name, "parents": [folder_id]},
        media_body=media,
        supportsAllDrives=True,
        fields="id,name,webViewLink",
    ).execute()


def render_drive_export_controls(
    section_key: str,
    section_label: str,
    drive_service,
    folder_setting_key: str,
    tracking_csv: str,
    reset_csv: str | None,
):
    st.markdown("**Google-Drive-Export**")
    folder_id = st.text_input(
        f"{section_label} Zielordner-ID",
        value=str(get_app_setting(folder_setting_key, DEFAULT_SCALE_DRIVE_FOLDER_ID)),
        key=f"{section_key}_drive_export_folder_id",
        placeholder="Google-Drive-Ordner-ID fuer Tracking/RESET",
    ).strip()
    set_app_settings({folder_setting_key: folder_id})
    st.caption("Ablauf: zuerst Tracking.csv hochladen. Sobald sie im Zielordner verarbeitet und entfernt wurde, kannst du bei Bedarf RESET.csv separat hochladen.")

    if drive_service is None:
        st.info("Google Drive ist nicht beschreibbar verfuegbar. Bitte Service-Account-Zugriff und installierte Google-API-Pakete pruefen.")
        return
    if not folder_id:
        st.info("Bitte zuerst eine Google-Drive-Zielordner-ID eintragen.")
        return

    try:
        folder_meta = drive_get_item_metadata(drive_service, folder_id)
    except Exception as exc:
        st.error(f"Google-Drive-Zielordner konnte nicht geprueft werden: {describe_drive_upload_error(exc)}")
        return

    if not folder_meta:
        st.error("Google-Drive-Zielordner konnte nicht gelesen werden.")
        return

    if not folder_meta.get("driveId"):
        st.warning(
            "Der Zielordner liegt nicht auf einem Shared Drive. Neue Dateien koennen dort mit dem Service Account scheitern. "
            "Wenn du diesen Ordner weiter nutzt, lege Tracking.csv und RESET.csv einmal manuell an, damit die App sie spaeter nur aktualisiert."
        )

    upload_col1, upload_col2 = st.columns(2)
    with upload_col1:
        if st.button("Tracking.csv nach Google Drive hochladen", key=f"{section_key}_upload_tracking", use_container_width=True):
            try:
                uploaded_file = drive_upsert_text_file(drive_service, folder_id, "Tracking.csv", tracking_csv)
                st.success(f"Tracking.csv in Google Drive hochgeladen: {uploaded_file.get('name', 'Tracking.csv')}")
            except Exception as exc:
                st.error(f"Tracking.csv konnte nicht nach Google Drive hochgeladen werden: {describe_drive_upload_error(exc)}")

    with upload_col2:
        if reset_csv is None:
            st.caption("RESET.csv ist fuer den aktuellen Lauf nicht vorhanden.")
        elif st.button("RESET.csv nach Google Drive hochladen", key=f"{section_key}_upload_reset", use_container_width=True):
            try:
                uploaded_file = drive_upsert_text_file(drive_service, folder_id, "RESET.csv", reset_csv)
                st.success(f"RESET.csv in Google Drive hochgeladen: {uploaded_file.get('name', 'RESET.csv')}")
            except Exception as exc:
                st.error(f"RESET.csv konnte nicht nach Google Drive hochgeladen werden: {describe_drive_upload_error(exc)}")


def render_scale_sync_controls(
    section_key: str,
    section_label: str,
    sync_export_dir: Path,
    sync_root_setting_key: str = "scale_sync_root",
    delete_template_setting_key: str = "scale_delete_template_path",
):
    st.markdown("**Scale-Server / Google-Drive-Sync**")
    sync_root = Path(str(get_app_setting(sync_root_setting_key, str(DEFAULT_SCALE_SYNC_ROOT))))
    delete_template_path = Path(str(get_app_setting(delete_template_setting_key, str(DEFAULT_SCALE_RESET_TEMPLATE))))
    set_app_settings({
        sync_root_setting_key: str(sync_root),
        delete_template_setting_key: str(delete_template_path),
    })
    st.caption(f"Standardziel fuer {section_label}: {sync_root}")
    st.caption(f"Aktueller Exportordner fuer die Waage: {sync_export_dir}")
    st.caption(f"DELETE.DEL Vorlage: {delete_template_path}")

    if st.button("DELETE.DEL in diesen Exportordner legen", key=f"{section_key}_deploy_delete_del", use_container_width=True):
        try:
            deployed_path = deploy_scale_reset_file(sync_export_dir, delete_template_path)
            st.success(f"DELETE.DEL bereitgestellt: {deployed_path}")
        except Exception as exc:
            st.error(f"DELETE.DEL konnte nicht bereitgestellt werden: {exc}")


def normalize_cutoff_label(cutoff: str) -> str:
    return str(cutoff or "").replace("_", "").strip().upper()


def parse_report_count(value) -> int:
    text = clean_sheet_cell(value)
    if not text:
        return 0
    compact = text.replace(" ", "")
    if re.fullmatch(r"-?\d+[\.,]\d{3}", compact):
        return int(compact.replace(".", "").replace(",", ""))
    try:
        return int(round(float(compact.replace(",", "."))))
    except ValueError:
        digits = re.sub(r"[^\d-]", "", compact)
        if not digits or digits == "-":
            return 0
        try:
            return int(digits)
        except ValueError:
            return 0


def normalize_factor_fulfillment_plan_df(raw_df: pd.DataFrame, region: str, week_label: str) -> pd.DataFrame:
    if raw_df is None or raw_df.empty:
        return pd.DataFrame()

    column_map = {
        "DE": {
            "slot_1": "Friday",
            "slot_2": "Sunday",
            "total": "Summe Soll",
            "slot_1_label": "Friday",
            "slot_2_label": "Sunday",
        },
        "Nordics": {
            "slot_1": "TK",
            "slot_2": "TV",
            "total": "Summe Soll [Meals]",
            "slot_1_label": "TK",
            "slot_2_label": "TV",
        },
    }.get(region)

    if column_map is None:
        return pd.DataFrame()

    required_columns = [
        "Woche",
        "Rezept",
        "Maitre Code",
        "SKU",
        "Artikel",
        column_map["slot_1"],
        column_map["slot_2"],
        column_map["total"],
    ]
    if not all(column in raw_df.columns for column in required_columns):
        return pd.DataFrame()

    work_df = raw_df[required_columns].copy()
    for column in ["Woche", "Rezept", "Maitre Code", "SKU", "Artikel"]:
        work_df[column] = work_df[column].apply(clean_sheet_cell)

    work_df = work_df[work_df["Woche"] == week_label].copy()
    work_df = work_df[work_df["Rezept"].str.fullmatch(r"\d+")]
    if work_df.empty:
        return pd.DataFrame()

    work_df[column_map["slot_1"]] = work_df[column_map["slot_1"]].apply(parse_report_count)
    work_df[column_map["slot_2"]] = work_df[column_map["slot_2"]].apply(parse_report_count)
    work_df[column_map["total"]] = work_df[column_map["total"]].apply(parse_report_count)
    work_df["Rezept"] = work_df["Rezept"].astype(int)

    renamed_df = work_df.rename(columns={
        "Woche": "Week",
        "Rezept": "Recipe",
        "Maitre Code": "Maitre Code",
        "SKU": "SKU",
        "Artikel": "Artikel",
        column_map["slot_1"]: column_map["slot_1_label"],
        column_map["slot_2"]: column_map["slot_2_label"],
        column_map["total"]: "Total Meals",
    })
    renamed_df.insert(0, "Region", region)
    return renamed_df.sort_values(["Region", "Recipe"], kind="stable").reset_index(drop=True)


def load_factor_fulfillment_picklist_by_week(sheets_service, spreadsheet_id: str, year: int, week: int) -> pd.DataFrame:
    if sheets_service is None:
        return pd.DataFrame()

    week_label = format_week_label(year, week)
    tab_configs = [
        ("DE", "Produktionsvorbereitung_DE", ["Woche", "Rezept", "Maitre Code", "SKU", "Artikel", "Friday", "Sunday", "Summe Soll"]),
        ("Nordics", "Produktionsvorbereitung_Nordics", ["Woche", "Rezept", "Maitre Code", "SKU", "Artikel", "TK", "TV", "Summe Soll [Meals]"]),
    ]

    frames = []
    for region, tab_name, required_headers in tab_configs:
        raw_df = load_sheet_dataframe_by_header(
            sheets_service,
            spreadsheet_id,
            tab_name,
            required_headers,
            end_col="AZ",
            end_row=400,
        )
        normalized_df = normalize_factor_fulfillment_plan_df(raw_df, region, week_label)
        if not normalized_df.empty:
            frames.append(normalized_df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def load_factor_meal_code_mapping(sheets_service, spreadsheet_id: str, week: int) -> pd.DataFrame:
    if sheets_service is None:
        return pd.DataFrame(columns=["Maitre Code", "Meal Code", "Meal Code Market", "Meal Code Week"])

    mapping_df = load_sheet_dataframe_by_header(
        sheets_service,
        spreadsheet_id,
        "Maitre Inputs DE/NO_Stamm",
        ["Market", "week", "Recipe Code", "Maitre Code"],
        end_col="Q",
        end_row=5000,
    )
    if mapping_df.empty:
        return pd.DataFrame(columns=["Maitre Code", "Meal Code", "Meal Code Market", "Meal Code Week"])

    mapping_df["Maitre Code"] = mapping_df["Maitre Code"].apply(clean_sheet_cell)
    mapping_df["Recipe Code"] = mapping_df["Recipe Code"].apply(clean_sheet_cell)
    mapping_df["week"] = mapping_df["week"].apply(clean_sheet_cell)
    mapping_df["Market"] = mapping_df["Market"].apply(clean_sheet_cell)
    mapping_df = mapping_df[(mapping_df["Maitre Code"] != "") & (mapping_df["Recipe Code"] != "")].copy()
    if mapping_df.empty:
        return pd.DataFrame(columns=["Maitre Code", "Meal Code", "Meal Code Market", "Meal Code Week"])

    week_token = f"W{int(week):02d}"
    mapping_df["_week_number"] = pd.to_numeric(mapping_df["week"].str.extract(r"(\d+)", expand=False), errors="coerce").fillna(0).astype(int)
    mapping_df["_selected_week_match"] = (mapping_df["week"] == week_token).astype(int)
    mapping_df = mapping_df.sort_values(["_selected_week_match", "_week_number"], ascending=[False, False], kind="stable")
    return mapping_df.rename(columns={
        "Recipe Code": "Meal Code",
        "Market": "Meal Code Market",
        "week": "Meal Code Week",
    })[["Maitre Code", "Meal Code", "Meal Code Market", "Meal Code Week"]]


def factor_region_to_meal_code_source_market(region: str) -> str:
    normalized_region = clean_sheet_cell(region).upper()
    if normalized_region == "DE":
        return "DE"
    if normalized_region == "NORDICS":
        return "NORDICS"
    return normalized_region


def factor_region_to_display_market(region: str) -> str:
    normalized_region = clean_sheet_cell(region).upper()
    if normalized_region == "DE":
        return "DE"
    if normalized_region == "NORDICS":
        return "NORDICS"
    return clean_sheet_cell(region)


def factor_region_to_tz_marker(region: str) -> str:
    normalized_region = clean_sheet_cell(region).upper()
    return "TZ" if normalized_region == "DE" else ""


def apply_factor_meal_code_mapping(fulfillment_df: pd.DataFrame | None, meal_code_mapping_df: pd.DataFrame | None) -> pd.DataFrame:
    if fulfillment_df is None or fulfillment_df.empty:
        return pd.DataFrame()

    result_df = fulfillment_df.copy()
    result_df["Maitre Code"] = result_df["Maitre Code"].apply(clean_sheet_cell)
    result_df["Meal Code Market"] = result_df["Region"].apply(factor_region_to_display_market) if "Region" in result_df.columns else ""
    result_df["TZ"] = result_df["Region"].apply(factor_region_to_tz_marker) if "Region" in result_df.columns else ""
    if meal_code_mapping_df is None or meal_code_mapping_df.empty:
        result_df["Meal Code"] = ""
        result_df["Meal Code Source Market"] = ""
        result_df["Meal Code Week"] = ""
        return result_df

    mapping_work_df = meal_code_mapping_df.copy()
    mapping_work_df["Maitre Code"] = mapping_work_df["Maitre Code"].apply(clean_sheet_cell)
    mapping_work_df["Meal Code Market"] = mapping_work_df["Meal Code Market"].apply(clean_sheet_cell)

    exact_lookup = {}
    fallback_lookup = {}
    for _, row in mapping_work_df.iterrows():
        maitre_code = row.get("Maitre Code", "")
        source_market = clean_sheet_cell(row.get("Meal Code Market", ""))
        if not maitre_code:
            continue
        exact_lookup[(maitre_code, source_market)] = row
        fallback_lookup.setdefault(maitre_code, row)

    meal_codes = []
    meal_code_source_markets = []
    meal_code_weeks = []
    for _, row in result_df.iterrows():
        maitre_code = clean_sheet_cell(row.get("Maitre Code", ""))
        preferred_market = factor_region_to_meal_code_source_market(row.get("Region", ""))
        mapped_row = exact_lookup.get((maitre_code, preferred_market))
        if mapped_row is None:
            mapped_row = fallback_lookup.get(maitre_code)
        if mapped_row is None:
            meal_codes.append("")
            meal_code_source_markets.append("")
            meal_code_weeks.append("")
            continue
        meal_codes.append(clean_sheet_cell(mapped_row.get("Meal Code", "")))
        meal_code_source_markets.append(clean_sheet_cell(mapped_row.get("Meal Code Market", "")))
        meal_code_weeks.append(clean_sheet_cell(mapped_row.get("Meal Code Week", "")))

    result_df["Meal Code"] = meal_codes
    result_df["Meal Code Source Market"] = meal_code_source_markets
    result_df["Meal Code Week"] = meal_code_weeks
    return add_factor_meal_code_transition_columns(result_df)


def derive_factor_meal_code_transition_fields(meal_code: str) -> tuple[str, str]:
    normalized_code = clean_sheet_cell(meal_code)
    if not normalized_code:
        return "", ""
    upper_code = normalized_code.upper()
    if upper_code.startswith("FE"):
        return normalized_code, f"FV{normalized_code[2:]}"
    if upper_code.startswith("FV"):
        return f"FE{normalized_code[2:]}", normalized_code
    return normalized_code, normalized_code


def add_factor_meal_code_transition_columns(df: pd.DataFrame | None) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame() if df is None else df

    result_df = df.copy()
    meal_code_series = result_df.get("Meal Code", pd.Series([""] * len(result_df)))
    transition_values = meal_code_series.apply(derive_factor_meal_code_transition_fields)
    result_df["Meal Code FE"] = transition_values.apply(lambda value: value[0])
    result_df["Meal Code FV"] = transition_values.apply(lambda value: value[1])
    return result_df


def extract_recipe_number(value) -> int | None:
    match = re.search(r"(\d+)", str(value))
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def build_factor_weekly_recipe_reference(fulfillment_df: pd.DataFrame) -> pd.DataFrame:
    if fulfillment_df is None or fulfillment_df.empty:
        return pd.DataFrame(columns=["Recipe", "Regions", "Planned Meals"])

    grouped = (
        fulfillment_df.groupby("Recipe", as_index=False)
        .agg(
            Planned_Meals=("Total Meals", "sum"),
            Regions=("Region", lambda values: ", ".join(unique_preserve_order([str(value) for value in values]))),
        )
        .sort_values("Recipe", kind="stable")
        .reset_index(drop=True)
    )
    return grouped.rename(columns={"Planned_Meals": "Planned Meals"})


def build_factor_pdl_recipe_mix_summary(factor_df: pd.DataFrame | None) -> pd.DataFrame:
    if factor_df is None or factor_df.empty or "meal_swap" not in factor_df.columns:
        return pd.DataFrame(columns=["Recipe", "PDL Meals", "PDL Boxes"])

    meal_counts: dict[int, int] = {}
    box_counts: dict[int, int] = {}
    for _, row in factor_df.iterrows():
        seen_in_box = set()
        for token in str(row.get("meal_swap", "")).split():
            recipe_text = token.strip()
            if not recipe_text:
                continue
            count = 1
            if ":" in recipe_text:
                recipe_part, count_text = recipe_text.split(":", 1)
                recipe_text = recipe_part
                try:
                    count = int(count_text)
                except ValueError:
                    count = 1
            if not recipe_text.isdigit():
                continue
            recipe_number = int(recipe_text)
            meal_counts[recipe_number] = meal_counts.get(recipe_number, 0) + count
            seen_in_box.add(recipe_number)
        for recipe_number in seen_in_box:
            box_counts[recipe_number] = box_counts.get(recipe_number, 0) + 1

    rows = [
        {
            "Recipe": recipe_number,
            "PDL Meals": meal_counts.get(recipe_number, 0),
            "PDL Boxes": box_counts.get(recipe_number, 0),
        }
        for recipe_number in sorted(set(meal_counts) | set(box_counts))
    ]
    return pd.DataFrame(rows)


def build_factor_plan_vs_pdl_report(fulfillment_df: pd.DataFrame | None, factor_df: pd.DataFrame | None) -> pd.DataFrame:
    planned_reference_df = build_factor_weekly_recipe_reference(fulfillment_df)
    pdl_mix_df = build_factor_pdl_recipe_mix_summary(factor_df)

    if planned_reference_df.empty and pdl_mix_df.empty:
        return pd.DataFrame()
    if planned_reference_df.empty:
        report_df = pdl_mix_df.copy()
        report_df["Regions"] = ""
        report_df["Planned Meals"] = 0
    elif pdl_mix_df.empty:
        report_df = planned_reference_df.copy()
        report_df["PDL Meals"] = 0
        report_df["PDL Boxes"] = 0
    else:
        report_df = planned_reference_df.merge(pdl_mix_df, on="Recipe", how="outer")

    report_df["Regions"] = report_df.get("Regions", "").fillna("")
    for column in ["Planned Meals", "PDL Meals", "PDL Boxes"]:
        report_df[column] = pd.to_numeric(report_df.get(column, 0), errors="coerce").fillna(0).astype(int)
    report_df["Delta Meals"] = report_df["PDL Meals"] - report_df["Planned Meals"]
    report_df["Status"] = report_df["Delta Meals"].apply(
        lambda value: "gleich" if value == 0 else ("mehr in PDL" if value > 0 else "mehr im Plan")
    )
    return report_df.sort_values(["Recipe"], kind="stable").reset_index(drop=True)


def join_named_items(items: list[str], empty_text: str = "nicht gefunden") -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    return ", ".join(cleaned) if cleaned else empty_text


def pick_bedarf_file_name(file_names: list[str], cutoff: str | None = None) -> str | None:
    if not file_names:
        return None
    normalized = {name.lower(): name for name in file_names}
    cutoff_label = normalize_cutoff_label(cutoff)
    preferred_names = []
    if cutoff_label:
        preferred_names.extend([
            f"Berechnung_Ergebnis{cutoff_label}.xlsx",
            f"Berechnung_Ergebnis-{cutoff_label}.xlsx",
        ])
    preferred_names.append("Berechnung_Ergebnis.xlsx")
    for preferred in preferred_names:
        match = normalized.get(preferred.lower())
        if match:
            return match
    if cutoff_label:
        cutoff_matches = sorted(name for name in file_names if cutoff_label.lower() in name.lower())
        if cutoff_matches:
            return cutoff_matches[0]
    return sorted(file_names)[0]


def drive_find_bedarf_workbook(drive_service, outputs_root_id: str, year: int, week: int, cutoff: str | None = None) -> dict | None:
    year_folder = drive_find_named_child(
        drive_service,
        outputs_root_id,
        str(year),
        "application/vnd.google-apps.folder",
    )
    if not year_folder:
        return None
    week_folder = drive_find_named_child(
        drive_service,
        year_folder["id"],
        f"KW-{int(week):02d}",
        "application/vnd.google-apps.folder",
    )
    if not week_folder:
        return None
    children = drive_list_children(drive_service, week_folder["id"])
    files = [child for child in children if child.get("name", "").lower().endswith(".xlsx")]
    selected_name = pick_bedarf_file_name([child["name"] for child in files], cutoff)
    if selected_name is None:
        return None
    return next((child for child in files if child.get("name") == selected_name), None)


def find_bedarf_workbook(base_dir: Path, year: int, week: int, cutoff: str | None = None) -> Path | None:
    week_dir = base_dir / str(year) / f"KW-{int(week):02d}"
    if not week_dir.exists():
        return None
    workbook_paths = [path for path in week_dir.glob("*.xlsx") if not path.name.startswith("~$")]
    selected_name = pick_bedarf_file_name([path.name for path in workbook_paths], cutoff)
    if selected_name is None:
        return None
    for path in workbook_paths:
        if path.name == selected_name:
            return path
    return None


def first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def make_bedarf_sheet_name(source_label: str, cutoff: str | None = None) -> str:
    cutoff_label = normalize_cutoff_label(cutoff)
    if cutoff_label:
        return f"{cutoff_label}.csv"[:31]
    source_text = Path(str(source_label)).stem if source_label else "Bedarf"
    source_text = source_text.replace("PDL-", "").replace("PDL_", "")
    return source_text[:31] or "Bedarf"


def compute_bedarf_counts(pdl_subset: pd.DataFrame, picklist_df: pd.DataFrame, selected_bag_sizes: list[int]) -> tuple[pd.DataFrame, dict[str, int], dict[str, int]]:
    recipe_col = first_existing_column(picklist_df, ["Recipe"])
    ingredient_col = first_existing_column(picklist_df, ["Ingredient"])
    quantity_col = first_existing_column(picklist_df, ["Quantity"])
    meal_swap_col = first_existing_column(pdl_subset, ["meal_swap", "Meal Swap"])
    bag_size_col = first_existing_column(pdl_subset, ["bag_size", "BagSize"])
    pouch_col = first_existing_column(pdl_subset, ["cool_pouch_name", "Cool Pouch Name"])
    box_col = first_existing_column(pdl_subset, ["box_size", "Box Size", "Box Size Official"])

    if not all([recipe_col, ingredient_col, quantity_col, meal_swap_col, bag_size_col, pouch_col, box_col]):
        return pd.DataFrame(columns=["Ingredient", "Insgesamte Menge"]), {}, {}

    picklist_core = picklist_df[[recipe_col, ingredient_col, quantity_col]].copy()
    picklist_core.columns = ["Recipe", "Ingredient", "Quantity"]
    picklist_core["Quantity"] = pd.to_numeric(picklist_core["Quantity"], errors="coerce").fillna(0)

    pouch_counts: dict[str, int] = {}
    box_counts: dict[str, int] = {}
    csv_results = []

    for bag_size in selected_bag_sizes:
        bag_subset = pdl_subset[pd.to_numeric(pdl_subset[bag_size_col], errors="coerce") == bag_size].copy()
        if bag_subset.empty:
            continue
        recipe_counts: dict[str, int] = {}
        for _, row in bag_subset.iterrows():
            for value in str(row.get(meal_swap_col, "")).split():
                token = value.strip()
                if not token or token.lower() == "nan":
                    continue
                recipe_key = token
                amount = 1
                if ":" in token:
                    recipe_key, amount_str = token.split(":", 1)
                    try:
                        amount = int(amount_str)
                    except ValueError:
                        continue
                recipe_counts[recipe_key] = recipe_counts.get(recipe_key, 0) + amount

            pouch_name = str(row.get(pouch_col, "")).strip()
            if pouch_name and pouch_name.lower() != "nan" and pouch_name.lower() != "null":
                pouch_counts[pouch_name] = pouch_counts.get(pouch_name, 0) + 1

            box_name = str(row.get(box_col, "")).strip()
            if box_name and box_name.lower() != "nan" and box_name.lower() != "null":
                box_counts[box_name] = box_counts.get(box_name, 0) + 1

        if not recipe_counts:
            continue
        tuple_df = pd.DataFrame(
            [(f"{recipe}r-{bag_size}p", amount) for recipe, amount in recipe_counts.items()],
            columns=["Recipe", "Amounts"],
        )
        merged_df = pd.merge(tuple_df, picklist_core, on="Recipe", how="inner")
        if merged_df.empty:
            continue
        merged_df["Insgesamte Menge"] = merged_df["Amounts"] * merged_df["Quantity"]
        csv_results.append(merged_df[["Ingredient", "Insgesamte Menge"]])

    if not csv_results:
        return pd.DataFrame(columns=["Ingredient", "Insgesamte Menge"]), pouch_counts, box_counts

    combined_df = pd.concat(csv_results, ignore_index=True)
    ingredient_df = combined_df.groupby("Ingredient", as_index=False)["Insgesamte Menge"].sum()
    ingredient_df["Insgesamte Menge"] = ingredient_df["Insgesamte Menge"].round(3)
    ingredient_df = ingredient_df.sort_values("Ingredient").reset_index(drop=True)
    return ingredient_df, pouch_counts, box_counts


def format_bedarf_sheet(
    ingredient_df: pd.DataFrame,
    pouch_counts: dict[str, int],
    box_counts: dict[str, int],
    all_pouches: list[str],
    all_boxes: list[str],
    pouch_label: str,
    box_label: str,
) -> pd.DataFrame:
    output_df = ingredient_df.copy()
    output_df["Leer 1"] = ""
    output_df["Leer 2"] = ""
    for pouch in all_pouches:
        output_df[pouch] = ""
    for box in all_boxes:
        output_df[box] = ""

    pouch_row = {"Ingredient": pouch_label, "Insgesamte Menge": "", "Leer 1": "", "Leer 2": ""}
    for pouch in all_pouches:
        pouch_row[pouch] = pouch_counts.get(pouch, "")
    for box in all_boxes:
        pouch_row[box] = ""

    box_row = {"Ingredient": box_label, "Insgesamte Menge": "", "Leer 1": "", "Leer 2": ""}
    for pouch in all_pouches:
        box_row[pouch] = ""
    for box in all_boxes:
        box_row[box] = box_counts.get(box, "")

    header_rows = pd.DataFrame([pouch_row, box_row])
    return pd.concat([header_rows, output_df], ignore_index=True)


def build_bedarf_workbook_bytes(
    sheet_data: list[tuple[str, pd.DataFrame]],
    total_df: pd.DataFrame,
    total_pouch_counts: dict[str, int],
    total_box_counts: dict[str, int],
    all_pouches: list[str],
    all_boxes: list[str],
) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        for sheet_name, sheet_df in sheet_data:
            safe_name = sheet_name[:31] if sheet_name else "Bedarf"
            sheet_df.to_excel(writer, sheet_name=safe_name, index=False)
        total_sheet = format_bedarf_sheet(
            total_df,
            total_pouch_counts,
            total_box_counts,
            all_pouches,
            all_boxes,
            "Gesamt-Pouch-Zählung",
            "Gesamt-Box-Zählung",
        )
        total_sheet.to_excel(writer, sheet_name="Gesamt", index=False)
    output.seek(0)
    return output.getvalue()


def build_tracking_correction_map(csv_text: str) -> dict[str, str]:
    correction_map = {}
    for line in csv_text.splitlines()[1:]:
        fields = line.split(";")
        if len(fields) > 11 and fields[0]:
            correction_map[fields[0]] = fields[11] if fields[11] else "0,000"
    return correction_map


def normalize_correction_value(value) -> str:
    try:
        return f"{float(str(value).replace(',', '.')):.3f}".replace(".", ",")
    except (TypeError, ValueError):
        return "0,000"


def build_packaging_weight_map(pkg_df: pd.DataFrame | None) -> dict[str, float]:
    if pkg_df is None or pkg_df.empty:
        return {}
    weight_map = {}
    for _, row in pkg_df.iterrows():
        name = str(row.iloc[0]).strip()
        if not name or name == "nan":
            continue
        try:
            weight_map[name] = float(str(row.iloc[1]).replace(",", "."))
        except (TypeError, ValueError):
            continue
    return weight_map


def build_sku_weight_lookup(sku_df: pd.DataFrame | None) -> tuple[dict[str, float], dict[str, float]]:
    if sku_df is None or sku_df.empty:
        return {}, {}
    sku_col = "Culinary SKU Code*" if "Culinary SKU Code*" in sku_df.columns else sku_df.columns[0]
    name_col = "SKU Name" if "SKU Name" in sku_df.columns else sku_df.columns[1]
    weight_col = "Pack weight new (g)" if "Pack weight new (g)" in sku_df.columns else sku_df.columns[2]
    by_code = {}
    by_name = {}
    for _, row in sku_df.iterrows():
        code = str(row[sku_col]).strip()
        name = str(row[name_col]).strip()
        try:
            weight = float(str(row[weight_col]).replace(",", "."))
        except (TypeError, ValueError):
            continue
        if code:
            by_code[code] = weight
        if name:
            by_name[name.lower()] = weight
    return by_code, by_name


def extract_sku_code(text: str) -> str | None:
    match = re.search(r"([A-Z]{3}-\d{2}-[A-Z0-9]+-\d)", str(text))
    return match.group(1) if match else None


def extract_weight_from_text(text: str) -> float | None:
    matches = re.findall(r"(\d+(?:[\.,]\d+)?)\s*g\b", str(text), flags=re.IGNORECASE)
    if not matches:
        return None
    try:
        return float(matches[-1].replace(",", "."))
    except ValueError:
        return None


def resolve_item_weight(text: str, sku_code: str | None, sku_weights_by_code: dict[str, float], sku_weights_by_name: dict[str, float]) -> float | None:
    if sku_code and sku_code in sku_weights_by_code:
        return sku_weights_by_code[sku_code]
    parsed_code = extract_sku_code(text)
    if parsed_code and parsed_code in sku_weights_by_code:
        return sku_weights_by_code[parsed_code]
    normalized = str(text).strip().lower()
    if normalized in sku_weights_by_name:
        return sku_weights_by_name[normalized]
    return extract_weight_from_text(text)


def clean_sheet_cell(value) -> str:
    text = str(value).strip()
    if text.lower() in {"", "nan", "#n/a", "none", "null"}:
        return ""
    return text


def build_substitute_slots_from_row(row: pd.Series) -> list[dict]:
    slot_definitions = [
        (1, "Substitute 1", None, None),
        (2, "Substitute 2", "Changed allergen?", "SKU Code_2"),
        (3, "Substitute 3", "Changed allergen?_2", "SKU Code_3"),
        (4, "Substitute 4", "Changed allergen?_3", "SKU Code_4"),
    ]
    slots = []
    for slot_number, text_column, allergen_column, sku_column in slot_definitions:
        slots.append({
            "slot": slot_number,
            "text": clean_sheet_cell(row.get(text_column, "")),
            "changed_allergen": clean_sheet_cell(row.get(allergen_column, "")) if allergen_column else "",
            "sku_code": clean_sheet_cell(row.get(sku_column, "")) if sku_column else "",
            "source": text_column,
        })
    return slots


def build_missing_substitute_slots_from_row(row: pd.Series) -> list[dict]:
    texts = [clean_sheet_cell(row.get(f"Substitute {index}", "")) for index in range(1, 4)]
    merged_suggestions = clean_sheet_cell(row.get("merged_suggested_sku_names", ""))
    if merged_suggestions and merged_suggestions.lower() not in {"no suggestions", "not found"}:
        texts.extend(clean_sheet_cell(value) for value in merged_suggestions.split("|"))

    unique_texts = unique_preserve_order([text for text in texts if text])[:4]
    while len(unique_texts) < 4:
        unique_texts.append("")

    slots = []
    for index, text in enumerate(unique_texts, start=1):
        allergen_column = f"Traffic light Sub {index}" if index > 1 else "Traffic light Sub1"
        slots.append({
            "slot": index,
            "text": text,
            "changed_allergen": clean_sheet_cell(row.get(allergen_column, "")),
            "sku_code": "",
            "source": f"Substitute {index}",
        })
    return slots


def build_substitute_options_from_row(row: pd.Series) -> list[dict]:
    options = []
    for slot in build_substitute_slots_from_row(row):
        if slot.get("text"):
            options.append(slot)
    deduped = []
    seen = set()
    for option in options:
        key = option.get("text", "")
        if key and key not in seen:
            seen.add(key)
            deduped.append(option)
    return deduped


def build_substitute_search_catalog(sub_master_df: pd.DataFrame, no_sub_df: pd.DataFrame) -> tuple[list[str], dict[str, str]]:
    labels = []
    label_to_query = {}

    def add_entry(name: str, sku_code: str, packaging_type: str = ""):
        parts = [part for part in [name, sku_code, packaging_type] if str(part).strip()]
        if not parts:
            return
        label = " | ".join(parts)
        if label in label_to_query:
            return
        label_to_query[label] = sku_code or name
        labels.append(label)

    if not sub_master_df.empty:
        for _, row in sub_master_df.iterrows():
            add_entry(
                clean_sheet_cell(row.get("Ingredient", "")),
                clean_sheet_cell(row.get("SKU Code", "")),
                clean_sheet_cell(row.get("Packaging Type", "")),
            )

    if not no_sub_df.empty:
        for _, row in no_sub_df.iterrows():
            add_entry(
                clean_sheet_cell(row.get("culinary_sku_name", "")),
                clean_sheet_cell(row.get("culinary_sku_code", "")),
                "",
            )

    return labels, label_to_query


def find_substitute_matches(query: str, sub_master_df: pd.DataFrame, no_sub_df: pd.DataFrame) -> list[dict]:
    query = str(query).strip()
    if not query:
        return []
    query_lower = query.lower()
    matches = []

    if not sub_master_df.empty:
        mask = (
            sub_master_df.get("SKU Code", pd.Series(dtype=str)).astype(str).str.lower().eq(query_lower)
            | sub_master_df.get("Ingredient", pd.Series(dtype=str)).astype(str).str.lower().str.contains(query_lower, na=False)
        )
        for _, row in sub_master_df[mask].iterrows():
            matches.append({
                "source_tab": "SKU Sub List",
                "original_sku": str(row.get("SKU Code", "")).strip(),
                "original_name": str(row.get("Ingredient", "")).strip(),
                "packaging_type": str(row.get("Packaging Type", "")).strip(),
                "quality_comment": str(row.get("[WIP] QUALITY COMMENT", "")).strip(),
                "slots": build_substitute_slots_from_row(row),
                "options": build_substitute_options_from_row(row),
            })

    if not no_sub_df.empty:
        mask = (
            no_sub_df.get("culinary_sku_code", pd.Series(dtype=str)).astype(str).str.lower().eq(query_lower)
            | no_sub_df.get("culinary_sku_name", pd.Series(dtype=str)).astype(str).str.lower().str.contains(query_lower, na=False)
        )
        for _, row in no_sub_df[mask].iterrows():
            slots = build_missing_substitute_slots_from_row(row)
            options = [slot for slot in slots if slot.get("text")]
            matches.append({
                "source_tab": "SKUs eigentlich ohne Substitut",
                "original_sku": str(row.get("culinary_sku_code", "")).strip(),
                "original_name": str(row.get("culinary_sku_name", "")).strip(),
                "packaging_type": "",
                "quality_comment": str(row.get("Regular status / traffic light", "")).strip(),
                "slots": slots,
                "options": options,
            })

    return matches


def evaluate_substitute_option(option: dict, original_weight: float | None, sku_weights_by_code: dict[str, float], sku_weights_by_name: dict[str, float]) -> dict:
    option_text = option.get("text", "")
    if not option_text:
        return {
            "text": "",
            "changed_allergen": option.get("changed_allergen", ""),
            "weight": None,
            "delta": None,
            "recommendation": "",
        }

    option_sku = option.get("sku_code") or extract_sku_code(option_text)
    option_weight = resolve_item_weight(option_text, option_sku, sku_weights_by_code, sku_weights_by_name)
    delta = None
    recommendation = "prüfen"
    if original_weight is not None and option_weight is not None:
        delta = round(option_weight - original_weight, 1)
        if delta > 0:
            recommendation = "schwerer -> RESET/Korrektur prüfen"
        elif delta < 0:
            recommendation = "leichter -> meist unkritisch, trotzdem prüfen"
        else:
            recommendation = "gleich schwer"
    elif "No suitable substitute" in option_text or "keine" in option_text.lower():
        recommendation = "kompensieren / manuell entscheiden"

    return {
        "text": option_text,
        "changed_allergen": option.get("changed_allergen", ""),
        "weight": option_weight,
        "delta": delta,
        "recommendation": recommendation,
    }


def combine_substitute_priority(priorities: list[str]) -> str:
    if "HIGH" in priorities:
        return "HIGH"
    if "MEDIUM" in priorities:
        return "MEDIUM"
    return "LOW"


def style_substitute_priority(value: str) -> str:
    priority = str(value).strip().upper()
    if priority == "HIGH":
        return "background-color: #f8d7da; color: #7a0019; font-weight: 700;"
    if priority == "MEDIUM":
        return "background-color: #fff3cd; color: #7a5a00; font-weight: 700;"
    if priority == "LOW":
        return "background-color: #d1e7dd; color: #0f5132; font-weight: 700;"
    return ""


def format_substitute_result_table(result_df: pd.DataFrame):
    display_columns = [
        "Original SKU",
        "Original Artikel",
        "Packaging Type",
        "Substitute 1",
        "Substitute 2",
        "Changed allergen? A",
        "Substitute 3",
        "Changed allergen? B",
        "Substitute 4",
        "Originalgewicht (g)",
        "Gewichtscheck",
        "Priorität",
        "Hinweis",
        "Quelle",
        "Suche",
    ]
    display_df = result_df[display_columns].copy()
    return display_df.style.map(style_substitute_priority, subset=["Priorität"])


def classify_substitute_priority(delta_g, recommendation: str) -> str:
    recommendation = str(recommendation).lower()
    if delta_g not in (None, ""):
        try:
            if float(delta_g) > 0:
                return "HIGH"
            if float(delta_g) < 0:
                return "MEDIUM"
            return "LOW"
        except (TypeError, ValueError):
            pass
    if "kompensieren" in recommendation or "manuell" in recommendation:
        return "HIGH"
    return "MEDIUM"


def classify_impact_priority(delta_kg, box_count: int) -> str:
    try:
        delta_kg = float(delta_kg)
    except (TypeError, ValueError):
        return "MEDIUM"
    if delta_kg > 0 and box_count > 0:
        return "HIGH"
    if delta_kg < 0 and box_count > 0:
        return "MEDIUM"
    return "LOW"


def meal_swap_to_recipe_keys(meal_swap: str) -> list[str]:
    parts = str(meal_swap).rsplit("-", 1)
    if len(parts) != 2:
        return []
    suffix = parts[1]
    return [f"{recipe}r-{suffix}" for recipe in parts[0].split() if recipe.isdigit()]


def compute_substitute_impacts(
    queries: list[str],
    sub_master_df: pd.DataFrame,
    no_sub_df: pd.DataFrame,
    picklist_df: pd.DataFrame,
    filtered_df: pd.DataFrame,
    sku_weights_by_code: dict[str, float],
    sku_weights_by_name: dict[str, float],
    current_reset_map: dict[str, float],
) -> pd.DataFrame:
    if picklist_df is None or picklist_df.empty or filtered_df.empty:
        return pd.DataFrame()

    impacted_rows = []
    combo_box_counts = filtered_df.groupby("batch")["box_id"].count().to_dict()
    picklist_work = picklist_df.copy()
    picklist_work["Recipe"] = picklist_work["Recipe"].astype(str)
    picklist_work["SKU"] = picklist_work["SKU"].astype(str)
    picklist_work["Quantity_num"] = pd.to_numeric(picklist_work["Quantity"], errors="coerce").fillna(1)

    unique_combos = sorted(filtered_df["batch"].astype(str).unique())
    combo_recipe_keys = {combo: meal_swap_to_recipe_keys(combo) for combo in unique_combos}

    for query in queries:
        matches = find_substitute_matches(query, sub_master_df, no_sub_df)
        for match in matches:
            original_sku = match["original_sku"]
            original_weight = resolve_item_weight(
                match["original_name"],
                original_sku,
                sku_weights_by_code,
                sku_weights_by_name,
            )
            for option in match["options"]:
                option_sku = option.get("sku_code") or extract_sku_code(option.get("text", ""))
                option_weight = resolve_item_weight(
                    option.get("text", ""),
                    option_sku,
                    sku_weights_by_code,
                    sku_weights_by_name,
                )
                if original_weight is None or option_weight is None:
                    continue
                delta_g = round(option_weight - original_weight, 1)
                if delta_g == 0:
                    continue

                for combo, recipe_keys in combo_recipe_keys.items():
                    combo_delta_g = 0.0
                    affected_recipe_keys = []
                    for recipe_key in recipe_keys:
                        mask = (picklist_work["Recipe"] == recipe_key) & (picklist_work["SKU"] == original_sku)
                        qty_sum = picklist_work.loc[mask, "Quantity_num"].sum()
                        if qty_sum > 0:
                            combo_delta_g += qty_sum * delta_g
                            affected_recipe_keys.append(f"{recipe_key} x {qty_sum:g}")
                    if combo_delta_g == 0:
                        continue

                    combo_delta_kg = round(combo_delta_g / 1000, 3)
                    current_reset = current_reset_map.get(combo)
                    new_reset = round(current_reset + combo_delta_kg, 3) if current_reset is not None else None
                    if combo_delta_kg > 0:
                        recommendation = f"schwerer -> RESET +{combo_delta_kg:.3f} kg; wenn nur Teilmenge ersetzt wird, betroffene Boxen Correction +{combo_delta_kg:.3f}"
                    else:
                        recommendation = f"leichter -> RESET {combo_delta_kg:.3f} kg; Correction nur bei Teilmengen neu prüfen"

                    impacted_rows.append({
                        "Suche": query,
                        "Original SKU": original_sku,
                        "Original Artikel": match["original_name"],
                        "Substitut": option.get("text", ""),
                        "Meal Swap": combo,
                        "Betroffene Boxen": int(combo_box_counts.get(combo, 0)),
                        "Betroffene Rezepte": ", ".join(affected_recipe_keys),
                        "Delta pro Box (kg)": combo_delta_kg,
                        "RESET aktuell (kg)": current_reset,
                        "RESET neu (kg)": new_reset,
                        "Empfehlung": recommendation,
                        "Quelle": match["source_tab"],
                    })

    return pd.DataFrame(impacted_rows)


def build_correction_signature(row: pd.Series) -> tuple:
    return (
        str(row.get("batch", "")).strip(),
        str(row.get("cool_pouch_name", "")).strip(),
        str(row.get("Number of ice packs OFFICIAL", "")).strip(),
        str(row.get("extra_ice", "")).strip(),
        str(row.get("service_type", "")).strip(),
        str(row.get("distribution_center", "")).strip(),
        str(row.get("country", "")).strip(),
    )


def build_correction_signature_map(pdl_df: pd.DataFrame, correction_map: dict[str, str]) -> dict[tuple, str]:
    signature_candidates: dict[tuple, list[str]] = {}
    for _, row in pdl_df.iterrows():
        box_id = str(row.get("box_id", "")).strip()
        correction = correction_map.get(box_id)
        if correction is None:
            continue
        signature = build_correction_signature(row)
        signature_candidates.setdefault(signature, []).append(normalize_correction_value(correction))

    resolved = {}
    for signature, values in signature_candidates.items():
        # pick the most common correction for the signature
        resolved[signature] = pd.Series(values).mode().iloc[0]
    return resolved


def compute_variant_weight_grams(row: pd.Series, packaging_weights: dict[str, float]) -> float:
    pouch_weight = packaging_weights.get(str(row.get("cool_pouch_name", "")).strip(), 0.0)
    try:
        official_ice = float(str(row.get("Number of ice packs OFFICIAL", 0)).replace(",", "."))
    except (TypeError, ValueError):
        official_ice = 0.0
    try:
        extra_ice = float(str(row.get("extra_ice", 0)).replace(",", "."))
    except (TypeError, ValueError):
        extra_ice = 0.0
    ice_weight = packaging_weights.get("Icepack", packaging_weights.get("Eispack", 506.0))
    return pouch_weight + (official_ice + extra_ice) * ice_weight


def build_batch_heaviest_weight_map(pdl_df: pd.DataFrame, packaging_weights: dict[str, float]) -> dict[str, float]:
    if pdl_df.empty:
        return {}
    variant_weights = pdl_df.apply(lambda row: compute_variant_weight_grams(row, packaging_weights), axis=1)
    batch_df = pdl_df[["batch"]].copy()
    batch_df["variant_weight"] = variant_weights
    return batch_df.groupby("batch")["variant_weight"].max().to_dict()


def estimate_correction_from_heaviest(row: pd.Series, packaging_weights: dict[str, float], heaviest_batch_weights: dict[str, float]) -> str:
    batch = str(row.get("batch", "")).strip()
    if not batch:
        return "0,000"
    heaviest = heaviest_batch_weights.get(batch)
    if heaviest is None:
        return "0,000"
    current_weight = compute_variant_weight_grams(row, packaging_weights)
    delta_kg = round((current_weight - heaviest) / 1000, 3)
    return normalize_correction_value(delta_kg)


# ──────────────────────────────────────────────
# HF BagSize encoding: recipe priority rule
# ──────────────────────────────────────────────
# Each recipe has a priority. The BagSize multiplier of a batch =
# max(priority of all recipes in the batch).
# Verified 100% on 10999 boxes (CO3 + CO4, W15).

HF_RECIPE_PRIORITY_DEFAULT = {
    59: 1000000,
    1: 10000,
    12: 1000, 17: 1000, 44: 1000, 52: 1000,
    19: 100, 23: 100, 29: 100, 30: 100,
}


def get_batch_multiplier(batch_str: str, recipe_priority: dict) -> int:
    """
    Parse recipe numbers from batch string (e.g. '10 17 28 30 60-4p')
    and return max(priority[r]) across all recipes.
    """
    parts = batch_str.rsplit('-', 1)
    recipes = [int(r) for r in parts[0].split() if r.isdigit()]
    if not recipes:
        return 1
    return max(recipe_priority.get(r, 1) for r in recipes)


# ──────────────────────────────────────────────
# Weight calculation (placeholder/partial)
# ──────────────────────────────────────────────

def compute_hf_recipe_weights(picklist_df: pd.DataFrame, sku_weights_df: pd.DataFrame) -> dict:
    """
    Build a dict: recipe_id (e.g. '45-2p') → total weight in grams.
    Uses picklist (recipe → ingredients) and SKU weights.
    """
    if picklist_df is None or sku_weights_df is None:
        return {}

    # Build SKU → weight lookup
    sku_col = "Culinary SKU Code*" if "Culinary SKU Code*" in sku_weights_df.columns else sku_weights_df.columns[0]
    weight_col = "Pack weight new (g)" if "Pack weight new (g)" in sku_weights_df.columns else sku_weights_df.columns[2]
    sku_weight_map = {}
    for _, row in sku_weights_df.iterrows():
        code = str(row[sku_col]).strip()
        try:
            w = float(row[weight_col])
        except (ValueError, TypeError):
            w = 0
        sku_weight_map[code] = w

    # Aggregate per recipe
    recipe_weights = {}
    recipe_col = "Recipe"
    sku_col_pl = "SKU"
    qty_col = "Quantity"

    for _, row in picklist_df.iterrows():
        recipe = str(row[recipe_col]).strip()
        sku = str(row[sku_col_pl]).strip()
        try:
            qty = int(row[qty_col])
        except (ValueError, TypeError):
            qty = 1
        w = sku_weight_map.get(sku, 0)
        recipe_weights[recipe] = recipe_weights.get(recipe, 0) + w * qty

    return recipe_weights


def compute_hf_packaging_weight(bag_size: int, num_meals: int, cool_pouch: str,
                                box_size_str: str, pkg_df: pd.DataFrame) -> float:
    """Estimate packaging weight in grams from packaging weights table."""
    if pkg_df is None:
        return 0
    pkg_map = {}
    for _, row in pkg_df.iterrows():
        name = str(row.iloc[0]).strip()
        try:
            w = float(row.iloc[1])
        except (ValueError, TypeError):
            w = 0
        pkg_map[name] = w

    total = 0
    # Box weight
    if box_size_str and box_size_str in pkg_map:
        total += pkg_map[box_size_str]
    # Pouch weight
    if cool_pouch and cool_pouch in pkg_map:
        total += pkg_map[cool_pouch]
    return total


# ──────────────────────────────────────────────
# Sidebar
# ──────────────────────────────────────────────


def render_company_page_links_in_sidebar(active_company: str | None = None):
    if not hasattr(st, "page_link"):
        return
    st.sidebar.page_link("pages/1_Factor.py", label="Factor")
    if active_company:
        st.sidebar.caption(f"Aktiv: {active_company}")

forced_company = os.environ.get("PDL_FAST_FORCE_COMPANY", "").strip()
pages_dir = Path(__file__).resolve().parent / "pages"
has_dedicated_factor_page = (pages_dir / "1_Factor.py").exists()
if forced_company == "Factor":
    company = "Factor"
    if has_dedicated_factor_page:
        render_company_page_links_in_sidebar(company)
    else:
        st.sidebar.caption(f"Direkte Seite aktiv: {company}")
elif has_dedicated_factor_page:
    company = "Factor"
    render_company_page_links_in_sidebar()
    st.subheader("Startseite")
    st.info("Bitte öffne links direkt Factor. Dieses Standort-Setup ist jetzt auf Factor begrenzt.")
    if hasattr(st, "page_link"):
        st.page_link("pages/1_Factor.py", label="Factor öffnen")
    st.stop()
else:
    company = "Factor"
set_app_settings({"company": company})

with st.sidebar.expander("Hilfe / Was kann dieses Tool?", expanded=False):
    st.markdown(
        """
        **PDL Fast kurz erklaert**

        - **Factor**: fuer Tracking, RESET, Weekly Fulfillment, Gewichte und neue GSheet-Kontrolle.
        - **Auto-Modus**: schneller, aber mit weniger Zusatzdaten.
        - **Tracking + RESET**: voller Lauf mit mehr Plausibilitaet und Zusatzdaten.

        **Wenn Du nicht weiter weisst:**

        1. Zuerst die richtige KW waehlen.
        2. Dann pruefen, ob Datenquellen geladen wurden.
        3. Erst danach CSVs hochladen oder rechnen.
        4. Admin- und Statusbereiche fuer Kontrolle nutzen.
        """
    )

# ============================================================
# FACTOR
# ============================================================
if company == "Factor":
    st.header("Factor – Import → Export")
    with st.expander("Hilfe: Factor Schritt fuer Schritt", expanded=False):
        st.markdown(
            """
            **Wofuer ist dieser Bereich da?**

            Hier verarbeitest Du Factor-PDLs, Weekly Fulfillment, Gewichtsdaten, neue GSheet-Daten und erzeugst daraus Tracking- und RESET-Ergebnisse.

            **Empfohlene Reihenfolge:**

            1. Jahr und KW korrekt einstellen.
            2. Entscheiden, ob **Auto-Modus** reicht oder ob der volle Lauf benoetigt wird.
            3. Im Adminbereich pruefen, ob Weekly Fulfillment, neue GSheets und Stammdaten geladen wurden.
            4. PDL-Dateien hochladen.
            5. Erst nach Plausibilitaetscheck exportieren.

            **Was gehoert wo rein?**

            - **PDL Upload**: die operative CSV aus dem aktuellen Lauf.
            - **Weekly Fulfillment / neue GSheets**: Soll-/Ist-Pruefung und Transparenz.
            - **Rezeptgewichte**: nur pflegen, wenn RESET oder Gewichtslogik angepasst werden muss.
            """
        )
    FACTOR_DIR = Path(r"Import Factor")
    FACTOR_EXPORT_DIR = Path(r"Export Factor")
    FACTOR_RUN_HISTORY_PATH = Path(".factor_run_history.json")
    SCALE_SYNC_ROOT = Path(str(get_app_setting("scale_sync_root", str(DEFAULT_SCALE_SYNC_ROOT))))
    FACTOR_MAITRE_WEIGHTS_PATH = FACTOR_DIR / "factor_maitre_weights.csv"
    SERVICE_ACCOUNT_PATH = Path(r"secrets\hellofresh-de-problem-solve-78fb952762cd.json")
    FACTOR_PICKLIST_DRIVE_FOLDER_ID = "1-95U6Vd60Qq36PLMxFRs-bHXjzNkQHq5"
    FACTOR_SWAP_SHEET_ID = "16bk1aWdkynIDgJtBLcP-JD-CTXHt5pRb7UnZa4_Bj84"
    FACTOR_FULFILLMENT_REPORT_SHEET_ID = "1YscgiuKYVI2pGcMJ3RcJwWGQEkG46RnVnji8q8a4AeE"
    FACTOR_PKG_PATH = FACTOR_DIR / "Packaging Weights (REMPS).xlsx"
    FACTOR_SKU_PATH = FACTOR_DIR / "Static SKU Weights (REMPS).xlsx"
    factor_default_year, factor_default_week = get_current_year_week()

    st.caption("Factor-PDLs kommen aus Databricks und werden deshalb manuell hochgeladen. Tracking kann auch ohne RESET erzeugt werden, damit die Waagen mit theoretischen Referenzwerten anlaufen.")

    factor_run_history = load_factor_run_history(FACTOR_RUN_HISTORY_PATH)
    with st.expander("Factor Laufhistorie", expanded=False):
        if factor_run_history:
            history_df = pd.DataFrame(factor_run_history)
            st.dataframe(
                history_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Zeit": st.column_config.TextColumn("Zeit", width="medium"),
                    "Status": st.column_config.TextColumn("Status", width="small"),
                    "KW": st.column_config.TextColumn("KW", width="small"),
                    "Modus": st.column_config.TextColumn("Modus", width="small"),
                    "Schnellstart": st.column_config.TextColumn("Schnellstart", width="small"),
                    "Dateien": st.column_config.NumberColumn("Dateien", format="%d"),
                    "Cutoffs": st.column_config.TextColumn("Cutoffs", width="small"),
                    "Boxen": st.column_config.NumberColumn("Boxen", format="%d"),
                    "Meal Swaps": st.column_config.NumberColumn("Meal Swaps", format="%d"),
                    "RESET Zeilen": st.column_config.NumberColumn("RESET Zeilen", format="%d"),
                    "Warnungen": st.column_config.NumberColumn("Warnungen", format="%d"),
                    "Fehler": st.column_config.NumberColumn("Fehler", format="%d"),
                    "Tracking Export": st.column_config.TextColumn("Tracking Export", width="large"),
                },
            )
        else:
            st.info("Noch keine gespeicherten Factor-Läufe vorhanden.")

    factor_source_col1, factor_source_col2 = st.columns(2)
    with factor_source_col1:
        factor_selected_year = int(st.number_input("Factor Jahr", min_value=2024, max_value=2035, value=int(get_app_setting("factor_selected_year", factor_default_year)), step=1, key="factor_selected_year_input"))
    with factor_source_col2:
        factor_selected_week = int(st.number_input("Factor KW", min_value=1, max_value=53, value=int(get_app_setting("factor_selected_week", factor_default_week)), step=1, key="factor_selected_week_input"))

    factor_auto_tracking_mode = st.toggle(
        "Tracking-Schnellstart / Auto-Modus",
        value=bool(get_app_setting("factor_auto_tracking_mode", True)),
        key="factor_auto_tracking_mode_toggle",
        help="Optimiert den Ablauf fuer Tracking-only: Nur Tracking ist aktiv und langsame Zusatzdaten fuer RESET, Picklist und Weekly Fulfillment werden uebersprungen.",
    )

    if factor_auto_tracking_mode:
        factor_mode = "Nur Tracking"
        st.info("Auto-Modus aktiv: Factor laeuft im schnellen Tracking-only-Betrieb mit reduzierter Datenladung.")
        st.warning("Neue GSheets, Weekly Fulfillment, Substitutfinder und die neue Admin-Kontrolle werden im Auto-Modus nicht geladen. Für volle Transparenz Auto-Modus ausschalten.")
    else:
        saved_factor_mode = get_app_setting("factor_mode", "Nur Tracking")
        factor_mode = st.radio(
            "Ausgabemodus",
            options=["Tracking + RESET", "Nur Tracking"],
            horizontal=True,
            index=(["Tracking + RESET", "Nur Tracking"].index(saved_factor_mode) if saved_factor_mode in ["Tracking + RESET", "Nur Tracking"] else 1),
            key="factor_mode_radio",
        )
    set_app_settings({
        "factor_selected_year": factor_selected_year,
        "factor_selected_week": factor_selected_week,
        "factor_auto_tracking_mode": factor_auto_tracking_mode,
        "factor_mode": factor_mode,
    })

    factor_pdl_files = st.file_uploader(
        "Factor PDL / Cutoff CSV hochladen",
        type=["csv"],
        accept_multiple_files=True,
        key="factor_pdl_upload",
    )
    st.caption("Die Pflege der Rezeptgewichte ist nicht von der PDL abhängig. Sie steht weiter unten im Bereich Weekly Fulfillment Report und kann bereits vor dem Upload gepflegt werden. Nur für die eigentliche RESET.csv wird später zusätzlich eine PDL benötigt.")

    factor_drive_service = get_google_drive_service(SERVICE_ACCOUNT_PATH, readonly=False)
    factor_sheets_service = None if factor_auto_tracking_mode else get_google_sheets_service(SERVICE_ACCOUNT_PATH, readonly=False)
    factor_weight_picklist_df = None
    factor_weight_picklist_source_label = "nicht gefunden"
    if not factor_auto_tracking_mode and factor_drive_service is not None:
        factor_picklist_name = f"Marcel_picklist_neu-{factor_selected_year}-W{factor_selected_week:02d}.xlsx"
        factor_picklist_meta = drive_find_named_child(factor_drive_service, FACTOR_PICKLIST_DRIVE_FOLDER_ID, factor_picklist_name)
        if factor_picklist_meta is not None:
            with st.spinner("Interne Gewichts-Picklist wird aus Google Drive geladen..."):
                factor_weight_picklist_df = drive_download_excel(factor_drive_service, factor_picklist_meta["id"])
            factor_weight_picklist_source_label = f"Google Drive: {factor_picklist_meta['name']}"

    factor_local_picklist_path = find_factor_picklist_file(FACTOR_DIR, factor_selected_year, factor_selected_week)
    if not factor_auto_tracking_mode and (factor_weight_picklist_df is None or factor_weight_picklist_df.empty):
        factor_weight_picklist_df = read_excel_from_path(str(factor_local_picklist_path), get_path_mtime_ns(factor_local_picklist_path)) if factor_local_picklist_path and factor_local_picklist_path.exists() else None
        if factor_weight_picklist_df is not None:
            factor_weight_picklist_source_label = f"Lokaler Fallback: {factor_local_picklist_path.name}"

    factor_pkg_df = read_excel_from_path(str(FACTOR_PKG_PATH), get_path_mtime_ns(FACTOR_PKG_PATH)) if (not factor_auto_tracking_mode and FACTOR_PKG_PATH.exists()) else None
    factor_sku_df = read_excel_from_path(str(FACTOR_SKU_PATH), get_path_mtime_ns(FACTOR_SKU_PATH)) if (not factor_auto_tracking_mode and FACTOR_SKU_PATH.exists()) else None
    factor_base_recipe_weight_map = build_factor_recipe_weight_map(factor_weight_picklist_df, factor_sku_df)
    factor_saved_maitre_weights_df = load_factor_saved_maitre_weights(FACTOR_MAITRE_WEIGHTS_PATH)
    factor_fulfillment_picklist_df = pd.DataFrame()
    factor_meal_code_mapping_df = pd.DataFrame()
    factor_maitre_admin_snapshot = None
    if not factor_auto_tracking_mode:
        factor_fulfillment_picklist_df = load_factor_fulfillment_picklist_by_week(
            factor_sheets_service,
            FACTOR_FULFILLMENT_REPORT_SHEET_ID,
            factor_selected_year,
            factor_selected_week,
        )
        factor_meal_code_mapping_df = load_factor_meal_code_mapping(
            factor_sheets_service,
            FACTOR_FULFILLMENT_REPORT_SHEET_ID,
            factor_selected_week,
        )
        factor_fulfillment_picklist_df = apply_factor_meal_code_mapping(
            factor_fulfillment_picklist_df,
            factor_meal_code_mapping_df,
        )
        factor_maitre_admin_snapshot = build_maitre_admin_snapshot(factor_selected_week)
    factor_manual_weight_reference_df = build_factor_manual_weight_reference_df(
        factor_fulfillment_picklist_df,
        factor_base_recipe_weight_map,
    )
    factor_manual_weight_reference_df = apply_factor_saved_maitre_weights(
        factor_manual_weight_reference_df,
        factor_saved_maitre_weights_df,
    )
    factor_manual_weight_context = format_week_label(factor_selected_year, factor_selected_week)
    if st.session_state.get("factor_manual_weight_context") != factor_manual_weight_context:
        st.session_state["factor_manual_weight_context"] = factor_manual_weight_context
        st.session_state["factor_manual_weight_reference_df"] = factor_manual_weight_reference_df.copy()
    elif "factor_manual_weight_reference_df" in st.session_state:
        factor_manual_weight_reference_df = st.session_state["factor_manual_weight_reference_df"].copy()

    if factor_auto_tracking_mode:
        st.caption("Schnellstart aktiv: Stammdaten, Substitutfinder und Weekly Fulfillment werden fuer den reinen Tracking-Lauf uebersprungen.")
    else:
        with st.expander("Factor-Stammdaten", expanded=False):
            st.caption("Diese Dateien werden nur für Theoriegewichte im RESET verwendet. Die sichtbare Picklist kommt aus dem Weekly Fulfillment Report. Änderungen hier gelten nur für die interne Gewichtsberechnung.")
            col_factor_1, col_factor_2, col_factor_3 = st.columns(3)
            with col_factor_1:
                picklist_override = st.file_uploader("Gewichts-Picklist (Excel)", type=["xlsx"], key="factor_picklist_override")
                if picklist_override is not None:
                    factor_weight_picklist_df = read_excel_from_bytes(picklist_override.getvalue())
                    factor_weight_picklist_source_label = f"Manueller Override: {picklist_override.name}"
                elif factor_weight_picklist_df is not None:
                    st.caption(f"Quelle: {factor_weight_picklist_source_label}")
            with col_factor_2:
                pkg_override = st.file_uploader("Packaging Weights (Excel)", type=["xlsx"], key="factor_pkg_override")
                if pkg_override is not None:
                    factor_pkg_df = read_excel_from_bytes(pkg_override.getvalue())
                elif factor_pkg_df is not None:
                    st.caption(f"Standard: {FACTOR_PKG_PATH.name}")
            with col_factor_3:
                sku_override = st.file_uploader("Static SKU Weights (Excel)", type=["xlsx"], key="factor_sku_override")
                if sku_override is not None:
                    factor_sku_df = read_excel_from_bytes(sku_override.getvalue())
                elif factor_sku_df is not None:
                    st.caption(f"Standard: {FACTOR_SKU_PATH.name}")

            factor_edit_col1, factor_edit_col2 = st.columns(2)
            with factor_edit_col1:
                st.markdown("**Static SKU Weights**")
                if factor_sku_df is not None:
                    factor_sku_df = st.data_editor(
                        factor_sku_df,
                        use_container_width=True,
                        num_rows="dynamic",
                        key="factor_sku_edit",
                    )
                else:
                    st.warning(f"{FACTOR_SKU_PATH.name} nicht gefunden.")
            with factor_edit_col2:
                st.markdown("**Packaging Weights**")
                if factor_pkg_df is not None:
                    factor_pkg_df = st.data_editor(
                        factor_pkg_df,
                        use_container_width=True,
                        num_rows="dynamic",
                        key="factor_pkg_edit",
                    )
                else:
                    st.warning(f"{FACTOR_PKG_PATH.name} nicht gefunden.")

        with st.expander("Factor Substitutfinder", expanded=False):
            st.caption("Direkte Anbindung an das Factor Dynamic Swaps Sheet. Markt, KW und Meal Short werden wie im Google Sheet gesetzt und die Swap-Prioritäten live zurückgelesen.")
            factor_swap_col1, factor_swap_col2, factor_swap_col3 = st.columns(3)
            with factor_swap_col1:
                saved_factor_swap_market = get_app_setting("factor_swap_market", "FA-DE")
                factor_swap_market = st.selectbox(
                    "Markt",
                    options=["FA-DE", "FA-BENL", "FA-DKSE"],
                    index=(["FA-DE", "FA-BENL", "FA-DKSE"].index(saved_factor_swap_market) if saved_factor_swap_market in ["FA-DE", "FA-BENL", "FA-DKSE"] else 0),
                    key="factor_swap_market",
                )
            week_options = factor_swap_get_week_options(factor_sheets_service, FACTOR_SWAP_SHEET_ID) if factor_sheets_service is not None else []
            default_week_label = f"W{factor_selected_week:02d}"
            with factor_swap_col2:
                saved_factor_swap_week = get_app_setting("factor_swap_week", default_week_label)
                factor_swap_week = st.selectbox(
                    "Substitut-KW",
                    options=week_options if week_options else [default_week_label],
                    index=(
                        (week_options.index(saved_factor_swap_week) if saved_factor_swap_week in week_options else (week_options.index(default_week_label) if default_week_label in week_options else 0))
                        if week_options else 0
                    ),
                    key="factor_swap_week",
                )
            meal_options = []
            if factor_sheets_service is not None:
                try:
                    meal_options = factor_swap_get_meal_options(
                        factor_sheets_service,
                        FACTOR_SWAP_SHEET_ID,
                        factor_swap_market,
                        factor_swap_week,
                    )
                except Exception as exc:
                    st.warning(f"Meal-Auswahl aus dem Substitutfinder konnte nicht geladen werden: {exc}")
            with factor_swap_col3:
                saved_factor_swap_meal = get_app_setting("factor_swap_meal", "")
                factor_swap_meal = st.selectbox(
                    "Meal Short",
                    options=meal_options if meal_options else ["keine Optionen gefunden"],
                    index=((meal_options.index(saved_factor_swap_meal) if saved_factor_swap_meal in meal_options else 0) if meal_options else 0),
                    key="factor_swap_meal",
                )
            set_app_settings({
                "factor_swap_market": factor_swap_market,
                "factor_swap_week": factor_swap_week,
                "factor_swap_meal": factor_swap_meal,
            })

            if factor_sheets_service is None:
                st.info("Kein Zugriff auf Google Sheets verfügbar.")
            elif meal_options and factor_swap_meal != "keine Optionen gefunden":
                if st.button("Substitute für Factor laden", key="factor_swap_load"):
                    summary, result_df = factor_swap_get_results(
                        factor_sheets_service,
                        FACTOR_SWAP_SHEET_ID,
                        factor_swap_market,
                        factor_swap_week,
                        factor_swap_meal,
                    )
                    st.session_state["factor_swap_summary"] = summary
                    st.session_state["factor_swap_result_df"] = result_df

            if "factor_swap_summary" in st.session_state:
                summary = st.session_state["factor_swap_summary"]
                metric_swap_1, metric_swap_2, metric_swap_3 = st.columns(3)
                metric_swap_1.metric("Markt", summary.get("Market", factor_swap_market))
                metric_swap_2.metric("KW", summary.get("Menu Week", factor_swap_week))
                metric_swap_3.metric("Meal", summary.get("Meal Name", summary.get("Meal Short", factor_swap_meal)))
            if "factor_swap_result_df" in st.session_state:
                result_df = st.session_state["factor_swap_result_df"]
                if isinstance(result_df, pd.DataFrame) and not result_df.empty:
                    factor_display_df = result_df.copy()
                    if "Swap Priority" in factor_display_df.columns:
                        factor_display_df["Swap Priority Numeric"] = pd.to_numeric(factor_display_df["Swap Priority"], errors="coerce")
                        factor_display_df = factor_display_df.sort_values(["Swap Priority Numeric", "Meal Short"], ascending=[True, True], na_position="last")
                        factor_display_df = factor_display_df.drop(columns=["Swap Priority Numeric"])

                    st.caption("Die Liste ist streng priorisiert: oberster Eintrag = Substitute 1, darunter Substitute 2, 3, 4 usw.")
                    st.dataframe(
                        factor_display_df[["Wahl", "Meal Short", "Meal Name"]],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Wahl": st.column_config.TextColumn("Substitut-Rang", width="small"),
                            "Meal Short": st.column_config.TextColumn("Meal Short", width="large"),
                            "Meal Name": st.column_config.TextColumn("Meal Name", width="large"),
                        },
                    )
                    st.download_button(
                        "Factor-Substitute herunterladen",
                        data=dataframe_to_csv_bytes(result_df),
                        file_name="factor_substitute_results.csv",
                        mime="text/csv",
                        on_click="ignore",
                    )
                else:
                    st.info("Für die Auswahl wurden keine Swap-Ergebnisse gefunden.")

        with st.expander("Neue GSheets: Admin-Kontrolle Forecast + Inbound", expanded=False):
            st.caption("Hier siehst du direkt, ob die neuen Google Sheets geladen wurden und was daraus für die gewählte Produktions-KW erzeugt wurde.")
            if factor_maitre_admin_snapshot is None:
                st.info("Die Admin-Kontrolle ist nur verfügbar, wenn Google-Sheets-Zugriff aktiv ist.")
            else:
                admin_week_label = factor_maitre_admin_snapshot["week_label"]
                admin_forecast_raw_df = factor_maitre_admin_snapshot["forecast_raw_df"]
                admin_inbound_raw_df = factor_maitre_admin_snapshot["inbound_raw_df"]
                admin_forecast_week_df = factor_maitre_admin_snapshot["forecast_week_df"]
                admin_inbound_week_df = factor_maitre_admin_snapshot["inbound_week_df"]
                admin_inbound_agg_df = factor_maitre_admin_snapshot["inbound_agg_df"]
                admin_comparison_df = factor_maitre_admin_snapshot["comparison_df"]
                admin_shortages_df = factor_maitre_admin_snapshot["shortages_df"]
                admin_overages_df = factor_maitre_admin_snapshot["overages_df"]

                maitre_metric_col1, maitre_metric_col2, maitre_metric_col3, maitre_metric_col4, maitre_metric_col5 = st.columns(5)
                maitre_metric_col1.metric("Produktions-KW", admin_week_label)
                maitre_metric_col2.metric("Forecast geladen", int(len(admin_forecast_raw_df)))
                maitre_metric_col3.metric("Inbound geladen", int(len(admin_inbound_raw_df)))
                maitre_metric_col4.metric("Vergleichszeilen", int(len(admin_comparison_df)))
                maitre_metric_col5.metric("Matches", int((admin_comparison_df["received"] > 0).sum()) if not admin_comparison_df.empty else 0)

                if admin_inbound_week_df.empty:
                    available_weeks = sorted(set(admin_inbound_raw_df.get("week", pd.Series(dtype=str)).dropna().unique()))
                    st.warning(
                        f"Für {admin_week_label} wurden im neuen Inbound-Sheet keine Produktionsdaten gefunden. Verfügbare Produktions-KWs: {', '.join(available_weeks) if available_weeks else '—'}"
                    )
                else:
                    unload_weeks = sorted(set(admin_inbound_week_df.get("unload_week", pd.Series(dtype=str)).dropna().unique()))
                    st.caption(
                        f"Inbound wird für {admin_week_label} aus Entlade-KW {', '.join(unload_weeks) if unload_weeks else '—'} + 1 Woche abgeleitet."
                    )

                source_col1, source_col2 = st.columns(2)
                with source_col1:
                    st.markdown("**Quelle 1: Forecast Bibel**")
                    st.caption("Tab: 2025 Verden Forecast (DE+NOR) | Header: Zeile 2 | Soll-Spalte: order")
                    st.write(f"Zeilen gesamt: {int(len(admin_forecast_raw_df))}")
                    st.write(f"Zeilen in {admin_week_label}: {int(len(admin_forecast_week_df))}")
                with source_col2:
                    st.markdown("**Quelle 2: Weekly Fulfillment Inbound**")
                    st.caption("Tab: Logistik - FCMS Meals | Join: normalisierter Recipe Code | KW: Produktions-KW")
                    st.write(f"Zeilen gesamt: {int(len(admin_inbound_raw_df))}")
                    st.write(f"Zeilen in {admin_week_label}: {int(len(admin_inbound_week_df))}")

                if admin_comparison_df.empty:
                    st.info("Aus den neuen GSheets konnte für die gewählte KW noch kein Soll-Ist-Vergleich aufgebaut werden.")
                else:
                    st.markdown("**Ergebnis aus den neuen GSheets**")
                    admin_show_df = admin_comparison_df[
                        ["Market", "week", "join_code", "recipe_codes", "sku_name", "forecast_qty", "received", "delta", "fulfillment_pct"]
                    ].copy()
                    st.dataframe(
                        admin_show_df.sort_values(["delta", "Market"], kind="stable"),
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Market": st.column_config.TextColumn("Markt", width="small"),
                            "week": st.column_config.TextColumn("KW", width="small"),
                            "join_code": st.column_config.TextColumn("Join Code", width="small"),
                            "recipe_codes": st.column_config.TextColumn("Forecast Code", width="medium"),
                            "sku_name": st.column_config.TextColumn("SKU Name", width="large"),
                            "forecast_qty": st.column_config.NumberColumn("Soll", format="%d"),
                            "received": st.column_config.NumberColumn("Ist", format="%d"),
                            "delta": st.column_config.NumberColumn("Delta", format="%d"),
                            "fulfillment_pct": st.column_config.NumberColumn("Fulfillment %", format="%.1f"),
                        },
                    )

                    admin_summary_col1, admin_summary_col2, admin_summary_col3 = st.columns(3)
                    admin_summary_col1.metric("Fehlmengen > 50", int(len(admin_shortages_df)))
                    admin_summary_col2.metric("Überdeckungen > 50", int(len(admin_overages_df)))
                    admin_summary_col3.metric("Inbound Codes aggregiert", int(len(admin_inbound_agg_df)))

                    st.download_button(
                        "Neuer GSheet-Vergleich herunterladen",
                        data=dataframe_to_csv_bytes(admin_comparison_df),
                        file_name=f"factor_new_gsheets_comparison_{format_week_label(factor_selected_year, factor_selected_week)}.csv",
                        mime="text/csv",
                        on_click="ignore",
                    )

                    with st.expander("Rohdaten aus neuen GSheets", expanded=False):
                        raw_tab1, raw_tab2, raw_tab3 = st.tabs(["Forecast", "Inbound", "Inbound aggregiert"])
                        with raw_tab1:
                            st.dataframe(admin_forecast_week_df, use_container_width=True, hide_index=True)
                        with raw_tab2:
                            st.dataframe(admin_inbound_week_df, use_container_width=True, hide_index=True)
                        with raw_tab3:
                            st.dataframe(admin_inbound_agg_df, use_container_width=True, hide_index=True)

        with st.expander("Factor Weekly Fulfillment Report und Gewichtspflege", expanded=not bool(factor_pdl_files)):
            st.caption("Die sichtbare Factor-Picklist wird direkt aus dem Weekly Fulfillment Report pro KW erzeugt. Die Zuordnung läuft über Woche, Rezeptnummer und SKU, nicht über feste Rezeptnamen, weil sich Rezepte und Namen jede Woche ändern können.")
            if factor_fulfillment_picklist_df.empty:
                st.info("Für die gewählte KW wurden im Weekly Fulfillment Report keine DE- oder Nordics-Daten gefunden.")
            else:
                factor_fulfillment_summary = (
                    factor_fulfillment_picklist_df.groupby("Region", as_index=False)
                    .agg(
                        Rezepte=("Recipe", "nunique"),
                        SKUs=("SKU", lambda values: len({value for value in values if str(value).strip()})),
                        Meals=("Total Meals", "sum"),
                    )
                    .sort_values("Region", kind="stable")
                )

                summary_cols = st.columns(max(1, len(factor_fulfillment_summary)))
                for index, (_, summary_row) in enumerate(factor_fulfillment_summary.iterrows()):
                    summary_cols[index].metric(
                        f"{summary_row['Region']}",
                        int(summary_row["Meals"]),
                        f"{int(summary_row['Rezepte'])} Rezepte / {int(summary_row['SKUs'])} SKUs",
                    )

                st.dataframe(
                    factor_fulfillment_picklist_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Region": st.column_config.TextColumn("Region", width="small"),
                        "Week": st.column_config.TextColumn("Woche", width="small"),
                        "Recipe": st.column_config.NumberColumn("Rezept", format="%d", width="small"),
                        "Maitre Code": st.column_config.TextColumn("Maitre Code", width="small"),
                        "Meal Code": st.column_config.TextColumn("Meal Code", width="small"),
                        "Meal Code FE": st.column_config.TextColumn("Meal Code FE", width="small"),
                        "Meal Code FV": st.column_config.TextColumn("Meal Code FV", width="small"),
                        "SKU": st.column_config.TextColumn("SKU", width="medium"),
                        "Artikel": st.column_config.TextColumn("Artikel", width="large"),
                        "Friday": st.column_config.NumberColumn("Friday", format="%d"),
                        "Sunday": st.column_config.NumberColumn("Sunday", format="%d"),
                        "TK": st.column_config.NumberColumn("TK", format="%d"),
                        "TV": st.column_config.NumberColumn("TV", format="%d"),
                        "TZ": st.column_config.TextColumn("TZ", width="small"),
                        "Total Meals": st.column_config.NumberColumn("Total Meals", format="%d"),
                    },
                )
                st.download_button(
                    "Factor Wochen-Picklist herunterladen",
                    data=dataframe_to_csv_bytes(factor_fulfillment_picklist_df),
                    file_name=f"factor_fulfillment_picklist_{format_week_label(factor_selected_year, factor_selected_week)}.csv",
                    mime="text/csv",
                    on_click="ignore",
                )
                if factor_weight_picklist_df is None or factor_weight_picklist_df.empty:
                    st.warning("Keine interne Gewichts-Picklist gefunden. Tracking funktioniert weiter, für theoretische RESET-Werte fehlt dann aber die ingredientbasierte Basis.")
                else:
                    st.caption(f"Interne Gewichtsbasis für RESET: {factor_weight_picklist_source_label}")

                st.markdown("**Relevante Rezeptgewichte für RESET**")
                st.caption("Hier kannst du Rezeptgewichte pro Maitre Code pflegen. Zusätzlich siehst du die Übergangsansicht Meal Code FE und Meal Code FV parallel. Ein Eintrag gilt automatisch für DE und Nordics, wenn die Maitre Nummer gleich ist. Diese Werte werden dauerhaft gesammelt und nur für Tracking + RESET verwendet, nicht für Nur Tracking.")
                if factor_manual_weight_reference_df.empty:
                    st.info("Für die gewählte KW konnten keine Maitre-Codes für eine manuelle Gewichtspflege vorbereitet werden.")
                else:
                    st.caption("Änderungen an Rezeptgewichten werden erst nach Klick auf 'Rezeptgewichte übernehmen' angewendet.")
                    with st.form("factor_manual_recipe_weight_form"):
                        factor_manual_weight_editor_df = st.data_editor(
                            factor_manual_weight_reference_df,
                            hide_index=True,
                            use_container_width=True,
                            column_config={
                                "Maitre Code": st.column_config.TextColumn("Maitre Code", disabled=True, width="small"),
                                "Meal Code": st.column_config.TextColumn("Meal Code", disabled=True, width="small"),
                                "Meal Code FE": st.column_config.TextColumn("Meal Code FE", disabled=True, width="small"),
                                "Meal Code FV": st.column_config.TextColumn("Meal Code FV", disabled=True, width="small"),
                                "Rezepte": st.column_config.TextColumn("Rezepte", disabled=True, width="medium"),
                                "Regionen": st.column_config.TextColumn("Regionen", disabled=True, width="small"),
                                "Artikel": st.column_config.TextColumn("Artikel", disabled=True, width="large"),
                                "Theorie Rezeptgewicht (g)": st.column_config.NumberColumn("Theorie Rezeptgewicht (g)", disabled=True, format="%.1f"),
                                "Manuelles Rezeptgewicht (g)": st.column_config.NumberColumn("Manuelles Rezeptgewicht (g)", format="%.1f", step=0.1),
                                "Gewicht gespeichert": st.column_config.TextColumn("Gespeichert", disabled=True, width="small"),
                            },
                            key="factor_manual_recipe_weight_editor",
                        )
                        factor_manual_weights_apply = st.form_submit_button("Rezeptgewichte übernehmen")
                    if factor_manual_weights_apply:
                        st.session_state["factor_manual_weight_reference_df"] = factor_manual_weight_editor_df.copy()
                        factor_manual_weight_reference_df = factor_manual_weight_editor_df.copy()
                        st.success("Rezeptgewichte übernommen.")
                    else:
                        factor_manual_weight_reference_df = st.session_state.get("factor_manual_weight_reference_df", factor_manual_weight_reference_df).copy()
                    factor_weight_stats_col1, factor_weight_stats_col2, factor_weight_stats_col3 = st.columns(3)
                    factor_weight_stats_col1.metric("Maitre Codes aktuell", int(len(factor_manual_weight_reference_df)))
                    factor_weight_stats_col2.metric("Bereits gespeichert", int((factor_manual_weight_reference_df["Gewicht gespeichert"] == "Ja").sum()))
                    factor_weight_stats_col3.metric("Gesamte Sammlung", int(len(factor_saved_maitre_weights_df)))

                    if st.button("Maitre-Gewichte dauerhaft speichern", key="factor_save_maitre_weights"):
                        saved_count, updated_saved_df = save_factor_maitre_weights(
                            FACTOR_MAITRE_WEIGHTS_PATH,
                            factor_manual_weight_reference_df,
                            format_week_label(factor_selected_year, factor_selected_week),
                        )
                        st.session_state["factor_saved_maitre_weight_count"] = saved_count
                        st.session_state["factor_saved_maitre_weight_total"] = len(updated_saved_df)
                        st.success(f"{saved_count} Maitre-Gewichte in {FACTOR_MAITRE_WEIGHTS_PATH.name} gespeichert.")

                    if "factor_saved_maitre_weight_count" in st.session_state:
                        st.caption(
                            f"Letzte Speicherung: {int(st.session_state['factor_saved_maitre_weight_count'])} Werte aktualisiert, Sammlung gesamt {int(st.session_state.get('factor_saved_maitre_weight_total', len(factor_saved_maitre_weights_df)))}."
                        )

                    st.download_button(
                        "Maitre-Gewichte herunterladen",
                        data=dataframe_to_csv_bytes(factor_manual_weight_reference_df),
                        file_name=f"factor_maitre_weights_{format_week_label(factor_selected_year, factor_selected_week)}.csv",
                        mime="text/csv",
                        on_click="ignore",
                    )

    if factor_pdl_files:
        frames = []
        factor_input_file_names = []
        for uploaded_file in factor_pdl_files:
            df = pd.read_csv(uploaded_file)
            df["_source"] = uploaded_file.name
            frames.append(df)
            factor_input_file_names.append(uploaded_file.name)
        factor_df = pd.concat(frames, ignore_index=True)

        expected_cols = {"boxid", "meal_swap", "meal_count", "bag_size"}
        if not expected_cols.issubset(set(factor_df.columns)):
            st.error(f"Die Factor-PDL muss folgende Spalten enthalten: {expected_cols}")
            st.stop()

        raw_factor_box_count = int(len(factor_df))
        factor_df["_box_id_str"] = factor_df["boxid"].astype(str)
        factor_df = factor_df.drop_duplicates(subset="_box_id_str", keep="first")
        duplicate_box_count = raw_factor_box_count - int(len(factor_df))
        factor_df["Expanded Meal Swap"] = factor_df.apply(
            lambda row: expand_factor_meal_swap(str(row.get("meal_swap", "")).strip(), int(row.get("bag_size", 1) or 1)),
            axis=1,
        )
        factor_df["Cutoff"] = factor_df["_source"].apply(parse_factor_cutoff_label)

        st.info(f"PDL geladen: {len(factor_df)} eindeutige Boxen aus {len(factor_pdl_files)} Datei(en)")
        metric_col1, metric_col2, metric_col3 = st.columns(3)
        metric_col1.metric("Cutoffs", int(factor_df["Cutoff"].nunique()))
        metric_col2.metric("Boxen", int(len(factor_df)))
        metric_col3.metric("Meal Swaps", int(factor_df["Expanded Meal Swap"].nunique()))

        factor_plan_vs_pdl_df = build_factor_plan_vs_pdl_report(
            factor_fulfillment_picklist_df,
            factor_df,
        )
        if not factor_plan_vs_pdl_df.empty:
            with st.expander("Factor Soll-Ist-Abgleich Plan vs. PDL", expanded=False):
                st.caption("Vergleicht die geplanten Meals aus dem Weekly Fulfillment Report mit den tatsächlich in der hochgeladenen PDL vorkommenden Meals pro Rezept. Reine Kontrollsicht, ohne Einfluss auf Tracking oder RESET.")
                compare_col1, compare_col2, compare_col3 = st.columns(3)
                compare_col1.metric("Rezepte im Vergleich", int(len(factor_plan_vs_pdl_df)))
                compare_col2.metric("Abweichungen", int((factor_plan_vs_pdl_df["Delta Meals"] != 0).sum()))
                compare_col3.metric("PDL Meals gesamt", int(factor_plan_vs_pdl_df["PDL Meals"].sum()))
                st.dataframe(
                    factor_plan_vs_pdl_df,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Recipe": st.column_config.NumberColumn("Rezept", format="%d", width="small"),
                        "Regions": st.column_config.TextColumn("Regionen", width="small"),
                        "Planned Meals": st.column_config.NumberColumn("Plan Meals", format="%d"),
                        "PDL Meals": st.column_config.NumberColumn("PDL Meals", format="%d"),
                        "PDL Boxes": st.column_config.NumberColumn("PDL Boxen", format="%d"),
                        "Delta Meals": st.column_config.NumberColumn("Delta Meals", format="%d"),
                        "Status": st.column_config.TextColumn("Status", width="small"),
                    },
                )
                st.download_button(
                    "Plan-vs-PDL-Abgleich herunterladen",
                    data=dataframe_to_csv_bytes(factor_plan_vs_pdl_df),
                    file_name=f"factor_plan_vs_pdl_{format_week_label(factor_selected_year, factor_selected_week)}.csv",
                    mime="text/csv",
                    on_click="ignore",
                )

        tracking_lines = []
        for _, row in factor_df.iterrows():
            box_id = str(row["boxid"]).strip()
            meal_count = int(row["meal_count"])
            bag_size = int(row["bag_size"])
            expanded = row["Expanded Meal Swap"]
            tracking_lines.append(
                build_tracking_line(box_id, bag_size, meal_count, expanded, correction=0)
            )
        tracking_csv = generate_tracking_csv(tracking_lines)

        reset_csv = None
        reset_input_df = pd.DataFrame()
        reset_input_base_df = pd.DataFrame()
        if factor_mode == "Tracking + RESET":
            factor_recipe_weight_map = build_factor_recipe_weight_map_with_manuals(
                factor_base_recipe_weight_map,
                factor_fulfillment_picklist_df,
                factor_manual_weight_reference_df,
            )
            factor_packaging_weight_map = build_factor_packaging_weight_map(factor_pkg_df)
            reset_input_base_df = build_factor_reset_input_df(factor_df, factor_recipe_weight_map, factor_packaging_weight_map)

            st.subheader("Gewichtsmaske für RESET")
            st.caption("Die theoretischen Gewichte berücksichtigen zuerst die gepflegten Maitre-Rezeptgewichte und danach die interne Gewichtsbasis. Du kannst sie hier pro Meal Swap zusätzlich direkt überschreiben, um einen benötigten Cutoff mit deinen Einwiegungen zu erzeugen.")
            if reset_input_base_df.empty:
                st.warning("Für RESET konnten keine Gewichtszeilen vorbereitet werden. Tracking kann trotzdem erstellt werden.")
            else:
                reset_editor_context = "|".join(sorted(factor_df["_source"].astype(str).tolist()))
                if st.session_state.get("factor_reset_editor_context") != reset_editor_context:
                    st.session_state["factor_reset_editor_context"] = reset_editor_context
                    st.session_state["factor_reset_input_df"] = reset_input_base_df.copy()
                st.caption("Änderungen an der RESET-Maske werden erst nach Klick auf 'RESET-Gewichte übernehmen' angewendet.")
                with st.form("factor_reset_input_form"):
                    factor_reset_editor_df = st.data_editor(
                        st.session_state.get("factor_reset_input_df", reset_input_base_df),
                        hide_index=True,
                        use_container_width=True,
                        column_config={
                            "Cutoff": st.column_config.TextColumn("Cutoff", disabled=True),
                            "Meal Swap": st.column_config.TextColumn("Meal Swap", disabled=True, width="large"),
                            "Referenz Box_ID": st.column_config.TextColumn("Referenz Box_ID", disabled=True),
                            "Boxen": st.column_config.NumberColumn("Boxen", disabled=True),
                            "Theoretisches Gewicht (kg)": st.column_config.NumberColumn("Theoretisches Gewicht (kg)", disabled=True, format="%.3f"),
                            "Manuelles Gewicht (kg)": st.column_config.NumberColumn("Manuelles Gewicht (kg)", format="%.3f", step=0.001),
                        },
                        key="factor_reset_input_editor",
                    )
                    factor_reset_apply = st.form_submit_button("RESET-Gewichte übernehmen")
                if factor_reset_apply:
                    st.session_state["factor_reset_input_df"] = factor_reset_editor_df.copy()
                    st.success("RESET-Gewichte übernommen.")
                reset_input_df = st.session_state.get("factor_reset_input_df", reset_input_base_df).copy()
                reset_map = build_factor_reset_map_from_input(reset_input_df)
                reset_csv = generate_reset_csv(reset_map)

        factor_quality_report = build_factor_quality_report(
            factor_df,
            factor_mode,
            factor_weight_picklist_df,
            factor_pkg_df,
            factor_sku_df,
            factor_manual_weight_reference_df,
            reset_input_base_df,
            factor_plan_vs_pdl_df,
            duplicate_box_count,
        )

        st.subheader("Factor Statusampel & Plausibilitätscheck")
        quality_col1, quality_col2, quality_col3, quality_col4 = st.columns(4)
        quality_col1.metric("Status", factor_quality_report["status"])
        quality_col2.metric("Warnungen", int(factor_quality_report["metrics"]["warning_count"]))
        quality_col3.metric("Fehler", int(factor_quality_report["metrics"]["error_count"]))
        quality_col4.metric("Duplikate entfernt", int(factor_quality_report["metrics"]["duplicate_box_count"]))

        if factor_quality_report["status"] == "ROT":
            st.error("Status ROT: Bitte die Hinweise prüfen, bevor du die erzeugten Dateien weitergibst.")
        elif factor_quality_report["status"] == "GELB":
            st.warning("Status GELB: Die Verarbeitung lief, aber es gibt Punkte, die du prüfen solltest.")
        else:
            st.success("Status GRUEN: Keine offensichtlichen Plausibilitätsprobleme erkannt.")

        for finding in factor_quality_report["findings"]:
            text = f"{finding['title']}: {finding['message']}"
            if finding["severity"] == "red":
                st.error(text)
            elif finding["severity"] == "yellow":
                st.warning(text)
            else:
                st.caption(text)

        st.subheader("Tracking.csv Vorschau")
        preview_lines = tracking_csv.split("\n")[:51]
        st.code("\n".join(preview_lines), language=None)
        st.caption(f"Gesamt: {len(tracking_lines)} Zeilen")

        if reset_csv is not None:
            st.subheader("RESET.csv Vorschau")
            preview_reset = reset_csv.split("\n")[:31]
            st.code("\n".join(preview_reset), language=None)
            st.caption(f"Gesamt: {len(reset_input_df)} eindeutige Kombinationen")

        if factor_mode == "Nur Tracking":
            st.info("Es wird nur Tracking.csv erzeugt. Das funktioniert auch ohne gespeicherte oder gepflegte Gewichte; die Gewichtssammlung wird in diesem Modus nicht benötigt.")

        factor_cutoffs = unique_preserve_order(factor_df["Cutoff"].astype(str).tolist())
        factor_export_dir = persist_export_files(
            FACTOR_EXPORT_DIR,
            factor_selected_week,
            factor_cutoffs,
            tracking_csv,
            reset_csv,
        )
        factor_scale_export_dir = persist_export_files(
            SCALE_SYNC_ROOT,
            factor_selected_week,
            factor_cutoffs,
            tracking_csv,
            reset_csv,
        )

        factor_run_history_entry = build_factor_run_history_entry(
            week_label=format_week_label(factor_selected_year, factor_selected_week),
            factor_mode=factor_mode,
            auto_tracking_mode=factor_auto_tracking_mode,
            input_file_count=len(factor_input_file_names),
            factor_cutoffs=factor_cutoffs,
            box_count=len(factor_df),
            meal_swap_count=int(factor_df["Expanded Meal Swap"].nunique()),
            reset_rows_count=int(len(reset_input_df) if not reset_input_df.empty else len(reset_input_base_df)),
            warning_count=int(factor_quality_report["metrics"]["warning_count"]),
            error_count=int(factor_quality_report["metrics"]["error_count"]),
            status=factor_quality_report["status"],
            tracking_export_path=str(factor_export_dir),
        )
        factor_run_signature = build_factor_run_signature(
            week_label=factor_run_history_entry["KW"],
            factor_mode=factor_mode,
            auto_tracking_mode=factor_auto_tracking_mode,
            input_file_names=factor_input_file_names,
            factor_cutoffs=factor_cutoffs,
            box_count=factor_run_history_entry["Boxen"],
            meal_swap_count=factor_run_history_entry["Meal Swaps"],
            reset_rows_count=factor_run_history_entry["RESET Zeilen"],
            status=factor_run_history_entry["Status"],
        )
        factor_run_id = start_run(
            tool="pdl-fast",
            input_files=factor_input_file_names,
        )
        if factor_run_id:
            add_artifact(factor_run_id, "tracking_csv", factor_export_dir / "Tracking.csv", "text/csv")
            if (factor_export_dir / "RESET.csv").exists():
                add_artifact(factor_run_id, "reset_csv", factor_export_dir / "RESET.csv", "text/csv")
            finish_run(
                factor_run_id,
                status=quality_to_run_status(factor_quality_report["status"]),
                metrics={
                    "company": "Factor",
                    "week": format_week_label(factor_selected_year, factor_selected_week),
                    "boxCount": int(len(factor_df)),
                    "trackingRows": int(len(tracking_lines)),
                    "resetRows": int(len(reset_input_df) if not reset_input_df.empty else len(reset_input_base_df)),
                    "warningCount": int(factor_quality_report["metrics"]["warning_count"]),
                    "errorCount": int(factor_quality_report["metrics"]["error_count"]),
                },
                warnings=[item["message"] for item in factor_quality_report["findings"] if item["severity"] == "yellow"],
                errors=[item["message"] for item in factor_quality_report["findings"] if item["severity"] == "red"],
            )
        if st.session_state.get("factor_last_history_signature") != factor_run_signature:
            factor_run_history = append_factor_run_history(FACTOR_RUN_HISTORY_PATH, factor_run_history_entry)
            st.session_state["factor_last_history_signature"] = factor_run_signature

        st.info(f"Exportordner: {factor_export_dir.resolve()}")
        st.caption(f"Waagenordner / Sync: {factor_scale_export_dir.resolve()}")
        render_scale_sync_controls(
            "factor",
            "Factor",
            factor_scale_export_dir,
        )
        render_drive_export_controls(
            "factor",
            "Factor",
            factor_drive_service,
            "factor_drive_export_folder_id",
            tracking_csv,
            reset_csv,
        )

        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            st.download_button(
                "📥 Tracking.csv herunterladen",
                data=csv_text_to_bytes(tracking_csv),
                file_name="Tracking.csv",
                mime="text/csv",
                on_click="ignore",
            )
        with col_dl2:
            if reset_csv is not None:
                st.download_button(
                    "📥 RESET.csv herunterladen",
                    data=csv_text_to_bytes(reset_csv),
                    file_name="RESET.csv",
                    mime="text/csv",
                    on_click="ignore",
                )

# ============================================================
# HELLOFRESH
# ============================================================
else:
    st.header("HelloFresh – Tracking.csv & RESET.csv Generator")
    hf_mode = st.radio(
        "Ausgabemodus",
        options=["Tracking + RESET", "Nur Tracking"],
        horizontal=True,
        index=(["Tracking + RESET", "Nur Tracking"].index(get_app_setting("hf_mode", "Tracking + RESET")) if get_app_setting("hf_mode", "Tracking + RESET") in ["Tracking + RESET", "Nur Tracking"] else 0),
        key="hf_output_mode",
    )

    HF_DIR = Path(r"Import HelloFresh")
    HF_EXPORT_DIR = Path(r"Export HelloFresh")
    SCALE_SYNC_ROOT = Path(str(get_app_setting("scale_sync_root", str(DEFAULT_SCALE_SYNC_ROOT))))
    SKU_WEIGHTS_PATH = HF_DIR / "SKU Weights (REMPS).xlsx"
    PKG_WEIGHTS_PATH = HF_DIR / "Packaging Weights (REMPS).xlsx"
    SERVICE_ACCOUNT_PATH = Path(r"secrets\hellofresh-de-problem-solve-78fb952762cd.json")
    PICKLIST_DRIVE_FOLDER_ID = "1zEg2NsGP9-0p-pov0CbIS13oBvbN0f4v"
    PDL_DRIVE_FOLDER_ID = "1eCsBqOA6dwfxG3KWAhOpLYnGJpYRLX0G"
    OUTPUTS_DRIVE_FOLDER_ID = "1Df2kQpcLZWwVNz_pdf_DEttfGv5uO5Yw"
    BEDARF_OUTPUTS_DRIVE_FOLDER_ID = "1_-5MosaYH0YYKlmP1f18NzH40jlWQXdo"
    HF_WEIGHING_SPREADSHEET_ID = "1u2WjPiGDBiIgia43jIm5mvfw8YUlK_O8nGDHjGRSIQ0"
    SUBSTITUTE_SHEET_ID = "1IuOiowJU52yqv1zf48aoPgwqZmkqNlCisXftFDAc0B8"
    PICKLIST_BASE_DIR = Path(
        r"G:\.shortcut-targets-by-id\14d5DvmVBQrhqRwDsGAy-ZJpAyzfe0VsN\Problem Solve Verden Basecamp und Co.Kg\Weight Calculator HF\Picklisten"
    )
    PDL_BASE_DIR = Path(
        r"G:\.shortcut-targets-by-id\1eCsBqOA6dwfxG3KWAhOpLYnGJpYRLX0G\ETL_OR"
    )
    BEDARF_BASE_DIR = Path(
        r"G:\.shortcut-targets-by-id\14d5DvmVBQrhqRwDsGAy-ZJpAyzfe0VsN\Problem Solve Verden Basecamp und Co.Kg\Bedarfsrechnung Hellofresh\Fertiger Bedarf"
    )

    default_year, default_week = get_hf_operational_year_week()
    recommended_cutoff = get_hf_recommended_cutoff()

    if "hf_selected_year_input" not in st.session_state:
        st.session_state["hf_selected_year_input"] = default_year
    if "hf_selected_week_input" not in st.session_state:
        st.session_state["hf_selected_week_input"] = default_week

    st.subheader("Automatische Quelldateien")
    col_src1, col_src2, col_src3 = st.columns([1, 1, 2])
    with col_src1:
        selected_year = int(st.number_input("Jahr", min_value=2024, max_value=2035, value=int(st.session_state["hf_selected_year_input"]), step=1, key="hf_selected_year_input"))
    with col_src2:
        selected_week = int(st.number_input("KW", min_value=1, max_value=53, value=int(st.session_state["hf_selected_week_input"]), step=1, key="hf_selected_week_input"))
    drive_service = get_google_drive_service(SERVICE_ACCOUNT_PATH, readonly=False)
    sheets_service = get_google_sheets_service(SERVICE_ACCOUNT_PATH)
    use_google_drive = drive_service is not None
    hf_sheet_recipe_weights, hf_hidden_recipe_keys, hf_sheet_week_label = load_hf_buero_recipe_weights(
        sheets_service,
        HF_WEIGHING_SPREADSHEET_ID,
    )

    if use_google_drive:
        available_cutoffs = drive_list_cutoff_dirs(drive_service, PDL_DRIVE_FOLDER_ID, selected_year, selected_week)
    else:
        available_cutoffs = list_cutoff_dirs(PDL_BASE_DIR, selected_year, selected_week)

    recommended_cutoffs = [recommended_cutoff] if recommended_cutoff in available_cutoffs else []
    cutoff_auto_signature = f"{selected_year}-W{selected_week:02d}-{recommended_cutoff}"
    current_cutoff_selection = [
        cutoff for cutoff in st.session_state.get("hf_selected_cutoffs_multiselect", [])
        if cutoff in available_cutoffs
    ]
    if st.session_state.get("hf_cutoff_auto_signature") != cutoff_auto_signature:
        if current_cutoff_selection:
            st.session_state["hf_selected_cutoffs_multiselect"] = current_cutoff_selection
        else:
            st.session_state["hf_selected_cutoffs_multiselect"] = recommended_cutoffs
        st.session_state["hf_cutoff_auto_signature"] = cutoff_auto_signature
    elif "hf_selected_cutoffs_multiselect" not in st.session_state:
        st.session_state["hf_selected_cutoffs_multiselect"] = recommended_cutoffs

    if recommended_cutoff in available_cutoffs:
        st.caption(
            f"HF-Woche startet am Donnerstag mit DELETE.DEL. Heutiger Vorschlag: {recommended_cutoff} fuer KW {default_week:02d}."
        )
    else:
        st.caption(
            f"HF-Woche startet am Donnerstag mit DELETE.DEL. Heutiger Standard-Cutoff laut Plan: {recommended_cutoff}."
        )
    st.caption("HF-Cutoff-Plan: CO1 Freitag, CO2 Samstag/Sonntag, CO3 Montag, CO4 Dienstag, CO5 Mittwoch, CO6 Donnerstag.")
    if hf_sheet_recipe_weights:
        source_week_text = hf_sheet_week_label or "unbekannt"
        st.caption(
            f"Einwiegungsquelle fuer RESET: GSheet Linien Wiegung / Büro ({source_week_text}), nur VE-Rezepte; ausgeblendete Rezepte werden ignoriert."
        )
        selected_week_label = format_week_label(selected_year, selected_week)
        if hf_sheet_week_label and hf_sheet_week_label != selected_week_label:
            st.warning(
                f"Das GSheet steht aktuell auf {hf_sheet_week_label}, die App aber auf {selected_week_label}. RESET verwendet die Gewichte aus dem GSheet-Stand."
            )
    else:
        st.caption("Einwiegungsquelle fuer RESET: GSheet Linien Wiegung / Büro konnte nicht gelesen werden.")

    with col_src3:
        selected_cutoffs = st.multiselect(
            "Cutoffs",
            options=available_cutoffs,
            key="hf_selected_cutoffs_multiselect",
        )
        primary_cutoff = selected_cutoffs[0] if selected_cutoffs else None
    set_app_settings({
        "hf_mode": hf_mode,
        "hf_selected_year": selected_year,
        "hf_selected_week": selected_week,
        "hf_selected_cutoffs": list(selected_cutoffs),
    })

    picklist_meta = None
    pdl_metas = []
    output_folder_metas = []
    output_reset_metas = []
    output_tracking_metas = []
    official_bedarf_metas = []
    official_bedarf_paths = []
    picklist_path = None
    pdl_paths = []

    if selected_cutoffs and use_google_drive:
        picklist_meta = drive_find_picklist_file(drive_service, PICKLIST_DRIVE_FOLDER_ID, selected_year, selected_week)
        for cutoff in selected_cutoffs:
            pdl_meta = drive_find_pdl_file(drive_service, PDL_DRIVE_FOLDER_ID, selected_year, selected_week, cutoff)
            if pdl_meta is not None:
                pdl_metas.append({**pdl_meta, "cutoff": cutoff})
            output_folder_meta = drive_find_latest_output_folder(drive_service, OUTPUTS_DRIVE_FOLDER_ID, selected_week, cutoff)
            if output_folder_meta is not None:
                output_folder_metas.append({**output_folder_meta, "cutoff": cutoff})
                output_reset_meta = drive_find_child_file(drive_service, output_folder_meta["id"], "RESET.csv")
                if output_reset_meta is not None:
                    output_reset_metas.append({**output_reset_meta, "cutoff": cutoff})
                output_tracking_meta = drive_find_child_file(drive_service, output_folder_meta["id"], "Tracking.csv")
                if output_tracking_meta is not None:
                    output_tracking_metas.append({**output_tracking_meta, "cutoff": cutoff})
            official_bedarf_meta = drive_find_bedarf_workbook(drive_service, BEDARF_OUTPUTS_DRIVE_FOLDER_ID, selected_year, selected_week, cutoff)
            if official_bedarf_meta is not None:
                official_bedarf_metas.append({**official_bedarf_meta, "cutoff": cutoff})
    elif selected_cutoffs:
        picklist_path = find_picklist_file(PICKLIST_BASE_DIR, selected_year, selected_week)
        for cutoff in selected_cutoffs:
            pdl_path = find_pdl_file(PDL_BASE_DIR, selected_year, selected_week, cutoff)
            if pdl_path is not None:
                pdl_paths.append((cutoff, pdl_path))
            official_bedarf_path = find_bedarf_workbook(BEDARF_BASE_DIR, selected_year, selected_week, cutoff)
            if official_bedarf_path is not None:
                official_bedarf_paths.append((cutoff, official_bedarf_path))

    if use_google_drive:
        st.caption("Quelle: Google Drive per Service Account")
    else:
        st.caption("Quelle: lokaler Fallback")
    if not available_cutoffs:
        st.info("Für die gewählte KW wurden keine Cutoffs gefunden.")
    elif not selected_cutoffs:
        st.info("Bitte zuerst mindestens einen Cutoff auswählen. Erst danach werden HelloFresh-Dateien geladen.")
    st.caption(f"Pickliste: {picklist_meta['name'] if picklist_meta else (picklist_path if picklist_path else 'nicht geladen')}")
    st.caption(f"PDL: {join_named_items([item['name'] for item in pdl_metas] if use_google_drive else [str(path) for _, path in pdl_paths])}")
    st.caption(f"Weight-Calculator Output: {join_named_items([item['name'] for item in output_folder_metas])}")
    st.caption(f"Bedarfsrechner Output: {join_named_items([item['name'] for item in official_bedarf_metas] if use_google_drive else [path.name for _, path in official_bedarf_paths])}")
    if selected_cutoffs and use_google_drive and len(output_reset_metas) < len(selected_cutoffs):
        st.warning("Für perfekte RESET- und Tracking-Ergebnisse sollte der passende Weight-Calculator-Output-Ordner vorhanden sein. Aktuell läuft die App teilweise im Fallback-Modus.")

    use_manual_pdl = st.toggle("Manuellen PDL-Upload verwenden", value=bool(get_app_setting("hf_use_manual_pdl", False)), key="hf_use_manual_pdl_toggle")
    set_app_settings({"hf_use_manual_pdl": use_manual_pdl})

    manual_pdl_files = None
    if use_manual_pdl:
        manual_pdl_files = st.file_uploader(
            "PDL Excel (.xlsx) – mehrere möglich",
            type=["xlsx"], key="hf_pdl", accept_multiple_files=True,
        )

    # ── Load fixed weight files from disk (editable in expander) ──
    sku_df_raw = None
    pkg_df_raw = None
    if SKU_WEIGHTS_PATH.exists():
        sku_df_raw = read_excel_from_path(str(SKU_WEIGHTS_PATH), get_path_mtime_ns(SKU_WEIGHTS_PATH))
    if PKG_WEIGHTS_PATH.exists():
        pkg_df_raw = read_excel_from_path(str(PKG_WEIGHTS_PATH), get_path_mtime_ns(PKG_WEIGHTS_PATH))

    with st.expander("Gewichts-Stammdaten (SKU & Packaging)", expanded=False):
        st.caption("Daten werden aus `Import HelloFresh/` geladen. Änderungen hier gelten nur für diese Session.")

        col_w1, col_w2 = st.columns(2)
        with col_w1:
            st.markdown("**SKU Weights**")
            if sku_df_raw is not None:
                sku_df_edit = st.data_editor(
                    sku_df_raw, use_container_width=True, num_rows="dynamic",
                    key="hf_sku_edit",
                )
            else:
                st.warning(f"`{SKU_WEIGHTS_PATH}` nicht gefunden.")
                sku_df_edit = None
        with col_w2:
            st.markdown("**Packaging Weights**")
            if pkg_df_raw is not None:
                pkg_df_edit = st.data_editor(
                    pkg_df_raw, use_container_width=True, num_rows="dynamic",
                    key="hf_pkg_edit",
                )
            else:
                st.warning(f"`{PKG_WEIGHTS_PATH}` nicht gefunden.")
                pkg_df_edit = None

    with st.expander("Substitutfinder", expanded=False):
        st.caption("Suche nach möglichen Substituten für fehlende Artikel oder SKU-Codes. Wenn der Ersatz schwerer ist, wird eine Empfehlung für RESET/Korrektur ausgegeben.")
        substitute_master_df = pd.DataFrame()
        substitute_missing_df = pd.DataFrame()
        if sheets_service is not None:
            substitute_master_df = load_sheet_dataframe(sheets_service, SUBSTITUTE_SHEET_ID, "SKU Sub List", header_row_index=1, end_col="AN", end_row=4000)
            substitute_missing_df = load_sheet_dataframe(sheets_service, SUBSTITUTE_SHEET_ID, "SKUs eigentlich ohne Substitut", header_row_index=0, end_col="AZ", end_row=7000)

        search_options, search_query_map = build_substitute_search_catalog(substitute_master_df, substitute_missing_df)
        search_col1, search_col2 = st.columns([1, 2])
        with search_col1:
            quick_search_item = st.selectbox(
                "Schnellwahl",
                options=[""] + search_options,
                index=0,
                help="Ein einzelner Artikel mit Suchfeld und Autovervollstandigung.",
                key="hf_substitute_quick_search",
            )
        with search_col2:
            selected_search_items = st.multiselect(
                "Mehrfachauswahl",
                options=search_options,
                default=[],
                placeholder="z. B. Aioli eingeben und passende Artikel auswahlen",
                help="Autocomplete direkt aus dem Substitutions-Sheet. Mehrere Artikel sind moglich.",
                key="hf_substitute_search_options",
            )
        missing_items_input = st.text_area(
            "Fehlende Artikel / SKU-Codes (eine Zeile pro Eintrag)",
            placeholder="z. B.\nPHF-11-135488-3\nKräuterseitlinge 150g",
            key="hf_substitute_queries",
            height=120,
        )

        sku_lookup_df = sku_df_edit if 'sku_df_edit' in locals() and sku_df_edit is not None else sku_df_raw
        sku_weights_by_code, sku_weights_by_name = build_sku_weight_lookup(sku_lookup_df)

        if st.button("Substitute prüfen", key="hf_substitute_check"):
            queries = []
            if quick_search_item and quick_search_item in search_query_map:
                queries.append(search_query_map[quick_search_item])
            queries = [search_query_map[item] for item in selected_search_items if item in search_query_map]
            if quick_search_item and quick_search_item in search_query_map:
                queries.insert(0, search_query_map[quick_search_item])
            queries.extend(line.strip() for line in missing_items_input.splitlines() if line.strip())
            queries = unique_preserve_order(queries)
            results = []
            for query in queries:
                matches = find_substitute_matches(query, substitute_master_df, substitute_missing_df)
                if not matches:
                    results.append({
                        "Suche": query,
                        "Original SKU": "",
                        "Original Artikel": "nicht gefunden",
                        "Packaging Type": "",
                        "Originalgewicht (g)": "",
                        "Substitute 1": "keine Treffer",
                        "Substitute 2": "",
                        "Changed allergen? A": "",
                        "Substitute 3": "",
                        "Changed allergen? B": "",
                        "Substitute 4": "",
                        "Gewichtscheck": "manuell prüfen",
                        "Priorität": "HIGH",
                        "Hinweis": "",
                        "Quelle": "",
                    })
                    continue

                for match in matches:
                    original_weight = resolve_item_weight(
                        match["original_name"],
                        match["original_sku"],
                        sku_weights_by_code,
                        sku_weights_by_name,
                    )
                    row_result = {
                        "Suche": query,
                        "Original SKU": match["original_sku"],
                        "Original Artikel": match["original_name"],
                        "Packaging Type": match.get("packaging_type", ""),
                        "Originalgewicht (g)": "" if original_weight is None else round(original_weight, 1),
                        "Substitute 1": "",
                        "Substitute 2": "",
                        "Changed allergen? A": "",
                        "Substitute 3": "",
                        "Changed allergen? B": "",
                        "Substitute 4": "",
                        "Gewichtscheck": "",
                        "Priorität": "LOW",
                        "Hinweis": match.get("quality_comment", ""),
                        "Quelle": match["source_tab"],
                    }
                    weight_notes = []
                    priorities = []
                    for slot in match.get("slots", []):
                        evaluated = evaluate_substitute_option(slot, original_weight, sku_weights_by_code, sku_weights_by_name)
                        slot_number = int(slot.get("slot", 0))
                        if slot_number < 1 or slot_number > 4:
                            continue
                        row_result[f"Substitute {slot_number}"] = evaluated["text"]
                        if slot_number == 2:
                            row_result["Changed allergen? A"] = evaluated["changed_allergen"]
                        elif slot_number == 3:
                            row_result["Changed allergen? B"] = evaluated["changed_allergen"]
                        if evaluated["text"]:
                            priorities.append(classify_substitute_priority(evaluated.get("delta"), evaluated.get("recommendation", "")))
                            if evaluated.get("recommendation"):
                                note = f"S{slot_number}: {evaluated['recommendation']}"
                                if evaluated.get("delta") not in (None, ""):
                                    note += f" ({evaluated['delta']:+.1f} g)"
                                weight_notes.append(note)
                    row_result["Gewichtscheck"] = " | ".join(weight_notes)
                    row_result["Priorität"] = combine_substitute_priority(priorities) if priorities else "LOW"
                    results.append(row_result)

            if results:
                result_df = pd.DataFrame(results)
                result_df = result_df.sort_values(["Priorität", "Suche", "Original Artikel"], ascending=[True, True, True])
                metric_col1, metric_col2, metric_col3 = st.columns(3)
                metric_col1.metric("HIGH", int((result_df["Priorität"] == "HIGH").sum()))
                metric_col2.metric("MEDIUM", int((result_df["Priorität"] == "MEDIUM").sum()))
                metric_col3.metric("LOW", int((result_df["Priorität"] == "LOW").sum()))
                st.dataframe(
                    format_substitute_result_table(result_df),
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Original SKU": st.column_config.TextColumn("SKU Code", width="medium"),
                        "Original Artikel": st.column_config.TextColumn("Ingredient", width="large"),
                        "Packaging Type": st.column_config.TextColumn("Packaging Type", width="small"),
                        "Substitute 1": st.column_config.TextColumn("Substitute 1", width="large"),
                        "Substitute 2": st.column_config.TextColumn("Substitute 2", width="large"),
                        "Changed allergen? A": st.column_config.TextColumn("Changed allergen?", width="small"),
                        "Substitute 3": st.column_config.TextColumn("Substitute 3", width="large"),
                        "Changed allergen? B": st.column_config.TextColumn("Changed allergen?", width="small"),
                        "Substitute 4": st.column_config.TextColumn("Substitute 4", width="large"),
                        "Originalgewicht (g)": st.column_config.NumberColumn("Originalgewicht (g)", format="%.1f"),
                        "Gewichtscheck": st.column_config.TextColumn("Gewichtscheck", width="large"),
                        "Priorität": st.column_config.TextColumn("Priorität", width="small"),
                        "Hinweis": st.column_config.TextColumn("Hinweis", width="large"),
                        "Quelle": st.column_config.TextColumn("Quelle", width="medium"),
                        "Suche": st.column_config.TextColumn("Suche", width="medium"),
                    },
                )
                st.download_button(
                    "Substitute-Ergebnis herunterladen",
                    data=dataframe_to_csv_bytes(result_df),
                    file_name="hf_substitute_results.csv",
                    mime="text/csv",
                    on_click="ignore",
                )
            else:
                st.info("Keine Eingaben vorhanden.")

    pdl_df = None
    pdl_source_label = ""

    if use_manual_pdl and manual_pdl_files:
        with st.spinner("PDL wird geladen..."):
            frames = []
            for f in manual_pdl_files:
                df = read_excel_from_bytes(f.getvalue())
                df["_source"] = f.name
                frames.append(df)
            pdl_df = pd.concat(frames, ignore_index=True)
            pdl_source_label = f"{len(manual_pdl_files)} Upload-Datei(en)"
    elif use_google_drive and pdl_metas:
        with st.spinner("PDL wird aus Google Drive geladen..."):
            frames = []
            for pdl_meta in pdl_metas:
                df = drive_download_excel(drive_service, pdl_meta["id"])
                df["_source"] = pdl_meta["name"]
                df["_cutoff"] = pdl_meta["cutoff"]
                frames.append(df)
            pdl_df = pd.concat(frames, ignore_index=True) if frames else None
            pdl_source_label = f"Google Drive: {join_named_items([item['name'] for item in pdl_metas])}"
    elif pdl_paths:
        with st.spinner("PDL wird von G: geladen..."):
            frames = []
            for cutoff, pdl_path in pdl_paths:
                if not pdl_path.exists():
                    continue
                df = read_excel_from_path(str(pdl_path), get_path_mtime_ns(pdl_path))
                df["_source"] = str(pdl_path)
                df["_cutoff"] = cutoff
                frames.append(df)
            pdl_df = pd.concat(frames, ignore_index=True) if frames else None
            pdl_source_label = join_named_items([str(path) for _, path in pdl_paths])

    if pdl_df is not None:
        # Deduplicate by box_id (keep first occurrence)
        pdl_df["_box_id_str"] = pdl_df["box_id"].astype(str)
        pdl_df = pdl_df.drop_duplicates(subset="_box_id_str", keep="first")

        st.info(f"PDL geladen: {len(pdl_df)} eindeutige Boxen aus {pdl_source_label}")

        # ── Distribution Center filter ──
        if "distribution_center" in pdl_df.columns:
            dc_values = sorted(pdl_df["distribution_center"].dropna().unique())
            selected_dc = ["VE"]
            if "VE" in dc_values:
                filtered_df = pdl_df[pdl_df["distribution_center"] == "VE"].copy()
                st.caption("Distribution Center fuer HelloFresh ist fest auf VE gesetzt.")
            else:
                filtered_df = pdl_df.iloc[0:0].copy()
                st.warning("Im geladenen HelloFresh-Cutoff sind keine VE-Boxen enthalten. BX wird nicht verwendet.")
            set_app_settings({"hf_selected_dc": selected_dc})
        else:
            filtered_df = pdl_df.copy()

        # ── Country filter ──
        countries = sorted(filtered_df["country"].unique())
        selected_countries = st.multiselect(
            "Länder filtern", countries, default=([value for value in get_app_setting("hf_selected_countries", (["DE"] if "DE" in countries else countries)) if value in countries] or (["DE"] if "DE" in countries else countries))
        )
        filtered_df = filtered_df[filtered_df["country"].isin(selected_countries)].copy()
        set_app_settings({"hf_selected_countries": selected_countries})
        st.info(f"Gefiltert: {len(filtered_df)} Boxen")

        picklist_df = None
        if use_google_drive and picklist_meta is not None:
            with st.spinner("Pickliste wird aus Google Drive geladen..."):
                picklist_df = drive_download_excel(drive_service, picklist_meta["id"])
            st.caption(f"Pickliste geladen: Google Drive / {picklist_meta['name']}")
        elif picklist_path is not None and picklist_path.exists():
            with st.spinner("Pickliste wird von G: geladen..."):
                picklist_df = read_excel_from_path(str(picklist_path), get_path_mtime_ns(picklist_path))
            st.caption(f"Pickliste geladen: {picklist_path}")
        else:
            st.warning("Pickliste für die gewählte KW wurde nicht gefunden.")

        available_bag_sizes = []
        bag_size_col = first_existing_column(filtered_df, ["bag_size", "BagSize"])
        if bag_size_col is not None:
            available_bag_sizes = sorted(
                {
                    int(value)
                    for value in pd.to_numeric(filtered_df[bag_size_col], errors="coerce").dropna().tolist()
                    if int(value) in {2, 3, 4}
                }
            )

        with st.expander("Bedarfsrechner", expanded=False):
            st.caption("Offiziellen Bedarfsrechner-Output anzeigen oder den Bedarf direkt aus der aktuell geladenen PDL und Pickliste neu berechnen.")

            official_bedarf_entries = []
            if use_google_drive:
                for item in official_bedarf_metas:
                    official_bedarf_entries.append({
                        "label": f"{item['cutoff']} - {item['name']}",
                        "name": item["name"],
                        "bytes": drive_download_bytes(drive_service, item["id"]),
                    })
            else:
                for cutoff, path in official_bedarf_paths:
                    if path.exists():
                        official_bedarf_entries.append({
                            "label": f"{cutoff} - {path.name}",
                            "name": path.name,
                            "bytes": path.read_bytes(),
                        })

            if official_bedarf_entries:
                st.success(f"Offizielle Bedarf-Dateien gefunden: {len(official_bedarf_entries)}")
                selected_official_entry_label = st.selectbox(
                    "Offizielle Bedarf-Datei",
                    options=[entry["label"] for entry in official_bedarf_entries],
                    index=(([entry["label"] for entry in official_bedarf_entries].index(get_app_setting("hf_official_bedarf_label", official_bedarf_entries[0]["label"])) if get_app_setting("hf_official_bedarf_label", official_bedarf_entries[0]["label"]) in [entry["label"] for entry in official_bedarf_entries] else 0) if official_bedarf_entries else 0),
                    key="hf_bedarf_official_file",
                )
                set_app_settings({"hf_official_bedarf_label": selected_official_entry_label})
                official_bedarf_entry = next(entry for entry in official_bedarf_entries if entry["label"] == selected_official_entry_label)
                official_sheet_names = read_excel_sheet_names(official_bedarf_entry["bytes"])
                official_sheet = st.selectbox(
                    "Offizielles Bedarf-Blatt",
                    options=official_sheet_names,
                    index=(official_sheet_names.index(get_app_setting("hf_official_bedarf_sheet", official_sheet_names[0])) if get_app_setting("hf_official_bedarf_sheet", official_sheet_names[0]) in official_sheet_names else 0),
                    key="hf_bedarf_official_sheet",
                )
                set_app_settings({"hf_official_bedarf_sheet": official_sheet})
                official_preview_df = read_excel_from_bytes(official_bedarf_entry["bytes"], sheet_name=official_sheet)
                st.dataframe(official_preview_df.head(50), use_container_width=True, hide_index=True)
                st.download_button(
                    "Offiziellen Bedarf herunterladen",
                    data=official_bedarf_entry["bytes"],
                    file_name=official_bedarf_entry["name"],
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    on_click="ignore",
                )
            else:
                st.info("Für die gewählte Woche wurde kein offizieller Bedarfsrechner-Output gefunden.")

            selected_bedarf_sizes = st.multiselect(
                "Größen für Live-Bedarfsrechnung",
                options=available_bag_sizes,
                default=([value for value in get_app_setting("hf_selected_bedarf_sizes", available_bag_sizes) if value in available_bag_sizes] or available_bag_sizes),
                key="hf_bedarf_sizes",
            )
            set_app_settings({"hf_selected_bedarf_sizes": selected_bedarf_sizes})

            if st.button("Bedarf live berechnen", key="hf_bedarf_calculate"):
                if picklist_df is None or picklist_df.empty:
                    st.error("Für die Live-Bedarfsrechnung wird eine Pickliste benötigt.")
                elif filtered_df.empty:
                    st.error("Für die Live-Bedarfsrechnung sind nach den Filtern keine Boxen vorhanden.")
                elif not selected_bedarf_sizes:
                    st.error("Bitte mindestens eine Größe für die Live-Bedarfsrechnung auswählen.")
                else:
                    sheet_groups = []
                    total_pouch_counts: dict[str, int] = {}
                    total_box_counts: dict[str, int] = {}
                    total_ingredient_df_list = []

                    if "_source" in filtered_df.columns and filtered_df["_source"].nunique() > 1:
                        grouped_items = list(filtered_df.groupby("_source", sort=False))
                    else:
                        grouped_items = [(primary_cutoff, filtered_df)]

                    for source_name, source_df in grouped_items:
                        ingredient_df, pouch_counts, box_counts = compute_bedarf_counts(source_df, picklist_df, selected_bedarf_sizes)
                        if ingredient_df.empty and not pouch_counts and not box_counts:
                            continue
                        for pouch_name, count in pouch_counts.items():
                            total_pouch_counts[pouch_name] = total_pouch_counts.get(pouch_name, 0) + count
                        for box_name, count in box_counts.items():
                            total_box_counts[box_name] = total_box_counts.get(box_name, 0) + count
                        total_ingredient_df_list.append(ingredient_df)
                        sheet_groups.append((make_bedarf_sheet_name(str(source_name), source_df.get("_cutoff", pd.Series([primary_cutoff])).iloc[0] if "_cutoff" in source_df.columns and not source_df.empty else primary_cutoff), ingredient_df, pouch_counts, box_counts))

                    if not sheet_groups:
                        st.error("Aus der aktuellen PDL konnten keine Bedarfsdaten erzeugt werden.")
                    else:
                        all_pouches = sorted(total_pouch_counts.keys())
                        all_boxes = sorted(total_box_counts.keys())
                        formatted_sheets = []
                        for sheet_name, ingredient_df, pouch_counts, box_counts in sheet_groups:
                            formatted_sheets.append((
                                sheet_name,
                                format_bedarf_sheet(
                                    ingredient_df,
                                    pouch_counts,
                                    box_counts,
                                    all_pouches,
                                    all_boxes,
                                    "Pouch-Zählung",
                                    "Box-Zählung",
                                ),
                            ))

                        total_ingredient_df = pd.concat(total_ingredient_df_list, ignore_index=True)
                        total_ingredient_df = total_ingredient_df.groupby("Ingredient", as_index=False)["Insgesamte Menge"].sum()
                        total_ingredient_df["Insgesamte Menge"] = total_ingredient_df["Insgesamte Menge"].round(3)
                        total_ingredient_df = total_ingredient_df.sort_values("Ingredient").reset_index(drop=True)

                        workbook_bytes = build_bedarf_workbook_bytes(
                            formatted_sheets,
                            total_ingredient_df,
                            total_pouch_counts,
                            total_box_counts,
                            all_pouches,
                            all_boxes,
                        )
                        st.session_state["hf_bedarf_workbook_bytes"] = workbook_bytes
                        if len(selected_cutoffs) == 1:
                            workbook_name = f"Berechnung_Ergebnis{normalize_cutoff_label(primary_cutoff)}.xlsx"
                        else:
                            workbook_name = f"Berechnung_Ergebnis_KW{int(selected_week):02d}_{len(selected_cutoffs)}Cutoffs.xlsx"
                        st.session_state["hf_bedarf_workbook_name"] = workbook_name
                        st.session_state["hf_bedarf_preview_sheets"] = {name: df for name, df in formatted_sheets}
                        st.session_state["hf_bedarf_preview_sheets"]["Gesamt"] = format_bedarf_sheet(
                            total_ingredient_df,
                            total_pouch_counts,
                            total_box_counts,
                            all_pouches,
                            all_boxes,
                            "Gesamt-Pouch-Zählung",
                            "Gesamt-Box-Zählung",
                        )

            if "hf_bedarf_workbook_bytes" in st.session_state and "hf_bedarf_preview_sheets" in st.session_state:
                preview_sheet_names = list(st.session_state["hf_bedarf_preview_sheets"].keys())
                preview_sheet = st.selectbox(
                    "Live-Bedarf Blatt",
                    options=preview_sheet_names,
                    key="hf_bedarf_live_sheet",
                )
                st.dataframe(
                    st.session_state["hf_bedarf_preview_sheets"][preview_sheet].head(50),
                    use_container_width=True,
                    hide_index=True,
                )
                st.download_button(
                    "Live-Bedarf herunterladen",
                    data=st.session_state["hf_bedarf_workbook_bytes"],
                    file_name=st.session_state.get("hf_bedarf_workbook_name", "Berechnung_Ergebnis.xlsx"),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    on_click="ignore",
                )

        if picklist_df is not None and not picklist_df.empty and missing_items_input.strip():
            with st.expander("Substitutfinder: Batch-/Box-Auswirkung", expanded=False):
                base_reset_map = {}
                sheet_reset_map, _, _ = build_hf_reset_map_from_sheet(
                    unique_preserve_order(filtered_df["batch"].astype(str).tolist()),
                    hf_sheet_recipe_weights,
                    hf_hidden_recipe_keys,
                )
                if sheet_reset_map:
                    base_reset_map.update(sheet_reset_map)
                elif use_google_drive and output_reset_metas:
                    for output_reset_meta in output_reset_metas:
                        base_reset_map.update(parse_reset_csv(drive_download_text(drive_service, output_reset_meta["id"])))
                queries = [line.strip() for line in missing_items_input.splitlines() if line.strip()]
                impact_df = compute_substitute_impacts(
                    queries,
                    substitute_master_df,
                    substitute_missing_df,
                    picklist_df,
                    filtered_df,
                    sku_weights_by_code,
                    sku_weights_by_name,
                    base_reset_map,
                )
                if impact_df.empty:
                    st.info("Für die aktuelle PDL/Pickliste wurden keine konkreten Batch-Auswirkungen gefunden.")
                else:
                    impact_df["Priorität"] = impact_df.apply(
                        lambda row: classify_impact_priority(row.get("Delta pro Box (kg)"), int(row.get("Betroffene Boxen", 0))),
                        axis=1,
                    )
                    impact_df = impact_df.sort_values(["Priorität", "Suche", "Delta pro Box (kg)"], ascending=[True, True, False])
                    impact_col1, impact_col2, impact_col3 = st.columns(3)
                    impact_col1.metric("HIGH", int((impact_df["Priorität"] == "HIGH").sum()))
                    impact_col2.metric("MEDIUM", int((impact_df["Priorität"] == "MEDIUM").sum()))
                    impact_col3.metric("LOW", int((impact_df["Priorität"] == "LOW").sum()))
                    st.dataframe(
                        impact_df,
                        use_container_width=True,
                        hide_index=True,
                    )
                    st.download_button(
                        "Impact-Tabelle herunterladen",
                        data=dataframe_to_csv_bytes(impact_df),
                        file_name="hf_substitute_impacts.csv",
                        mime="text/csv",
                        on_click="ignore",
                    )

        # ── Recipe Priority Mapping (BagSize encoding) ──
        all_recipes = set()
        for batch in filtered_df["batch"].astype(str):
            parts = batch.rsplit("-", 1)
            for r in parts[0].split():
                if r.isdigit():
                    all_recipes.add(int(r))

        MULTIPLIER_OPTIONS = [1, 100, 1000, 10000, 1000000]

        with st.expander("Rezept-Prioritäten (BagSize-Multiplikator)", expanded=False):
            st.markdown(
                "Jedes Rezept hat eine Priorität. Der BagSize-Multiplikator eines Batches "
                "= **max(Priorität aller Rezepte im Batch)**.  \n"
                "Mögliche Werte: **1** (Standard), **100**, **1000**, **10000**, **1000000**"
            )

            priority_data = []
            for r in sorted(all_recipes):
                priority_data.append({
                    "Rezept": r,
                    "Priorität": HF_RECIPE_PRIORITY_DEFAULT.get(r, 1),
                })
            priority_df = pd.DataFrame(priority_data)

            edited_priority = st.data_editor(
                priority_df,
                column_config={
                    "Rezept": st.column_config.NumberColumn("Rezept", disabled=True),
                    "Priorität": st.column_config.SelectboxColumn(
                        "Priorität", options=MULTIPLIER_OPTIONS, required=True,
                    ),
                },
                hide_index=True,
                use_container_width=True,
                key="hf_recipe_priority",
            )

            recipe_priority_map = dict(
                zip(
                    edited_priority["Rezept"].astype(int),
                    edited_priority["Priorität"].astype(int),
                )
            )

        # Fall back to defaults when expander was never opened
        if "recipe_priority_map" not in dir():
            recipe_priority_map = {r: HF_RECIPE_PRIORITY_DEFAULT.get(r, 1) for r in all_recipes}

        # ── Process ──
        if st.button("Generieren", key="hf_process", type="primary"):
            tracking_lines = []
            reset_map = OrderedDict()  # combo → weight_kg
            hidden_reset_combos: list[str] = []
            missing_reset_combos: list[str] = []
            correction_map = {}
            correction_signature_map = {}
            correction_audit_rows = []
            correction_source_counts = {"exact": 0, "signature": 0, "fallback": 0}
            packaging_weights = build_packaging_weight_map(pkg_df_edit if 'pkg_df_edit' in locals() else pkg_df_raw)
            heaviest_batch_weights = build_batch_heaviest_weight_map(pdl_df, packaging_weights)
            unique_filtered_combos = unique_preserve_order(filtered_df["batch"].astype(str).tolist()) if not filtered_df.empty else []

            if use_google_drive and output_tracking_metas:
                for output_tracking_meta in output_tracking_metas:
                    tracking_output_text = drive_download_text(drive_service, output_tracking_meta["id"])
                    correction_map.update(build_tracking_correction_map(tracking_output_text))
                correction_signature_map = build_correction_signature_map(pdl_df, correction_map)

            for _, row in filtered_df.iterrows():
                box_id = str(row["box_id"]).strip()
                batch = str(row["batch"]).strip()
                bag_size = int(row["bag_size"])
                num_meals = int(row["Number of meals"])

                # meal_swap = batch field (already "recipes-Xp" format)
                meal_swap = batch

                # BagSize encoding via recipe priority
                multiplier = get_batch_multiplier(batch, recipe_priority_map)
                encoded_bag_size = bag_size * multiplier

                # Correction priority:
                # 1. exact box match from Weight-Calculator Tracking output
                # 2. same batch/variant signature
                # 3. fallback: difference to heaviest same-batch variant (pouch + ice)
                correction = correction_map.get(box_id)
                correction_source = "exact"
                if correction is None:
                    correction = correction_signature_map.get(build_correction_signature(row))
                    correction_source = "signature"
                if correction is None:
                    correction = estimate_correction_from_heaviest(row, packaging_weights, heaviest_batch_weights)
                    correction_source = "fallback"
                correction_source_counts[correction_source] = correction_source_counts.get(correction_source, 0) + 1
                correction_audit_rows.append({
                    "Box_ID": box_id,
                    "Meal Swap": meal_swap,
                    "Correction": normalize_correction_value(correction),
                    "Quelle": correction_source,
                })

                tracking_lines.append(
                    build_tracking_line(box_id, encoded_bag_size, num_meals,
                                        meal_swap, correction)
                )

                # RESET fallback: only used when no output RESET.csv exists
                if meal_swap not in reset_map:
                    reset_map[meal_swap] = 0.0

            tracking_csv = generate_tracking_csv(tracking_lines)
            reset_csv = None
            reset_preview_count = 0
            if hf_mode == "Tracking + RESET":
                sheet_reset_map, hidden_reset_combos, missing_reset_combos = build_hf_reset_map_from_sheet(
                    unique_filtered_combos,
                    hf_sheet_recipe_weights,
                    hf_hidden_recipe_keys,
                )
                if sheet_reset_map:
                    reset_csv = generate_reset_csv(sheet_reset_map)
                    reset_preview_count = len(sheet_reset_map)
                elif use_google_drive and output_reset_metas:
                    merged_reset_map = OrderedDict()
                    for output_reset_meta in output_reset_metas:
                        current_reset_map = parse_reset_csv(drive_download_text(drive_service, output_reset_meta["id"]))
                        for combo, weight in current_reset_map.items():
                            merged_reset_map[combo] = weight
                    reset_csv = generate_reset_csv(merged_reset_map)
                    reset_preview_count = len(merged_reset_map)
                else:
                    st.warning(
                        "Es wurde kein echter Weight-Calculator-RESET fuer die gewaehlte Woche/Cutoffs gefunden. "
                        "RESET.csv wird deshalb nicht mit 0,000-Platzhaltern erzeugt."
                    )
                if hidden_reset_combos:
                    st.warning(
                        f"{len(hidden_reset_combos)} Batch-Kombination(en) wurden fuer RESET uebersprungen, weil die Rezepte im GSheet fuer VE ausgeblendet sind: "
                        f"{join_named_items(hidden_reset_combos[:10])}"
                    )
                if missing_reset_combos:
                    st.info(
                        f"{len(missing_reset_combos)} Batch-Kombination(en) haben noch keine vollstaendigen Einwiegungen im GSheet und fehlen deshalb in RESET: "
                        f"{join_named_items(missing_reset_combos[:10])}"
                    )

            # ── Preview ──
            st.subheader("Tracking.csv Vorschau")
            preview_lines = tracking_csv.split("\n")[:51]
            st.code("\n".join(preview_lines), language=None)
            st.caption(f"Gesamt: {len(tracking_lines)} Zeilen")

            with st.expander("Correction Audit", expanded=False):
                audit_col1, audit_col2, audit_col3 = st.columns(3)
                audit_col1.metric("Exact", correction_source_counts.get("exact", 0))
                audit_col2.metric("Signature", correction_source_counts.get("signature", 0))
                audit_col3.metric("Fallback", correction_source_counts.get("fallback", 0))
                correction_audit_df = pd.DataFrame(correction_audit_rows)
                st.dataframe(correction_audit_df, use_container_width=True, hide_index=True)
                st.download_button(
                    "Correction-Audit herunterladen",
                    data=dataframe_to_csv_bytes(correction_audit_df),
                    file_name="hf_correction_audit.csv",
                    mime="text/csv",
                    on_click="ignore",
                )

            if hf_mode == "Nur Tracking":
                st.info("Es wird nur Tracking.csv erzeugt. Das überbrückt die Einwiegung; die Waagen können mit den verfügbaren Korrekturwerten bereits anlaufen.")

            if reset_csv is not None:
                st.subheader("RESET.csv Vorschau")
                preview_reset = reset_csv.split("\n")[:31]
                st.code("\n".join(preview_reset), language=None)
                st.caption(f"Gesamt: {reset_preview_count} eindeutige Kombinationen")

            hf_export_dir = persist_export_files(
                HF_EXPORT_DIR,
                selected_week,
                selected_cutoffs,
                tracking_csv,
                reset_csv,
            )
            hf_scale_export_dir = persist_export_files(
                SCALE_SYNC_ROOT,
                selected_week,
                selected_cutoffs,
                tracking_csv,
                reset_csv,
            )
            hf_run_id = start_run(
                tool="pdl-fast",
                input_files=[str(item) for item in uploaded_files] if "uploaded_files" in locals() else [],
            )
            if hf_run_id:
                add_artifact(hf_run_id, "tracking_csv", hf_export_dir / "Tracking.csv", "text/csv")
                if (hf_export_dir / "RESET.csv").exists():
                    add_artifact(hf_run_id, "reset_csv", hf_export_dir / "RESET.csv", "text/csv")
                finish_run(
                    hf_run_id,
                    status="success",
                    metrics={
                        "company": "HelloFresh",
                        "week": format_week_label(selected_year, selected_week),
                        "trackingRows": int(len(tracking_lines)),
                        "resetRows": int(reset_preview_count),
                        "cutoffCount": int(len(selected_cutoffs)),
                    },
                    warnings=[],
                    errors=[],
                )
            st.info(f"Exportordner: {hf_export_dir.resolve()}")
            st.caption(f"Waagenordner / Sync: {hf_scale_export_dir.resolve()}")
            render_scale_sync_controls(
                "hf",
                "HelloFresh",
                hf_scale_export_dir,
            )
            render_drive_export_controls(
                "hf",
                "HelloFresh",
                drive_service,
                "hf_drive_export_folder_id",
                tracking_csv,
                reset_csv,
            )

            # ── Downloads ──
            col_dl1, col_dl2 = st.columns(2)
            with col_dl1:
                st.download_button(
                    "📥 Tracking.csv herunterladen",
                    data=csv_text_to_bytes(tracking_csv),
                    file_name="Tracking.csv",
                    mime="text/csv",
                    on_click="ignore",
                )
            with col_dl2:
                if reset_csv is not None:
                    st.download_button(
                        "📥 RESET.csv herunterladen",
                        data=csv_text_to_bytes(reset_csv),
                        file_name="RESET.csv",
                        mime="text/csv",
                        on_click="ignore",
                    )
