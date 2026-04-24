from __future__ import annotations

import io
import json
import re
import shutil
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
IMPORTS_DIR = BASE_DIR / "Imports"
EXPORTS_DIR = BASE_DIR / "Exports"
CENTRAL_RESULTS_DIR = BASE_DIR.parent / "Unified-Platform-Blueprint" / "results" / "incident-tool"
CONFIG_PATH = BASE_DIR / "config" / "country_rules.json"

BOXES_FILE_HINT = "boxes produced"
CUSTOMER_FILE_HINT = "custom_id"
PICKLIST_FILE_HINT = "marcel_picklist"
RESULT_FILE_HINT = "factor_incident_result"

BOXES_REQUIRED_COLUMNS = ["inducted_ts", "boxid", "recipes"]
CUSTOMER_REQUIRED_COLUMNS = ["boxid", "customer_id"]
TIME_PLACEHOLDER = "HH:MM"
DEFAULT_INCIDENT_ROWS = [
    {
        "enabled": True,
        "affected_recipe": "",
        "substitute_recipe": "ERSATZLOS",
        "start_mode": "TIME+BOX",
        "start_time": "",
        "start_box": "",
        "prefixes": "AUTO",
    }
]


@dataclass(frozen=True)
class CountryRule:
    country: str
    code: str
    prefixes: Tuple[str, ...]
    recipe_ranges: Tuple[Tuple[int, int], ...]


@dataclass(frozen=True)
class IncidentDefinition:
    incident_key: str
    affected_recipe: str
    affected_recipe_ids: Tuple[str, ...]
    substitute_recipe: str
    start_mode: str
    start_time: str
    start_box: str
    prefixes: Tuple[str, ...]
    start_dt: datetime


@dataclass
class ComputationArtifacts:
    log_df: pd.DataFrame
    details_df: pd.DataFrame
    per_incident: Dict[str, pd.DataFrame]
    errors: List[str]
    warnings: List[str]
    production_date: date


def load_country_rules(config_path: Path = CONFIG_PATH) -> Tuple[List[CountryRule], Dict[str, List[str]]]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    countries = [
        CountryRule(
            country=item["country"],
            code=item["code"],
            prefixes=tuple(str(prefix).upper() for prefix in item["prefixes"]),
            recipe_ranges=tuple((int(start), int(end)) for start, end in item["recipe_ranges"]),
        )
        for item in payload.get("countries", [])
    ]
    aliases = {
        str(key).upper(): [str(value).upper() for value in values]
        for key, values in payload.get("aliases", {}).items()
    }
    return countries, aliases


def list_import_files(imports_dir: Path = IMPORTS_DIR) -> Dict[str, List[str]]:
    if not imports_dir.exists():
        return {"csv": [], "xlsx": [], "all": []}
    files = sorted([item.name for item in imports_dir.iterdir() if item.is_file()])
    return {
        "csv": [name for name in files if name.lower().endswith(".csv")],
        "xlsx": [name for name in files if name.lower().endswith((".xlsx", ".xls")) and not name.startswith("~$")],
        "all": files,
    }


def detect_latest_file(candidates: Iterable[str], hint: str) -> Optional[str]:
    ranked = [name for name in candidates if hint.lower() in name.lower() and not name.startswith("~$")]
    return sorted(ranked)[-1] if ranked else None


def guess_default_files(imports_dir: Path = IMPORTS_DIR) -> Dict[str, Optional[str]]:
    files = list_import_files(imports_dir)
    return {
        "boxes": detect_latest_file(files["csv"], BOXES_FILE_HINT),
        "customers": detect_latest_file(files["csv"], CUSTOMER_FILE_HINT),
        "picklist": detect_latest_file(files["xlsx"], PICKLIST_FILE_HINT),
        "example_result": detect_latest_file(files["xlsx"], RESULT_FILE_HINT),
    }


def parse_datetime(value: Any) -> Optional[datetime]:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    dt = pd.to_datetime(text, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        return None
    return dt.to_pydatetime()


def normalize_box_id(value: Any) -> str:
    return str(value or "").strip().upper()


def parse_recipe_tokens(value: Any) -> List[str]:
    return re.findall(r"\d{3}", str(value or ""))


def recipe_occurrence_count(value: Any, target_recipe: str) -> int:
    return sum(1 for token in parse_recipe_tokens(value) if token == str(target_recipe).strip())


def has_recipe(value: Any, target_recipe: str) -> bool:
    return recipe_occurrence_count(value, target_recipe) > 0


def has_any_recipe(value: Any, target_recipes: Iterable[str]) -> bool:
    token_set = set(parse_recipe_tokens(value))
    return any(recipe in token_set for recipe in target_recipes)


def recipe_occurrence_count_any(value: Any, target_recipes: Iterable[str]) -> int:
    targets = set(target_recipes)
    return sum(1 for token in parse_recipe_tokens(value) if token in targets)


def load_boxes_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str, sep=None, engine="python")
    missing = [column for column in BOXES_REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Produktions-CSV fehlt Spalten: {', '.join(missing)}")
    prepared = df.copy()
    prepared["boxid"] = prepared["boxid"].astype(str).str.strip()
    prepared["__box_upper"] = prepared["boxid"].map(normalize_box_id)
    prepared["__dt"] = prepared["inducted_ts"].map(parse_datetime)
    prepared = prepared[prepared["__dt"].notna()].copy()
    if prepared.empty:
        raise ValueError("Produktions-CSV enthaelt keine gueltigen Zeiten in inducted_ts.")
    prepared.sort_values(["__dt", "__box_upper"], inplace=True, kind="mergesort")
    prepared.reset_index(drop=True, inplace=True)
    return prepared


def load_customer_dataframe(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(path, dtype=str)
    else:
        df = pd.read_csv(path, dtype=str, sep=None, engine="python")
    missing = [column for column in CUSTOMER_REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise ValueError(f"Custom-ID-Datei fehlt Spalten: {', '.join(missing)}")
    prepared = df.copy()
    prepared["boxid"] = prepared["boxid"].astype(str).str.strip()
    prepared["customer_id"] = prepared["customer_id"].astype(str).str.strip()
    prepared["__box_upper"] = prepared["boxid"].map(normalize_box_id)
    prepared = prepared[prepared["__box_upper"] != ""].drop_duplicates(subset=["__box_upper"], keep="last")
    return prepared[["__box_upper", "customer_id"]]


def parse_recipe_id(value: Any) -> str:
    match = re.search(r"(\d{3})", str(value or ""))
    return match.group(1) if match else ""


def load_picklist_mapping(path: Path) -> Dict[str, str]:
    df = pd.read_excel(path, dtype=str)
    columns = {str(column).strip().lower(): column for column in df.columns}
    recipe_column = None
    for candidate in columns:
        if candidate.startswith("recipe") or candidate.startswith("rez") or "rezept" in candidate:
            recipe_column = columns[candidate]
            break
    if recipe_column is None:
        recipe_column = df.columns[0]

    fe_column = None
    maitre_column = None
    for candidate, original in columns.items():
        if fe_column is None and (candidate == "fe nummer" or candidate.startswith("fe")):
            fe_column = original
        if maitre_column is None and candidate.startswith("maitre"):
            maitre_column = original

    mapping: Dict[str, str] = {}
    for _, row in df.iterrows():
        recipe_id = parse_recipe_id(row.get(recipe_column))
        if not recipe_id:
            continue
        preferred_value = ""
        if fe_column is not None:
            preferred_value = str(row.get(fe_column) or "").strip()
        if (not preferred_value or preferred_value.lower() == "nan") and maitre_column is not None:
            preferred_value = str(row.get(maitre_column) or "").strip()
        if not preferred_value or preferred_value.lower() == "nan":
            continue
        mapping[recipe_id] = preferred_value
    if not mapping:
        raise ValueError("Aus der Picklist konnte kein Rezept-Mapping gelesen werden.")
    return mapping


def load_picklist_bundle(path: Path) -> Tuple[Dict[str, str], Dict[str, Tuple[str, ...]]]:
    df = pd.read_excel(path, dtype=str)
    columns = {str(column).strip().lower(): column for column in df.columns}
    recipe_column = None
    for candidate in columns:
        if candidate.startswith("recipe") or candidate.startswith("rez") or "rezept" in candidate:
            recipe_column = columns[candidate]
            break
    if recipe_column is None:
        recipe_column = df.columns[0]

    fe_column = None
    maitre_column = None
    for candidate, original in columns.items():
        if fe_column is None and (candidate == "fe nummer" or candidate.startswith("fe") or candidate.startswith("fv")):
            fe_column = original
        if maitre_column is None and candidate.startswith("maitre"):
            maitre_column = original

    display_mapping: Dict[str, str] = {}
    reverse: Dict[str, set[str]] = {}

    for _, row in df.iterrows():
        recipe_id = parse_recipe_id(row.get(recipe_column))
        if not recipe_id:
            continue

        fe_value = str(row.get(fe_column) or "").strip() if fe_column is not None else ""
        maitre_value = str(row.get(maitre_column) or "").strip() if maitre_column is not None else ""

        preferred_value = fe_value if fe_value and fe_value.lower() != "nan" else maitre_value
        if preferred_value and preferred_value.lower() != "nan":
            display_mapping[recipe_id] = preferred_value

        for identifier in [recipe_id, fe_value, maitre_value]:
            normalized = str(identifier or "").strip().upper()
            if not normalized or normalized == "NAN":
                continue
            reverse.setdefault(normalized, set()).add(recipe_id)

    if not display_mapping:
        raise ValueError("Aus der Picklist konnte kein Rezept-Mapping gelesen werden.")

    reverse_index = {key: tuple(sorted(values)) for key, values in reverse.items()}
    return display_mapping, reverse_index


def infer_production_date(boxes_df: pd.DataFrame) -> date:
    return boxes_df["__dt"].iloc[0].date()


def recipe_matches_country(recipe_id: str, rule: CountryRule) -> bool:
    if not recipe_id.isdigit():
        return False
    number = int(recipe_id)
    return any(start <= number <= end for start, end in rule.recipe_ranges)


def resolve_prefixes(raw_prefix_value: Any, affected_recipe: str, start_box_value: str, countries: List[CountryRule], aliases: Dict[str, List[str]]) -> Tuple[str, ...]:
    all_prefixes = tuple(sorted({prefix for country in countries for prefix in country.prefixes}))
    raw_text = str(raw_prefix_value or "AUTO").strip().upper()
    if not raw_text or raw_text == "AUTO":
        start_prefix = normalize_box_id(start_box_value)[:2]
        if start_prefix in all_prefixes:
            return (start_prefix,)
        recipe_based = sorted({prefix for country in countries if recipe_matches_country(affected_recipe, country) for prefix in country.prefixes})
        if recipe_based:
            return tuple(recipe_based)
        return all_prefixes

    if raw_text in aliases:
        alias_values = aliases[raw_text]
        if alias_values == ["*"]:
            return all_prefixes
        return tuple(sorted(set(alias_values)))

    resolved: List[str] = []
    for token in re.split(r"[,;\s]+", raw_text):
        token = token.strip().upper()
        if not token:
            continue
        if token in aliases:
            alias_values = aliases[token]
            if alias_values == ["*"]:
                return all_prefixes
            resolved.extend(alias_values)
        else:
            resolved.append(token)
    cleaned = tuple(sorted(prefix for prefix in set(resolved) if prefix in all_prefixes))
    return cleaned or all_prefixes


def normalize_start_time(production_date: date, value: str) -> datetime:
    match = re.fullmatch(r"(\d{2}):(\d{2})", str(value or "").strip())
    if not match:
        raise ValueError(f"Ungueltige Startzeit '{value}'. Erwartet HH:MM.")
    hour, minute = int(match.group(1)), int(match.group(2))
    return datetime.combine(production_date, time(hour=hour, minute=minute))


def find_box_start_datetime(boxes_df: pd.DataFrame, start_box_value: str) -> datetime:
    box_upper = normalize_box_id(start_box_value)
    if not box_upper:
        raise ValueError("Startbox ist leer.")
    matches = boxes_df[boxes_df["__box_upper"] == box_upper]
    if matches.empty:
        raise ValueError(f"Start-Box '{start_box_value}' wurde in der Produktions-CSV nicht gefunden.")
    return matches.iloc[0]["__dt"]


def determine_start_datetime(
    boxes_df: pd.DataFrame,
    production_date: date,
    start_mode: str,
    start_time_value: str,
    start_box_value: str,
) -> datetime:
    normalized_mode = str(start_mode or "BOX").strip().upper()
    if normalized_mode == "TIME":
        return normalize_start_time(production_date, start_time_value)
    if normalized_mode == "TIME+BOX":
        if str(start_time_value or "").strip():
            return normalize_start_time(production_date, start_time_value)
        return find_box_start_datetime(boxes_df, start_box_value)
    return find_box_start_datetime(boxes_df, start_box_value)


def resolve_affected_recipe_ids(
    affected_recipe: str,
    recipe_mapping: Dict[str, str],
    identifier_index: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> Tuple[str, ...]:
    value = str(affected_recipe or "").strip().upper()
    if not value:
        raise ValueError("Betroffenes Rezept ist leer.")
    if value.isdigit() and len(value) == 3:
        return (value,)

    if identifier_index and value in identifier_index:
        return identifier_index[value]

    reverse_map: Dict[str, List[str]] = {}
    for recipe_id, mapped_value in recipe_mapping.items():
        key = str(mapped_value or "").strip().upper()
        if key:
            reverse_map.setdefault(key, []).append(str(recipe_id).strip())

    if value in reverse_map:
        return tuple(sorted(set(reverse_map[value])))

    if value.isdigit():
        candidates = [recipe_id for recipe_id in recipe_mapping.keys() if str(recipe_id).strip() == value[:3]]
        if candidates:
            return tuple(sorted(set(candidates)))

    fallback = parse_recipe_id(value)
    if fallback:
        return (fallback,)
    raise ValueError(
        f"'{affected_recipe}' konnte nicht auf Rezept-ID(s) gemappt werden. "
        "Bitte 3-stellige Rezept-ID oder gueltige FE/FV-Kennung aus der Picklist verwenden."
    )


def sanitize_sheet_name(value: str) -> str:
    cleaned = re.sub(r"[\\/*?:\[\]]", "_", str(value or ""))
    return cleaned[:31] or "Sheet1"


def map_label(value: str, mapping: Dict[str, str]) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.upper() == "ERSATZLOS":
        return "ERSATZLOS"
    return mapping.get(text, text)


def canonical_recipe_id(
    value: str,
    recipe_mapping: Dict[str, str],
    identifier_index: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.upper() == "ERSATZLOS":
        return "ERSATZLOS"
    return resolve_affected_recipe_ids(text, recipe_mapping, identifier_index)[0]


def sheet_recipe_label(
    value: str,
    recipe_mapping: Dict[str, str],
    identifier_index: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> str:
    recipe_id = canonical_recipe_id(value, recipe_mapping, identifier_index)
    if recipe_id == "ERSATZLOS":
        return "ERSATZLOS"
    return map_label(recipe_id, recipe_mapping)


def build_incidents(
    raw_rows: Iterable[Dict[str, Any]],
    boxes_df: pd.DataFrame,
    production_date: date,
    countries: List[CountryRule],
    aliases: Dict[str, List[str]],
    recipe_mapping: Dict[str, str],
    identifier_index: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> Tuple[List[IncidentDefinition], List[str]]:
    incidents: List[IncidentDefinition] = []
    errors: List[str] = []
    counter = 1
    for raw in raw_rows:
        if not bool(raw.get("enabled", True)):
            continue
        affected_recipe = str(raw.get("affected_recipe") or "").strip()
        substitute_recipe = str(raw.get("substitute_recipe") or "ERSATZLOS").strip() or "ERSATZLOS"
        start_mode = str(raw.get("start_mode") or "BOX").strip().upper()
        start_time_value = str(raw.get("start_time") or "").strip()
        start_box_value = str(raw.get("start_box") or "").strip()
        legacy_start_value = str(raw.get("start_value") or "").strip()
        if not start_time_value and start_mode == "TIME":
            start_time_value = legacy_start_value
        if not start_box_value and start_mode == "BOX":
            start_box_value = legacy_start_value
        if start_mode == "TIME+BOX" and not start_time_value and legacy_start_value and ":" in legacy_start_value:
            start_time_value = legacy_start_value
        if not affected_recipe:
            continue
        try:
            affected_recipe_ids = resolve_affected_recipe_ids(affected_recipe, recipe_mapping, identifier_index)
            prefixes = resolve_prefixes(raw.get("prefixes"), affected_recipe_ids[0], start_box_value, countries, aliases)
            start_dt = determine_start_datetime(
                boxes_df,
                production_date,
                start_mode,
                start_time_value,
                start_box_value,
            )
            incidents.append(
                IncidentDefinition(
                    incident_key=f"RAW{counter:03d}",
                    affected_recipe=affected_recipe,
                    affected_recipe_ids=affected_recipe_ids,
                    substitute_recipe=substitute_recipe,
                    start_mode=start_mode,
                    start_time=start_time_value,
                    start_box=start_box_value,
                    prefixes=prefixes,
                    start_dt=start_dt,
                )
            )
            counter += 1
        except ValueError as exc:
            errors.append(str(exc))
    return incidents, errors


def compute_results(
    raw_rows: Iterable[Dict[str, Any]],
    boxes_df: pd.DataFrame,
    customers_df: Optional[pd.DataFrame],
    recipe_mapping: Dict[str, str],
    countries: List[CountryRule],
    aliases: Dict[str, List[str]],
    production_date: Optional[date] = None,
    identifier_index: Optional[Dict[str, Tuple[str, ...]]] = None,
) -> ComputationArtifacts:
    warnings: List[str] = []
    production_day = production_date or infer_production_date(boxes_df)
    incidents, errors = build_incidents(
        raw_rows,
        boxes_df,
        production_day,
        countries,
        aliases,
        recipe_mapping,
        identifier_index,
    )
    if not incidents:
        return ComputationArtifacts(
            log_df=pd.DataFrame(),
            details_df=pd.DataFrame(),
            per_incident={},
            errors=errors or ["Keine gueltigen Vorfaelle vorhanden."],
            warnings=warnings,
            production_date=production_day,
        )

    grouped: Dict[Tuple[str, Tuple[str, ...], Tuple[str, ...]], List[IncidentDefinition]] = {}
    for incident in incidents:
        grouped.setdefault((incident.affected_recipe, incident.affected_recipe_ids, incident.prefixes), []).append(incident)
    for value in grouped.values():
        value.sort(key=lambda item: item.start_dt)

    customer_lookup = customers_df if customers_df is not None else pd.DataFrame(columns=["__box_upper", "customer_id"])
    log_rows: List[Dict[str, Any]] = []
    detail_rows: List[Dict[str, Any]] = []
    per_incident: Dict[str, pd.DataFrame] = {}
    incident_counter = 1
    production_end = datetime.combine(production_day, time(23, 59, 59))

    for (_, affected_recipe_ids, prefixes), items in grouped.items():
        for index, incident in enumerate(items):
            end_dt = items[index + 1].start_dt if index + 1 < len(items) else production_end
            subset = boxes_df.copy()
            subset = subset[subset["__box_upper"].str[:2].isin(prefixes)]
            subset = subset[(subset["__dt"] >= incident.start_dt) & (subset["__dt"] < end_dt)]
            subset = subset[subset["recipes"].map(lambda value: has_any_recipe(value, affected_recipe_ids))].copy()
            subset["subs_for_this_box"] = subset["recipes"].map(lambda value: recipe_occurrence_count_any(value, affected_recipe_ids))
            if not customer_lookup.empty:
                subset = subset.merge(customer_lookup, on="__box_upper", how="left")
            else:
                subset["customer_id"] = ""

            subset.drop_duplicates(subset=["__box_upper"], keep="first", inplace=True)
            subset.sort_values(["__dt", "__box_upper"], inplace=True, kind="mergesort")

            incident_id = f"I{incident_counter:03d}"
            incident_counter += 1
            affected_recipe_id = incident.affected_recipe_ids[0]
            substitute_recipe_id = canonical_recipe_id(incident.substitute_recipe, recipe_mapping, identifier_index)
            sheet_name = sanitize_sheet_name(
                f"{incident_id}_{sheet_recipe_label(incident.affected_recipe, recipe_mapping, identifier_index)}_zu_{sheet_recipe_label(incident.substitute_recipe, recipe_mapping, identifier_index)}"
            )

            detail_df = pd.DataFrame(
                {
                    "incident_id": incident_id,
                    "affected_recipe": affected_recipe_id,
                    "substitute_recipe": substitute_recipe_id,
                    "boxid": subset["boxid"].astype(str),
                    "customer_id": subset.get("customer_id", "").fillna("").astype(str),
                    "subs_for_this_box": subset["subs_for_this_box"].astype(int),
                    "recipes": subset["recipes"].astype(str),
                    "inducted_ts": subset["__dt"].map(lambda value: value.strftime("%Y-%m-%d %H:%M:%S")),
                }
            )
            per_incident[sheet_name] = detail_df
            detail_rows.extend(detail_df.to_dict(orient="records"))

            log_rows.append(
                {
                    "incident_id": incident_id,
                    "affected_recipe": affected_recipe_id,
                    "substitute_recipe": substitute_recipe_id,
                    "start_mode": incident.start_mode,
                    "start_anchor": incident.start_box or incident.start_time,
                    "start_time": incident.start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_time": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "count_boxes": int(len(detail_df)),
                }
            )

    log_df = pd.DataFrame(log_rows)
    details_df = pd.DataFrame(detail_rows)
    if log_df.empty:
        warnings.append("Es wurden keine betroffenen Boxen gefunden.")
    return ComputationArtifacts(
        log_df=log_df,
        details_df=details_df,
        per_incident=per_incident,
        errors=errors,
        warnings=warnings,
        production_date=production_day,
    )


def preview_columns(df: pd.DataFrame, columns: List[str], limit: int = 8) -> pd.DataFrame:
    available = [column for column in columns if column in df.columns]
    preview = df[available].head(limit).copy()
    if "__dt" in preview.columns:
        preview["__dt"] = preview["__dt"].map(lambda value: value.strftime("%Y-%m-%d %H:%M:%S"))
    return preview


def export_excel_bytes(artifacts: ComputationArtifacts) -> bytes:
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        artifacts.log_df.to_excel(writer, index=False, sheet_name="Incidents_Log")
        artifacts.details_df.to_excel(writer, index=False, sheet_name="Details")
        for sheet_name, df in artifacts.per_incident.items():
            df.to_excel(writer, index=False, sheet_name=sanitize_sheet_name(sheet_name))

        for current_name, current_df in [("Incidents_Log", artifacts.log_df), ("Details", artifacts.details_df), *artifacts.per_incident.items()]:
            worksheet = writer.sheets.get(sanitize_sheet_name(current_name) if current_name not in {"Incidents_Log", "Details"} else current_name)
            if worksheet is None or current_df.empty:
                continue
            for column_index, column_name in enumerate(current_df.columns):
                width = max(len(str(column_name)), current_df[column_name].astype(str).map(len).max()) + 2
                worksheet.set_column(column_index, column_index, min(width, 60))
    return output.getvalue()


def ensure_export_path(base_name: str, exports_dir: Path = EXPORTS_DIR) -> Path:
    exports_dir.mkdir(parents=True, exist_ok=True)
    candidate = exports_dir / base_name
    if not candidate.exists():
        return candidate
    stem = candidate.stem
    suffix = candidate.suffix
    counter = 2
    while True:
        versioned = exports_dir / f"{stem}_v{counter}{suffix}"
        if not versioned.exists():
            return versioned
        counter += 1


def save_export_file(artifacts: ComputationArtifacts, exports_dir: Path = EXPORTS_DIR) -> Path:
    filename = f"Factor_Incident_Result_{artifacts.production_date.strftime('%Y-%m-%d')}.xlsx"
    target_path = ensure_export_path(filename, exports_dir)
    target_path.write_bytes(export_excel_bytes(artifacts))
    mirror_dir = CENTRAL_RESULTS_DIR / artifacts.production_date.strftime("%Y") / artifacts.production_date.strftime("%m") / artifacts.production_date.strftime("%d")
    mirror_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(target_path, mirror_dir / target_path.name)
    return target_path