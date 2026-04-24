import json
from pathlib import Path

import pandas as pd


FACTOR_RUN_HISTORY_LIMIT = 30


def load_json_file(file_path: Path, default):
    if not file_path.exists():
        return default
    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return default


def save_json_file(file_path: Path, payload):
    file_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def load_factor_run_history(history_path: Path) -> list[dict]:
    history = load_json_file(history_path, [])
    return history if isinstance(history, list) else []


def append_factor_run_history(history_path: Path, entry: dict, limit: int = FACTOR_RUN_HISTORY_LIMIT) -> list[dict]:
    history = load_factor_run_history(history_path)
    history.insert(0, entry)
    trimmed_history = history[:limit]
    save_json_file(history_path, trimmed_history)
    return trimmed_history


def build_factor_quality_report(
    factor_df: pd.DataFrame,
    factor_mode: str,
    factor_weight_picklist_df: pd.DataFrame | None,
    factor_pkg_df: pd.DataFrame | None,
    factor_sku_df: pd.DataFrame | None,
    factor_manual_weight_reference_df: pd.DataFrame | None,
    reset_input_base_df: pd.DataFrame | None,
    factor_plan_vs_pdl_df: pd.DataFrame | None,
    duplicate_box_count: int,
) -> dict:
    findings: list[dict] = []
    box_count = int(len(factor_df)) if factor_df is not None else 0
    meal_swap_count = int(factor_df["Expanded Meal Swap"].nunique()) if factor_df is not None and "Expanded Meal Swap" in factor_df.columns else 0
    cutoff_count = int(factor_df["Cutoff"].nunique()) if factor_df is not None and "Cutoff" in factor_df.columns else 0

    if box_count == 0:
        findings.append({
            "severity": "red",
            "title": "Keine Boxen geladen",
            "message": "Es wurden keine verwertbaren Factor-Boxen aus der PDL geladen.",
        })

    if duplicate_box_count > 0:
        findings.append({
            "severity": "yellow",
            "title": "Doppelte Box_ID entfernt",
            "message": f"{duplicate_box_count} doppelte Box_ID-Einträge wurden beim Einlesen verworfen.",
        })

    if factor_plan_vs_pdl_df is not None and not factor_plan_vs_pdl_df.empty:
        delta_mask = factor_plan_vs_pdl_df["Delta Meals"] != 0
        delta_count = int(delta_mask.sum())
        delta_total = int(factor_plan_vs_pdl_df.loc[delta_mask, "Delta Meals"].abs().sum()) if delta_count else 0
        if delta_count > 0:
            findings.append({
                "severity": "yellow",
                "title": "Abweichung zwischen Plan und PDL",
                "message": f"{delta_count} Rezepte weichen im Soll-Ist-Abgleich ab, insgesamt {delta_total} Meals Differenz.",
            })

    reset_rows_count = 0
    zero_weight_rows = 0
    manual_zero_reference_count = 0
    if factor_mode == "Tracking + RESET":
        if factor_weight_picklist_df is None or factor_weight_picklist_df.empty:
            findings.append({
                "severity": "yellow",
                "title": "Interne Gewichts-Picklist fehlt",
                "message": "RESET kann erzeugt werden, basiert dann aber nicht auf einer vollständigen ingredientbasierten Gewichtsbasis.",
            })
        if factor_sku_df is None or factor_sku_df.empty:
            findings.append({
                "severity": "yellow",
                "title": "SKU-Gewichte fehlen",
                "message": "Theoretische RESET-Gewichte können ohne SKU-Gewichte unvollständig sein.",
            })
        if factor_pkg_df is None or factor_pkg_df.empty:
            findings.append({
                "severity": "yellow",
                "title": "Packaging-Gewichte fehlen",
                "message": "Theoretische RESET-Gewichte enthalten dann keine saubere Verpackungsbasis.",
            })

        if factor_manual_weight_reference_df is not None and not factor_manual_weight_reference_df.empty:
            manual_zero_reference_count = int((pd.to_numeric(factor_manual_weight_reference_df["Theorie Rezeptgewicht (g)"], errors="coerce").fillna(0) <= 0).sum())
            if manual_zero_reference_count > 0:
                findings.append({
                    "severity": "yellow",
                    "title": "Rezeptgewichte ohne Theoriebasis",
                    "message": f"Für {manual_zero_reference_count} Maitre Codes fehlt aktuell eine theoretische Rezeptgewichtsbasis.",
                })

        if reset_input_base_df is None or reset_input_base_df.empty:
            findings.append({
                "severity": "red",
                "title": "RESET nicht vorbereitbar",
                "message": "Für die aktuelle PDL konnten keine RESET-Zeilen vorbereitet werden.",
            })
        else:
            reset_rows_count = int(len(reset_input_base_df))
            zero_weight_rows = int((pd.to_numeric(reset_input_base_df["Theoretisches Gewicht (kg)"], errors="coerce").fillna(0) <= 0).sum())
            if zero_weight_rows > 0:
                findings.append({
                    "severity": "yellow",
                    "title": "RESET-Zeilen ohne Gewicht",
                    "message": f"{zero_weight_rows} RESET-Kombinationen haben aktuell 0 kg als theoretisches Gewicht.",
                })

    severity_rank = {"green": 0, "yellow": 1, "red": 2}
    status = "GRUEN"
    if findings:
        highest = max(findings, key=lambda item: severity_rank.get(item["severity"], 0))["severity"]
        status = {"green": "GRUEN", "yellow": "GELB", "red": "ROT"}.get(highest, "GRUEN")

    if not findings:
        findings.append({
            "severity": "green",
            "title": "Keine Auffälligkeiten",
            "message": "Die aktuelle Factor-Verarbeitung zeigt keine offensichtlichen Plausibilitätsprobleme.",
        })

    return {
        "status": status,
        "findings": findings,
        "metrics": {
            "box_count": box_count,
            "meal_swap_count": meal_swap_count,
            "cutoff_count": cutoff_count,
            "reset_rows_count": reset_rows_count,
            "zero_weight_rows": zero_weight_rows,
            "duplicate_box_count": duplicate_box_count,
            "manual_zero_reference_count": manual_zero_reference_count,
            "warning_count": int(sum(1 for item in findings if item["severity"] == "yellow")),
            "error_count": int(sum(1 for item in findings if item["severity"] == "red")),
        },
    }


def build_factor_run_history_entry(
    *,
    week_label: str,
    factor_mode: str,
    auto_tracking_mode: bool,
    input_file_count: int,
    factor_cutoffs: list[str],
    box_count: int,
    meal_swap_count: int,
    reset_rows_count: int,
    warning_count: int,
    error_count: int,
    status: str,
    tracking_export_path: str,
) -> dict:
    return {
        "Zeit": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Status": status,
        "KW": week_label,
        "Modus": factor_mode,
        "Schnellstart": "Ja" if auto_tracking_mode else "Nein",
        "Dateien": int(input_file_count),
        "Cutoffs": ", ".join(factor_cutoffs),
        "Boxen": int(box_count),
        "Meal Swaps": int(meal_swap_count),
        "RESET Zeilen": int(reset_rows_count),
        "Warnungen": int(warning_count),
        "Fehler": int(error_count),
        "Tracking Export": tracking_export_path,
    }


def build_factor_run_signature(
    *,
    week_label: str,
    factor_mode: str,
    auto_tracking_mode: bool,
    input_file_names: list[str],
    factor_cutoffs: list[str],
    box_count: int,
    meal_swap_count: int,
    reset_rows_count: int,
    status: str,
) -> str:
    return json.dumps(
        {
            "week": week_label,
            "mode": factor_mode,
            "auto_tracking_mode": auto_tracking_mode,
            "files": input_file_names,
            "cutoffs": factor_cutoffs,
            "boxen": int(box_count),
            "meal_swaps": int(meal_swap_count),
            "reset_rows": int(reset_rows_count),
            "status": status,
        },
        ensure_ascii=True,
        sort_keys=True,
    )