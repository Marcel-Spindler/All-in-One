from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib import request

import pandas as pd
import streamlit as st

SHARED_DIR = Path(__file__).resolve().parents[2] / "Unified-Platform-Blueprint" / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.append(str(SHARED_DIR))

from hub_client import add_artifact, finish_run, start_run

from modern_incident_tool.core import (
    BASE_DIR,
    CONFIG_PATH,
    DEFAULT_INCIDENT_ROWS,
    IMPORTS_DIR,
    ComputationArtifacts,
    compute_results,
    export_excel_bytes,
    guess_default_files,
    list_import_files,
    load_boxes_dataframe,
    load_country_rules,
    load_customer_dataframe,
    load_picklist_bundle,
    preview_columns,
    save_export_file,
)


DRAFT_PATH = BASE_DIR / "data" / "incident_draft_v2.json"


def current_week_key() -> str:
    iso = datetime.now().isocalendar()
    return f"{iso.year}-W{iso.week:02d}"


st.set_page_config(
    page_title="Factor Incident Tool v2",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)


def init_state() -> None:
    draft = load_incident_draft()
    defaults = guess_default_files(IMPORTS_DIR)
    st.session_state.setdefault("imports_dir", str(IMPORTS_DIR))
    st.session_state.setdefault("boxes_file", draft.get("boxes_file") or defaults.get("boxes"))
    st.session_state.setdefault("customers_file", draft.get("customers_file") or defaults.get("customers"))
    st.session_state.setdefault("picklist_file", draft.get("picklist_file") or defaults.get("picklist"))
    st.session_state.setdefault("incidents", normalize_incident_rows(draft.get("incidents") or DEFAULT_INCIDENT_ROWS))
    st.session_state.setdefault("artifacts", None)
    st.session_state.setdefault("last_export_path", "")
    st.session_state.setdefault("last_saved_incidents_at", draft.get("saved_at", ""))
    st.session_state.setdefault("week_key", current_week_key())
    st.session_state.setdefault("week_reset_notice", bool(draft.get("week_reset_notice", False)))


def normalize_incident_rows(rows: list[dict]) -> list[dict]:
    normalized_rows: list[dict] = []
    for raw in rows or []:
        row = dict(raw)
        mode = str(row.get("start_mode") or "TIME+BOX").upper()
        legacy_start_value = str(row.get("start_value") or "").strip()
        row["enabled"] = bool(row.get("enabled", True))
        row["affected_recipe"] = str(row.get("affected_recipe") or "").strip()
        row["substitute_recipe"] = str(row.get("substitute_recipe") or "ERSATZLOS").strip() or "ERSATZLOS"
        row["start_mode"] = mode if mode in {"TIME+BOX", "BOX", "TIME"} else "TIME+BOX"
        row["start_time"] = str(row.get("start_time") or "").strip()
        row["start_box"] = str(row.get("start_box") or "").strip()
        row["prefixes"] = str(row.get("prefixes") or "AUTO").strip() or "AUTO"
        if not row["start_time"] and row["start_mode"] == "TIME" and legacy_start_value:
            row["start_time"] = legacy_start_value
        if not row["start_box"] and row["start_mode"] == "BOX" and legacy_start_value:
            row["start_box"] = legacy_start_value
        normalized_rows.append(row)
    return normalized_rows or [row.copy() for row in DEFAULT_INCIDENT_ROWS]


def load_incident_draft() -> dict:
    if not DRAFT_PATH.exists():
        return {}
    try:
        payload = json.loads(DRAFT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    saved_week = str(payload.get("week_key") or "").strip()
    active_week = current_week_key()
    if saved_week and saved_week != active_week:
        return {"week_reset_notice": True}
    return payload


def save_incident_draft() -> str:
    DRAFT_PATH.parent.mkdir(parents=True, exist_ok=True)
    saved_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    payload = {
        "saved_at": saved_at,
        "week_key": current_week_key(),
        "boxes_file": st.session_state.get("boxes_file"),
        "customers_file": st.session_state.get("customers_file"),
        "picklist_file": st.session_state.get("picklist_file"),
        "incidents": normalize_incident_rows(st.session_state.get("incidents") or []),
    }
    DRAFT_PATH.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    st.session_state["last_saved_incidents_at"] = saved_at
    return saved_at


def clear_incident_draft() -> None:
    if DRAFT_PATH.exists():
        DRAFT_PATH.unlink()
    st.session_state["incidents"] = [row.copy() for row in DEFAULT_INCIDENT_ROWS]
    st.session_state["last_saved_incidents_at"] = ""


def handle_upload(label: str, key: str, allowed_types: list[str], imports_dir: Path) -> None:
    uploaded = st.file_uploader(label, type=allowed_types, key=key)
    if uploaded is None:
        return
    imports_dir.mkdir(parents=True, exist_ok=True)
    target = imports_dir / uploaded.name
    target.write_bytes(uploaded.getvalue())
    st.success(f"Datei hochgeladen: {uploaded.name}")


def fetch_grafana_dataframe(endpoint_url: str, token: str, fmt: str) -> pd.DataFrame:
    url = str(endpoint_url or "").strip()
    if not url:
        raise ValueError("Grafana-Endpoint fehlt.")

    headers = {"Accept": "application/json, text/csv"}
    if token.strip():
        headers["Authorization"] = f"Bearer {token.strip()}"

    req = request.Request(url=url, headers=headers, method="GET")
    with request.urlopen(req, timeout=30) as response:  # nosec B310 - explicit admin endpoint feature
        payload = response.read()
        content_type = str(response.headers.get("Content-Type") or "").lower()

    detected = fmt.lower().strip()
    if detected == "auto":
        if "text/csv" in content_type or url.lower().endswith(".csv"):
            detected = "csv"
        else:
            detected = "json"

    if detected == "csv":
        return pd.read_csv(pd.io.common.BytesIO(payload), dtype=str)

    data = json.loads(payload.decode("utf-8"))
    if isinstance(data, list):
        return pd.DataFrame(data)

    if isinstance(data, dict):
        if isinstance(data.get("data"), list):
            return pd.DataFrame(data["data"])
        if isinstance(data.get("rows"), list):
            return pd.DataFrame(data["rows"])
        if isinstance(data.get("results"), list):
            return pd.DataFrame(data["results"])

    raise ValueError("Grafana-Antwort konnte nicht als Tabelle gelesen werden (CSV oder JSON-Array).")


def normalize_boxes_columns(raw_df: pd.DataFrame, ts_col: str, box_col: str, recipes_col: str) -> pd.DataFrame:
    required = [ts_col.strip(), box_col.strip(), recipes_col.strip()]
    missing = [col for col in required if col not in raw_df.columns]
    if missing:
        raise ValueError(f"Spalten aus Grafana fehlen: {', '.join(missing)}")
    prepared = raw_df.rename(
        columns={
            ts_col.strip(): "inducted_ts",
            box_col.strip(): "boxid",
            recipes_col.strip(): "recipes",
        }
    ).copy()
    return prepared[["inducted_ts", "boxid", "recipes"]].astype(str)


def save_boxes_import(df: pd.DataFrame, imports_dir: Path) -> str:
    imports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"Boxes Produced (planned box source PCS)-grafana-{timestamp}.csv"
    target = imports_dir / filename
    df.to_csv(target, index=False)
    st.session_state["boxes_file"] = filename
    return filename


def render_header() -> None:
    st.markdown(
        """
        <style>
            :root {
                --panel-bg: rgba(14, 21, 34, 0.82);
                --panel-border: rgba(99, 169, 255, 0.28);
                --accent: #8ad7ff;
                --text-strong: #dce9ff;
            }
            .stApp {
                background: radial-gradient(circle at 20% 0%, #1b2438 0%, #0d111a 40%, #0a0e15 100%);
            }
            .stTabs [data-baseweb="tab-list"] {
                gap: 8px;
            }
            .stTabs [data-baseweb="tab"] {
                border-radius: 12px;
                border: 1px solid var(--panel-border);
                background: rgba(18, 27, 43, 0.7);
                color: var(--text-strong);
                padding: 8px 14px;
            }
            .stTabs [aria-selected="true"] {
                background: linear-gradient(120deg, #173861 0%, #1f6aa0 100%);
                border-color: rgba(152, 220, 255, 0.9);
                color: #f4fbff;
            }
            div[data-testid="stDataFrame"] {
                border: 1px solid var(--panel-border);
                border-radius: 14px;
                background: var(--panel-bg);
                box-shadow: 0 10px 30px rgba(0, 0, 0, 0.25);
            }
            div[data-testid="stForm"] {
                border: 1px solid var(--panel-border);
                border-radius: 14px;
                background: var(--panel-bg);
                padding: 14px;
            }
            .incident-hint {
                border: 1px solid var(--panel-border);
                border-radius: 14px;
                background: linear-gradient(135deg, rgba(31, 51, 78, 0.9), rgba(15, 29, 48, 0.88));
                padding: 14px;
                margin-bottom: 10px;
            }
            [data-testid="stMetricValue"] {
                color: #8ad7ff;
            }
            [data-testid="stAlertContainer"] {
                border-radius: 14px;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.title("Factor Incident Tool v2")
    st.caption(
        "Neubau fuer stabile Incident-Auswertung: Produktions-CSV sortieren, Vorfaelle aufloesen, Customer-IDs mappen, Picklist-FE uebernehmen und Excel wie im bekannten Factor-Format exportieren."
    )


def render_sidebar(country_summary: str) -> None:
    st.sidebar.header("Setup")
    st.sidebar.text_input("Imports-Ordner", key="imports_dir", disabled=True)
    st.sidebar.info(country_summary)
    st.sidebar.caption(f"Regeldatei: {CONFIG_PATH.name}")
    with st.sidebar.expander("Hilfe / Ablauf", expanded=False):
        st.markdown(
            """
            **Kurzablauf**

            1. Im Tab **Admin** Produktions-CSV, Custom-ID-Datei und Picklist laden.
            2. Im Tab **Vorfalle** die Incidents pflegen.
            3. Danach **Berechnen** klicken.
            4. Im Tab **Ergebnisse** Log, Details und Excel pruefen.

            **Was gehoert wohin?**

            - **Produktions-CSV**: Boxen in Produktionsreihenfolge.
            - **Custom-ID-Datei**: Kundenzuordnung pro Box.
            - **Picklist**: FE-/Maitre-Mapping fuer Rezepte.
            """
        )


def select_import_file(label: str, key: str, options: list[str], help_text: str) -> None:
    current_value = st.session_state.get(key)
    if current_value not in options:
        current_value = options[0] if options else None
        st.session_state[key] = current_value
    st.selectbox(label, options=options, key=key, help=help_text)


def load_inputs(imports_dir: Path):
    boxes_path = imports_dir / st.session_state["boxes_file"]
    customers_path = imports_dir / st.session_state["customers_file"] if st.session_state.get("customers_file") else None
    picklist_path = imports_dir / st.session_state["picklist_file"] if st.session_state.get("picklist_file") else None

    boxes_df = load_boxes_dataframe(boxes_path)
    customers_df = load_customer_dataframe(customers_path) if customers_path and customers_path.exists() else None
    recipe_mapping: dict[str, str] = {}
    identifier_index: dict[str, tuple[str, ...]] = {}
    if picklist_path and picklist_path.exists():
        recipe_mapping, identifier_index = load_picklist_bundle(picklist_path)
    return boxes_df, customers_df, recipe_mapping, identifier_index


def render_import_tab(
    imports_dir: Path,
) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], dict[str, str], dict[str, tuple[str, ...]], list[str]]:
    st.subheader("Admin: Datenquellen und Upload")
    st.caption("Dieser Bereich ist fuer Import/Dateipflege gedacht und liegt absichtlich nicht auf der ersten Seite.")

    st.markdown(
        """
        <div class="incident-hint">
            <strong>Grafana ohne technische URL benutzen</strong><br/>
            1. In Grafana erst Land, Site, Woche und Tag korrekt setzen.<br/>
            2. Im Panel <code>Boxes Produced (planned box source PCS)</code> auf <code>Inspect</code> gehen.<br/>
            3. Im Tab <code>Daten</code> auf <code>CSV herunterladen</code> klicken.<br/>
            4. Die heruntergeladene Datei hier bei <code>Grafana / Produktions-CSV hochladen</code> hochladen.<br/>
            Der URL/API-Weg bleibt unten als Expertenmodus erhalten und ist nicht noetig fuer den normalen Ablauf.
        </div>
        """,
        unsafe_allow_html=True,
    )

    upload_left, upload_mid, upload_right = st.columns(3)
    with upload_left:
        handle_upload("Grafana / Produktions-CSV hochladen", "upload_boxes", ["csv"], imports_dir)
    with upload_mid:
        handle_upload("Upload Custom-ID-Datei", "upload_customers", ["csv", "xlsx", "xls"], imports_dir)
    with upload_right:
        handle_upload("Upload Picklist-Datei", "upload_picklist", ["xlsx", "xls"], imports_dir)

    with st.expander("Expertenmodus: direkte Grafana-URL oder API", expanded=False):
        left, right = st.columns(2)
        with left:
            grafana_url = st.text_input(
                "Grafana Endpoint URL",
                value=st.session_state.get("grafana_url", ""),
                key="grafana_url",
                help="Nur noetig, wenn direkt gegen eine Grafana-CSV- oder JSON-Quelle geladen werden soll.",
            )
            grafana_token = st.text_input(
                "Grafana API Token (optional)",
                value=st.session_state.get("grafana_token", ""),
                key="grafana_token",
                type="password",
            )
        with right:
            grafana_fmt = st.selectbox("Antwortformat", options=["auto", "csv", "json"], key="grafana_fmt")
            ts_col = st.text_input("Zeitspalte", value=st.session_state.get("grafana_ts_col", "inducted_ts"), key="grafana_ts_col")
            box_col = st.text_input("Box-ID-Spalte", value=st.session_state.get("grafana_box_col", "boxid"), key="grafana_box_col")
            rec_col = st.text_input("Recipes-Spalte", value=st.session_state.get("grafana_rec_col", "recipes"), key="grafana_rec_col")

        if st.button("Boxes Produced aus Grafana laden", width="stretch"):
            try:
                raw_df = fetch_grafana_dataframe(grafana_url, grafana_token, grafana_fmt)
                normalized_df = normalize_boxes_columns(raw_df, ts_col, box_col, rec_col)
                created_name = save_boxes_import(normalized_df, imports_dir)
                st.success(f"Grafana-Import erfolgreich: {created_name}")
            except Exception as exc:
                st.error(f"Grafana-Import fehlgeschlagen: {exc}")

    files = list_import_files(imports_dir)
    errors: list[str] = []
    if not files["all"]:
        st.error("Im Imports-Ordner wurden noch keine Dateien gefunden.")
        return None, None, {}, {}, ["Imports-Ordner ist leer."]

    csv_options = files["csv"]
    xlsx_options = files["xlsx"]
    col1, col2, col3 = st.columns(3)
    with col1:
        select_import_file(
            "Produktions-CSV (Boxes Produced)",
            "boxes_file",
            csv_options,
            "Am Produktionsende wird hier die chronologisch auszuwertende Box-Liste geladen.",
        )
    with col2:
        select_import_file(
            "Custom-ID-Datei",
            "customers_file",
            csv_options,
            "Ordnet Box-ID einer Kundennummer zu.",
        )
    with col3:
        select_import_file(
            "Picklist-Excel",
            "picklist_file",
            xlsx_options,
            "Liefert FE-/Maitre-Mapping fuer Rezeptnummern.",
        )

    try:
        boxes_df, customers_df, recipe_mapping, identifier_index = load_inputs(imports_dir)
    except Exception as exc:
        st.error(str(exc))
        return None, None, {}, {}, [str(exc)]

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Boxen", f"{len(boxes_df):,}".replace(",", "."))
    metric2.metric("Custom-IDs", f"{0 if customers_df is None else len(customers_df):,}".replace(",", "."))
    metric3.metric("Picklist-Mapping", f"{len(recipe_mapping):,}".replace(",", "."))

    preview_left, preview_right = st.columns(2)
    with preview_left:
        st.subheader("Produktionsdaten")
        preview_df = preview_columns(boxes_df, ["inducted_ts", "boxid", "recipes"], limit=10)
        st.dataframe(preview_df, width="stretch", hide_index=True)
    with preview_right:
        st.subheader("Customer-ID-Zuordnung")
        customer_preview = pd.DataFrame()
        if customers_df is not None:
            customer_preview = customers_df.head(10).rename(columns={"__box_upper": "boxid"})
        st.dataframe(customer_preview, width="stretch", hide_index=True)

    if recipe_mapping:
        st.subheader("Picklist-Mapping")
        mapping_preview = pd.DataFrame(
            [{"recipe": recipe, "fe_or_maitre": value} for recipe, value in sorted(recipe_mapping.items())[:20]]
        )
        st.dataframe(mapping_preview, width="stretch", hide_index=True)

    return boxes_df, customers_df, recipe_mapping, identifier_index, errors


def render_incident_tab() -> list[dict]:
    st.subheader("Vorfalle")
    st.markdown(
        """
        <div class="incident-hint">
            <strong>Schnelle Erfassung mit guter Lesbarkeit</strong><br/>
            Empfohlen ist <code>TIME+BOX</code> (beides angeben): Zeit fuer Startfenster und Startbox fuer Land/Prefix-Erkennung.
            <br/>Betroffenes Rezept akzeptiert 3-stellige Rezept-ID sowie FE/FV-/numerische Kennung (z. B. <code>FE1787A</code>, <code>565206</code>).
        </div>
        """,
        unsafe_allow_html=True,
    )

    with st.form("incident_quick_add"):
        st.markdown("**Neuen Vorfall schnell hinzufuegen**")
        row1, row2, row3 = st.columns(3)
        with row1:
            new_affected = st.text_input("Betroffenes Rezept", value="")
            new_substitute = st.text_input("Ersatz / Freitext", value="ERSATZLOS")
        with row2:
            new_mode = st.selectbox("Startmodus", options=["TIME+BOX", "BOX", "TIME"], index=0)
            new_time = st.text_input("Startzeit (HH:MM)", value="")
        with row3:
            new_box = st.text_input("Startbox", value="")
            new_prefix = st.text_input("Prefix-Scope", value="AUTO")

        submitted = st.form_submit_button("Vorfall hinzufuegen", type="primary", use_container_width=True)
        if submitted:
            if not new_affected.strip():
                st.warning("Bitte mindestens ein betroffenes Rezept angeben.")
            else:
                new_row = {
                    "enabled": True,
                    "affected_recipe": new_affected.strip(),
                    "substitute_recipe": (new_substitute or "ERSATZLOS").strip() or "ERSATZLOS",
                    "start_mode": new_mode,
                    "start_time": new_time.strip(),
                    "start_box": new_box.strip(),
                    "prefixes": (new_prefix or "AUTO").strip() or "AUTO",
                }
                st.session_state["incidents"] = normalize_incident_rows(st.session_state.get("incidents", []) + [new_row])
                st.success("Vorfall hinzugefuegt.")

    incident_rows = []
    for row in st.session_state["incidents"]:
        normalized = dict(row)
        legacy_start_value = str(normalized.get("start_value") or "").strip()
        mode = str(normalized.get("start_mode") or "TIME+BOX").upper()
        normalized.setdefault("start_time", "")
        normalized.setdefault("start_box", "")
        if not normalized["start_time"] and mode == "TIME" and legacy_start_value:
            normalized["start_time"] = legacy_start_value
        if not normalized["start_box"] and mode == "BOX" and legacy_start_value:
            normalized["start_box"] = legacy_start_value
        incident_rows.append(normalized)

    incident_df = pd.DataFrame(incident_rows)
    edited = st.data_editor(
        incident_df,
        width="stretch",
        num_rows="dynamic",
        hide_index=True,
        column_config={
            "enabled": st.column_config.CheckboxColumn("aktiv"),
            "affected_recipe": st.column_config.TextColumn("betroffenes Rezept"),
            "substitute_recipe": st.column_config.TextColumn("Ersatz / Freitext"),
            "start_mode": st.column_config.SelectboxColumn("Startmodus", options=["TIME+BOX", "BOX", "TIME"]),
            "start_time": st.column_config.TextColumn("Startzeit (HH:MM)"),
            "start_box": st.column_config.TextColumn("Startbox"),
            "prefixes": st.column_config.TextColumn("Prefix-Scope"),
        },
    )
    cleaned = edited.fillna("").to_dict(orient="records")
    normalized = normalize_incident_rows(cleaned)
    st.session_state["incidents"] = normalized

    active_count = sum(1 for row in normalized if row.get("enabled") and str(row.get("affected_recipe") or "").strip())
    st.caption(f"Aktive Vorfaelle im Entwurf: {active_count}")

    left, right, _ = st.columns([1, 1, 2])
    with left:
        if st.button("Zwischenspeichern", width="stretch"):
            saved_at = save_incident_draft()
            st.success(f"Vorfaelle gespeichert ({saved_at}).")
    with right:
        if st.button("Entwurf leeren", width="stretch"):
            clear_incident_draft()
            st.success("Entwurf geloescht. Neuer Incident kann erfasst werden.")

    return normalized


def render_results(artifacts: Optional[ComputationArtifacts]) -> None:
    st.subheader("Ergebnisse")
    if artifacts is None:
        st.info("Noch keine Berechnung ausgefuehrt.")
        return

    for error in artifacts.errors:
        st.error(error)
    for warning in artifacts.warnings:
        st.warning(warning)

    if artifacts.log_df.empty:
        st.info("Keine betroffenen Boxen gefunden.")
        return

    metrics = st.columns(3)
    metrics[0].metric("Incidents", len(artifacts.log_df))
    metrics[1].metric("Detailzeilen", len(artifacts.details_df))
    metrics[2].metric("Exportdatum", artifacts.production_date.strftime("%Y-%m-%d"))

    left, right = st.columns([1, 1])
    with left:
        st.markdown("**Incidents Log**")
        st.dataframe(artifacts.log_df, width="stretch", hide_index=True)
    with right:
        st.markdown("**Details gesamt**")
        st.dataframe(artifacts.details_df, width="stretch", hide_index=True)

    selected_sheet = st.selectbox("Incident-Sheet", options=list(artifacts.per_incident.keys()))
    st.dataframe(artifacts.per_incident[selected_sheet], width="stretch", hide_index=True)

    excel_bytes = export_excel_bytes(artifacts)
    st.download_button(
        label="Excel herunterladen",
        data=excel_bytes,
        file_name=f"Factor_Incident_Result_{artifacts.production_date.strftime('%Y-%m-%d')}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )


def main() -> None:
    init_state()
    countries, aliases = load_country_rules()
    country_summary = "Laenderregeln: " + " | ".join(
        f"{rule.code}: {','.join(rule.prefixes)} -> "
        f"{', '.join(f'{start}-{end}' for start, end in rule.recipe_ranges)}"
        for rule in countries
    )
    render_header()
    render_sidebar(country_summary)

    st.info(
        "Dieser Incident-Neubau ist absichtlich in 3 Bereiche getrennt: **Vorfalle** fuer Eingabe, **Ergebnisse** fuer Kontrolle und Export, **Admin** fuer Datenquellen. Arbeite immer in dieser Reihenfolge."
    )

    if st.session_state.get("week_reset_notice"):
        st.info("Neue Woche erkannt: Entwurf wurde auf Standard gesetzt. Bitte aktuelle Wochen-Dateien laden und speichern.")
        st.session_state["week_reset_notice"] = False

    imports_dir = Path(st.session_state["imports_dir"])
    tab_incidents, tab_results, tab_imports = st.tabs(["Vorfalle", "Ergebnisse", "Admin"])

    with tab_incidents:
        incident_rows = render_incident_tab()

    with tab_imports:
        boxes_df, customers_df, recipe_mapping, identifier_index, load_errors = render_import_tab(imports_dir)

    if st.session_state.get("last_saved_incidents_at"):
        st.caption(f"Zwischenspeicher zuletzt aktualisiert: {st.session_state['last_saved_incidents_at']}")

    if boxes_df is not None:
        if st.button("Berechnen", type="primary", width="stretch"):
            try:
                save_incident_draft()
                artifacts = compute_results(
                    raw_rows=incident_rows,
                    boxes_df=boxes_df,
                    customers_df=customers_df,
                    recipe_mapping=recipe_mapping,
                    countries=countries,
                    aliases=aliases,
                    identifier_index=identifier_index,
                )
                st.session_state["artifacts"] = artifacts
                if not artifacts.log_df.empty:
                    export_path = save_export_file(artifacts)
                    run_id = start_run(
                        tool="incident-tool",
                        input_files=[
                            str(imports_dir / st.session_state.get("boxes_file", "")),
                            str(imports_dir / st.session_state.get("customers_file", "")),
                            str(imports_dir / st.session_state.get("picklist_file", "")),
                        ],
                    )
                    if run_id:
                        add_artifact(run_id, "incident_export", export_path, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        finish_run(
                            run_id,
                            status="warning" if artifacts.warnings else "success",
                            metrics={
                                "productionDate": artifacts.production_date.strftime("%Y-%m-%d"),
                                "incidentCount": int(len(artifacts.per_incident)),
                                "affectedBoxes": int(len(artifacts.details_df)),
                                "mappedCustomers": int(artifacts.details_df["customer_id"].notna().sum()) if "customer_id" in artifacts.details_df.columns else 0,
                            },
                            warnings=artifacts.warnings,
                            errors=artifacts.errors,
                        )
                    st.session_state["last_export_path"] = str(export_path)
            except Exception as exc:
                st.session_state["artifacts"] = None
                st.error(f"Berechnung fehlgeschlagen: {exc}")

    with tab_results:
        if st.session_state.get("last_export_path"):
            st.caption(f"Letzter Auto-Export: {st.session_state['last_export_path']}")
        render_results(st.session_state.get("artifacts"))


if __name__ == "__main__":
    main()