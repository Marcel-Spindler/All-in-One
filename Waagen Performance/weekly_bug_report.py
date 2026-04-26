import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from streamlit_plotly_events import plotly_events
from datetime import datetime, timedelta
from html import escape as html_escape
import hashlib
import io
import json
import sys
from pathlib import Path
from urllib.parse import quote


ERROR_COLOR_MAP = {
    "missing": "#ff6b6b",
    "wrong_item": "#ffb347",
    "extra": "#3dd9b4",
}

BRAND_NAME = "Waagen Performance"
REPORT_TITLE = "Assembly QC Weekly Bug Report"
EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(exist_ok=True)
CENTRAL_RESULTS_DIR = Path(__file__).resolve().parents[1] / "Unified-Platform-Blueprint" / "results" / "waagen-performance"
SHARED_DIR = Path(__file__).resolve().parents[1] / "Unified-Platform-Blueprint" / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.append(str(SHARED_DIR))

from hub_client import add_artifact, finish_run, start_run


def fmt_int(value) -> str:
    if pd.isna(value):
        return "0"
    return f"{int(value):,}".replace(",", ".")


def fmt_pct(value) -> str:
    if pd.isna(value):
        return "0.0%"
    return f"{value:.1f}%"


def build_highlight(text: str, tone: str) -> str:
    return f'<span class="insight-pill {tone}">{text}</span>'


def classify_status(value: float, warning_threshold: float, critical_threshold: float) -> str:
    if pd.isna(value):
        return "Stabil"
    if value >= critical_threshold:
        return "Kritisch"
    if value >= warning_threshold:
        return "Beobachten"
    return "Stabil"


def status_tone(status: str) -> str:
    return {
        "Kritisch": "alert",
        "Beobachten": "warn",
        "Stabil": "good",
    }.get(status, "good")


def build_status_badge(status: str) -> str:
    return f'<span class="status-badge {status_tone(status)}">{status}</span>'


def build_quality_posture(error_rate: float, box_error_rate: float, warning_threshold: float, critical_threshold: float) -> dict:
    if error_rate >= critical_threshold or box_error_rate >= critical_threshold:
        return {
            "status": "Kritisch",
            "label": "Sofortige Priorisierung",
            "summary": "Die aktuelle Qualitätslage ist eskalationswürdig und sollte direkt in den operativen Fokus gezogen werden.",
        }
    if error_rate >= warning_threshold or box_error_rate >= warning_threshold:
        return {
            "status": "Beobachten",
            "label": "Gezielt nachsteuern",
            "summary": "Die Lage ist nicht entgleist, aber klar auffällig. Review und Gegenmaßnahmen sollten zeitnah erfolgen.",
        }
    return {
        "status": "Stabil",
        "label": "Kontrolliert weiterführen",
        "summary": "Das Qualitätsbild ist aktuell stabil. Relevante Hotspots bleiben sichtbar, aber ohne akute Eskalation.",
    }


def build_management_actions(errors_df: pd.DataFrame, hotspot_df: pd.DataFrame, line_exec_df: pd.DataFrame) -> list[dict]:
    actions = []

    critical_lines = line_exec_df[line_exec_df["Priorität"] == "Kritisch"].copy() if not line_exec_df.empty else pd.DataFrame()
    if not critical_lines.empty:
        first_line = critical_lines.iloc[0]
        actions.append({
            "title": f"Linie {first_line['Assembly Line']} zuerst prüfen",
            "copy": f"Die Linie liegt mit {first_line['Fehlerquote (%)']:.1f}% Fehlerquote aktuell im kritischen Bereich und sollte im nächsten Review als erstes aufgerufen werden.",
            "tone": "alert",
        })

    if not hotspot_df.empty:
        hotspot = hotspot_df.iloc[0]
        actions.append({
            "title": f"Hotspot {hotspot['Dimension']}: {hotspot['Wert']}",
            "copy": f"Hier kumulieren derzeit {fmt_int(hotspot['Vorfälle'])} Vorfälle. Das ist der stärkste sichtbare Hebel für kurzfristige Wirkung.",
            "tone": "warn",
        })

    if not errors_df.empty and "checks_error_category" in errors_df.columns:
        top_error = errors_df["checks_error_category"].value_counts()
        if not top_error.empty:
            actions.append({
                "title": f"Fehlertyp {top_error.index[0]} fokussieren",
                "copy": f"Der dominierende Fehlertyp bringt aktuell {fmt_int(top_error.iloc[0])} Fälle mit und sollte als Hauptspur für Gegenmaßnahmen genutzt werden.",
                "tone": "good",
            })

    if not errors_df.empty and "checks_error_sku" in errors_df.columns:
        top_sku = (
            errors_df.dropna(subset=["checks_error_sku"])
            .groupby("checks_error_sku")
            .size()
            .sort_values(ascending=False)
        )
        if not top_sku.empty:
            actions.append({
                "title": f"Top-SKU {top_sku.index[0]} monitoren",
                "copy": f"Diese SKU taucht aktuell am häufigsten in Fehlerfällen auf und sollte in Ursachenanalyse und Kommunikation explizit genannt werden.",
                "tone": "good",
            })

    return actions[:4]


def persist_html_report(html: str) -> tuple[Path, Path]:
    EXPORT_DIR.mkdir(exist_ok=True)
    latest_path = EXPORT_DIR / "weekly_bug_report_latest.html"
    dated_path = EXPORT_DIR / f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.html"
    latest_path.write_text(html, encoding="utf-8")
    dated_path.write_text(html, encoding="utf-8")
    persist_unified_artifact(latest_path.name, html.encode("utf-8"))
    persist_unified_artifact(dated_path.name, html.encode("utf-8"))
    return latest_path, dated_path


def persist_unified_artifact(filename: str, payload: bytes) -> Path:
    now = datetime.now()
    target_dir = CENTRAL_RESULTS_DIR / now.strftime("%Y") / now.strftime("%m") / now.strftime("%d")
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / filename
    target_path.write_bytes(payload)
    return target_path


def report_qc_run_once(latest_path: Path, dated_path: Path) -> None:
    report_signature = f"{datetime.now().strftime('%Y-%m-%d')}|{total_checks}|{total_errors}|{error_rate:.2f}|{box_error_rate:.2f}"
    if st.session_state.get("qc_report_signature") == report_signature:
        return

    run_id = start_run(tool="waagen-performance", input_files=[data_source_label or "uploaded-csv"])
    if not run_id:
        return

    add_artifact(run_id, "html_report_latest", latest_path, "text/html")
    add_artifact(run_id, "html_report_dated", dated_path, "text/html")
    finish_run(
        run_id,
        status="warning" if error_rate >= 3 or box_error_rate >= 5 else "success",
        metrics={
            "totalChecks": int(total_checks),
            "totalErrors": int(total_errors),
            "errorRate": float(error_rate),
            "boxesWithErrors": int(unique_boxes_with_errors),
            "boxErrorRate": float(box_error_rate),
        },
        warnings=insight_lines[:3] if "insight_lines" in globals() else [],
        errors=[],
    )
    st.session_state["qc_report_signature"] = report_signature


def build_public_report_url(share_target: str, file_name: str) -> str:
    target = (share_target or "").strip()
    if not target:
        return ""
    if target.endswith("/"):
        return f"{target}{quote(file_name)}"
    return target


def build_report_access_links(latest_path: Path, dated_path: Path, share_target: str = "") -> dict:
    latest_resolved = latest_path.resolve()
    dated_resolved = dated_path.resolve()
    public_target = build_public_report_url(share_target, latest_path.name)
    return {
        "local_latest_path": latest_resolved.as_posix(),
        "local_latest_uri": latest_resolved.as_uri(),
        "local_dated_path": dated_resolved.as_posix(),
        "local_dated_uri": dated_resolved.as_uri(),
        "public_latest_url": public_target,
        "public_dated_url": public_target,
    }


def build_week_over_week_summary(scoped_df: pd.DataFrame, selected_errors: list[str]) -> dict:
    if scoped_df.empty or "year_week" not in scoped_df.columns:
        return {"available": False}

    available_weeks = sorted(scoped_df["year_week"].dropna().astype(str).unique())
    if not available_weeks:
        return {"available": False}

    current_week = available_weeks[-1]
    previous_week = available_weeks[-2] if len(available_weeks) >= 2 else None

    def week_metrics(week_key: str | None) -> dict:
        if not week_key:
            return {
                "week": None,
                "total_checks": 0,
                "total_errors": 0,
                "error_rate": 0.0,
                "box_error_rate": 0.0,
                "boxes_with_errors": 0,
                "boxes_total": 0,
            }

        week_df = scoped_df[scoped_df["year_week"] == week_key].copy()
        week_errors = week_df[
            week_df["has_error"]
            & week_df["checks_error_category"].isin(selected_errors)
        ].copy()

        total_checks = len(week_df)
        total_errors = len(week_errors)
        boxes_total = week_df["box_id"].nunique()
        boxes_with_errors = week_errors["box_id"].nunique()

        return {
            "week": week_key,
            "total_checks": int(total_checks),
            "total_errors": int(total_errors),
            "error_rate": float((total_errors / total_checks * 100) if total_checks > 0 else 0.0),
            "box_error_rate": float((boxes_with_errors / boxes_total * 100) if boxes_total > 0 else 0.0),
            "boxes_with_errors": int(boxes_with_errors),
            "boxes_total": int(boxes_total),
        }

    current = week_metrics(current_week)
    previous = week_metrics(previous_week)

    return {
        "available": True,
        "current": current,
        "previous": previous,
        "delta_error_rate": round(current["error_rate"] - previous["error_rate"], 1),
        "delta_box_error_rate": round(current["box_error_rate"] - previous["box_error_rate"], 1),
        "delta_errors": current["total_errors"] - previous["total_errors"],
    }


def build_mail_package(
    report_url: str,
    date_from: str,
    date_to: str,
    total_checks: int,
    total_errors: int,
    error_rate: float,
    unique_boxes_with_errors: int,
    total_boxes: int,
    box_error_rate: float,
    selected_errors: list[str],
    hotspot_df: pd.DataFrame,
    source_label: str,
    generated_at: str,
) -> dict:
    if error_rate >= 6 or box_error_rate >= 10:
        status_phrase = "im aktuellen Zeitraum ist ein erhöhter Handlungsbedarf erkennbar"
        intro_sentence = "die aktuelle Auswertung zeigt ein klares Qualitätsrisiko, das kurzfristig priorisiert werden sollte."
    elif error_rate >= 3 or box_error_rate >= 5:
        status_phrase = "im aktuellen Zeitraum ist ein beobachtungswürdiges Qualitätsniveau sichtbar"
        intro_sentence = "die Entwicklung sollte eng verfolgt und mit den betroffenen Teams nachgesteuert werden."
    else:
        status_phrase = "im aktuellen Zeitraum zeigt sich ein insgesamt stabiles Qualitätsbild"
        intro_sentence = "der Report zeigt aktuell keine außergewöhnliche Eskalation, einzelne Hotspots bleiben aber relevant."

    hotspot_sentence = ""
    if not hotspot_df.empty:
        top_row = hotspot_df.iloc[0]
        hotspot_sentence = (
            f"Der stärkste aktuelle Hotspot liegt bei {top_row['Dimension']} {top_row['Wert']} "
            f"mit {fmt_int(top_row['Vorfälle'])} Vorfällen."
        )

    error_filter_text = ", ".join(selected_errors) if selected_errors else "keine spezifischen Fehlertypen"
    subject = f"{REPORT_TITLE} | {date_to} | Fehlerquote {error_rate:.1f}%"
    link_available = bool((report_url or "").strip())

    plain_lines = [
        "Hallo zusammen,",
        "",
        f"anbei der aktuelle Assembly-QC-Report für den Zeitraum {date_from} bis {date_to}.",
        f"Zusammenfassend {status_phrase}. {intro_sentence}",
    ]
    if hotspot_sentence:
        plain_lines.append(hotspot_sentence)
    plain_lines.extend([
        "",
        "Die wichtigsten Kennzahlen im Überblick:",
        f"- Gesamte Prüfungen: {fmt_int(total_checks)}",
        f"- Fehlerhafte Prüfungen: {fmt_int(total_errors)}",
        f"- Fehlerquote: {error_rate:.1f}%",
        f"- Boxen mit Fehlern: {fmt_int(unique_boxes_with_errors)} von {fmt_int(total_boxes)}",
        f"- Box-Fehlerquote: {box_error_rate:.1f}%",
        f"- Aktive Fehlertypen im Filter: {error_filter_text}",
        f"- Datenquelle: {source_label}",
        f"- Generiert am: {generated_at}",
        "",
    ])
    if link_available:
        plain_lines.append(f"Interaktiver Report: {report_url}")
    else:
        plain_lines.append("Interaktiver Report: Noch keine öffentliche Freigabe-URL hinterlegt. Bitte Report zuerst in eine erreichbare Ablage hochladen.")
    plain_lines.extend(["", "Viele Grüße"])
    plain_body = "\n".join(plain_lines).strip()

    hotspot_html = ""
    if hotspot_sentence:
        hotspot_html = f"<p style='margin:0 0 14px; color:#d6d6d6; font-size:15px; line-height:1.6;'>{html_escape(hotspot_sentence)}</p>"

    report_cta_html = (
        f"<p style='margin:10px 0 0;'><a href='{html_escape(report_url)}' style='display:inline-block; padding:12px 18px; background:linear-gradient(135deg,#111827,#374151); color:#ffffff; border-radius:12px; text-decoration:none; font-weight:700;'>Interaktiven Report öffnen</a></p>"
        if link_available
        else "<div style='margin-top:14px; padding:14px 16px; border-radius:14px; background:#fff7ed; border:1px solid #fed7aa; color:#9a3412; font-size:14px; line-height:1.6;'><strong>Hinweis:</strong> Für Empfänger außerhalb deines Rechners ist noch kein funktionierender Report-Link hinterlegt. Bitte oben im Tool zuerst eine Freigabe-URL eintragen.</div>"
    )

    html_body = f"""
<div style="font-family:'Segoe UI',Arial,sans-serif; background:#f3f4f6; padding:24px; color:#111827;">
    <div style="max-width:760px; margin:0 auto; background:#ffffff; border-radius:22px; overflow:hidden; box-shadow:0 18px 45px rgba(15,23,42,0.14); border:1px solid rgba(15,23,42,0.08);">
        <div style="background:#050505; padding:24px 28px;">
            <div style="color:#ffffff; font-size:30px; font-weight:800; letter-spacing:0.42em; text-transform:uppercase;">FACTOR_</div>
            <div style="margin-top:10px; color:#bdbdbd; font-size:13px; letter-spacing:0.18em; text-transform:uppercase;">Assembly QC Weekly Bug Report</div>
        </div>
        <div style="padding:28px;">
            <p style="margin:0 0 12px; font-size:16px; line-height:1.6;">Hallo zusammen,</p>
            <p style="margin:0 0 14px; font-size:15px; line-height:1.7; color:#374151;">anbei der aktuelle Assembly-QC-Report für den Zeitraum <strong>{html_escape(date_from)}</strong> bis <strong>{html_escape(date_to)}</strong>.</p>
            <div style="background:linear-gradient(135deg,#111827,#1f2937); color:#ffffff; border-radius:18px; padding:20px 22px; margin:0 0 18px;">
                <div style="font-size:12px; text-transform:uppercase; letter-spacing:0.18em; color:#9ca3af; margin-bottom:10px;">Executive Summary</div>
                <div style="font-size:18px; line-height:1.6; font-weight:600;">{html_escape(status_phrase).capitalize()}.</div>
                <div style="margin-top:8px; font-size:15px; line-height:1.7; color:#d1d5db;">{html_escape(intro_sentence)}</div>
            </div>
            {hotspot_html}
            <table style="width:100%; border-collapse:separate; border-spacing:12px; margin:10px 0 16px;">
                <tr>
                    <td style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:16px; padding:16px;"><div style="font-size:12px; color:#6b7280; text-transform:uppercase; letter-spacing:0.12em;">Checks</div><div style="font-size:24px; font-weight:800; margin-top:6px;">{fmt_int(total_checks)}</div></td>
                    <td style="background:#fff7ed; border:1px solid #fed7aa; border-radius:16px; padding:16px;"><div style="font-size:12px; color:#9a3412; text-transform:uppercase; letter-spacing:0.12em;">Fehler</div><div style="font-size:24px; font-weight:800; margin-top:6px; color:#c2410c;">{fmt_int(total_errors)}</div></td>
                    <td style="background:#eff6ff; border:1px solid #bfdbfe; border-radius:16px; padding:16px;"><div style="font-size:12px; color:#1d4ed8; text-transform:uppercase; letter-spacing:0.12em;">Fehlerquote</div><div style="font-size:24px; font-weight:800; margin-top:6px; color:#1d4ed8;">{error_rate:.1f}%</div></td>
                </tr>
                <tr>
                    <td style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:16px; padding:16px;"><div style="font-size:12px; color:#6b7280; text-transform:uppercase; letter-spacing:0.12em;">Boxen mit Fehlern</div><div style="font-size:24px; font-weight:800; margin-top:6px;">{fmt_int(unique_boxes_with_errors)}</div></td>
                    <td style="background:#f9fafb; border:1px solid #e5e7eb; border-radius:16px; padding:16px;"><div style="font-size:12px; color:#6b7280; text-transform:uppercase; letter-spacing:0.12em;">Gesamtboxen</div><div style="font-size:24px; font-weight:800; margin-top:6px;">{fmt_int(total_boxes)}</div></td>
                    <td style="background:#ecfeff; border:1px solid #a5f3fc; border-radius:16px; padding:16px;"><div style="font-size:12px; color:#0f766e; text-transform:uppercase; letter-spacing:0.12em;">Box-Fehlerquote</div><div style="font-size:24px; font-weight:800; margin-top:6px; color:#0f766e;">{box_error_rate:.1f}%</div></td>
                </tr>
            </table>
            <p style="margin:0 0 8px; color:#374151; font-size:14px; line-height:1.7;"><strong>Aktive Fehlertypen:</strong> {html_escape(error_filter_text)}</p>
            <p style="margin:0 0 8px; color:#374151; font-size:14px; line-height:1.7;"><strong>Datenquelle:</strong> {html_escape(source_label)}</p>
            <p style="margin:0 0 8px; color:#374151; font-size:14px; line-height:1.7;"><strong>Generiert am:</strong> {html_escape(generated_at)}</p>
            <p style="margin:18px 0 0; color:#374151; font-size:15px; line-height:1.7;">Der vollständige interaktive Report ist hier erreichbar:</p>
            {report_cta_html}
            <p style="margin:22px 0 0; color:#6b7280; font-size:13px; line-height:1.6;">Viele Grüße</p>
        </div>
    </div>
</div>
""".strip()

    return {
        "subject": subject,
        "plain_body": plain_body,
        "html_body": html_body,
        "has_public_link": link_available,
    }


def build_gmail_compose_url(recipient: str, subject: str, body: str) -> str:
    recipient_value = quote((recipient or "").strip())
    subject_value = quote(subject)
    body_value = quote(body)
    return (
        "https://mail.google.com/mail/?view=cm&fs=1&tf=1"
        f"&to={recipient_value}&su={subject_value}&body={body_value}"
    )


def build_prepared_message() -> dict:
    date_from = _date_min.strftime("%d.%m.%Y") if pd.notna(_date_min) else "–"
    date_to = _date_max.strftime("%d.%m.%Y") if pd.notna(_date_max) else "–"
    hotspot_df = top_issue_table(df_errors_selected)
    insight_lines = create_insight_summary(df_filtered, df_errors_selected)
    report_url = build_public_report_url(REPORT_SHARE_TARGET, "weekly_bug_report_latest.html")
    compare_line_de = ""
    compare_line_en = ""
    if week_over_week.get("available"):
        current_week = week_over_week["current"].get("week") or "aktuelle Woche"
        previous_week = week_over_week["previous"].get("week") or "Vorwoche"
        compare_line_de = (
            f"Vorwochenvergleich: {current_week} liegt bei {week_over_week['current'].get('error_rate', 0.0):.1f}% Fehlerquote "
            f"und damit {week_over_week.get('delta_error_rate', 0.0):+.1f} Prozentpunkte vs. {previous_week}."
        )
        compare_line_en = (
            f"Week-over-week: {current_week} is at {week_over_week['current'].get('error_rate', 0.0):.1f}% error rate, "
            f"which is {week_over_week.get('delta_error_rate', 0.0):+.1f} percentage points vs. {previous_week}."
        )

    if error_rate >= 6 or box_error_rate >= 10:
        status_line_de = "die Qualitätslage ist gerade nicht charmant, sondern klar handlungsbedürftig."
        status_line_en = "quality is not in a cute mood right now, and this needs action."
    elif error_rate >= 3 or box_error_rate >= 5:
        status_line_de = "die Lage ist noch kontrollierbar, aber definitiv nichts zum Zurücklehnen."
        status_line_en = "the situation is still manageable, but definitely not a lean-back-and-relax moment."
    else:
        status_line_de = "das Gesamtbild ist stabil, auch wenn ein paar übliche Verdächtige weiter nerven."
        status_line_en = "the overall picture is stable, even if a few usual suspects are still being annoying."

    hotspot_line_de = ""
    hotspot_line_en = ""
    if not hotspot_df.empty:
        top_row = hotspot_df.iloc[0]
        hotspot_line_de = f"Größter Hotspot: {top_row['Dimension']} {top_row['Wert']} mit {fmt_int(top_row['Vorfälle'])} Vorfällen."
        hotspot_line_en = f"Biggest hotspot: {top_row['Dimension']} {top_row['Wert']} with {fmt_int(top_row['Vorfälle'])} incidents."

    subject = f"{REPORT_TITLE} | {date_to} | PDF attached | Bitte anschauen"
    lines = [
        "Hallo zusammen, hi all,",
        "",
        f"anbei / attached: der aktuelle Assembly QC Weekly Bug Report als PDF fuer den Zeitraum {date_from} bis {date_to}.",
        "",
        "DE:",
        f"Kurz gesagt: {status_line_de}",
    ]
    if hotspot_line_de:
        lines.append(hotspot_line_de)
    if compare_line_de:
        lines.append(compare_line_de)
    lines.extend([
        "",
        "Wichtigste Kennzahlen:",
        f"- Gesamte Prüfungen: {fmt_int(total_checks)}",
        f"- Fehlerhafte Prüfungen: {fmt_int(total_errors)}",
        f"- Fehlerquote: {error_rate:.1f}%",
        f"- Boxen mit Fehlern: {fmt_int(unique_boxes_with_errors)} von {fmt_int(total_boxes)}",
        f"- Box-Fehlerquote: {box_error_rate:.1f}%",
    ])
    if insight_lines:
        lines.extend(["", "Kurzfazit:"])
        lines.extend([f"- {line}" for line in insight_lines[:3]])
    lines.extend([
        "",
        "EN:",
        f"Short version: {status_line_en}",
    ])
    if hotspot_line_en:
        lines.append(hotspot_line_en)
    if compare_line_en:
        lines.append(compare_line_en)
    lines.extend([
        "",
        "Key numbers:",
        f"- Total checks: {fmt_int(total_checks)}",
        f"- Failed checks: {fmt_int(total_errors)}",
        f"- Error rate: {error_rate:.1f}%",
        f"- Boxes with errors: {fmt_int(unique_boxes_with_errors)} of {fmt_int(total_boxes)}",
        f"- Box error rate: {box_error_rate:.1f}%",
        "",
        f"Interactive report: {report_url or 'No public share URL stored yet.'}",
        "",
        "Bottom line: the PDF has the details, the hotspots, and the names of the usual troublemakers.",
        "",
        "Viele Gruesse / Best regards",
    ])
    body = "\n".join(lines)
    return {"subject": subject, "body": body}


def build_pdf_report() -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.enums import TA_LEFT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    output = io.BytesIO()
    doc = SimpleDocTemplate(
        output,
        pagesize=A4,
        leftMargin=14 * mm,
        rightMargin=14 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
        title=REPORT_TITLE,
        author=BRAND_NAME,
    )

    styles = getSampleStyleSheet()
    styles.add(ParagraphStyle(name="ReportTitle", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=24, leading=28, textColor=colors.HexColor("#111827"), spaceAfter=6, alignment=TA_LEFT))
    styles.add(ParagraphStyle(name="SectionTitle", parent=styles["Heading2"], fontName="Helvetica-Bold", fontSize=15, leading=19, textColor=colors.HexColor("#0f172a"), spaceBefore=8, spaceAfter=8))
    styles.add(ParagraphStyle(name="BodyMuted", parent=styles["BodyText"], fontName="Helvetica", fontSize=10, leading=14, textColor=colors.HexColor("#4b5563"), spaceAfter=8))
    styles.add(ParagraphStyle(name="MetricValue", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=18, leading=22, textColor=colors.HexColor("#111827"), alignment=TA_LEFT))
    styles.add(ParagraphStyle(name="MetricLabel", parent=styles["BodyText"], fontName="Helvetica", fontSize=8, leading=10, textColor=colors.HexColor("#6b7280"), alignment=TA_LEFT))
    styles.add(ParagraphStyle(name="HeroBrand", parent=styles["Title"], fontName="Helvetica-Bold", fontSize=28, leading=30, textColor=colors.white, spaceAfter=6, alignment=TA_LEFT))
    styles.add(ParagraphStyle(name="HeroCopy", parent=styles["BodyText"], fontName="Helvetica", fontSize=10, leading=14, textColor=colors.HexColor("#d1d5db"), spaceAfter=6))
    styles.add(ParagraphStyle(name="HeroMeta", parent=styles["BodyText"], fontName="Helvetica-Bold", fontSize=9, leading=12, textColor=colors.white, spaceAfter=4))

    def draw_page(canvas, document):
        canvas.saveState()
        canvas.setStrokeColor(colors.HexColor("#d1d5db"))
        canvas.setLineWidth(0.4)
        canvas.line(document.leftMargin, 12 * mm, A4[0] - document.rightMargin, 12 * mm)
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(colors.HexColor("#6b7280"))
        canvas.drawString(document.leftMargin, 7 * mm, f"{BRAND_NAME} | {REPORT_TITLE}")
        canvas.drawRightString(A4[0] - document.rightMargin, 7 * mm, f"Seite {canvas.getPageNumber()}")
        canvas.restoreState()

    def make_metric_card(label: str, value: str, subtitle: str) -> Table:
        table = Table(
            [[Paragraph(label.upper(), styles["MetricLabel"])], [Paragraph(value, styles["MetricValue"])], [Paragraph(subtitle, styles["MetricLabel"])]],
            colWidths=[56 * mm],
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
            ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#dbe4ee")),
            ("INNERPADDING", (0, 0), (-1, -1), 8),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        return table

    def fig_to_image(fig, width: int = 1200, height: int = 650):
        try:
            fig.update_layout(paper_bgcolor="white", plot_bgcolor="white")
            return io.BytesIO(fig.to_image(format="png", width=width, height=height, scale=2))
        except Exception:
            return None

    story = []
    report_created = datetime.now().strftime("%d.%m.%Y %H:%M")
    date_from = _date_min.strftime("%d.%m.%Y") if pd.notna(_date_min) else "–"
    date_to = _date_max.strftime("%d.%m.%Y") if pd.notna(_date_max) else "–"
    insight_lines = create_insight_summary(df_filtered, df_errors_selected)
    hotspot_table_pdf = top_issue_table(df_errors_selected).copy()
    line_exec_pdf = prepare_line_exec_table(line_rate, warning_threshold, critical_threshold).head(8).copy()

    if error_rate >= 6 or box_error_rate >= 10:
        headline_status = "Erhöhter Handlungsbedarf"
        headline_copy = "Die aktuelle Qualitätslage erfordert kurzfristige Priorisierung und fokussierte Gegenmaßnahmen."
    elif error_rate >= 3 or box_error_rate >= 5:
        headline_status = "Beobachten und nachsteuern"
        headline_copy = "Die Entwicklung ist auffällig und sollte im operativen Review eng begleitet werden."
    else:
        headline_status = "Stabiles Qualitätsbild"
        headline_copy = "Die aktuelle Lage ist insgesamt stabil, relevante Hotspots bleiben im Report ausgewiesen."

    hero = Table(
        [[
            Paragraph("FACTOR_", styles["HeroBrand"]),
            Paragraph(f"<b>{REPORT_TITLE}</b><br/>{headline_status}<br/>{headline_copy}<br/><br/>Zeitraum: {date_from} bis {date_to}<br/>Erstellt: {report_created}<br/>Quelle: {html_escape(data_source_label)}", styles["HeroCopy"]),
        ]],
        colWidths=[55 * mm, 118 * mm],
    )
    hero.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#050505")),
        ("BOX", (0, 0), (-1, -1), 0, colors.white),
        ("LEFTPADDING", (0, 0), (-1, -1), 18),
        ("RIGHTPADDING", (0, 0), (-1, -1), 18),
        ("TOPPADDING", (0, 0), (-1, -1), 18),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 18),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
    ]))
    story.append(hero)
    story.append(Spacer(1, 10))

    summary_band = Table(
        [[Paragraph("Executive Summary", styles["SectionTitle"]), Paragraph(headline_copy, styles["BodyMuted"])]],
        colWidths=[48 * mm, 125 * mm],
    )
    summary_band.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#eef2ff")),
        ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#c7d2fe")),
        ("LEFTPADDING", (0, 0), (-1, -1), 12),
        ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(summary_band)
    story.append(Spacer(1, 6))

    metrics = [
        ("Gesamte Checks", fmt_int(total_checks), "Operatives Volumen im Filter"),
        ("Fehlerhafte Checks", fmt_int(total_errors), "Erkannte Fälle nach Fehlertyp-Filter"),
        ("Fehlerquote", fmt_pct(error_rate), "Anteil fehlerhafter Prüfungen"),
        ("Boxen mit Fehlern", fmt_int(unique_boxes_with_errors), "Eindeutig betroffene Boxen"),
        ("Gesamtboxen", fmt_int(total_boxes), "Vergleichsbasis Box-Level"),
        ("Box-Fehlerquote", fmt_pct(box_error_rate), "Anteil betroffener Boxen"),
    ]
    metric_rows = [metrics[:3], metrics[3:]]
    metric_table = Table([[make_metric_card(*item) for item in row] for row in metric_rows], colWidths=[60 * mm, 60 * mm, 60 * mm], hAlign="LEFT")
    metric_table.setStyle(TableStyle([("BOTTOMPADDING", (0, 0), (-1, -1), 6), ("TOPPADDING", (0, 0), (-1, -1), 0)]))
    story.append(metric_table)
    story.append(Spacer(1, 10))

    story.append(Paragraph("Executive Summary", styles["SectionTitle"]))
    for insight in insight_lines:
        story.append(Paragraph(f"• {insight}", styles["BodyMuted"]))

    if not hotspot_table_pdf.empty:
        story.append(Spacer(1, 10))
        story.append(Paragraph("Hotspots & Prioritäten", styles["SectionTitle"]))
        hotspot_rows = [["Dimension", "Wert", "Vorfälle"]] + hotspot_table_pdf.astype(str).values.tolist()
        hotspot_pdf_table = Table(hotspot_rows, colWidths=[42 * mm, 78 * mm, 28 * mm], repeatRows=1)
        hotspot_pdf_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#ffffff"), colors.HexColor("#f8fafc")]),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("LEADING", (0, 0), (-1, -1), 12),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ]))
        story.append(hotspot_pdf_table)

    if not line_exec_pdf.empty:
        line_exec_pdf = line_exec_pdf.copy()
        line_exec_pdf["Fehlerquote (%)"] = line_exec_pdf["Fehlerquote (%)"].map(lambda value: f"{value:.1f}%")
        line_rows = [line_exec_pdf.columns.tolist()] + line_exec_pdf.astype(str).values.tolist()
        line_pdf_table = Table(line_rows, repeatRows=1)
        line_pdf_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f766e")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#ffffff"), colors.HexColor("#f8fafc")]),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d1d5db")),
            ("FONTSIZE", (0, 0), (-1, -1), 8),
            ("LEADING", (0, 0), (-1, -1), 11),
            ("LEFTPADDING", (0, 0), (-1, -1), 5),
            ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ]))
        story.append(Spacer(1, 10))
        story.append(line_pdf_table)

    story.append(PageBreak())
    story.append(Paragraph("Visual Deep Dive", styles["SectionTitle"]))
    story.append(Paragraph("Die wichtigsten Grafiken sind direkt in das PDF eingebettet, damit der Bericht auch ohne Browser vollständig lesbar ist.", styles["BodyMuted"]))

    charts = []
    if not df_errors_selected.empty:
        error_dist = df_errors_selected["checks_error_category"].value_counts().reset_index()
        error_dist.columns = ["Fehlertyp", "Anzahl"]
        fig_error = px.pie(error_dist, values="Anzahl", names="Fehlertyp", color="Fehlertyp", color_discrete_map=ERROR_COLOR_MAP, hole=0.45, title="Fehlertypen-Verteilung")
        charts.append(("Fehlertypen-Verteilung", fig_error))

        line_dist = df_errors_selected.groupby(["assembly_line", "checks_error_category"]).size().reset_index(name="Anzahl")
        fig_line = px.bar(line_dist, x="assembly_line", y="Anzahl", color="checks_error_category", color_discrete_map=ERROR_COLOR_MAP, barmode="group", title="Fehler pro Assembly Line", labels={"assembly_line": "Assembly Line", "checks_error_category": "Fehlertyp"})
        charts.append(("Fehler pro Assembly Line", fig_line))

    if not sku_errors.empty:
        fig_sku = px.bar(sku_errors, x="Anzahl Fehler", y="Label", orientation="h", title="Top fehlerhafte SKUs", color="Anzahl Fehler", color_continuous_scale=["#fde68a", "#f59e0b", "#dc2626"], text="Anzahl Fehler")
        fig_sku.update_traces(textposition="outside")
        fig_sku.update_layout(yaxis=dict(autorange="reversed"), coloraxis_showscale=False, margin=dict(t=50, r=40, b=30, l=20))
        charts.append(("Top fehlerhafte SKUs", fig_sku))

    if not df_errors_selected.empty:
        user_dist = df_errors_selected.groupby(["user_id", "checks_error_category"]).size().reset_index(name="Anzahl")
        user_dist["user_id"] = "User " + user_dist["user_id"].astype(str)
        fig_user = px.bar(user_dist, x="user_id", y="Anzahl", color="checks_error_category", color_discrete_map=ERROR_COLOR_MAP, barmode="stack", title="Fehler pro Mitarbeiter")
        charts.append(("Fehler pro Mitarbeiter", fig_user))

    chart_count = 0
    for chart_title, figure in charts:
        image_stream = fig_to_image(figure)
        if image_stream is None:
            continue
        story.append(Spacer(1, 6))
        story.append(Paragraph(chart_title, styles["SectionTitle"]))
        story.append(Image(image_stream, width=180 * mm, height=95 * mm))
        chart_count += 1

    if chart_count == 0:
        story.append(Paragraph("Für die aktuelle Auswahl konnten keine Diagramme in das PDF eingebettet werden.", styles["BodyMuted"]))

    if not df_errors_selected.empty:
        story.append(PageBreak())
        story.append(Paragraph("Fehlerlog Auszug", styles["SectionTitle"]))
        story.append(Paragraph("Die wichtigsten aktuellen Vorgänge als kompakter Auszug für den Mail-Anhang.", styles["BodyMuted"]))
        detail_df = df_errors_selected[["start_date", "assembly_line", "box_id", "checks_error_category", "checks_error_sku", "checks_error_sku_name", "checks_error_quantity"]].copy().head(18)
        detail_df["start_date"] = detail_df["start_date"].dt.strftime("%d.%m.%Y")
        detail_df.columns = ["Datum", "Linie", "Box", "Fehlertyp", "SKU", "Artikelname", "Menge"]
        detail_rows = [detail_df.columns.tolist()] + detail_df.fillna("–").astype(str).values.tolist()
        detail_table = Table(detail_rows, repeatRows=1, colWidths=[20 * mm, 16 * mm, 22 * mm, 24 * mm, 22 * mm, 62 * mm, 14 * mm])
        detail_table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#ffffff"), colors.HexColor("#f8fafc")]),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#d1d5db")),
            ("FONTSIZE", (0, 0), (-1, -1), 7),
            ("LEADING", (0, 0), (-1, -1), 9),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ]))
        story.append(detail_table)

    doc.build(story, onFirstPage=draw_page, onLaterPages=draw_page)
    output.seek(0)
    return output.getvalue()


def truncate_text(value, limit: int = 42) -> str:
    if pd.isna(value):
        return "Unbekannt"
    text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def build_sku_ranking(errors_df: pd.DataFrame, limit: int = 15) -> pd.DataFrame:
    if errors_df.empty:
        return pd.DataFrame(columns=["SKU", "Artikelname", "Anzahl Fehler", "Fehlermenge", "Label"])

    sku_rank = (
        errors_df.dropna(subset=["checks_error_sku"])
        .groupby(["checks_error_sku", "checks_error_sku_name"], dropna=False)
        .agg(
            Anzahl_Fehler=("box_id", "count"),
            Fehlermenge=("checks_error_quantity", "sum"),
        )
        .sort_values(["Anzahl_Fehler", "Fehlermenge"], ascending=[False, False])
        .head(limit)
        .reset_index()
    )
    if sku_rank.empty:
        return pd.DataFrame(columns=["SKU", "Artikelname", "Anzahl Fehler", "Fehlermenge", "Label"])

    sku_rank.columns = ["SKU", "Artikelname", "Anzahl Fehler", "Fehlermenge"]
    sku_rank["Artikelname"] = sku_rank["Artikelname"].fillna("Unbekannt")
    sku_rank["Label"] = sku_rank.apply(
        lambda row: f"{row['SKU']} • {truncate_text(row['Artikelname'], 34)}",
        axis=1,
    )
    return sku_rank


def build_selection_summary(subset: pd.DataFrame, title: str, subtitle: str) -> dict:
    if subset.empty:
        return {
            "title": title,
            "subtitle": subtitle,
            "count": 0,
            "articles": [],
            "cases": [],
        }

    normalized = subset.copy()
    normalized["start_date"] = pd.to_datetime(normalized["start_date"], errors="coerce")
    normalized["checks_error_sku_name"] = normalized["checks_error_sku_name"].fillna("Unbekannt")
    normalized["checks_error_sku"] = normalized["checks_error_sku"].fillna("–")

    top_articles = (
        normalized.groupby(["checks_error_sku", "checks_error_sku_name"], dropna=False)
        .agg(
            incidents=("box_id", "count"),
            quantity=("checks_error_quantity", "sum"),
            lines=("assembly_line", lambda x: ", ".join(sorted(pd.Series(x).dropna().astype(str).unique())[:4])),
        )
        .sort_values(["incidents", "quantity"], ascending=[False, False])
        .head(8)
        .reset_index()
    )

    recent_cases = (
        normalized.sort_values("start_date", ascending=False)
        [[
            "start_date", "assembly_line", "box_id", "checks_error_category",
            "checks_error_sku", "checks_error_sku_name", "checks_error_quantity"
        ]]
        .head(8)
        .copy()
    )

    articles = [
        {
            "sku": row["checks_error_sku"],
            "name": row["checks_error_sku_name"],
            "incidents": int(row["incidents"]),
            "quantity": int(row["quantity"]) if pd.notna(row["quantity"]) else 0,
            "lines": row["lines"] or "–",
        }
        for _, row in top_articles.iterrows()
    ]

    cases = [
        {
            "date": row["start_date"].strftime("%d.%m.%Y") if pd.notna(row["start_date"]) else "–",
            "line": row["assembly_line"],
            "box": row["box_id"],
            "error_type": row["checks_error_category"],
            "sku": row["checks_error_sku"],
            "name": row["checks_error_sku_name"],
            "quantity": int(row["checks_error_quantity"]) if pd.notna(row["checks_error_quantity"]) else 0,
        }
        for _, row in recent_cases.iterrows()
    ]

    top_line = normalized.groupby("assembly_line").size().sort_values(ascending=False)
    top_error_type = normalized.groupby("checks_error_category").size().sort_values(ascending=False)
    top_station = pd.Series(dtype="int64")
    if "stations" in normalized.columns:
        top_station = (
            normalized.dropna(subset=["stations"])
            .assign(stations=lambda data: pd.to_numeric(data["stations"], errors="coerce"))
            .dropna(subset=["stations"])
            .groupby("stations")
            .size()
            .sort_values(ascending=False)
        )

    recommendations = []
    if not top_line.empty:
        recommendations.append(
            f"Prüfe Linie {top_line.index[0]} zuerst; dort liegen aktuell die meisten Fälle innerhalb dieser Auswahl."
        )
    if articles:
        recommendations.append(
            f"Priorisiere SKU {articles[0]['sku']} ({articles[0]['name']}) für Ursachenanalyse und Gegenmaßnahme."
        )
    if not top_error_type.empty:
        recommendations.append(
            f"Fokussiere die Qualitätsmaßnahme auf den Fehlertyp {top_error_type.index[0]}."
        )
    if not top_station.empty:
        recommendations.append(
            f"Station {int(top_station.index[0])} sollte im nächsten Review gezielt geprüft werden."
        )

    return {
        "title": title,
        "subtitle": subtitle,
        "count": int(len(normalized)),
        "articles": articles,
        "cases": cases,
        "recommendations": recommendations[:3],
    }


def build_chart_click_payloads(errors_df: pd.DataFrame) -> dict:
    payloads = {
        "error_type": {},
        "week_type": {},
        "line_type": {},
        "sku": {},
        "user_type": {},
        "station_line": {},
    }
    if errors_df.empty:
        return payloads

    for error_type, subset in errors_df.groupby("checks_error_category"):
        payloads["error_type"][str(error_type)] = build_selection_summary(
            subset,
            f"Fehlertyp: {error_type}",
            "Top Artikel und aktuelle Beispiele für den gewählten Fehlertyp",
        )

    for (year_week, error_type), subset in errors_df.groupby(["year_week", "checks_error_category"]):
        payloads["week_type"][f"{year_week}|{error_type}"] = build_selection_summary(
            subset,
            f"{year_week} • {error_type}",
            "Artikelliste für die gewählte Woche und den gewählten Fehlertyp",
        )

    for (assembly_line, error_type), subset in errors_df.groupby(["assembly_line", "checks_error_category"]):
        payloads["line_type"][f"{assembly_line}|{error_type}"] = build_selection_summary(
            subset,
            f"Linie {assembly_line} • {error_type}",
            "Artikel und Fälle hinter dem gewählten Liniensegment",
        )

    for sku, subset in errors_df.dropna(subset=["checks_error_sku"]).groupby("checks_error_sku"):
        payloads["sku"][str(sku)] = build_selection_summary(
            subset,
            f"SKU {sku}",
            "Detailansicht für die gewählte SKU",
        )

    for (user_id, error_type), subset in errors_df.groupby(["user_id", "checks_error_category"]):
        payloads["user_type"][f"User {user_id}|{error_type}"] = build_selection_summary(
            subset,
            f"User {user_id} • {error_type}",
            "Artikel und Fälle hinter dem gewählten Mitarbeitersegment",
        )

    station_df = errors_df.dropna(subset=["stations"]).copy()
    if not station_df.empty:
        station_df["stations"] = pd.to_numeric(station_df["stations"], errors="coerce")
        station_df = station_df.dropna(subset=["stations"])
        station_df["stations"] = station_df["stations"].astype(int).astype(str)
        for (assembly_line, station), subset in station_df.groupby(["assembly_line", "stations"]):
            payloads["station_line"][f"{assembly_line}|{station}"] = build_selection_summary(
                subset,
                f"Linie {assembly_line} • Station {station}",
                "Artikel und Fälle an der gewählten Stationszelle",
            )

    return payloads


def default_selection_payload() -> dict:
    return {
        "title": "Kein Chart-Segment ausgewählt",
        "subtitle": "Klicke in einem interaktiven Diagramm auf ein Segment. Dann erscheinen hier die betroffenen Artikel und aktuelle Beispielvorgänge.",
        "count": 0,
        "articles": [],
        "cases": [],
        "recommendations": [],
    }


def resolve_plotly_event_key(interaction: str, event: dict, figure) -> str | None:
    if not event:
        return None

    curve_number = event.get("curveNumber", 0)
    point_number = event.get("pointNumber", 0)
    trace = figure.data[curve_number]

    if interaction == "error_type":
        labels = list(trace.labels)
        return str(labels[point_number]) if point_number < len(labels) else None
    if interaction in {"week_type", "line_type", "user_type"}:
        x_value = event.get("x")
        trace_name = getattr(trace, "name", None)
        return f"{x_value}|{trace_name}" if x_value is not None and trace_name is not None else None
    if interaction == "sku":
        y_value = event.get("y")
        return str(y_value).split(" • ")[0] if y_value is not None else None
    if interaction == "station_line":
        x_value = event.get("x")
        y_value = event.get("y")
        return f"{y_value}|{x_value}" if x_value is not None and y_value is not None else None
    return None


def render_streamlit_selection_panel(payload: dict) -> None:
    st.markdown('<div class="control-card dashboard-sticky">', unsafe_allow_html=True)
    st.subheader(payload.get("title", "Drilldown-Details"))
    st.caption(payload.get("subtitle", ""))

    stat_col1, stat_col2, stat_col3 = st.columns(3)
    stat_col1.metric("Vorfälle", payload.get("count", 0))
    stat_col2.metric("Artikel", len(payload.get("articles", [])))
    stat_col3.metric("Beispielfälle", len(payload.get("cases", [])))

    st.markdown("**Betroffene Artikel**")
    articles = payload.get("articles", [])
    if articles:
        article_df = pd.DataFrame(articles)
        article_df = article_df.rename(columns={
            "sku": "SKU",
            "name": "Artikelname",
            "incidents": "Vorfälle",
            "quantity": "Fehlermenge",
            "lines": "Linien",
        })
        st.dataframe(article_df, width="stretch", hide_index=True, height=260)
    else:
        st.info("Noch keine Artikeldetails vorhanden.")

    st.markdown("**Empfohlene Maßnahmen**")
    recommendations = payload.get("recommendations", [])
    if recommendations:
        for recommendation in recommendations:
            st.markdown(f"- {recommendation}")
    else:
        st.info("Noch keine Handlungsempfehlungen für diese Auswahl.")

    st.markdown("**Aktuelle Beispielvorgänge**")
    cases = payload.get("cases", [])
    if cases:
        case_df = pd.DataFrame(cases)
        case_df = case_df.rename(columns={
            "date": "Datum",
            "line": "Linie",
            "box": "Box",
            "error_type": "Fehlertyp",
            "sku": "SKU",
            "name": "Artikelname",
            "quantity": "Menge",
        })
        st.dataframe(case_df, width="stretch", hide_index=True, height=260)
    else:
        st.info("Noch keine Beispielvorgänge vorhanden.")

    export_col1, export_col2 = st.columns(2)
    export_payload = {
        "title": payload.get("title"),
        "subtitle": payload.get("subtitle"),
        "count": payload.get("count", 0),
        "recommendations": recommendations,
        "articles": articles,
        "cases": cases,
    }
    export_col1.download_button(
        "Auswahl als JSON",
        data=json.dumps(export_payload, ensure_ascii=False, indent=2),
        file_name="drilldown_selection.json",
        mime="application/json",
        width="stretch",
    )
    case_export_df = pd.DataFrame(cases) if cases else pd.DataFrame(columns=["date", "line", "box", "error_type", "sku", "name", "quantity"])
    export_col2.download_button(
        "Auswahl als CSV",
        data=case_export_df.to_csv(index=False, sep=";"),
        file_name="drilldown_selection.csv",
        mime="text/csv",
        width="stretch",
    )
    st.markdown('</div>', unsafe_allow_html=True)


def create_insight_summary(filtered_df: pd.DataFrame, errors_df: pd.DataFrame) -> list[str]:
    insights = []
    if filtered_df.empty:
        return ["Keine Daten im aktuellen Filterbereich vorhanden."]

    if not errors_df.empty:
        top_line = errors_df.groupby("assembly_line").size().sort_values(ascending=False)
        top_user = errors_df.groupby("user_id").size().sort_values(ascending=False)
        top_type = errors_df["checks_error_category"].value_counts()
        top_day = errors_df.groupby("day_of_week").size().sort_values(ascending=False)

        if not top_line.empty:
            insights.append(
                f"Höchste Fehlerlast auf Linie {top_line.index[0]} mit {fmt_int(top_line.iloc[0])} Vorfällen."
            )
        if not top_user.empty:
            insights.append(
                f"User {top_user.index[0]} hat aktuell die meisten erkannten Fehler mit {fmt_int(top_user.iloc[0])} Fällen."
            )
        if not top_type.empty:
            insights.append(
                f"Dominanter Fehlertyp ist {top_type.index[0]} mit {fmt_int(top_type.iloc[0])} Ereignissen."
            )
        if not top_day.empty:
            insights.append(
                f"Peak im Wochenverlauf liegt auf {top_day.index[0]} mit {fmt_int(top_day.iloc[0])} Fehlern."
            )
    else:
        insights.append("Im gewählten Filterbereich wurden keine Fehler erkannt.")

    return insights[:4]


def top_issue_table(errors_df: pd.DataFrame) -> pd.DataFrame:
    if errors_df.empty:
        return pd.DataFrame(columns=["Dimension", "Wert", "Vorfälle"])

    line_rank = errors_df.groupby("assembly_line").size().reset_index(name="Vorfälle")
    line_rank["Dimension"] = "Assembly Line"
    line_rank = line_rank.rename(columns={"assembly_line": "Wert"})

    type_rank = errors_df.groupby("checks_error_category").size().reset_index(name="Vorfälle")
    type_rank["Dimension"] = "Fehlertyp"
    type_rank = type_rank.rename(columns={"checks_error_category": "Wert"})

    station_rank = pd.DataFrame(columns=["Dimension", "Wert", "Vorfälle"])
    if "stations" in errors_df.columns:
        station_rank = (
            errors_df.dropna(subset=["stations"])
            .assign(stations=lambda data: pd.to_numeric(data["stations"], errors="coerce"))
            .dropna(subset=["stations"])
            .groupby("stations")
            .size()
            .reset_index(name="Vorfälle")
        )
        if not station_rank.empty:
            station_rank["stations"] = station_rank["stations"].astype(int).astype(str)
            station_rank["Dimension"] = "Station"
            station_rank = station_rank.rename(columns={"stations": "Wert"})

    ranking = pd.concat([line_rank, type_rank, station_rank], ignore_index=True)
    return ranking.sort_values("Vorfälle", ascending=False).head(12)


def prepare_line_exec_table(line_rate_df: pd.DataFrame, warning_threshold: float, critical_threshold: float) -> pd.DataFrame:
    exec_table = line_rate_df.sort_values(["Fehlerquote (%)", "Fehler"], ascending=[False, False]).copy()
    exec_table["Priorität"] = exec_table["Fehlerquote (%)"].apply(
        lambda value: classify_status(value, warning_threshold, critical_threshold)
    )
    return exec_table

# ─── Page Config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Assembly QC – Weekly Bug Report",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=Manrope:wght@400;500;600;700;800&display=swap');

    :root {
        --bg-main: #f4f1ea;
        --surface: rgba(255, 252, 247, 0.88);
        --surface-strong: #fffdfa;
        --ink: #1f2937;
        --muted: #6b7280;
        --accent: #0f766e;
        --accent-2: #d97706;
        --accent-3: #d9485f;
        --line: rgba(15, 23, 42, 0.08);
        --shadow: 0 18px 45px rgba(31, 41, 55, 0.08);
    }

    .stApp {
        background:
            radial-gradient(circle at top left, rgba(15, 118, 110, 0.16), transparent 28%),
            radial-gradient(circle at top right, rgba(217, 119, 6, 0.18), transparent 24%),
            linear-gradient(180deg, #fbf8f3 0%, #f4f1ea 100%);
        color: var(--ink);
    }

    html, body, [class*="css"]  {
        font-family: 'Manrope', sans-serif;
    }

    h1, h2, h3 {
        font-family: 'Space Grotesk', sans-serif;
        letter-spacing: -0.03em;
    }

    section[data-testid="stSidebar"] {
        background: linear-gradient(180deg, rgba(18, 36, 52, 0.98) 0%, rgba(11, 24, 38, 0.98) 100%);
        border-right: 1px solid rgba(255,255,255,0.08);
    }

    section[data-testid="stSidebar"] * {
        color: #f7fafc;
    }

    section[data-testid="stSidebar"] input,
    section[data-testid="stSidebar"] textarea,
    section[data-testid="stSidebar"] [data-baseweb="input"] input,
    section[data-testid="stSidebar"] [data-baseweb="select"] input,
    section[data-testid="stSidebar"] [data-baseweb="tag"] span,
    section[data-testid="stSidebar"] [data-baseweb="select"] span,
    section[data-testid="stSidebar"] [data-baseweb="select"] div {
        color: #111827 !important;
    }

    section[data-testid="stSidebar"] [data-baseweb="input"],
    section[data-testid="stSidebar"] [data-baseweb="select"],
    section[data-testid="stSidebar"] [data-baseweb="popover"] {
        color: #111827 !important;
    }

    .stTabs [data-baseweb="tab"] {
        color: var(--ink) !important;
    }

    .stDataFrame, .stTable {
        color: var(--ink);
    }

    .hero-panel {
        padding: 1.6rem 1.8rem;
        border-radius: 24px;
        background:
            linear-gradient(135deg, rgba(15, 118, 110, 0.96), rgba(17, 94, 89, 0.86)),
            linear-gradient(135deg, rgba(255,255,255,0.18), rgba(255,255,255,0));
        color: #fff;
        box-shadow: var(--shadow);
        position: relative;
        overflow: hidden;
        margin-bottom: 1.2rem;
    }

    .brand-mark {
        width: 58px;
        height: 58px;
        border-radius: 18px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(145deg, rgba(255,255,255,0.24), rgba(255,255,255,0.08));
        border: 1px solid rgba(255,255,255,0.22);
        font-family: 'Space Grotesk', sans-serif;
        font-size: 1.2rem;
        font-weight: 800;
        margin-bottom: 0.9rem;
        backdrop-filter: blur(10px);
    }

    .hero-panel::after {
        content: "";
        position: absolute;
        inset: auto -40px -60px auto;
        width: 220px;
        height: 220px;
        background: radial-gradient(circle, rgba(255,255,255,0.18), transparent 68%);
        pointer-events: none;
    }

    .hero-kicker {
        text-transform: uppercase;
        letter-spacing: 0.18em;
        font-size: 0.74rem;
        font-weight: 800;
        opacity: 0.82;
        margin-bottom: 0.55rem;
    }

    .hero-title {
        font-size: 2.35rem;
        font-weight: 800;
        margin: 0;
    }

    .hero-subtitle {
        margin-top: 0.75rem;
        max-width: 900px;
        opacity: 0.94;
        line-height: 1.55;
        font-size: 0.98rem;
    }

    .info-strip {
        display: flex;
        gap: 0.7rem;
        flex-wrap: wrap;
        margin: 0.9rem 0 1rem 0;
    }

    .metric-card {
        background: linear-gradient(145deg, rgba(255,255,255,0.94), rgba(255,249,243,0.9));
        padding: 1.2rem;
        border-radius: 20px;
        color: var(--ink);
        text-align: center;
        box-shadow: var(--shadow);
        border: 1px solid rgba(255,255,255,0.7);
    }
    .metric-card.red {
        background: linear-gradient(145deg, rgba(255,241,243,0.98), rgba(255,226,231,0.92));
    }
    .metric-card.green {
        background: linear-gradient(145deg, rgba(233,255,249,0.98), rgba(213,250,241,0.95));
    }
    .metric-card.orange {
        background: linear-gradient(145deg, rgba(255,247,237,0.98), rgba(254,240,213,0.95));
    }
    .metric-card h2 { margin: 0; font-size: 2rem; font-family: 'Space Grotesk', sans-serif; }
    .metric-card p { margin: 0.3rem 0 0 0; font-size: 0.9rem; opacity: 0.84; }
    .section-header {
        border-left: 4px solid var(--accent);
        padding-left: 12px;
        margin: 1.6rem 0 1rem 0;
    }
    .control-card {
        padding: 1rem 1.1rem;
        border-radius: 18px;
        background: var(--surface);
        border: 1px solid var(--line);
        box-shadow: var(--shadow);
        margin-bottom: 1rem;
        backdrop-filter: blur(14px);
    }
    .dashboard-sticky {
        position: sticky;
        top: 1rem;
    }
    .insight-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
        gap: 0.9rem;
        margin: 1rem 0 1.25rem 0;
    }
    .insight-card {
        background: rgba(255,255,255,0.76);
        border: 1px solid var(--line);
        border-radius: 18px;
        padding: 1rem 1rem 1.05rem 1rem;
        box-shadow: var(--shadow);
    }
    .insight-card strong {
        display: block;
        margin-bottom: 0.35rem;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: var(--muted);
    }
    .insight-card span {
        font-size: 1rem;
        line-height: 1.45;
        color: var(--ink);
    }
    .insight-pill {
        display: inline-flex;
        align-items: center;
        padding: 0.4rem 0.75rem;
        border-radius: 999px;
        font-size: 0.83rem;
        font-weight: 700;
        margin-right: 0.45rem;
        margin-bottom: 0.45rem;
    }
    .insight-pill.good {
        background: rgba(16, 185, 129, 0.14);
        color: #047857;
    }
    .insight-pill.warn {
        background: rgba(217, 119, 6, 0.14);
        color: #b45309;
    }
    .insight-pill.alert {
        background: rgba(220, 38, 38, 0.12);
        color: #b91c1c;
    }
    .status-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 0.38rem 0.7rem;
        border-radius: 999px;
        font-size: 0.78rem;
        font-weight: 800;
        letter-spacing: 0.03em;
    }
    .status-badge.good {
        background: rgba(16, 185, 129, 0.16);
        color: #047857;
    }
    .status-badge.warn {
        background: rgba(245, 158, 11, 0.15);
        color: #b45309;
    }
    .status-badge.alert {
        background: rgba(239, 68, 68, 0.15);
        color: #b91c1c;
    }
    .download-hero {
        padding: 1.2rem 1.3rem;
        border-radius: 20px;
        background: linear-gradient(135deg, rgba(31,41,55,0.96), rgba(17,24,39,0.9));
        color: white;
        margin-bottom: 1rem;
        box-shadow: var(--shadow);
    }
    .download-hero p {
        margin-top: 0.35rem;
        opacity: 0.88;
        line-height: 1.5;
    }
</style>
""", unsafe_allow_html=True)


# ─── Data Loading ──────────────────────────────────────────────────────────────
@st.cache_data
def load_data(file_source, file_bytes: bytes | None = None) -> pd.DataFrame:
    if file_bytes is None:
        df = pd.read_csv(file_source)
    else:
        df = pd.read_csv(io.BytesIO(file_bytes))

    required_columns = [
        "start_date",
        "assembly_line",
        "user_id",
        "box_id",
        "checks_error_category",
        "checks_error_quantity",
        "checks_error_sku",
        "checks_error_sku_name",
    ]
    missing_columns = [column for column in required_columns if column not in df.columns]
    if missing_columns:
        raise ValueError(
            "Fehlende Pflichtspalten in der CSV: " + ", ".join(missing_columns)
        )

    if "stations" not in df.columns:
        df["stations"] = pd.NA

    if "day_of_week" not in df.columns:
        df["day_of_week"] = pd.NA

    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df = df.dropna(subset=["start_date"]).copy()
    if df.empty:
        raise ValueError("Die CSV enthält keine gültigen Datumswerte in 'start_date'.")

    df["day_of_week"] = df["day_of_week"].fillna(df["start_date"].dt.day_name())
    df["week"] = df["start_date"].dt.isocalendar().week.astype(int)
    df["year"] = df["start_date"].dt.isocalendar().year.astype(int)
    df["year_week"] = df["year"].astype(str) + "-KW" + df["week"].astype(str).str.zfill(2)
    df["has_error"] = df["checks_error_category"].notna() & (df["checks_error_category"] != "null")
    df["checks_error_category"] = df["checks_error_category"].replace("null", pd.NA)
    df["checks_error_quantity"] = pd.to_numeric(df["checks_error_quantity"], errors="coerce")
    df["checks_error_sku"] = df["checks_error_sku"].replace("null", pd.NA)
    df["checks_error_sku_name"] = df["checks_error_sku_name"].replace("null", pd.NA)
    df["assembly_line"] = df["assembly_line"].astype(str)
    df["user_id"] = df["user_id"].astype(str)
    df["box_id"] = df["box_id"].astype(str)
    return df


# ─── Sidebar ───────────────────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/fluency/96/bug.png", width=64)
st.sidebar.title("Weekly Bug Report")
st.sidebar.markdown(
    """
    <div style="padding:0.9rem 1rem; border-radius:18px; background:linear-gradient(135deg, rgba(34,197,94,0.18), rgba(16,185,129,0.08)); border:1px solid rgba(255,255,255,0.1); margin-bottom:0.85rem;">
        <div style="font-size:0.72rem; letter-spacing:0.16em; text-transform:uppercase; opacity:0.8; font-weight:800;">Brand Layer</div>
        <div style="font-size:1.05rem; font-weight:800; margin-top:0.25rem;">Waagen Performance</div>
        <div style="font-size:0.82rem; opacity:0.85; margin-top:0.25rem;">Executive QA cockpit for operational reviews</div>
    </div>
    """,
    unsafe_allow_html=True,
)
st.sidebar.markdown("---")
# ─── Databricks Query Link ─────────────────────────────────────────────────
st.sidebar.markdown("### Datenquelle")
DATABRICKS_QUERY_URL = st.sidebar.text_input(
    "Databricks Query URL",
    value="",
    help="Trage hier optional deine Databricks SQL Query URL ein",
)
if DATABRICKS_QUERY_URL.strip():
    st.sidebar.markdown(
        f'<a href="{DATABRICKS_QUERY_URL}" target="_blank" style="'
        f'display:inline-block; width:100%; text-align:center; padding:0.6rem 1rem; '
        f'background:linear-gradient(135deg,#FF3621,#FF6A33); color:white; '
        f'border-radius:8px; text-decoration:none; font-weight:600; '
        f'box-shadow:0 2px 8px rgba(255,54,33,0.3);'
        f'">🔗 Databricks SQL Query öffnen</a>',
        unsafe_allow_html=True,
    )
else:
    st.sidebar.caption("Keine Databricks-URL hinterlegt.")
st.sidebar.markdown("---")
REPORT_SHARE_TARGET = st.sidebar.text_input(
    "Freigabe-URL für Report",
    value="",
    help="Optional: HTTP(S)-Ziel oder Ordner-URL für den freigegebenen HTML-Report. Wenn ein Slash am Ende steht, wird der Dateiname automatisch ergänzt.",
)
st.sidebar.markdown("---")
df = None
data_source_label = ""
data_source_signature = ""
uploaded_file = st.sidebar.file_uploader("CSV-Datei hochladen", type=["csv"])

if uploaded_file is None:
    st.markdown(
        """
        <div class="hero-panel">
            <div class="brand-mark">WP</div>
            <div class="hero-kicker">Assembly QC Intelligence</div>
            <h1 class="hero-title">Weekly Bug Report</h1>
            <p class="hero-subtitle">
                Das Tool startet jetzt bewusst leer. Lade zuerst eine CSV-Datei hoch, dann werden Analyse,
                Mailtext, HTML-Report und alle Downloads frisch aus genau dieser Datei erzeugt.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.info("Bitte links eine CSV-Datei hochladen. Erst danach werden Report, Mailtext und Downloads erzeugt.")
    st.stop()
else:
    uploaded_bytes = uploaded_file.getvalue()
    data_source_label = uploaded_file.name
    data_source_signature = hashlib.md5(uploaded_bytes).hexdigest()
    df = load_data(uploaded_file.name, uploaded_bytes)
    st.sidebar.success("Datei erfolgreich hochgeladen")

if df is None:
    st.error("Es konnten keine Daten geladen werden.")
    st.stop()

assert df is not None

# Filter
st.sidebar.markdown("### Filter")

available_weeks = sorted(df["year_week"].dropna().astype(str).unique())
selected_weeks = st.sidebar.multiselect("Kalenderwochen", available_weeks, default=available_weeks)

available_lines = sorted(df["assembly_line"].dropna().astype(str).unique())
selected_lines = st.sidebar.multiselect("Assembly Lines", available_lines, default=available_lines)

available_users = sorted(df["user_id"].dropna().astype(str).unique())
selected_users = st.sidebar.multiselect("Mitarbeiter (User-ID)", available_users, default=available_users)

error_types = ["missing", "wrong_item", "extra"]
selected_errors = st.sidebar.multiselect("Fehlertypen", error_types, default=error_types)

st.sidebar.markdown("### Analyse-Modus")
focus_dimension = st.sidebar.radio(
    "Fokusbereich",
    ["Assembly Lines", "Mitarbeiter", "Stationen", "SKUs"],
    index=0,
)
top_n = st.sidebar.slider("Top-N in Rankings", min_value=5, max_value=25, value=10, step=5)
show_only_hotspots = st.sidebar.toggle("Nur Hotspots zeigen", value=False)
st.sidebar.markdown("### Ampellogik")
warning_threshold = st.sidebar.slider("Beobachten ab Fehlerquote (%)", min_value=2.0, max_value=15.0, value=6.0, step=0.5)
critical_threshold = st.sidebar.slider("Kritisch ab Fehlerquote (%)", min_value=warning_threshold + 0.5, max_value=25.0, value=max(12.0, warning_threshold + 0.5), step=0.5)

# Daten filtern
scope_mask = (
    df["assembly_line"].isin(selected_lines)
    & df["user_id"].isin(selected_users)
)
df_scope = df[scope_mask].copy()

mask = (
    df_scope["year_week"].isin(selected_weeks)
)
df_filtered = df_scope[mask].copy()

# ─── Header ────────────────────────────────────────────────────────────────────
_date_min = df_filtered["start_date"].min()
_date_max = df_filtered["start_date"].max()

st.markdown(
    f"""
    <div class="hero-panel">
        <div class="brand-mark">WP</div>
        <div class="hero-kicker">Assembly QC Intelligence</div>
        <h1 class="hero-title">Weekly Bug Report</h1>
        <p class="hero-subtitle">
            Interaktives Qualitätscockpit für Linien, Mitarbeiter, Stationen und SKU-Hotspots.
            Der Fokus liegt auf schnellen Management-Signalen, belastbaren Drilldowns und einem professionellen Export.
        </p>
        <div class="info-strip">
            {build_highlight(f"Fokus: {focus_dimension}", "good")}
            {build_highlight(f"Zeitraum: {_date_min.strftime('%d.%m.%Y') if pd.notna(_date_min) else '–'} bis {_date_max.strftime('%d.%m.%Y') if pd.notna(_date_max) else '–'}", "warn")}
            {build_highlight(f"Letzte Aktualisierung: {datetime.now().strftime('%d.%m.%Y %H:%M')}", "good")}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

if pd.notna(_date_min) and pd.notna(_date_max):
    st.markdown(f"**Datenbereich:** {_date_min.strftime('%d.%m.%Y')} – {_date_max.strftime('%d.%m.%Y')} | **Letzte Aktualisierung:** {datetime.now().strftime('%d.%m.%Y %H:%M')}")
else:
    st.warning("Keine Daten für die gewählten Filter vorhanden.")
st.markdown("---")

# ─── KPI Berechnung ───────────────────────────────────────────────────────────
total_checks = len(df_filtered)
df_errors = df_filtered[df_filtered["has_error"]].copy()
df_errors_selected = df_errors[df_errors["checks_error_category"].isin(selected_errors)]
total_errors = len(df_errors_selected)
error_rate = (total_errors / total_checks * 100) if total_checks > 0 else 0
total_error_qty = df_errors_selected["checks_error_quantity"].sum()
unique_boxes_with_errors = df_errors_selected["box_id"].nunique()
total_boxes = df_filtered["box_id"].nunique()
box_error_rate = (unique_boxes_with_errors / total_boxes * 100) if total_boxes > 0 else 0
insight_items = create_insight_summary(df_filtered, df_errors_selected)
streamlit_click_payloads = build_chart_click_payloads(df_errors_selected)
week_over_week = build_week_over_week_summary(df_scope, selected_errors)

if "streamlit_selection_payload" not in st.session_state:
    st.session_state["streamlit_selection_payload"] = default_selection_payload()

line_total = df_filtered.groupby("assembly_line").size().reset_index(name="total")
line_err = df_errors_selected.groupby("assembly_line").size().reset_index(name="errors")
line_rate = line_total.merge(line_err, on="assembly_line", how="left").fillna(0)
line_rate["Fehlerquote (%)"] = (line_rate["errors"] / line_rate["total"] * 100).round(1)
line_rate.columns = ["Assembly Line", "Gesamtprüfungen", "Fehler", "Fehlerquote (%)"]
line_rate["Priorität"] = line_rate["Fehlerquote (%)"].apply(
    lambda value: classify_status(value, warning_threshold, critical_threshold)
)

user_total = df_filtered.groupby("user_id").agg(
    Gesamtprüfungen=("box_id", "count"),
    Fehler=("has_error", "sum")
).reset_index()
user_total["Fehlerquote (%)"] = (user_total["Fehler"] / user_total["Gesamtprüfungen"] * 100).round(1)
user_total["user_id"] = "User " + user_total["user_id"].astype(str)
user_total = user_total.rename(columns={"user_id": "Mitarbeiter"})
user_total["Priorität"] = user_total["Fehlerquote (%)"].apply(
    lambda value: classify_status(value, warning_threshold, critical_threshold)
)

sku_errors = pd.DataFrame(columns=["SKU", "Artikelname", "Anzahl Fehler", "Fehlermenge", "Label"])
if not df_errors_selected.empty:
    sku_errors = build_sku_ranking(df_errors_selected, limit=15)

df_stations = df_errors_selected.dropna(subset=["stations"]).copy()
df_stations["stations"] = pd.to_numeric(df_stations["stations"], errors="coerce")
df_stations = df_stations.dropna(subset=["stations"])

hotspot_table = top_issue_table(df_errors_selected)
if show_only_hotspots and not hotspot_table.empty:
    hotspot_table = hotspot_table.head(min(top_n, len(hotspot_table)))

color_map = {"missing": "#f5576c", "wrong_item": "#ffa726", "extra": "#66bb6a"}
error_dist = pd.DataFrame(columns=["Fehlertyp", "Anzahl"])
weekly_trend = pd.DataFrame(columns=["Kalenderwoche", "Fehlertyp", "Anzahl"])
if not df_errors_selected.empty:
    error_dist = df_errors_selected["checks_error_category"].value_counts().reset_index()
    error_dist.columns = ["Fehlertyp", "Anzahl"]
    weekly_trend = (
        df_errors_selected.groupby(["year_week", "checks_error_category"])
        .size()
        .reset_index(name="Anzahl")
    )
    weekly_trend.columns = ["Kalenderwoche", "Fehlertyp", "Anzahl"]

critical_lines = int((line_rate["Priorität"] == "Kritisch").sum()) if not line_rate.empty else 0
watch_lines = int((line_rate["Priorität"] == "Beobachten").sum()) if not line_rate.empty else 0
stable_lines = int((line_rate["Priorität"] == "Stabil").sum()) if not line_rate.empty else 0

# ─── KPI Cards ─────────────────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Kennzahlen Übersicht</h3></div>', unsafe_allow_html=True)

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <h2>{total_checks:,}</h2>
        <p>Gesamte Prüfungen</p>
    </div>""", unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card red">
        <h2>{total_errors:,}</h2>
        <p>Fehlerhafte Prüfungen</p>
    </div>""", unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card orange">
        <h2>{error_rate:.1f}%</h2>
        <p>Fehlerquote (Checks)</p>
    </div>""", unsafe_allow_html=True)

with col4:
    st.markdown(f"""
    <div class="metric-card green">
        <h2>{unique_boxes_with_errors}</h2>
        <p>Boxen mit Fehlern</p>
    </div>""", unsafe_allow_html=True)

with col5:
    st.markdown(f"""
    <div class="metric-card">
        <h2>{box_error_rate:.1f}%</h2>
        <p>Box-Fehlerquote</p>
    </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

st.markdown('<div class="section-header"><h3>Management Insights</h3></div>', unsafe_allow_html=True)
st.markdown(
    "<div class='insight-grid'>"
    + "".join(
        f"<div class='insight-card'><strong>Insight {index + 1}</strong><span>{message}</span></div>"
        for index, message in enumerate(insight_items)
    )
    + "</div>",
    unsafe_allow_html=True,
)

col_ctrl1, col_ctrl2 = st.columns([1.3, 1])
with col_ctrl1:
    st.markdown('<div class="control-card">', unsafe_allow_html=True)
    st.subheader("Hotspot-Ranking")
    st.dataframe(hotspot_table, width="stretch", hide_index=True, height=320)
    st.markdown('</div>', unsafe_allow_html=True)
with col_ctrl2:
    st.markdown('<div class="control-card">', unsafe_allow_html=True)
    st.subheader("Filterabdeckung")
    st.metric("Aktive Wochen", len(selected_weeks), delta=f"von {len(available_weeks)}")
    st.metric("Aktive Linien", len(selected_lines), delta=f"von {len(available_lines)}")
    st.metric("Aktive Mitarbeiter", len(selected_users), delta=f"von {len(available_users)}")
    st.markdown(
        """
        <div style="margin-top:0.9rem;">
        """
        + f'<span class="status-badge alert">Kritisch: {critical_lines}</span>'
        + f'<span class="status-badge warn">Beobachten: {watch_lines}</span>'
        + f'<span class="status-badge good">Stabil: {stable_lines}</span>'
        + "</div>",
        unsafe_allow_html=True,
    )
    st.markdown('</div>', unsafe_allow_html=True)

# ─── Fehlertypen Verteilung + Trend ───────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Fehleranalyse</h3></div>', unsafe_allow_html=True)

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Fehlertypen-Verteilung")
    if not df_errors_selected.empty:
        fig_pie = px.pie(
            error_dist, values="Anzahl", names="Fehlertyp",
            color="Fehlertyp", color_discrete_map=color_map,
            hole=0.45
        )
        fig_pie.update_traces(textposition="inside", textinfo="percent+value")
        fig_pie.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig_pie, width="stretch")
    else:
        st.info("Keine Fehler im ausgewählten Zeitraum.")

with col_right:
    st.subheader("Wöchentlicher Fehler-Trend")
    if not df_errors_selected.empty:
        fig_trend = px.bar(
            weekly_trend, x="Kalenderwoche", y="Anzahl", color="Fehlertyp",
            color_discrete_map=color_map,
            barmode="stack"
        )
        fig_trend.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig_trend, width="stretch")
    else:
        st.info("Keine Daten für Trend verfügbar.")

st.markdown('<div class="section-header"><h3>Live Chart Drilldown</h3></div>', unsafe_allow_html=True)
drilldown_left, drilldown_right = st.columns([1.4, 1])

with drilldown_left:
    chart_choice = st.selectbox(
        "Interaktiver Chart",
        [
            "Fehlertypen",
            "Wochen-Trend",
            "Assembly Lines",
            "Top SKUs",
            "Mitarbeiter",
            "Stationen",
        ],
        index=0,
    )

    interactive_fig = None
    interactive_mode = None

    if chart_choice == "Fehlertypen" and not df_errors_selected.empty:
        interactive_mode = "error_type"
        interactive_fig = px.pie(
            error_dist, values="Anzahl", names="Fehlertyp",
            color="Fehlertyp", color_discrete_map=color_map,
            hole=0.45,
        )
        interactive_fig.update_traces(textposition="inside", textinfo="percent+value")
        interactive_fig.update_layout(height=430, margin=dict(t=20, b=20))
    elif chart_choice == "Wochen-Trend" and not df_errors_selected.empty:
        interactive_mode = "week_type"
        interactive_fig = px.bar(
            weekly_trend, x="Kalenderwoche", y="Anzahl", color="Fehlertyp",
            color_discrete_map={"missing": "#f5576c", "wrong_item": "#ffa726", "extra": "#66bb6a"},
            barmode="stack",
        )
        interactive_fig.update_layout(height=430, margin=dict(t=20, b=20))
    elif chart_choice == "Assembly Lines" and not df_errors_selected.empty:
        interactive_mode = "line_type"
        line_errors = (
            df_errors_selected.groupby(["assembly_line", "checks_error_category"])
            .size()
            .reset_index(name="Anzahl")
        )
        interactive_fig = px.bar(
            line_errors, x="assembly_line", y="Anzahl", color="checks_error_category",
            color_discrete_map={"missing": "#f5576c", "wrong_item": "#ffa726", "extra": "#66bb6a"},
            barmode="group",
            labels={"assembly_line": "Assembly Line", "checks_error_category": "Fehlertyp"},
        )
        interactive_fig.update_layout(height=430, margin=dict(t=20, b=20))
    elif chart_choice == "Top SKUs" and not sku_errors.empty:
        interactive_mode = "sku"
        interactive_fig = px.bar(
            sku_errors,
            x="Anzahl Fehler",
            y="Label",
            orientation="h",
            color="Anzahl Fehler",
            color_continuous_scale=["#fde68a", "#f59e0b", "#dc2626"],
            hover_data={"SKU": True, "Artikelname": True, "Fehlermenge": True, "Label": False},
            text="Anzahl Fehler",
        )
        interactive_fig.update_traces(textposition="outside")
        interactive_fig.update_layout(
            height=560,
            margin=dict(t=20, b=20, l=20, r=40),
            yaxis=dict(autorange="reversed", title=None),
            xaxis_title="Fehleranzahl",
            coloraxis_showscale=False,
        )
    elif chart_choice == "Mitarbeiter" and not df_errors_selected.empty:
        interactive_mode = "user_type"
        user_errors = (
            df_errors_selected.groupby(["user_id", "checks_error_category"])
            .size()
            .reset_index(name="Anzahl")
        )
        user_errors["user_id"] = "User " + user_errors["user_id"].astype(str)
        interactive_fig = px.bar(
            user_errors, x="user_id", y="Anzahl", color="checks_error_category",
            color_discrete_map={"missing": "#f5576c", "wrong_item": "#ffa726", "extra": "#66bb6a"},
            barmode="stack",
            labels={"user_id": "Mitarbeiter", "checks_error_category": "Fehlertyp"},
        )
        interactive_fig.update_layout(height=430, margin=dict(t=20, b=20))
    elif chart_choice == "Stationen" and not df_stations.empty:
        interactive_mode = "station_line"
        heatmap_data = (
            df_stations.groupby(["assembly_line", "stations"])
            .size()
            .reset_index(name="Fehler")
        )
        heatmap_data["stations"] = heatmap_data["stations"].astype(int)
        heatmap_pivot = heatmap_data.pivot_table(
            index="assembly_line", columns="stations", values="Fehler", fill_value=0
        )
        interactive_fig = px.imshow(
            heatmap_pivot,
            labels=dict(x="Station", y="Assembly Line", color="Fehler"),
            color_continuous_scale="RdYlGn_r",
            aspect="auto",
        )
        interactive_fig.update_layout(height=430, margin=dict(t=20, b=20))

    if interactive_fig is not None and interactive_mode is not None:
        clicked_points = plotly_events(
            interactive_fig,
            click_event=True,
            hover_event=False,
            select_event=False,
            override_height=interactive_fig.layout.height if interactive_fig.layout.height else 430,
            key=f"interactive-{chart_choice}",
        )
        if clicked_points:
            event_key = resolve_plotly_event_key(interactive_mode, clicked_points[0], interactive_fig)
            st.session_state["streamlit_selection_payload"] = streamlit_click_payloads.get(interactive_mode, {}).get(
                event_key,
                default_selection_payload(),
            )
    else:
        st.info("Für die aktuelle Auswahl stehen keine interaktiven Chartdaten zur Verfügung.")

with drilldown_right:
    render_streamlit_selection_panel(st.session_state["streamlit_selection_payload"])

st.markdown('<div class="section-header"><h3>Interaktive Drilldowns</h3></div>', unsafe_allow_html=True)
drill_tab1, drill_tab2, drill_tab3 = st.tabs(["Performance Map", "Treibervergleich", "Executive Table"])

with drill_tab1:
    if not df_errors_selected.empty:
        scatter_source = line_rate.copy()
        scatter_source["Markergröße"] = scatter_source["Fehler"].clip(lower=1) * 6
        fig_scatter = px.scatter(
            scatter_source,
            x="Gesamtprüfungen",
            y="Fehlerquote (%)",
            size="Markergröße",
            color="Fehlerquote (%)",
            hover_name="Assembly Line",
            text="Assembly Line",
            color_continuous_scale=[[0, "#c7f9cc"], [0.5, "#fcbf49"], [1, "#d62828"]],
        )
        fig_scatter.update_traces(textposition="top center")
        fig_scatter.update_layout(height=450, margin=dict(t=20, b=20))
        st.plotly_chart(fig_scatter, width="stretch")
    else:
        st.info("Keine Fehlerdaten für Performance Map vorhanden.")

with drill_tab2:
    if focus_dimension == "Mitarbeiter":
        compare_df = user_total.sort_values("Fehlerquote (%)", ascending=False).head(top_n)
        fig_compare = px.bar(
            compare_df,
            x="Mitarbeiter",
            y="Fehlerquote (%)",
            color="Fehlerquote (%)",
            text="Fehlerquote (%)",
            color_continuous_scale=[[0, "#99f6e4"], [0.5, "#fbbf24"], [1, "#ef4444"]],
        )
        fig_compare.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_compare.update_layout(height=420, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig_compare, width="stretch")
    elif focus_dimension == "Stationen":
        station_view = df_errors_selected.dropna(subset=["stations"]).copy()
        station_view["stations"] = pd.to_numeric(station_view["stations"], errors="coerce")
        station_view = station_view.dropna(subset=["stations"])
        if not station_view.empty:
            station_rank = station_view.groupby("stations").size().reset_index(name="Vorfälle").sort_values("Vorfälle", ascending=False).head(top_n)
            station_rank["stations"] = station_rank["stations"].astype(int).astype(str)
            fig_compare = px.funnel(
                station_rank,
                y="stations",
                x="Vorfälle",
                color_discrete_sequence=["#0f766e"],
            )
            fig_compare.update_layout(height=420, margin=dict(t=20, b=20))
            st.plotly_chart(fig_compare, width="stretch")
        else:
            st.info("Keine Stationsdaten im aktuellen Fehlerausschnitt.")
    elif focus_dimension == "SKUs":
        if not sku_errors.empty:
            fig_compare = px.treemap(
                sku_errors.head(top_n),
                path=[px.Constant("SKUs"), "SKU"],
                values="Anzahl Fehler",
                color="Anzahl Fehler",
                color_continuous_scale=[[0, "#fde68a"], [0.5, "#fb923c"], [1, "#dc2626"]],
            )
            fig_compare.update_layout(height=430, margin=dict(t=20, b=20))
            st.plotly_chart(fig_compare, width="stretch")
        else:
            st.info("Keine SKU-Hotspots verfügbar.")
    else:
        compare_df = line_rate.sort_values("Fehlerquote (%)", ascending=False).head(top_n)
        fig_compare = px.bar(
            compare_df,
            x="Assembly Line",
            y="Fehlerquote (%)",
            color="Fehlerquote (%)",
            text="Fehlerquote (%)",
            color_continuous_scale=[[0, "#99f6e4"], [0.5, "#fbbf24"], [1, "#ef4444"]],
        )
        fig_compare.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_compare.update_layout(height=420, margin=dict(t=20, b=20), showlegend=False)
        st.plotly_chart(fig_compare, width="stretch")

with drill_tab3:
    exec_table = prepare_line_exec_table(line_rate, warning_threshold, critical_threshold)
    st.dataframe(exec_table.head(max(top_n, 8)), width="stretch", hide_index=True, height=360)

# ─── Assembly Line Analyse ─────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Assembly Line Analyse</h3></div>', unsafe_allow_html=True)

col_al1, col_al2 = st.columns(2)

with col_al1:
    st.subheader("Fehler pro Assembly Line")
    if not df_errors_selected.empty:
        line_errors = (
            df_errors_selected.groupby(["assembly_line", "checks_error_category"])
            .size()
            .reset_index(name="Anzahl")
        )
        fig_line = px.bar(
            line_errors, x="assembly_line", y="Anzahl", color="checks_error_category",
            color_discrete_map={"missing": "#f5576c", "wrong_item": "#ffa726", "extra": "#66bb6a"},
            barmode="group",
            labels={"assembly_line": "Assembly Line", "checks_error_category": "Fehlertyp"}
        )
        fig_line.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig_line, width="stretch")

with col_al2:
    st.subheader("Fehlerquote pro Assembly Line")
    fig_rate = px.bar(
        line_rate, x="Assembly Line", y="Fehlerquote (%)",
        text="Fehlerquote (%)",
        color="Fehlerquote (%)",
        color_continuous_scale=["#4facfe", "#f5576c"]
    )
    fig_rate.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
    fig_rate.update_layout(height=400, margin=dict(t=20, b=20), showlegend=False)
    st.plotly_chart(fig_rate, width="stretch")

# ─── Mitarbeiter-Analyse ──────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Mitarbeiter-Analyse (QC Checker)</h3></div>', unsafe_allow_html=True)

col_u1, col_u2 = st.columns(2)

with col_u1:
    st.subheader("Erkannte Fehler pro Mitarbeiter")
    if not df_errors_selected.empty:
        user_errors = (
            df_errors_selected.groupby(["user_id", "checks_error_category"])
            .size()
            .reset_index(name="Anzahl")
        )
        user_errors["user_id"] = "User " + user_errors["user_id"].astype(str)
        fig_user = px.bar(
            user_errors, x="user_id", y="Anzahl", color="checks_error_category",
            color_discrete_map={"missing": "#f5576c", "wrong_item": "#ffa726", "extra": "#66bb6a"},
            barmode="stack",
            labels={"user_id": "Mitarbeiter", "checks_error_category": "Fehlertyp"}
        )
        fig_user.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig_user, width="stretch")

with col_u2:
    st.subheader("Prüfleistung pro Mitarbeiter")
    st.dataframe(user_total, width="stretch", hide_index=True)

# ─── Top fehlerhafte SKUs ─────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Top fehlerhafte SKUs / Artikel</h3></div>', unsafe_allow_html=True)

col_s1, col_s2 = st.columns(2)

with col_s1:
    st.subheader("Top 15 SKUs mit Fehlern")
    if not sku_errors.empty:
        fig_sku = px.bar(
            sku_errors, x="Anzahl Fehler", y="Label", orientation="h",
            color="Anzahl Fehler",
            color_continuous_scale=["#fee140", "#f5576c"],
            hover_data={"SKU": True, "Artikelname": True, "Fehlermenge": True, "Label": False},
            text="Anzahl Fehler",
        )
        fig_sku.update_traces(textposition="outside")
        fig_sku.update_layout(
            height=560,
            margin=dict(t=20, b=20, l=20, r=40),
            yaxis=dict(autorange="reversed", title=None),
            xaxis_title="Fehleranzahl",
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_sku, width="stretch")

with col_s2:
    st.subheader("SKU-Details")
    if not df_errors_selected.empty:
        sku_detail = (
            df_errors_selected.dropna(subset=["checks_error_sku_name"])
            .groupby(["checks_error_sku", "checks_error_sku_name", "checks_error_category"])
            .agg(Anzahl=("box_id", "count"), Menge=("checks_error_quantity", "sum"))
            .sort_values("Anzahl", ascending=False)
            .reset_index()
        )
        sku_detail.columns = ["SKU", "Artikelname", "Fehlertyp", "Vorfälle", "Menge"]
        st.dataframe(sku_detail, width="stretch", hide_index=True, height=500)

# ─── Stations-Heatmap ─────────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Stations-Analyse</h3></div>', unsafe_allow_html=True)

if not df_stations.empty:
    col_st1, col_st2 = st.columns(2)
    with col_st1:
        st.subheader("Fehler nach Station")
        station_errors = (
            df_stations.groupby("stations")
            .size()
            .reset_index(name="Anzahl")
            .sort_values("stations")
        )
        station_errors["stations"] = station_errors["stations"].astype(int)
        fig_station = px.bar(
            station_errors, x="stations", y="Anzahl",
            labels={"stations": "Station", "Anzahl": "Fehleranzahl"},
            color="Anzahl",
            color_continuous_scale=["#4facfe", "#f5576c"],
        )
        fig_station.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig_station, width="stretch")

    with col_st2:
        st.subheader("Station × Assembly Line Heatmap")
        heatmap_data = (
            df_stations.groupby(["assembly_line", "stations"])
            .size()
            .reset_index(name="Fehler")
        )
        heatmap_data["stations"] = heatmap_data["stations"].astype(int)
        heatmap_pivot = heatmap_data.pivot_table(
            index="assembly_line", columns="stations", values="Fehler", fill_value=0
        )
        fig_heatmap = px.imshow(
            heatmap_pivot,
            labels=dict(x="Station", y="Assembly Line", color="Fehler"),
            color_continuous_scale="RdYlGn_r",
            aspect="auto"
        )
        fig_heatmap.update_layout(height=400, margin=dict(t=20, b=20))
        st.plotly_chart(fig_heatmap, width="stretch")

# ─── Tages-Analyse ─────────────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Tages- und Wochentagsanalyse</h3></div>', unsafe_allow_html=True)

col_d1, col_d2 = st.columns(2)

with col_d1:
    st.subheader("Fehler pro Tag")
    if not df_errors_selected.empty:
        daily = (
            df_errors_selected.groupby("start_date")
            .size()
            .reset_index(name="Fehler")
        )
        fig_daily = px.line(
            daily, x="start_date", y="Fehler",
            markers=True,
            labels={"start_date": "Datum", "Fehler": "Fehleranzahl"},
        )
        fig_daily.update_traces(line_color="#f5576c", marker_size=8)
        fig_daily.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig_daily, width="stretch")

with col_d2:
    st.subheader("Fehler nach Wochentag")
    if not df_errors_selected.empty:
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow = (
            df_errors_selected.groupby("day_of_week")
            .size()
            .reindex(dow_order)
            .fillna(0)
            .reset_index(name="Fehler")
        )
        dow.columns = ["Wochentag", "Fehler"]
        fig_dow = px.bar(
            dow, x="Wochentag", y="Fehler",
            color="Fehler",
            color_continuous_scale=["#4facfe", "#f5576c"],
        )
        fig_dow.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig_dow, width="stretch")

# ─── Fehlermengen-Analyse ─────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Fehlermengen-Verteilung</h3></div>', unsafe_allow_html=True)

col_q1, col_q2 = st.columns(2)

with col_q1:
    st.subheader("Verteilung der Fehlermenge pro Vorfall")
    qty_data = df_errors_selected.dropna(subset=["checks_error_quantity"])
    if not qty_data.empty:
        fig_hist = px.histogram(
            qty_data, x="checks_error_quantity",
            nbins=20,
            labels={"checks_error_quantity": "Fehlermenge"},
            color_discrete_sequence=["#667eea"]
        )
        fig_hist.update_layout(height=350, margin=dict(t=20, b=20))
        st.plotly_chart(fig_hist, width="stretch")

with col_q2:
    st.subheader("Durchschnittliche Fehlermenge nach Typ")
    if not df_errors_selected.empty:
        avg_qty = (
            df_errors_selected.groupby("checks_error_category")["checks_error_quantity"]
            .agg(["mean", "sum", "count"])
            .reset_index()
        )
        avg_qty.columns = ["Fehlertyp", "Ø Menge", "Gesamtmenge", "Vorfälle"]
        avg_qty["Ø Menge"] = avg_qty["Ø Menge"].round(2)
        st.dataframe(avg_qty, width="stretch", hide_index=True)

# ─── Detaillierte Fehlertabelle ───────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>Detaillierte Fehlertabelle</h3></div>', unsafe_allow_html=True)

if not df_errors_selected.empty:
    detail_cols = [
        "start_date", "day_of_week", "assembly_line", "user_id", "box_id",
        "checks_error_category", "checks_error_quantity", "stations",
        "checks_error_sku", "checks_error_sku_name"
    ]
    df_detail = df_errors_selected[detail_cols].copy()
    df_detail.columns = [
        "Datum", "Wochentag", "Assembly Line", "Mitarbeiter", "Box-ID",
        "Fehlertyp", "Menge", "Station", "SKU", "Artikelname"
    ]
    df_detail = df_detail.sort_values("Datum", ascending=False)

    # Suchfilter
    search = st.text_input("🔎 Suche in Fehlertabelle (Box-ID, SKU, Artikel...)")
    if search:
        mask_search = df_detail.astype(str).apply(lambda x: x.str.contains(search, case=False, na=False)).any(axis=1)
        df_detail = df_detail[mask_search]

    st.dataframe(df_detail, width="stretch", hide_index=True, height=400)

    st.markdown(f"**{len(df_detail)} Fehlereinträge** angezeigt")
else:
    st.info("Keine Fehler im ausgewählten Zeitraum/Filter.")

# ─── Download Center ─────────────────────────────────────────────────────────
st.markdown('<div class="section-header"><h3>📦 Download Center</h3></div>', unsafe_allow_html=True)

st.markdown(
    """
    <div class="download-hero">
        <h3 style="margin:0; color:white;">Professional Export Suite</h3>
        <p>Der HTML-Report ist jetzt als präsentationsfähiges Executive Deck aufgebaut: klare Story, KPI-Panel, Hotspot-Tabelle, Deep-Dive-Charts und eine sauber formatierte Detailansicht für Management, Operations und QA.</p>
    </div>
    """,
    unsafe_allow_html=True,
)

# --- Helper: Excel mit mehreren Sheets ---
def build_excel_report():
    """Erstellt eine Excel-Datei mit allen Auswertungs-Sheets."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book

        # Formate
        header_fmt = workbook.add_format({
            "bold": True, "bg_color": "#667eea", "font_color": "white",
            "border": 1, "text_wrap": True, "valign": "vcenter",
        })

        # Sheet 1: Zusammenfassung
        summary_data = {
            "Kennzahl": [
                "Gesamte Prüfungen", "Fehlerhafte Prüfungen", "Fehlerquote (%)",
                "Gesamte Fehlermenge", "Boxen mit Fehlern", "Gesamte Boxen",
                "Box-Fehlerquote (%)", "Zeitraum von", "Zeitraum bis",
                "Report erstellt am",
            ],
            "Wert": [
                total_checks, total_errors, f"{error_rate:.1f}%",
                int(total_error_qty) if pd.notna(total_error_qty) else 0,
                unique_boxes_with_errors, total_boxes,
                f"{box_error_rate:.1f}%",
                _date_min.strftime("%d.%m.%Y") if pd.notna(_date_min) else "–",
                _date_max.strftime("%d.%m.%Y") if pd.notna(_date_max) else "–",
                datetime.now().strftime("%d.%m.%Y %H:%M"),
            ],
        }
        df_summary = pd.DataFrame(summary_data)
        df_summary.to_excel(writer, sheet_name="Zusammenfassung", index=False, startrow=1)
        ws = writer.sheets["Zusammenfassung"]
        ws.write(0, 0, "Kennzahl", header_fmt)
        ws.write(0, 1, "Wert", header_fmt)
        ws.set_column(0, 0, 30)
        ws.set_column(1, 1, 25)

        # Sheet 2: Alle Fehler (Rohdaten)
        if not df_errors_selected.empty:
            df_raw = df_errors_selected.copy()
            df_raw["start_date"] = df_raw["start_date"].dt.strftime("%d.%m.%Y")
            df_raw.to_excel(writer, sheet_name="Alle Fehler", index=False, startrow=1)
            ws2 = writer.sheets["Alle Fehler"]
            for col_idx, col_name in enumerate(df_raw.columns):
                ws2.write(0, col_idx, col_name, header_fmt)
                ws2.set_column(col_idx, col_idx, max(14, len(str(col_name)) + 4))
            ws2.autofilter(0, 0, len(df_raw), len(df_raw.columns) - 1)

        # Sheet 3: Fehler nach Assembly Line
        line_total_xl = df_filtered.groupby("assembly_line").size().reset_index(name="Gesamtprüfungen")
        line_err_xl = df_errors_selected.groupby("assembly_line").size().reset_index(name="Fehler")
        line_rate_xl = line_total_xl.merge(line_err_xl, on="assembly_line", how="left").fillna(0)
        line_rate_xl["Fehler"] = line_rate_xl["Fehler"].astype(int)
        line_rate_xl["Fehlerquote (%)"] = (line_rate_xl["Fehler"] / line_rate_xl["Gesamtprüfungen"] * 100).round(1)
        for etype in ["missing", "wrong_item", "extra"]:
            cnt = df_errors_selected[df_errors_selected["checks_error_category"] == etype].groupby("assembly_line").size().reset_index(name=etype)
            line_rate_xl = line_rate_xl.merge(cnt, on="assembly_line", how="left").fillna(0)
            line_rate_xl[etype] = line_rate_xl[etype].astype(int)
        line_rate_xl.columns = ["Assembly Line", "Gesamtprüfungen", "Fehler gesamt", "Fehlerquote (%)", "Missing", "Wrong Item", "Extra"]
        line_rate_xl.to_excel(writer, sheet_name="Assembly Lines", index=False, startrow=1)
        ws3 = writer.sheets["Assembly Lines"]
        for col_idx, col_name in enumerate(line_rate_xl.columns):
            ws3.write(0, col_idx, col_name, header_fmt)
            ws3.set_column(col_idx, col_idx, 20)

        # Sheet 4: Mitarbeiter-Analyse
        user_xl = df_filtered.groupby("user_id").agg(
            Gesamtprüfungen=("box_id", "count"),
            Fehler=("has_error", "sum")
        ).reset_index()
        user_xl["Fehlerquote (%)"] = (user_xl["Fehler"] / user_xl["Gesamtprüfungen"] * 100).round(1)
        for etype in ["missing", "wrong_item", "extra"]:
            cnt = df_errors_selected[df_errors_selected["checks_error_category"] == etype].groupby("user_id").size().reset_index(name=etype)
            user_xl = user_xl.merge(cnt, on="user_id", how="left").fillna(0)
            user_xl[etype] = user_xl[etype].astype(int)
        user_xl.columns = ["Mitarbeiter", "Gesamtprüfungen", "Fehler gesamt", "Fehlerquote (%)", "Missing", "Wrong Item", "Extra"]
        user_xl.to_excel(writer, sheet_name="Mitarbeiter", index=False, startrow=1)
        ws4 = writer.sheets["Mitarbeiter"]
        for col_idx, col_name in enumerate(user_xl.columns):
            ws4.write(0, col_idx, col_name, header_fmt)
            ws4.set_column(col_idx, col_idx, 20)

        # Sheet 5: Top SKUs
        if not df_errors_selected.empty:
            sku_xl = (
                df_errors_selected.dropna(subset=["checks_error_sku"])
                .groupby(["checks_error_sku", "checks_error_sku_name"])
                .agg(
                    Anzahl=("box_id", "count"),
                    Menge=("checks_error_quantity", "sum"),
                    Fehlertypen=("checks_error_category", lambda x: ", ".join(x.dropna().unique())),
                    Linien=("assembly_line", lambda x: ", ".join(sorted(x.unique()))),
                )
                .sort_values("Anzahl", ascending=False)
                .reset_index()
            )
            sku_xl.columns = ["SKU", "Artikelname", "Vorfälle", "Fehlermenge", "Fehlertypen", "Betroffene Linien"]
            sku_xl.to_excel(writer, sheet_name="Top SKUs", index=False, startrow=1)
            ws5 = writer.sheets["Top SKUs"]
            for col_idx, col_name in enumerate(sku_xl.columns):
                ws5.write(0, col_idx, col_name, header_fmt)
                ws5.set_column(col_idx, col_idx, max(18, len(str(col_name)) + 4))
            ws5.autofilter(0, 0, len(sku_xl), len(sku_xl.columns) - 1)

        # Sheet 6: Wöchentlicher Trend
        if not df_errors_selected.empty:
            weekly_xl = (
                df_errors_selected.groupby(["year_week", "checks_error_category"])
                .size()
                .reset_index(name="Anzahl")
                .pivot_table(index="year_week", columns="checks_error_category", values="Anzahl", fill_value=0)
                .reset_index()
            )
            weekly_xl.to_excel(writer, sheet_name="Wöchentlicher Trend", index=False, startrow=1)
            ws6 = writer.sheets["Wöchentlicher Trend"]
            for col_idx, col_name in enumerate(weekly_xl.columns):
                ws6.write(0, col_idx, str(col_name), header_fmt)
                ws6.set_column(col_idx, col_idx, 18)

        # Sheet 7: Tagesdetail
        if not df_errors_selected.empty:
            daily_xl = (
                df_errors_selected.groupby(["start_date", "day_of_week", "checks_error_category"])
                .size()
                .reset_index(name="Anzahl")
            )
            daily_xl["start_date"] = daily_xl["start_date"].dt.strftime("%d.%m.%Y")
            daily_xl.columns = ["Datum", "Wochentag", "Fehlertyp", "Anzahl"]
            daily_xl.to_excel(writer, sheet_name="Tagesdetail", index=False, startrow=1)
            ws7 = writer.sheets["Tagesdetail"]
            for col_idx, col_name in enumerate(daily_xl.columns):
                ws7.write(0, col_idx, col_name, header_fmt)
                ws7.set_column(col_idx, col_idx, 18)

    return output.getvalue()


# --- Helper: Interaktiver HTML Report ---
def build_html_report():
    """Erstellt einen vollständigen interaktiven HTML-Report mit eingebetteten Plotly-Charts."""
    report_created = datetime.now().strftime("%d.%m.%Y %H:%M")
    insight_lines = create_insight_summary(df_filtered, df_errors_selected)
    executive_line_table = prepare_line_exec_table(line_rate, warning_threshold, critical_threshold)
    quality_posture = build_quality_posture(error_rate, box_error_rate, warning_threshold, critical_threshold)
    chart_specs = []
    click_payloads = build_chart_click_payloads(df_errors_selected)
    report_access = build_report_access_links(
        EXPORT_DIR / "weekly_bug_report_latest.html",
        EXPORT_DIR / f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.html",
        REPORT_SHARE_TARGET,
    )

    # Chart 1: Fehlertypen Donut
    if not df_errors_selected.empty:
        error_dist = df_errors_selected["checks_error_category"].value_counts().reset_index()
        error_dist.columns = ["Fehlertyp", "Anzahl"]
        fig1 = px.pie(error_dist, values="Anzahl", names="Fehlertyp",
                      color="Fehlertyp", color_discrete_map=ERROR_COLOR_MAP,
                      hole=0.45, title="Fehlertypen-Verteilung")
        fig1.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        chart_specs.append({
            "id": "chart-error-type",
            "title": "Fehlertypen-Verteilung",
            "caption": "Klick zeigt die betroffenen Artikel und Beispielvorgänge.",
            "html": fig1.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id="chart-error-type",
                config={"responsive": True, "displaylogo": False},
            ),
            "interaction": "error_type",
        })

    # Chart 2: Wöchentlicher Trend
    if not df_errors_selected.empty:
        wt = df_errors_selected.groupby(["year_week", "checks_error_category"]).size().reset_index(name="Anzahl")
        wt.columns = ["Kalenderwoche", "Fehlertyp", "Anzahl"]
        fig2 = px.bar(wt, x="Kalenderwoche", y="Anzahl", color="Fehlertyp",
                      color_discrete_map=ERROR_COLOR_MAP,
                      barmode="stack", title="Wöchentlicher Fehler-Trend")
        fig2.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        chart_specs.append({
            "id": "chart-week-trend",
            "title": "Wöchentlicher Fehler-Trend",
            "caption": "Klick auf ein Segment öffnet die Artikelliste der gewählten Woche.",
            "html": fig2.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id="chart-week-trend",
                config={"responsive": True, "displaylogo": False},
            ),
            "interaction": "week_type",
        })

    # Chart 3: Assembly Line
    if not df_errors_selected.empty:
        le = df_errors_selected.groupby(["assembly_line", "checks_error_category"]).size().reset_index(name="Anzahl")
        fig3 = px.bar(le, x="assembly_line", y="Anzahl", color="checks_error_category",
                      color_discrete_map=ERROR_COLOR_MAP,
                      barmode="group", title="Fehler pro Assembly Line",
                      labels={"assembly_line": "Assembly Line", "checks_error_category": "Fehlertyp"})
        fig3.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        chart_specs.append({
            "id": "chart-line-errors",
            "title": "Fehler pro Assembly Line",
            "caption": "Klick zeigt die Artikel hinter der gewählten Linie und dem Fehlertyp.",
            "html": fig3.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id="chart-line-errors",
                config={"responsive": True, "displaylogo": False},
            ),
            "interaction": "line_type",
        })

    # Chart 4: Top SKUs
    if not sku_errors.empty:
        fig4 = px.bar(
            sku_errors,
            x="Anzahl Fehler",
            y="Label",
            orientation="h",
            title="Top 15 fehlerhafte SKUs",
            color="Anzahl Fehler",
            color_continuous_scale=["#fde68a", "#f59e0b", "#dc2626"],
            hover_data={"SKU": True, "Artikelname": True, "Fehlermenge": True, "Label": False},
            text="Anzahl Fehler",
        )
        fig4.update_traces(textposition="outside")
        fig4.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        fig4.update_layout(
            yaxis=dict(autorange="reversed", title=None),
            xaxis_title="Fehleranzahl",
            height=560,
            margin=dict(t=50, r=40, b=30, l=20),
            coloraxis_showscale=False,
        )
        chart_specs.append({
            "id": "chart-top-skus",
            "title": "Top 15 fehlerhafte SKUs",
            "caption": "Klick auf einen Balken zeigt Artikelprofil, Mengen und aktuelle Fälle.",
            "html": fig4.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id="chart-top-skus",
                config={"responsive": True, "displaylogo": False},
            ),
            "interaction": "sku",
        })

    # Chart 5: Mitarbeiter
    if not df_errors_selected.empty:
        ue = df_errors_selected.groupby(["user_id", "checks_error_category"]).size().reset_index(name="Anzahl")
        ue["user_id"] = "User " + ue["user_id"].astype(str)
        fig5 = px.bar(ue, x="user_id", y="Anzahl", color="checks_error_category",
                      color_discrete_map=ERROR_COLOR_MAP,
                      barmode="stack", title="Fehler pro Mitarbeiter",
                      labels={"user_id": "Mitarbeiter", "checks_error_category": "Fehlertyp"})
        fig5.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        chart_specs.append({
            "id": "chart-user-errors",
            "title": "Fehler pro Mitarbeiter",
            "caption": "Klick zeigt die dahinterliegenden Artikel für Mitarbeiter und Fehlertyp.",
            "html": fig5.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id="chart-user-errors",
                config={"responsive": True, "displaylogo": False},
            ),
            "interaction": "user_type",
        })

    # Chart 6: Heatmap
    df_st = df_errors_selected.dropna(subset=["stations"]).copy()
    df_st["stations"] = pd.to_numeric(df_st["stations"], errors="coerce")
    df_st = df_st.dropna(subset=["stations"])
    if not df_st.empty:
        hm = df_st.groupby(["assembly_line", "stations"]).size().reset_index(name="Fehler")
        hm["stations"] = hm["stations"].astype(int)
        hm_pivot = hm.pivot_table(index="assembly_line", columns="stations", values="Fehler", fill_value=0)
        fig6 = px.imshow(hm_pivot, labels=dict(x="Station", y="Assembly Line", color="Fehler"),
                         color_continuous_scale="RdYlGn_r", aspect="auto", title="Station × Assembly Line Heatmap")
        fig6.update_layout(paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        chart_specs.append({
            "id": "chart-station-heatmap",
            "title": "Station × Assembly Line Heatmap",
            "caption": "Klick auf eine Zelle zeigt die betroffenen Artikel an dieser Station.",
            "html": fig6.to_html(
                full_html=False,
                include_plotlyjs=False,
                div_id="chart-station-heatmap",
                config={"responsive": True, "displaylogo": False},
            ),
            "interaction": "station_line",
        })

    overview_cards = [
        ("Gesamte Checks", fmt_int(total_checks), "Operatives Volumen im aktuellen Filter"),
        ("Fehlerhafte Checks", fmt_int(total_errors), "Erkannte Fälle nach ausgewählten Typen"),
        ("Fehlerquote", fmt_pct(error_rate), "Anteil fehlerhafter Prüfungen"),
        ("Boxen mit Fehlern", fmt_int(unique_boxes_with_errors), "Eindeutige betroffene Boxen"),
        ("Box-Fehlerquote", fmt_pct(box_error_rate), "Anteil betroffener Boxen"),
        ("Fehlermenge", fmt_int(total_error_qty if pd.notna(total_error_qty) else 0), "Summierte Fehlerquantität"),
    ]

    html_hotspots = top_issue_table(df_errors_selected).copy()
    if not html_hotspots.empty:
        hotspot_html = html_hotspots.to_html(index=False, classes="data-table hotspot-table", border=0)
    else:
        hotspot_html = "<p>Keine Hotspots im ausgewählten Zeitraum.</p>"

    management_actions = build_management_actions(df_errors_selected, html_hotspots, executive_line_table)
    if management_actions:
        management_actions_html = "".join(
            f'<div class="action-card {item["tone"]}"><strong>{html_escape(item["title"])}</strong><p>{html_escape(item["copy"])}</p></div>'
            for item in management_actions
        )
    else:
        management_actions_html = '<div class="empty-state">Keine priorisierten Maßnahmen für die aktuelle Auswahl vorhanden.</div>'

    week_compare_html = '<div class="empty-state">Kein Vorwochenvergleich verfügbar, weil aktuell nur eine Kalenderwoche im Datenraum vorhanden ist.</div>'
    if week_over_week.get("available"):
        current_week = week_over_week["current"]
        previous_week = week_over_week["previous"]
        week_compare_html = f'''
            <div class="compare-grid">
                <div class="compare-card">
                    <strong>{html_escape(current_week.get("week") or "Aktuell")}</strong>
                    <div class="compare-value">{current_week.get("error_rate", 0.0):.1f}%</div>
                    <div class="muted">Fehlerquote | {fmt_int(current_week.get("total_errors", 0))} Fehler bei {fmt_int(current_week.get("total_checks", 0))} Checks</div>
                </div>
                <div class="compare-card">
                    <strong>{html_escape(previous_week.get("week") or "Vorwoche")}</strong>
                    <div class="compare-value">{previous_week.get("error_rate", 0.0):.1f}%</div>
                    <div class="muted">Fehlerquote | {fmt_int(previous_week.get("total_errors", 0))} Fehler bei {fmt_int(previous_week.get("total_checks", 0))} Checks</div>
                </div>
                <div class="compare-card emphasis">
                    <strong>Delta</strong>
                    <div class="compare-value">{week_over_week.get("delta_error_rate", 0.0):+.1f} pp</div>
                    <div class="muted">Box-Fehlerquote {week_over_week.get("delta_box_error_rate", 0.0):+.1f} pp | Fehler {week_over_week.get("delta_errors", 0):+d}</div>
                </div>
            </div>
        '''

    report_access_html = f'''
        <div class="access-grid">
            <div class="access-card">
                <strong>Lokal aktuell</strong>
                <p>{html_escape(report_access.get("local_latest_path") or "-")}</p>
            </div>
            <div class="access-card">
                <strong>Lokal dated</strong>
                <p>{html_escape(report_access.get("local_dated_path") or "-")}</p>
            </div>
            <div class="access-card">
                <strong>Freigabe</strong>
                <p>{html_escape(report_access.get("public_latest_url") or "Noch keine Freigabe-URL hinterlegt")}</p>
            </div>
        </div>
    '''

    line_exec = executive_line_table.head(8).copy()
    if not line_exec.empty:
        line_exec["Priorität"] = line_exec["Priorität"].apply(
            lambda value: f'<span class="status-badge {status_tone(value)}">{value}</span>'
        )
        line_exec_html = line_exec.to_html(index=False, classes="data-table", border=0, escape=False)
    else:
        line_exec_html = "<p>Keine Linienübersicht verfügbar.</p>"

    brand_signature = f"{BRAND_NAME} | {REPORT_TITLE}"

    # Fehler-Tabelle als interaktives HTML
    if not df_errors_selected.empty:
        df_tbl = df_errors_selected[[
            "start_date", "day_of_week", "assembly_line", "user_id", "box_id",
            "checks_error_category", "checks_error_quantity", "stations",
            "checks_error_sku", "checks_error_sku_name"
        ]].copy()
        df_tbl.columns = [
            "Datum", "Wochentag", "Assembly Line", "Mitarbeiter", "Box-ID",
            "Fehlertyp", "Menge", "Station", "SKU", "Artikelname"
        ]
        df_tbl["Datum"] = df_tbl["Datum"].dt.strftime("%d.%m.%Y")
        table_html = df_tbl.to_html(index=False, classes="data-table", border=0, na_rep="–")
    else:
        table_html = "<p>Keine Fehler im gewählten Zeitraum.</p>"

    date_from = _date_min.strftime("%d.%m.%Y") if pd.notna(_date_min) else "–"
    date_to = _date_max.strftime("%d.%m.%Y") if pd.notna(_date_max) else "–"

    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Assembly QC Weekly Bug Report</title>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=Manrope:wght@400;500;600;700;800&display=swap');
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Manrope', sans-serif;
            background:
                radial-gradient(circle at top left, rgba(15, 118, 110, 0.13), transparent 22%),
                radial-gradient(circle at top right, rgba(217, 119, 6, 0.16), transparent 24%),
                linear-gradient(180deg, #fbf8f3 0%, #f3efe7 100%);
            color: #1f2937;
            padding: 28px;
        }}
        h1, h2, h3, h4 {{ font-family: 'Space Grotesk', sans-serif; letter-spacing: -0.03em; }}
        .page {{ max-width: 1440px; margin: 0 auto; }}
        .header {{
            background: linear-gradient(135deg, rgba(15,118,110,0.96), rgba(17,94,89,0.88));
            color: white;
            padding: 30px;
            border-radius: 28px;
            margin-bottom: 22px;
            position: relative;
            overflow: hidden;
            box-shadow: 0 24px 55px rgba(15, 23, 42, 0.16);
        }}
        .header::after {{
            content: "";
            position: absolute;
            width: 300px;
            height: 300px;
            right: -70px;
            top: -90px;
            background: radial-gradient(circle, rgba(255,255,255,0.22), transparent 65%);
        }}
        .header .kicker {{ text-transform: uppercase; letter-spacing: 0.18em; font-size: 12px; font-weight: 800; opacity: 0.82; margin-bottom: 8px; }}
        .logo-mark {{ width: 64px; height: 64px; border-radius: 20px; display: inline-flex; align-items: center; justify-content: center; background: rgba(255,255,255,0.14); border: 1px solid rgba(255,255,255,0.2); font-family: 'Space Grotesk', sans-serif; font-size: 24px; font-weight: 800; margin-bottom: 14px; }}
        .header h1 {{ font-size: 42px; margin-bottom: 10px; }}
        .header p {{ max-width: 860px; opacity: 0.93; line-height: 1.6; }}
        .header-meta {{ display: flex; gap: 10px; flex-wrap: wrap; margin-top: 18px; }}
        .chip {{ background: rgba(255,255,255,0.15); border: 1px solid rgba(255,255,255,0.18); padding: 8px 12px; border-radius: 999px; font-size: 13px; font-weight: 700; }}
        .top-bar {{ display: grid; grid-template-columns: 1.4fr 0.9fr; gap: 18px; margin-bottom: 20px; }}
        .panel {{ background: rgba(255, 252, 247, 0.92); border: 1px solid rgba(15,23,42,0.08); border-radius: 24px; padding: 22px; box-shadow: 0 18px 45px rgba(31,41,55,0.08); }}
        .panel h3 {{ margin-bottom: 12px; font-size: 22px; }}
        .panel p {{ color: #6b7280; line-height: 1.55; }}
        .kpi-row {{ display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 14px; margin-bottom: 22px; }}
        .kpi {{ min-width: 0; padding: 18px; border-radius: 22px; color: #1f2937; background: linear-gradient(145deg, rgba(255,255,255,0.95), rgba(255,249,243,0.94)); border: 1px solid rgba(255,255,255,0.75); box-shadow: 0 18px 45px rgba(31,41,55,0.08); }}
        .kpi h2 {{ font-size: 30px; margin-bottom: 6px; }}
        .kpi p {{ font-size: 13px; color: #6b7280; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; }}
        .kpi span {{ display: block; margin-top: 8px; color: #4b5563; font-size: 13px; line-height: 1.45; }}
        .insight-list {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }}
        .insight {{ padding: 16px; border-radius: 18px; background: linear-gradient(145deg, rgba(247,250,252,0.92), rgba(255,255,255,0.95)); border: 1px solid rgba(15,23,42,0.07); }}
        .insight strong {{ display: block; margin-bottom: 6px; font-size: 12px; color: #6b7280; letter-spacing: 0.08em; text-transform: uppercase; }}
        .section-title {{ display: flex; align-items: center; justify-content: space-between; margin: 22px 0 14px 0; }}
        .section-title small {{ color: #6b7280; }}
        .interactive-layout {{ display:grid; grid-template-columns: minmax(0, 1.5fr) 360px; gap: 18px; align-items:start; }}
        .chart-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 18px; }}
        .chart-section {{ background: rgba(255, 252, 247, 0.92); border-radius: 24px; padding: 18px; box-shadow: 0 18px 45px rgba(31,41,55,0.08); border: 1px solid rgba(15,23,42,0.08); }}
        .chart-header {{ display:flex; align-items:flex-start; justify-content:space-between; gap:14px; margin-bottom:10px; }}
        .chart-header h4 {{ font-size: 20px; }}
        .chart-caption {{ font-size: 13px; color:#6b7280; line-height:1.5; max-width: 360px; }}
        .plot-host {{ min-height: 360px; }}
        .detail-card {{ background: rgba(255, 252, 247, 0.96); border-radius: 24px; padding: 18px; box-shadow: 0 18px 45px rgba(31,41,55,0.08); border: 1px solid rgba(15,23,42,0.08); }}
        .sticky-panel {{ position: sticky; top: 24px; }}
        .detail-card h4 {{ margin-bottom: 6px; }}
        .detail-card p {{ color:#6b7280; line-height:1.55; }}
        .detail-actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top: 14px; }}
        .action-btn {{ border:none; cursor:pointer; padding:10px 12px; border-radius:12px; font-weight:800; font-size:12px; background:#0f766e; color:white; }}
        .recommendation-list {{ display:grid; gap:10px; margin-top: 14px; }}
        .recommendation-item {{ background: rgba(255,255,255,0.84); border:1px solid rgba(15,23,42,0.08); border-radius:16px; padding: 12px; color:#1f2937; }}
        .article-list, .case-list {{ display:grid; gap: 12px; margin-top: 16px; }}
        .article-item, .case-item {{ background: rgba(255,255,255,0.84); border:1px solid rgba(15,23,42,0.08); border-radius: 18px; padding: 14px; }}
        .article-item strong, .case-item strong {{ display:block; margin-bottom: 5px; font-size: 14px; color:#111827; }}
        .meta-row {{ display:flex; flex-wrap:wrap; gap:8px; margin-top: 8px; }}
        .meta-chip {{ background: rgba(15,118,110,0.08); color:#0f766e; border-radius:999px; padding:5px 9px; font-size:12px; font-weight:700; }}
        .empty-state {{ color:#6b7280; padding: 12px 0 4px 0; line-height:1.55; }}
        .data-table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        .data-table th {{ background: #0f766e; color: white; padding: 12px 14px; text-align: left; position: sticky; top: 0; cursor: pointer; }}
        .data-table td {{ padding: 10px 14px; border-bottom: 1px solid rgba(15,23,42,0.08); color: #111827; }}
        .data-table tr:hover {{ background: rgba(15,118,110,0.05); }}
        .data-table tr:nth-child(even) {{ background: rgba(255,255,255,0.62); }}
        .hotspot-table td:first-child, .hotspot-table th:first-child {{ width: 140px; }}
        .table-container {{ max-height: 560px; overflow-y: auto; border-radius: 16px; border: 1px solid rgba(15,23,42,0.08); background: rgba(255,255,255,0.72); }}
        .filter-bar {{ background: rgba(255,252,247,0.92); padding: 18px 20px; border-radius: 22px; margin-bottom: 16px; box-shadow: 0 18px 45px rgba(31,41,55,0.08); border: 1px solid rgba(15,23,42,0.08); }}
        .filter-bar input {{ padding: 12px 16px; border: 1px solid rgba(15,23,42,0.16); border-radius: 14px; font-size: 15px; width: min(420px, 100%); background: rgba(255,255,255,0.9); }}
        .filter-bar input:focus {{ outline: none; border-color: #0f766e; box-shadow: 0 0 0 4px rgba(15,118,110,0.12); }}
        .databricks-btn {{ display: inline-block; padding: 12px 18px; background: linear-gradient(135deg, #FF3621, #FF6A33); color: white; border-radius: 14px; text-decoration: none; font-weight: 700; margin-top: 18px; }}
        .toolbar-actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top: 18px; }}
        .utility-btn {{ display:inline-flex; align-items:center; justify-content:center; padding: 12px 16px; border-radius: 14px; text-decoration:none; font-weight:700; border:1px solid rgba(255,255,255,0.2); background: rgba(255,255,255,0.12); color:white; cursor:pointer; }}
        .utility-btn.dark {{ background: rgba(15,23,42,0.18); }}
        .status-badge {{ display: inline-flex; align-items: center; justify-content: center; padding: 6px 10px; border-radius: 999px; font-size: 12px; font-weight: 800; letter-spacing: 0.03em; }}
        .status-badge.good {{ background: rgba(16, 185, 129, 0.16); color: #047857; }}
        .status-badge.warn {{ background: rgba(245, 158, 11, 0.15); color: #b45309; }}
        .status-badge.alert {{ background: rgba(239, 68, 68, 0.15); color: #b91c1c; }}
        .pulse-band {{ display:grid; grid-template-columns: 1.2fr 0.8fr; gap:18px; margin-bottom:22px; }}
        .pulse-card {{ background: linear-gradient(135deg, rgba(255,255,255,0.94), rgba(250,247,240,0.92)); border:1px solid rgba(15,23,42,0.08); border-radius:24px; padding:22px; box-shadow: 0 18px 45px rgba(31,41,55,0.08); }}
        .pulse-card h3 {{ margin-bottom: 8px; font-size: 24px; }}
        .pulse-card p {{ color:#4b5563; line-height:1.65; }}
        .anchor-nav {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:14px; }}
        .anchor-link {{ display:inline-flex; align-items:center; justify-content:center; padding:10px 12px; border-radius:999px; text-decoration:none; color:#0f766e; background:rgba(15,118,110,0.08); border:1px solid rgba(15,118,110,0.14); font-size:13px; font-weight:800; }}
        .action-grid {{ display:grid; gap:12px; margin-top:16px; }}
        .action-card {{ padding:16px; border-radius:18px; border:1px solid rgba(15,23,42,0.08); background:rgba(255,255,255,0.84); }}
        .action-card strong {{ display:block; margin-bottom:6px; font-size:15px; color:#111827; }}
        .action-card p {{ color:#4b5563; line-height:1.55; }}
        .action-card.alert {{ background:linear-gradient(145deg, rgba(254,242,242,0.98), rgba(255,255,255,0.95)); border-color: rgba(239,68,68,0.18); }}
        .action-card.warn {{ background:linear-gradient(145deg, rgba(255,247,237,0.98), rgba(255,255,255,0.95)); border-color: rgba(245,158,11,0.18); }}
        .action-card.good {{ background:linear-gradient(145deg, rgba(236,253,245,0.98), rgba(255,255,255,0.95)); border-color: rgba(16,185,129,0.18); }}
        .compare-grid {{ display:grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap:12px; margin-top:16px; }}
        .compare-card {{ padding:16px; border-radius:18px; border:1px solid rgba(15,23,42,0.08); background:rgba(255,255,255,0.84); }}
        .compare-card.emphasis {{ background:linear-gradient(145deg, rgba(239,246,255,0.98), rgba(255,255,255,0.95)); border-color: rgba(37,99,235,0.16); }}
        .compare-card strong {{ display:block; margin-bottom:6px; color:#6b7280; text-transform:uppercase; letter-spacing:0.08em; font-size:12px; }}
        .compare-value {{ font-family:'Space Grotesk', sans-serif; font-size:28px; font-weight:800; color:#111827; margin-bottom:4px; }}
        .access-grid {{ display:grid; gap:12px; margin-top:16px; }}
        .access-card {{ padding:14px 16px; border-radius:18px; border:1px solid rgba(15,23,42,0.08); background:rgba(255,255,255,0.82); }}
        .access-card strong {{ display:block; margin-bottom:6px; color:#111827; }}
        .access-card p {{ color:#4b5563; line-height:1.55; word-break:break-word; }}
        .mini-list {{ margin-top: 10px; padding-left: 18px; color: #4b5563; }}
        .mini-list li {{ margin-bottom: 8px; }}
        .footer {{ text-align: center; color: #6b7280; font-size: 12px; margin-top: 24px; padding: 16px; }}
        @media (max-width: 1100px) {{
            .top-bar, .interactive-layout, .chart-grid, .insight-list, .kpi-row, .pulse-band, .compare-grid {{ grid-template-columns: 1fr; }}
            body {{ padding: 18px; }}
            .header h1 {{ font-size: 34px; }}
        }}
        @media print {{ body {{ padding: 8px; background: white; }} .filter-bar {{ display: none; }} .panel, .chart-section, .kpi {{ box-shadow: none; }} }}
    </style>
</head>
<body>
    <div class="page">
        <div class="header">
            <div class="logo-mark">WP</div>
            <div class="kicker">Assembly QC Intelligence</div>
            <h1>Weekly Bug Report</h1>
            <p>Präsentationsfähiger Qualitätsreport mit Executive Summary, Hotspot-Ranking, Deep-Dive-Charts und durchsuchbarer Fehlerbasis. Optimiert für Review-Meetings, operative Steuerung und Versand als eigenständiges HTML-Dokument unter dem Branding von {BRAND_NAME}.</p>
            <div class="header-meta">
                <span class="chip">Zeitraum: {date_from} bis {date_to}</span>
                <span class="chip">Erstellt: {report_created}</span>
                <span class="chip">Fokus: {focus_dimension}</span>
                <span class="chip">Ampel: Beobachten ab {warning_threshold:.1f}% | Kritisch ab {critical_threshold:.1f}%</span>
            </div>
            <div class="toolbar-actions">
                <a href="{DATABRICKS_QUERY_URL}" target="_blank" class="databricks-btn">Databricks SQL Query öffnen</a>
                <button class="utility-btn" onclick="window.print()">Als PDF drucken</button>
                <a href="#error-log" class="utility-btn dark">Zum Fehlerlog</a>
            </div>
        </div>

        <div class="pulse-band">
            <div class="pulse-card">
                <div class="section-title" style="margin:0 0 10px 0;"><h3>Executive Pulse</h3><small>{build_status_badge(quality_posture['status'])}</small></div>
                <p><strong>{html_escape(quality_posture['label'])}.</strong> {html_escape(quality_posture['summary'])}</p>
                <div class="anchor-nav">
                    <a class="anchor-link" href="#hotspots">Hotspots</a>
                    <a class="anchor-link" href="#deep-dive">Deep Dive</a>
                    <a class="anchor-link" href="#error-log">Error Log</a>
                </div>
            </div>
            <div class="pulse-card">
                <h3>Management Actions</h3>
                <p>Konkrete Prioritäten aus der aktuellen Filterlage, direkt aus dem Report ableitbar.</p>
                <div class="action-grid">{management_actions_html}</div>
            </div>
        </div>

        <div class="kpi-row">
            {''.join(f'<div class="kpi"><p>{title}</p><h2>{value}</h2><span>{subtitle}</span></div>' for title, value, subtitle in overview_cards)}
        </div>

        <div class="top-bar">
            <div class="panel">
                <h3>Vorwochenvergleich</h3>
                <p>Ein schneller Reality Check, ob sich die Lage gegenüber der letzten verfügbaren Kalenderwoche verbessert oder verschärft hat.</p>
                {week_compare_html}
            </div>
            <div class="panel">
                <h3>Report Access</h3>
                <p>So ist der erzeugte Report lokal und optional für andere Empfänger erreichbar.</p>
                {report_access_html}
            </div>
        </div>

        <div class="top-bar">
            <div class="panel">
                <h3>Executive Summary</h3>
                <p>Die folgenden Aussagen verdichten die aktuelle Lage auf die wichtigsten operativen Treiber. Sie sollen ohne zusätzliche Interpretation im Weekly Review verwendbar sein.</p>
                <div class="insight-list">
                    {''.join(f'<div class="insight"><strong>Insight</strong>{line}</div>' for line in insight_lines)}
                </div>
            </div>
            <div class="panel">
                <h3>Management Fokus</h3>
                <p>Priorisierte Blickrichtung auf Basis der aktiven Streamlit-Filter.</p>
                <ul class="mini-list">
                    <li>Top-N Ranking aktiv: {top_n}</li>
                    <li>Aktive Kalenderwochen: {len(selected_weeks)}</li>
                    <li>Aktive Linien: {len(selected_lines)}</li>
                    <li>Aktive Mitarbeiter: {len(selected_users)}</li>
                    <li>Fehlertyp-Filter: {', '.join(selected_errors) if selected_errors else 'keine'}</li>
                    <li>Kritische Linien: {int((executive_line_table['Priorität'] == 'Kritisch').sum())}</li>
                </ul>
            </div>
        </div>

        <div id="hotspots" class="section-title"><h3>Hotspots & Prioritäten</h3><small>Ranking für schnelle Steuerungsmaßnahmen</small></div>
        <div class="top-bar">
            <div class="panel">
                <h3>Hotspot-Ranking</h3>
                <div class="table-container">{hotspot_html}</div>
            </div>
            <div class="panel">
                <h3>Linienpriorisierung</h3>
                <div class="table-container">{line_exec_html}</div>
            </div>
        </div>

        <div id="deep-dive" class="section-title"><h3>Visual Deep Dive</h3><small>Interaktive Plotly-Charts für weitere Analyse</small></div>
        <div class="interactive-layout">
            <div class="chart-grid">
                {''.join(f'<div class="chart-section"><div class="chart-header"><div><h4>{spec["title"]}</h4><div class="chart-caption">{spec["caption"]}</div></div></div><div class="plot-host">{spec["html"]}</div></div>' for spec in chart_specs)}
            </div>
            <div class="detail-card sticky-panel">
                <h4 id="selectionTitle">Kein Element ausgewählt</h4>
                <p id="selectionSubtitle">Wähle ein Segment in einem Diagramm aus. Hier erscheinen dann direkt die betroffenen Artikel statt nur einer Zahl.</p>
                <div id="selectionStats" class="meta-row"></div>
                <div class="detail-actions">
                    <button class="action-btn" onclick="downloadSelectionJson()">Auswahl als JSON</button>
                    <button class="action-btn" onclick="downloadSelectionCsv()">Auswahl als CSV</button>
                </div>
                <div id="recommendationList" class="recommendation-list">
                    <div class="empty-state">Noch keine Handlungsempfehlungen vorhanden.</div>
                </div>
                <div id="articleList" class="article-list">
                    <div class="empty-state">Noch keine Artikeldetails geladen.</div>
                </div>
                <h4>Aktuelle Beispielvorgänge</h4>
                <p>Die letzten passenden Fälle aus dem gewählten Segment, damit man direkt operativ einsteigen kann.</p>
                <div id="caseList" class="case-list">
                    <div class="empty-state">Noch keine Vorgänge ausgewählt.</div>
                </div>
            </div>
        </div>

        <div id="error-log" class="section-title"><h3>Detailed Error Log</h3><small>Volltextsuche und Sortierung direkt im Browser</small></div>
        <div class="filter-bar">
            <label style="display:block; margin-bottom:8px; font-weight:700;">Tabelle durchsuchen</label>
            <input type="text" id="tableSearch" onkeyup="filterTable()" placeholder="Box-ID, SKU, Artikel, Mitarbeiter suchen...">
        </div>

        <div class="chart-section">
            <h3 style="margin-bottom:1rem;">Detaillierte Fehlertabelle ({total_errors} Einträge)</h3>
            <div class="table-container">
                {table_html}
            </div>
        </div>

        <div class="footer">
            {brand_signature} | Erstellt {report_created}
        </div>
    </div>

    <script>
    const chartConfigs = {json.dumps([{"id": spec["id"], "interaction": spec["interaction"]} for spec in chart_specs], ensure_ascii=False)};
    const clickPayloads = {json.dumps(click_payloads, ensure_ascii=False)};

    let activeSelection = null;

    function renderSelection(payload) {{
        const title = document.getElementById('selectionTitle');
        const subtitle = document.getElementById('selectionSubtitle');
        const stats = document.getElementById('selectionStats');
        const recommendationList = document.getElementById('recommendationList');
        const articleList = document.getElementById('articleList');
        const caseList = document.getElementById('caseList');
        activeSelection = payload;

        if (!payload) {{
            title.textContent = 'Keine Details gefunden';
            subtitle.textContent = 'Für dieses Segment liegen keine Artikeldetails vor.';
            stats.innerHTML = '';
            recommendationList.innerHTML = '<div class="empty-state">Keine Handlungsempfehlungen für die aktuelle Auswahl.</div>';
            articleList.innerHTML = '<div class="empty-state">Keine Artikel für die aktuelle Auswahl.</div>';
            caseList.innerHTML = '<div class="empty-state">Keine Fälle für die aktuelle Auswahl.</div>';
            return;
        }}

        title.textContent = payload.title;
        subtitle.textContent = payload.subtitle;
        stats.innerHTML = `<span class="meta-chip">Vorfälle: ${{payload.count}}</span><span class="meta-chip">Artikel: ${{payload.articles.length}}</span><span class="meta-chip">Beispielfälle: ${{payload.cases.length}}</span>`;
        recommendationList.innerHTML = payload.recommendations && payload.recommendations.length
            ? payload.recommendations.map(item => `<div class="recommendation-item">${{item}}</div>`).join('')
            : '<div class="empty-state">Keine Handlungsempfehlungen für die aktuelle Auswahl.</div>';

        articleList.innerHTML = payload.articles.length
            ? payload.articles.map(article => `
                <div class="article-item">
                    <strong>${{article.sku}} • ${{article.name}}</strong>
                    <div>${{article.incidents}} Vorfälle, Fehlermenge ${{article.quantity}}</div>
                    <div class="meta-row"><span class="meta-chip">Linien: ${{article.lines}}</span></div>
                </div>
            `).join('')
            : '<div class="empty-state">Keine Artikel für die aktuelle Auswahl.</div>';

        caseList.innerHTML = payload.cases.length
            ? payload.cases.map(item => `
                <div class="case-item">
                    <strong>${{item.date}} • Linie ${{item.line}}</strong>
                    <div>Box ${{item.box}} • ${{item.error_type}}</div>
                    <div class="meta-row"><span class="meta-chip">${{item.sku}}</span><span class="meta-chip">${{item.name}}</span><span class="meta-chip">Menge ${{item.quantity}}</span></div>
                </div>
            `).join('')
            : '<div class="empty-state">Keine Fälle für die aktuelle Auswahl.</div>';
    }}

    function selectionKey(interaction, point) {{
        if (interaction === 'error_type') return String(point.label);
        if (interaction === 'week_type') return `${{point.x}}|${{point.data.name}}`;
        if (interaction === 'line_type') return `${{point.x}}|${{point.data.name}}`;
        if (interaction === 'sku') return String(point.y).split(' • ')[0];
        if (interaction === 'user_type') return `${{point.x}}|${{point.data.name}}`;
        if (interaction === 'station_line') return `${{point.y}}|${{point.x}}`;
        return null;
    }}

    function triggerDownload(filename, content, mimeType) {{
        const blob = new Blob([content], {{ type: mimeType }});
        const url = URL.createObjectURL(blob);
        const anchor = document.createElement('a');
        anchor.href = url;
        anchor.download = filename;
        document.body.appendChild(anchor);
        anchor.click();
        document.body.removeChild(anchor);
        URL.revokeObjectURL(url);
    }}

    function downloadSelectionJson() {{
        if (!activeSelection) return;
        triggerDownload('drilldown_selection.json', JSON.stringify(activeSelection, null, 2), 'application/json');
    }}

    function downloadSelectionCsv() {{
        if (!activeSelection || !activeSelection.cases) return;
        const headers = ['date', 'line', 'box', 'error_type', 'sku', 'name', 'quantity'];
        const rows = activeSelection.cases.map(item => headers.map(header => '"' + String(item[header] ?? '').replaceAll('"', '""') + '"').join(';'));
        const csv = [headers.join(';'), ...rows].join('\\n');
        triggerDownload('drilldown_selection.csv', csv, 'text/csv');
    }}

    chartConfigs.forEach(config => {{
        const plot = document.getElementById(config.id);
        if (!plot || typeof plot.on !== 'function') return;
        plot.on('plotly_click', (event) => {{
            const point = event.points && event.points[0];
            if (!point) return;
            const key = selectionKey(config.interaction, point);
            const payload = key ? clickPayloads[config.interaction]?.[key] : null;
            renderSelection(payload);
        }});
    }});

    // Tabellen-Suche
    function filterTable() {{
        const input = document.getElementById('tableSearch').value.toLowerCase();
        const rows = document.querySelectorAll('.data-table tbody tr');
        rows.forEach(row => {{
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(input) ? '' : 'none';
        }});
    }}
    // Spalten-Sortierung
    document.querySelectorAll('.data-table th').forEach((th, idx) => {{
        th.addEventListener('click', () => {{
            const table = th.closest('table');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const dir = th.dataset.dir === 'asc' ? 'desc' : 'asc';
            th.dataset.dir = dir;
            rows.sort((a, b) => {{
                const aVal = a.children[idx]?.textContent || '';
                const bVal = b.children[idx]?.textContent || '';
                const aNum = parseFloat(aVal); const bNum = parseFloat(bVal);
                if (!isNaN(aNum) && !isNaN(bNum)) return dir === 'asc' ? aNum - bNum : bNum - aNum;
                return dir === 'asc' ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
            }});
            rows.forEach(r => tbody.appendChild(r));
        }});
    }});
    </script>
</body>
</html>"""
    return html


# --- Helper: JSON Export ---
def build_json_report():
    """Erstellt einen strukturierten JSON-Report."""
    report = {
        "meta": {
            "report_name": "Assembly QC Weekly Bug Report",
            "created": datetime.now().isoformat(),
            "date_from": _date_min.isoformat() if pd.notna(_date_min) else None,
            "date_to": _date_max.isoformat() if pd.notna(_date_max) else None,
            "databricks_url": DATABRICKS_QUERY_URL,
        },
        "kpis": {
            "total_checks": int(total_checks),
            "total_errors": int(total_errors),
            "error_rate_pct": round(error_rate, 1),
            "total_error_quantity": int(total_error_qty) if pd.notna(total_error_qty) else 0,
            "unique_boxes_with_errors": int(unique_boxes_with_errors),
            "total_boxes": int(total_boxes),
            "box_error_rate_pct": round(box_error_rate, 1),
        },
        "errors_by_type": {},
        "errors_by_assembly_line": {},
        "errors_by_user": {},
        "top_skus": [],
        "errors": [],
    }
    if not df_errors_selected.empty:
        for etype in ["missing", "wrong_item", "extra"]:
            report["errors_by_type"][etype] = int(len(df_errors_selected[df_errors_selected["checks_error_category"] == etype]))
    for line in sorted(df_errors_selected["assembly_line"].unique()):
        report["errors_by_assembly_line"][line] = int(len(df_errors_selected[df_errors_selected["assembly_line"] == line]))
    for uid in sorted(df_errors_selected["user_id"].unique()):
        report["errors_by_user"][str(uid)] = int(len(df_errors_selected[df_errors_selected["user_id"] == uid]))
    if not df_errors_selected.empty:
        sku_j = (df_errors_selected.dropna(subset=["checks_error_sku"])
                 .groupby(["checks_error_sku", "checks_error_sku_name"])
                 .agg(count=("box_id", "count"), qty=("checks_error_quantity", "sum"))
                 .sort_values("count", ascending=False).head(20).reset_index())
        for _, row in sku_j.iterrows():
            report["top_skus"].append({
                "sku": row["checks_error_sku"],
                "name": row["checks_error_sku_name"] if pd.notna(row["checks_error_sku_name"]) else None,
                "incidents": int(row["count"]),
                "quantity": int(row["qty"]) if pd.notna(row["qty"]) else 0,
            })
    if not df_errors_selected.empty:
        for _, row in df_errors_selected.iterrows():
            report["errors"].append({
                "date": row["start_date"].isoformat(),
                "day": row["day_of_week"],
                "assembly_line": row["assembly_line"],
                "user_id": str(row["user_id"]),
                "box_id": row["box_id"],
                "error_type": row["checks_error_category"],
                "quantity": int(row["checks_error_quantity"]) if pd.notna(row["checks_error_quantity"]) else None,
                "station": int(row["stations"]) if pd.notna(row.get("stations")) and str(row["stations"]) not in ["null", "nan"] else None,
                "sku": row["checks_error_sku"] if pd.notna(row["checks_error_sku"]) else None,
                "sku_name": row["checks_error_sku_name"] if pd.notna(row["checks_error_sku_name"]) else None,
            })
    return json.dumps(report, indent=2, ensure_ascii=False)


# --- Download Buttons Layout ---
tab_dl1, tab_dl2, tab_dl3, tab_dl4, tab_dl5, tab_dl6 = st.tabs([
    "🌐 HTML Executive",
    "📄 PDF-Report",
    "📗 Excel (Multi-Sheet)",
    "📥 CSV Rohdaten",
    "🔧 JSON (API-Format)",
    "📋 Text-Zusammenfassung",
])

with tab_dl1:
    st.markdown("**Das ist die stärkere Exportform für diesen Report**: dieselbe zusammenhängende Story wie im guten HTML-Look, statt einer künstlich zerlegten PDF-Seitenlogik.")
    st.info("Empfehlung: HTML herunterladen, im Browser öffnen und bei Bedarf dort als PDF drucken. So bleibt das Layout deutlich näher an der starken Executive-Ansicht.")
    try:
        html_report = build_html_report()
        latest_html_path, dated_html_path = persist_html_report(html_report)
        access_links = build_report_access_links(latest_html_path, dated_html_path, REPORT_SHARE_TARGET)
        prepared_message = build_prepared_message()
        report_qc_run_once(latest_html_path, dated_html_path)
        st.download_button(
            label="🌐 HTML-Executive-Report herunterladen",
            data=html_report,
            file_name=f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.html",
            mime="text/html",
            type="primary",
        )
        st.success("Der HTML-Executive-Report ist bereit. Das ist die visuell stärkere Variante und die beste Basis, wenn du daraus manuell ein sauberes PDF drucken willst.")
        st.caption(f"Lokal gespeichert unter: {latest_html_path.resolve().as_posix()}")
        st.markdown("**Report Access**")
        st.text_input("Lokaler Pfad (latest)", value=access_links["local_latest_path"], key="report_access_local_latest")
        st.text_input("Lokaler URI (latest)", value=access_links["local_latest_uri"], key="report_access_local_uri")
        st.text_input("Freigabe-URL", value=access_links["public_latest_url"], key="report_access_public_latest")
        mail_recipient = st.text_input("Mail-Empfänger für Gmail-Shortcut", value="", key="prepared_message_recipient")
        with st.expander("HTML-Vorschau im Tool", expanded=False):
            components.html(html_report, height=900, scrolling=True)
        st.markdown("**Vorbereiteter Begleittext**")
        st.text_input("Betreff-Vorschlag", value=prepared_message["subject"], key="prepared_message_subject")
        st.text_area(
            "Text zum Copy-Paste in die Mail",
            value=prepared_message["body"],
            height=260,
            key="prepared_message_body",
        )
        if mail_recipient.strip():
            gmail_url = build_gmail_compose_url(mail_recipient, prepared_message["subject"], prepared_message["body"])
            st.markdown(f"[Gmail-Entwurf öffnen]({gmail_url})")
        st.download_button(
            label="🧾 Begleittext als TXT herunterladen",
            data=f"Betreff: {prepared_message['subject']}\n\n{prepared_message['body']}",
            file_name=f"weekly_bug_report_begleittext_{datetime.now().strftime('%Y-%m-%d')}.txt",
            mime="text/plain",
        )
    except Exception as exc:
        st.error(f"HTML-Executive-Report konnte nicht erzeugt werden: {exc}")

with tab_dl2:
    st.markdown("**PDF bleibt verfügbar** – aber eher als kompakter Dateianhang, nicht als schönste Darstellungsform dieses Reports.")
    st.info("Wenn dir die Optik wichtig ist, nimm den HTML-Export als Hauptweg und drucke ihn aus dem Browser als PDF.")
    try:
        pdf_report = build_pdf_report()
        persist_unified_artifact(
            f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.pdf",
            pdf_report,
        )
        st.download_button(
            label="📄 PDF-Report herunterladen",
            data=pdf_report,
            file_name=f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.pdf",
            mime="application/pdf",
            type="primary",
        )
    except ImportError:
        st.error("Für den PDF-Report fehlen Pakete. Bitte `pip install -r requirements.txt` ausführen.")
    except Exception as exc:
        st.error(f"PDF-Report konnte nicht erzeugt werden: {exc}")

with tab_dl3:
    st.markdown("""**Excel mit 7 Sheets:**
- Zusammenfassung (KPIs)
- Alle Fehler (Rohdaten mit Autofilter)
- Assembly Lines (Fehlerquoten + Typen)
- Mitarbeiter (Prüfleistung + Fehlertypen)
- Top SKUs (Artikel-Ranking)
- Wöchentlicher Trend (Pivot)
- Tagesdetail
""")
    try:
        excel_data = build_excel_report()
        persist_unified_artifact(
            f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
            excel_data,
        )
        st.download_button(
            label="📗 Excel-Report herunterladen",
            data=excel_data,
            file_name=f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
        )
    except ImportError:
        st.error("Bitte `xlsxwriter` installieren: `pip install xlsxwriter`")

with tab_dl4:
    st.markdown("**CSV-Rohdaten** – alle gefilterten Fehler als Semikolon-separierte Datei.")
    col_csv1, col_csv2 = st.columns(2)
    with col_csv1:
        csv_buf = io.StringIO()
        df_errors_selected.to_csv(csv_buf, index=False, sep=";")
        persist_unified_artifact(
            f"fehler_gefiltert_{datetime.now().strftime('%Y-%m-%d')}.csv",
            csv_buf.getvalue().encode("utf-8"),
        )
        st.download_button(
            label="📥 Fehler-CSV (gefiltert)",
            data=csv_buf.getvalue(),
            file_name=f"fehler_gefiltert_{datetime.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv",
            type="primary",
        )
    with col_csv2:
        csv_all_buf = io.StringIO()
        df_filtered.to_csv(csv_all_buf, index=False, sep=";")
        persist_unified_artifact(
            f"alle_checks_{datetime.now().strftime('%Y-%m-%d')}.csv",
            csv_all_buf.getvalue().encode("utf-8"),
        )
        st.download_button(
            label="📥 Komplett-CSV (alle Checks)",
            data=csv_all_buf.getvalue(),
            file_name=f"alle_checks_{datetime.now().strftime('%Y-%m-%d')}.csv",
            mime="text/csv",
        )

with tab_dl5:
    st.markdown("**JSON-Report** – strukturiert mit KPIs, Fehler nach Typ/Linie/User, Top-SKUs und allen Einzelfehlern. Ideal für Weiterverarbeitung oder API-Integration.")
    json_report = build_json_report()
    persist_unified_artifact(
        f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.json",
        json_report.encode("utf-8"),
    )
    st.download_button(
        label="🔧 JSON-Report herunterladen",
        data=json_report,
        file_name=f"weekly_bug_report_{datetime.now().strftime('%Y-%m-%d')}.json",
        mime="application/json",
        type="primary",
    )
    with st.expander("JSON-Vorschau"):
        st.json(json.loads(json_report))

with tab_dl6:
    summary_lines = [
        f"Assembly QC – Weekly Bug Report",
        f"{'=' * 50}",
        f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        f"Zeitraum: {_date_min.strftime('%d.%m.%Y') if pd.notna(_date_min) else '–'} – {_date_max.strftime('%d.%m.%Y') if pd.notna(_date_max) else '–'}",
        f"Databricks: {DATABRICKS_QUERY_URL}",
        f"",
        f"=== ZUSAMMENFASSUNG ===",
        f"Gesamte Prüfungen:     {total_checks}",
        f"Fehlerhafte Prüfungen: {total_errors}",
        f"Fehlerquote:           {error_rate:.1f}%",
        f"Boxen mit Fehlern:     {unique_boxes_with_errors} von {total_boxes}",
        f"Box-Fehlerquote:       {box_error_rate:.1f}%",
        f"",
        f"=== FEHLER NACH TYP ===",
    ]
    for etype in selected_errors:
        count = len(df_errors_selected[df_errors_selected["checks_error_category"] == etype])
        summary_lines.append(f"  {etype:15s}: {count}")
    summary_lines.append("")
    summary_lines.append("=== FEHLER NACH ASSEMBLY LINE ===")
    for ln in sorted(df_errors_selected["assembly_line"].unique()):
        count = len(df_errors_selected[df_errors_selected["assembly_line"] == ln])
        summary_lines.append(f"  {ln}: {count}")
    summary_lines.append("")
    summary_lines.append("=== FEHLER NACH MITARBEITER ===")
    for uid in sorted(df_errors_selected["user_id"].unique()):
        count = len(df_errors_selected[df_errors_selected["user_id"] == uid])
        summary_lines.append(f"  User {uid}: {count}")
    summary_lines.append("")
    summary_lines.append("=== TOP 10 FEHLERHAFTE SKUs ===")
    if not df_errors_selected.empty:
        top_sku_txt = df_errors_selected.dropna(subset=["checks_error_sku"]).groupby("checks_error_sku").size().sort_values(ascending=False).head(10)
        for sku, cnt in top_sku_txt.items():
            summary_lines.append(f"  {sku}: {cnt}")

    summary_text = "\n".join(summary_lines)
    persist_unified_artifact(
        f"weekly_summary_{datetime.now().strftime('%Y-%m-%d')}.txt",
        summary_text.encode("utf-8"),
    )
    st.text_area("Zusammenfassung", summary_text, height=300)
    st.download_button(
        label="📋 Text-Report herunterladen",
        data=summary_text,
        file_name=f"weekly_summary_{datetime.now().strftime('%Y-%m-%d')}.txt",
        mime="text/plain",
        type="primary",
    )

# ─── Databricks Quick Access (Bottom) ─────────────────────────────────────────
st.markdown("---")
col_db, col_footer = st.columns([1, 2])
with col_db:
    st.markdown(
        f'<a href="{DATABRICKS_QUERY_URL}" target="_blank" style="'
        f'display:inline-block; padding:0.8rem 2rem; '
        f'background:linear-gradient(135deg,#FF3621,#FF6A33); color:white; '
        f'border-radius:10px; text-decoration:none; font-weight:700; font-size:1.1rem; '
        f'box-shadow:0 4px 15px rgba(255,54,33,0.3);'
        f'">🔗 Databricks SQL Query öffnen</a>',
        unsafe_allow_html=True,
    )
with col_footer:
    st.markdown(
        "<div style='text-align:right; color: #888; font-size:0.85rem; padding-top:0.8rem;'>"
        f"{BRAND_NAME} | Hauptversand jetzt als PDF-Anhang | Datenquelle: {data_source_label}"
        "</div>",
        unsafe_allow_html=True,
    )

