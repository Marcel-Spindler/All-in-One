"""Maitre Sync — Forecast vs Inbound + Swap-Vorschläge.

Etappe 1: Lokal, liest beide Sheets über Service-Account, cached lokal.
Etappe 2 (später): zusätzlich Firestore-Writer.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib import maitre_logic as ML
from lib.storage import LocalParquetStorage


st.set_page_config(page_title="Maitre Sync", layout="wide")
st.title("🍱 Maitre Sync — Forecast vs. Inbound")

with st.expander("Hilfe: Was macht Maitre Sync?", expanded=True):
    st.markdown(
        """
        **Zweck**

        Diese Seite vergleicht die neue Forecast-Bibel mit dem Inbound-Sheet und zeigt pro Produktions-KW, ob Soll und Ist zueinander passen.

        **Was passiert hier?**

        1. Forecast wird aus dem Forecast-GSheet gelesen.
        2. Inbound wird aus dem Weekly-Fulfillment-GSheet gelesen.
        3. Die Entlade-KW wird auf die Produktions-KW verschoben.
        4. Rezeptcodes werden normalisiert, damit Forecast und Inbound sauber matchen.
        5. Danach siehst Du Vergleich, Fehlmengen und moegliche Swap-Ideen.

        **Wie benutzt man die Seite richtig?**

        - Zuerst die richtige **Produktions-KW** einstellen.
        - Dann Maerkte und Soll-Spalte kontrollieren.
        - Danach auf **Jetzt aus Sheets ziehen** klicken.
        - Im Tab **Vergleich** pruefen, ob Fehlmengen oder Luecken sichtbar sind.
        - Im Tab **Inbound** kontrollieren, ob die Rohdaten fuer die KW wirklich angekommen sind.

        **Wichtig**

        Wenn keine Daten erscheinen, ist meist entweder die falsche KW eingestellt oder im Inbound-Sheet gibt es fuer diese Produktions-KW noch keine Zeilen.
        """
    )

CACHE_ROOT = ROOT / "cache" / "maitre"
storage = LocalParquetStorage(CACHE_ROOT)


# ---------------------------------------------------------------------------
# Zentrale KW-Auswahl (Anker für ALLES)
# ---------------------------------------------------------------------------
today = date.today()
calendar_year, iso_week_today, _ = today.isocalendar()
production_anchor = today + timedelta(days=7)
production_year, production_week_today, _ = production_anchor.isocalendar()

if "maitre_week" not in st.session_state:
    st.session_state["maitre_week"] = int(production_week_today)

top1, top2, top3, top4 = st.columns([1, 1, 2, 1])
with top1:
    week_num = st.number_input(
        "📅 Kalenderwoche (KW)",
        min_value=1, max_value=53,
        value=int(st.session_state["maitre_week"]),
        step=1,
        help=(
            f"Heute ist Kalender-KW {iso_week_today}/{calendar_year}. "
            f"Für die Produktion wird standardmäßig KW {production_week_today}/{production_year} verwendet. "
            "Alle Tabellen, Filter und Swap-Vorschläge richten sich nach dieser Produktions-KW."
        ),
    )
    st.session_state["maitre_week"] = int(week_num)
with top2:
    if st.button("← Vorwoche", use_container_width=True):
        st.session_state["maitre_week"] = max(1, int(week_num) - 1)
        st.rerun()
    if st.button("Folgewoche →", use_container_width=True):
        st.session_state["maitre_week"] = min(53, int(week_num) + 1)
        st.rerun()
with top3:
    st.markdown(
        f"### Produktions-KW: **W{int(week_num):02d} / {production_year}**"
        + ("  ✅ *aktuelle Produktionswoche*" if int(week_num) == production_week_today else "")
        + f"  
Kalender heute: W{iso_week_today:02d} / {calendar_year}"
    )
with top4:
    if st.button("⏮️ Heute", use_container_width=True):
        st.session_state["maitre_week"] = int(production_week_today)
        st.rerun()

week = f"W{int(week_num):02d}"

st.divider()


# ---------------------------------------------------------------------------
# Sidebar — sekundäre Filter
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("Filter & Optionen")

    markets = st.multiselect(
        "Märkte",
        ["DE", "NORDICS", "BNL"],
        default=["DE", "NORDICS"],
    )

    forecast_qty_col = st.selectbox(
        "Forecast-Mengenspalte (Soll)",
        ["order", "Thurs", "Weds", "Tues", "Mon", "Fri -1wk", "wed-1wk"],
        index=0,
        help="Welche Forecast-Spalte gilt als ‚Soll'?",
    )

    swap_threshold = st.number_input(
        "Swap-Schwelle (Stück)", min_value=0, max_value=2000, value=50, step=10,
        help="Differenzen unter diesem Wert werden ignoriert.",
    )

    st.divider()
    if st.button("🔄 Jetzt aus Sheets ziehen", type="primary", use_container_width=True):
        st.session_state["_pull_now"] = True


# ---------------------------------------------------------------------------
# Data pull
# ---------------------------------------------------------------------------
def _build_forecast_config() -> ML.ForecastConfig:
    tabs = []
    if "DE" in markets or "NORDICS" in markets:
        tabs.append(ML.FORECAST_TABS["DE_NOR"])
    if "BNL" in markets:
        tabs.append(ML.FORECAST_TABS["BNL"])
    return ML.ForecastConfig(tabs=tuple(tabs) or (ML.FORECAST_TABS["DE_NOR"],))


@st.cache_data(ttl=3600, show_spinner="Lade Forecast …")
def cached_forecast(_cfg_key: tuple) -> pd.DataFrame:
    return ML.load_forecast(_build_forecast_config())


@st.cache_data(ttl=3600, show_spinner="Lade Inbound …")
def cached_inbound() -> pd.DataFrame:
    return ML.load_inbound()


pull_now = st.session_state.pop("_pull_now", False)
if pull_now:
    cached_forecast.clear()
    cached_inbound.clear()

cfg_key = (tuple(markets),)

try:
    forecast_raw = cached_forecast(cfg_key)
    inbound_raw = cached_inbound()
except Exception as exc:
    st.error(f"Fehler beim Lesen der Sheets: {exc}")
    st.stop()

if pull_now and not forecast_raw.empty:
    storage.save("forecast", week, forecast_raw)
    storage.save("inbound", week, inbound_raw)
    st.success(f"✅ Forecast & Inbound für {week} aus Sheets geladen und lokal gecached.")

forecast_filtered = ML.filter_forecast(forecast_raw, week=week, markets=markets)
inbound_week = ML.filter_inbound(inbound_raw, week=week)
inbound_agg = ML.aggregate_inbound(inbound_week)
comparison = ML.compare_forecast_vs_inbound(
    forecast_filtered, inbound_agg, forecast_qty_col=forecast_qty_col,
)
swaps = ML.suggest_swaps(comparison, min_shortfall=swap_threshold)


# ---------------------------------------------------------------------------
# KPI bar (alles für die gewählte KW)
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("KW", week)
col2.metric("Forecast Zeilen", len(forecast_filtered))
col3.metric("Inbound POs (KW)", f"{len(inbound_week)} / {len(inbound_raw)}")
shortfall = comparison[comparison["delta"] < -swap_threshold] if not comparison.empty else pd.DataFrame()
surplus = comparison[comparison["delta"] > swap_threshold] if not comparison.empty else pd.DataFrame()
col4.metric("Fehlmengen", len(shortfall))
col5.metric("Überdeckung", len(surplus))

# Verfügbarkeits-Hinweis falls die KW im Inbound noch keine Daten hat
if not inbound_raw.empty and inbound_week.empty:
    avail = sorted(set(inbound_raw["week"].dropna().unique()))
    st.warning(
        f"⚠️ Für **{week}** liegen noch keine Inbound-POs vor. "
        f"Verfügbare KWs im Inbound-Sheet: {', '.join(avail) if avail else '—'}"
    )
else:
    unload_weeks = sorted(set(inbound_week.get("unload_week", pd.Series(dtype=str)).dropna().unique()))
    if unload_weeks:
        st.caption(
            f"Inbound wird fachlich als Produktions-KW **{week}** bewertet "
            f"(basierend auf Entlade-KW {', '.join(unload_weeks)} + 1 Woche)."
        )


# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_compare, tab_swap, tab_forecast, tab_inbound = st.tabs(
    ["📊 Vergleich", "🔁 Swap-Vorschläge", "📋 Forecast (Soll)", "📦 Inbound (Ist)"]
)

with tab_compare:
    st.subheader(f"Vergleich Forecast (`{forecast_qty_col}`) vs. Inbound — {week}")
    if comparison.empty:
        st.info("Keine Vergleichsdaten — prüfe Wochenfilter.")
    else:
        # color hints
        def _row_style(row):
            if row["delta"] <= -swap_threshold:
                return ["background-color: #ffcccc"] * len(row)
            if row["delta"] >= swap_threshold:
                return ["background-color: #ccffcc"] * len(row)
            return [""] * len(row)
        show = comparison[
            ["Market", "week", "join_code", "recipe_codes", "sku_name", "slots",
             "forecast_qty", "received", "delta", "fulfillment_pct"]
        ]
        st.dataframe(
            show.style.apply(_row_style, axis=1).format(
                {"forecast_qty": "{:,.0f}", "received": "{:,.0f}",
                 "delta": "{:+,.0f}", "fulfillment_pct": "{:.1f}%"}
            ),
            use_container_width=True, height=550,
        )
        st.download_button(
            "⬇️ Vergleich als CSV",
            comparison.to_csv(index=False).encode("utf-8"),
            file_name=f"forecast_vs_inbound_{week}.csv",
            mime="text/csv",
        )

with tab_swap:
    st.subheader(f"Swap-Vorschläge — Schwelle ≥ {swap_threshold} PCS")
    if swaps.empty:
        st.success("Keine Fehlmengen außerhalb der Toleranz, oder keine Donor-Slots vorhanden.")
    else:
        st.dataframe(
            swaps.style.format({
                "short_qty": "{:,.0f}", "donor_surplus": "{:,.0f}", "swap_qty": "{:,.0f}",
            }),
            use_container_width=True, height=500,
        )
        st.download_button(
            "⬇️ Swap-Liste als CSV",
            swaps.to_csv(index=False).encode("utf-8"),
            file_name=f"swap_suggestions_{week}.csv",
            mime="text/csv",
        )

with tab_forecast:
    st.subheader("Forecast (gefiltert)")
    st.dataframe(forecast_filtered, use_container_width=True, height=500)

with tab_inbound:
    st.subheader(f"Inbound — aggregiert pro Recipe Code (KW {week})")
    st.dataframe(
        inbound_agg.style.format({"expected": "{:,.0f}", "received": "{:,.0f}"}),
        use_container_width=True, height=400,
    )
    with st.expander(f"Rohdaten Inbound — nur {week} ({len(inbound_week)} Zeilen)"):
        st.dataframe(inbound_week, use_container_width=True, height=400)
    with st.expander(f"Rohdaten Inbound — ALLE Wochen ({len(inbound_raw)} Zeilen)"):
        st.dataframe(inbound_raw, use_container_width=True, height=400)


# ---------------------------------------------------------------------------
# Footer / cache info
# ---------------------------------------------------------------------------
with st.expander("📁 Lokaler Cache"):
    st.caption(f"Cache-Verzeichnis: `{CACHE_ROOT}`")
    for ds in ["forecast", "inbound"]:
        weeks_avail = storage.list_weeks(ds)
        st.write(f"**{ds}**: {', '.join(weeks_avail) if weeks_avail else '— noch leer —'}")
