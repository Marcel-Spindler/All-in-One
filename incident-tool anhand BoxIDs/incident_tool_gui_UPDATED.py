import os
import io
import json
import glob
import re
from datetime import datetime, date, time
from typing import List, Optional, Tuple, Dict

import pandas as pd
from dateutil import tz

import ttkbootstrap as tb
from ttkbootstrap.constants import *
from tkinter import filedialog, messagebox
from tkinter.scrolledtext import ScrolledText  # ScrolledText ohne autohide

# =========================
# Konfiguration
# =========================
APP_TITLE = "📦 Incident Box Finder – Box-ID Startmodus + Nordics + FE/Maitre-Mapping (Edit + Freitext + Auto-Pickliste)"
VALID_PREFIXES = ["TZ", "TK", "TV"]
BERLIN = tz.gettz("Europe/Berlin")

# CSV-Spalten (Datenextrakt)
COL_TS = "inducted_ts"   # Datum+Uhrzeit
COL_BOX = "boxid"        # Box-ID
COL_REC = "recipes"      # Rezeptkette ("301-301-304-305")

# Candidate-Namen im Custom-ID-CSV/XLSX
BOX_COL_CAND = [r"^box[_\- ]?id$", r"^boxid$", r"^box$", r"^shipment[_\- ]?id$"]
CUST_COL_CAND = [
    r"^customer[_\- ]?id$", r"^customerid$", r"^customerId$",
    r"^kunde[n]?(nr)?$", r"^kunden?[_\- ]?id$", r"^kundennummer$",
    r"^customer$", r"^recipient[_\- ]?id$"
]

# Basisordner
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Settings-Datei (letzte Pfade)
SETTINGS_PATH = os.path.join(DATA_DIR, "settings.json")

# Auto-Export-Zielordner (anpassbar)
EXPORT_TARGET_DIR = r"G:\.shortcut-targets-by-id\18zdNMeIZjDT-p4l-0voTTT0dt9xaJ8Z5\Factor Incident Reports"

# Picklisten-Verzeichnis für Auto-Mapping (Excel-Dateien) – anpassen falls nötig
PICKLIST_DIR = r"G:\.shortcut-targets-by-id\14d5DvmVBQrhqRwDsYxxxxxxx\Weight Calculator Factor\Picklisten"

TIME_ = "HH:MM"

# =========================
# Core-Logik
# =========================
def parse_datetime(s: str) -> Optional[datetime]:
    if pd.isna(s):
        return None
    s = str(s).strip()
    fmts = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M",
        "%Y/%m/%d %H:%M:%S", "%Y/%m/%d %H:%M",
        "%d.%m.%y %H:%M", "%d.%m.%y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
    ]
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=BERLIN)
        except ValueError:
            continue
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.isna(dt):
            return None
        return dt.to_pydatetime().replace(tzinfo=BERLIN)
    except Exception:
        return None

def is_valid_hhmm(hhmm: str) -> bool:
    if not hhmm or hhmm.strip() == "" or hhmm.strip().upper() == TIME_:
        return False
    m = re.fullmatch(r"(\d{2}):(\d{2})", hhmm.strip())
    if not m:
        return False
    hh, mm = int(m.group(1)), int(m.group(2))
    return 0 <= hh <= 23 and 0 <= mm <= 59

def normalize_incident_dt(base_date: date, hhmm: str) -> Optional[datetime]:
    if not is_valid_hhmm(hhmm):
        return None
    hh, mm = hhmm.strip().split(":")
    t = time(hour=int(hh), minute=int(mm))
    return datetime(base_date.year, base_date.month, base_date.day, t.hour, t.minute, 0, tzinfo=BERLIN)

def has_recipe(recipes_str: str, target_recipe: str) -> bool:
    if pd.isna(recipes_str) or not str(target_recipe).strip():
        return False
    tokens = [tok.strip() for tok in str(recipes_str).split("-") if tok.strip()]
    return any(tok == str(target_recipe).strip() for tok in tokens)

def count_recipe(recipes_str: str, target_recipe: str) -> int:
    if pd.isna(recipes_str) or not str(target_recipe).strip():
        return 0
    tokens = [tok.strip() for tok in str(recipes_str).split("-") if tok.strip()]
    trg = str(target_recipe).strip()
    return sum(1 for t in tokens if t == trg)

def filter_prefix(boxid: str, allowed_prefixes: List[str]) -> bool:
    if pd.isna(boxid):
        return False
    s = str(boxid).strip().upper()
    return any(s.startswith(p) for p in allowed_prefixes)

def compute_time_windows(incidents_sorted: List[Dict]) -> List[Tuple[datetime, datetime, Dict]]:
    windows = []
    for i, inc in enumerate(incidents_sorted):
        start = inc["start_dt"]
        if i < len(incidents_sorted) - 1:
            end = incidents_sorted[i + 1]["start_dt"]
        else:
            day_end = datetime(start.year, start.month, start.day, 23, 59, 59, tzinfo=BERLIN)
            end = day_end
        windows.append((start, end, inc))
    return windows

def sanitize_sheet_name(name: str) -> str:
    bad = set('[]:*?/\\')
    s = "".join(ch for ch in name if ch not in bad)
    if len(s) > 31:
        s = s[:31]
    return s or "Sheet"

def ts_now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# =========================
# Persistenz
# =========================
def incidents_path_for(d: date) -> str:
    return os.path.join(DATA_DIR, f"incidents_{d.strftime('%Y-%m-%d')}.json")

def mapping_path_for(d: date) -> str:
    return os.path.join(DATA_DIR, f"mapping_{d.strftime('%Y-%m-%d')}.json")

def save_incidents(d: date, incidents: List[Dict]) -> None:
    payload = {"date": d.strftime("%Y-%m-%d"), "incidents": incidents, "saved_at": ts_now()}
    with open(incidents_path_for(d), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def load_incidents(d: date) -> Optional[Dict]:
    p = incidents_path_for(d)
    if not os.path.isfile(p):
        return None
    with open(p, "r", encoding="utf-8") as f:
        return json.load(f)

def save_mapping(d: date, mapping: Dict[str, str]) -> None:
    payload = {"date": d.strftime("%Y-%m-%d"), "mapping": mapping, "saved_at": ts_now()}
    with open(mapping_path_for(d), "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

def load_mapping(d: date) -> Dict[str, str]:
    p = mapping_path_for(d)
    if not os.path.isfile(p):
        return {}
    with open(p, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data.get("mapping", {})

def delete_mapping(d: date) -> None:
    p = mapping_path_for(d)
    if os.path.isfile(p):
        os.remove(p)

def delete_all_data() -> int:
    count = 0
    for p in glob.glob(os.path.join(DATA_DIR, "*.json")):
        try:
            os.remove(p)
            count += 1
        except Exception:
            pass
    return count

# ---------- Settings (letzte Pfade) ----------
def load_settings() -> Dict[str, str]:
    if os.path.isfile(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_settings(settings: Dict[str, str]) -> None:
    try:
        with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# =========================
# CSV sicher laden
# =========================
def read_csv_safely(path: str) -> Optional[pd.DataFrame]:
    try:
        df = pd.read_csv(path, dtype=str)
        missing = [c for c in [COL_TS, COL_BOX, COL_REC] if c not in df.columns]
        if missing:
            raise ValueError(f"CSV fehlt Spalte(n): {missing} – erwartet: {COL_TS}, {COL_BOX}, {COL_REC}")
        df[COL_BOX] = df[COL_BOX].astype(str).str.strip()
        df["__box_upper"] = df[COL_BOX].str.upper()
        df["__dt"] = df[COL_TS].apply(parse_datetime)
        return df
    except Exception as e:
        messagebox.showerror("CSV-Fehler", f"{e}")
        return None

def _find_col_by_patterns(cols: List[str], patterns: List[str]) -> Optional[str]:
    for c in cols:
        name = str(c).strip()
        for pat in patterns:
            if re.match(pat, name, re.IGNORECASE):
                return c
    return None

def read_custom_id_csv(path: str) -> Optional[pd.DataFrame]:
    """
    Erwartet CSV (oder XLSX) mit:
      - boxid (oder Varianten)
      - customer_id (oder Varianten)
    """
    try:
        if path.lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(path, dtype=str)
        else:
            df = pd.read_csv(path, dtype=str)

        if df.empty:
            raise ValueError("Custom-ID-Datei ist leer.")
        cols = list(df.columns)
        box_col = _find_col_by_patterns(cols, BOX_COL_CAND)
        cust_col = _find_col_by_patterns(cols, CUST_COL_CAND)
        if not box_col or not cust_col:
            raise ValueError(
                f"Custom-ID-Datei: Spalten nicht gefunden.\n"
                f"Gefundene Spalten: {cols}\n"
                f"Erwartet: boxid/box_id & customer_id/kundennummer"
            )
        out = pd.DataFrame({
            "boxid": df[box_col].astype(str).str.strip(),
            "customer_id": df[cust_col].astype(str).str.strip()
        })
        out["__box_upper"] = out["boxid"].str.upper()
        out = out.dropna(subset=["__box_upper"]).drop_duplicates(subset=["__box_upper"], keep="last")
        return out
    except Exception as e:
        messagebox.showerror("Custom-ID", f"{e}")
        return None

# =========================
# Auto-Mapping via Picklisten (Excel)
# =========================

# =========================
# Verbesserte Picklisten-Erkennung (wöchentlicher Name) + robustes Mapping
# =========================

WEEKLY_PREFIXES = ["Marcel_picklist_", "Marcel_picklist-", "Marcel_picklist_neu-"]
WEEKLY_PATTERNS = [
    re.compile(r"^Marcel_picklist[-_](?P<year>\d{4})-W(?P<week>\d{2})\.xlsx$", re.IGNORECASE),
    re.compile(r"^Marcel_picklist_neu-(?P<year>\d{4})-W(?P<week>\d{2})\.xlsx$", re.IGNORECASE),
]

def _parse_recipe_id(s: str) -> str:
    """Extrahiert die ersten 3 Ziffern aus einem Rezeptfeld wie '301r-1p' → '301'."""
    if pd.isna(s):
        return ""
    m = re.search(r"(\d{3})", str(s))
    return m.group(1) if m else ""

def _find_latest_weekly_by_name(directory: str) -> Optional[str]:
    """Sucht nach Dateien 'Marcel_picklist-YYYY-Www.xlsx' ODER 'Marcel_picklist_neu-YYYY-Www.xlsx'.
    Nimmt die neueste nach (year, week). Fallback: neueste nach mtime für Dateien mit den Prefixen.
    """
    try:
        found = []
        for p in glob.glob(os.path.join(directory, '*.xlsx')):
            name = os.path.basename(p)
            for rx in WEEKLY_PATTERNS:
                m = rx.match(name)
                if m:
                    y, w = int(m.group('year')), int(m.group('week'))
                    found.append(((y, w), p))
                    break
        if found:
            found.sort(key=lambda t: t[0], reverse=True)
            return found[0][1]
        # Fallback: mtime mit Prefixen
        pref_globs = []
        for pref in WEEKLY_PREFIXES:
            pref_globs.extend(glob.glob(os.path.join(directory, f'{pref}*.xlsx')))
        mtime_sorted = sorted(pref_globs, key=lambda p: os.path.getmtime(p), reverse=True)
        return mtime_sorted[0] if mtime_sorted else None
    except Exception:
        return None


def build_mapping_from_picklist(xlsx_path: str) -> Dict[str, str]:
    """
    Erzeugt Mapping {RezeptID3 -> FE/Maitre-Code} aus einer Pickliste.
    Priorität Spalten:
      1) 'FE Nummer' (oder Header, der mit 'fe' beginnt)
      2) 'Maitre'
    Rezeptspalte: 'Recipe' (oder Header, der mit 'rec'/'rez' beginnt); Fallback: Spalte 0.
    Letztes Vorkommen gewinnt.
    """
    # Erst Versuch mit Header
    try:
        df = pd.read_excel(xlsx_path, dtype=str, header=0)
        cols = [str(c).strip() for c in df.columns]
        # Rezept-Spalte finden
        recipe_idx = None
        for i,c in enumerate(cols):
            cl = c.lower()
            if cl.startswith("recipe") or cl.startswith("rez") or "rezept" in cl:
                recipe_idx = i; break
        # FE/Maitre Spalten finden
        fe_idx = None; maitre_idx = None
        for i,c in enumerate(cols):
            cl = c.lower().strip()
            if fe_idx is None and (cl == "fe nummer" or cl.startswith("fe")):
                fe_idx = i
            if maitre_idx is None and cl.startswith("maitre"):
                maitre_idx = i
        if recipe_idx is None:
            # Fallback: erste Spalte
            recipe_idx = 0
        # Falls FE und Maitre fehlen, später auf header=None fallbacken
        if fe_idx is not None or maitre_idx is not None:
            rec_series = df.iloc[:, recipe_idx].astype(str).fillna("")
            fe_series  = df.iloc[:, fe_idx].astype(str).fillna("") if fe_idx is not None else None
            ma_series  = df.iloc[:, maitre_idx].astype(str).fillna("") if maitre_idx is not None else None
            mapping: Dict[str,str] = {}
            for r, fe_val, ma_val in zip(rec_series, fe_series if fe_series is not None else [None]*len(rec_series),
                                         ma_series if ma_series is not None else [None]*len(rec_series)):
                rid = _parse_recipe_id(r)
                if not rid: 
                    continue
                val = (fe_val or "").strip() if fe_val else ""
                if not val and ma_val:
                    val = str(ma_val).strip()
                if not val or val.lower() == "nan":
                    continue
                mapping[rid] = val  # letztes gewinnt
            if mapping:
                return mapping
    except Exception:
        pass

    # Fallback: header=None, wie bisher (A=Rezept, H=FE/Maitre)
    df = pd.read_excel(xlsx_path, dtype=str, header=None)
    if df.shape[1] < 8:
        raise ValueError("Pickliste hat weniger als 8 Spalten (A..H).")
    recipes_raw = df.iloc[:, 0].astype(str).fillna("")
    fe_col      = df.iloc[:, 7].astype(str).fillna("")
    mapping: Dict[str,str] = {}
    for raw, fe in zip(recipes_raw, fe_col):
        rid = _parse_recipe_id(raw)
        if not rid:
            continue
        fe = str(fe).strip()
        if not fe or fe.lower()=="nan":
            continue
        mapping[rid] = fe
    return mapping

def find_best_picklist_with_fe(directory: str, lookback: int = 12, min_fe_rows: int = 5) -> Optional[str]:
    """
    Erweiterung:
      1) Versuche zuerst die neueste Datei mit Prefix 'Marcel_picklist-YYYY-Www.xlsx' zu nehmen.
         - Nur wenn sie mindestens 'min_fe_rows' valide FE-Einträge hat.
      2) Fallback: wie bisher – prüfe die letzten Dateien (glob *.xlsx) und nimm die mit meisten FE-Zeilen.
    """
    # Schritt 1: Wöchentliche nach Namen
    weekly = _find_latest_weekly_by_name(directory)
    if weekly:
        try:
            if _count_fe_rows_in_xlsx(weekly) >= min_fe_rows:
                return weekly
        except Exception:
            pass
    # Schritt 2: Bisherige Heuristik
    try:
        candidates = sorted(glob.glob(os.path.join(directory, "*.xlsx")), key=os.path.getmtime, reverse=True)
        candidates = candidates[:lookback]
        scored = []
        for p in candidates:
            score = _count_fe_rows_in_xlsx(p)
            scored.append((score, p))
        scored.sort(reverse=True)
        if not scored or scored[0][0] < min_fe_rows:
            return None
        return scored[0][1]
    except Exception:
        return None

def _count_fe_rows_in_xlsx(path: str) -> int:
    """Zählt valide FE/Maitre-Einträge in Spalte H (Index 7)."""
    try:
        df = pd.read_excel(path, dtype=str, header=None)
        if df.shape[1] < 8:
            return 0
        fe_col = df.iloc[:, 7].astype(str).fillna("")
        return int((fe_col.str.strip().str.len() >= 2).sum())
    except Exception:
        return 0





class IncidentToolApp:
    def __init__(self, app: tb.Window):
        self.app = app
        self.app.title(APP_TITLE)
        self.app.geometry("1340x980")
        self.app.minsize(1120, 780)
        try:
            self.app.style.theme_use('flatly')
        except Exception:
            pass

        # State
        self.incidents: List[Dict] = []
        self.log_df: Optional[pd.DataFrame] = None
        self.details_df: Optional[pd.DataFrame] = None
        self.per_incident_details: Dict[str, pd.DataFrame] = {}
        self.autosave = tb.BooleanVar(value=True)
        self.mapping_cache: Dict[str, str] = {}

        # Pfade & Settings
        self.settings = load_settings()
        self.csv_path = tb.StringVar(value=self.settings.get("csv_path",""))
        self.custom_csv_path = tb.StringVar(value=self.settings.get("custom_csv_path",""))
        self.date_str = tb.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        self.map_date_str = tb.StringVar(value=datetime.now().strftime("%Y-%m-%d"))

        # Statusbar/Progress
        self.status_var = tb.StringVar(value="Bereit.")
        self.progress_var = tb.IntVar(value=0)

        # Header
        header = tb.Frame(self.app, padding=(10, 8))
        header.pack(fill=X)
        tb.Label(header, text="📦 Incident Box Finder", bootstyle=PRIMARY, font=("Segoe UI", 18, "bold")).pack(side=LEFT)
        tb.Button(header, text="Gesamthilfe", bootstyle=INFO, command=self.help_all).pack(side=RIGHT, padx=6)

        # Tabs
        self.tabs = tb.Notebook(self.app, bootstyle="primary")
        self.tabs.pack(fill=BOTH, expand=YES, padx=10, pady=10)

        self.tab_setup = tb.Frame(self.tabs)
        self.tab_mapping = tb.Frame(self.tabs)
        self.tab_results = tb.Frame(self.tabs)
        self.tabs.add(self.tab_setup,  text="Vorfälle & Einstellungen")
        self.tabs.add(self.tab_mapping, text="FE-/Maitre-Mapping")
        self.tabs.add(self.tab_results, text="Ergebnisse")

        self.build_setup_tab()
        self.build_mapping_tab()
        self.build_results_tab()

        # Beim Start: falls noch kein Mapping für heute vorhanden, beste Pickliste testen
        try:
            today = datetime.now().date()
            if not load_mapping(today):
                self.load_mapping_from_latest_picklist(silent=True)  # nicht stören, nur wenn vorhanden
        except Exception:
            pass

        self.auto_load_today()
        self.set_status("Bereit.")

        # Footer Statusbar
        footer = tb.Frame(self.app, padding=(8, 6))
        footer.pack(fill=X, side=BOTTOM)
        self.prog = tb.Progressbar(footer, orient=HORIZONTAL, variable=self.progress_var, maximum=100, bootstyle="info-striped")
        self.prog.pack(fill=X, side=LEFT, expand=YES, padx=(0,8))
        self.status_lbl = tb.Label(footer, textvariable=self.status_var, width=52, anchor="w")
        self.status_lbl.pack(side=RIGHT)

    # ---------- Setup Tab ----------
    def build_setup_tab(self):
        card = tb.Labelframe(self.tab_setup, text="Schichtdaten & Speicher", padding=10, bootstyle=INFO)
        card.pack(fill=X, padx=6, pady=6)

        # CSV-Zeile (Datenextrakt)
        row1 = tb.Frame(card); row1.pack(fill=X, pady=4)
        tb.Label(row1, text="Datenextrakt CSV:", width=22).pack(side=LEFT)
        tb.Entry(row1, textvariable=self.csv_path).pack(side=LEFT, fill=X, expand=YES, padx=6)
        tb.Button(row1, text="Durchsuchen…", bootstyle=SECONDARY, command=self.browse_csv).pack(side=LEFT)
        tb.Button(row1, text="Hilfe", bootstyle=INFO, command=self.help_extrakt).pack(side=LEFT, padx=(6,0))

        # Custom-ID-Datei
        row1b = tb.Frame(card); row1b.pack(fill=X, pady=4)
        tb.Label(row1b, text="Custom ID Datei:", width=22).pack(side=LEFT)
        tb.Entry(row1b, textvariable=self.custom_csv_path).pack(side=LEFT, fill=X, expand=YES, padx=6)
        tb.Button(row1b, text="Durchsuchen…", bootstyle=SECONDARY, command=self.browse_custom_csv).pack(side=LEFT)
        tb.Button(row1b, text="Hilfe", bootstyle=INFO, command=self.help_custom).pack(side=LEFT, padx=(6,0))

        # Datum + Save/Load + Reset
        row2 = tb.Frame(card); row2.pack(fill=X, pady=4)
        tb.Label(row2, text="Produktionsdatum:", width=22).pack(side=LEFT)
        tb.Entry(row2, textvariable=self.date_str, width=16).pack(side=LEFT)
        tb.Button(row2, text="Hilfe", bootstyle=INFO, command=self.help_datum).pack(side=LEFT, padx=(6,12))
        tb.Label(row2, text="Auto-Save:", padding=(12, 0)).pack(side=LEFT)
        tb.Checkbutton(row2, variable=self.autosave, bootstyle=SUCCESS).pack(side=LEFT, padx=(4, 8))
        tb.Button(row2, text="💾 Vorfälle speichern", bootstyle=SUCCESS, command=self.do_save).pack(side=LEFT, padx=6)
        tb.Button(row2, text="📂 Vorfälle laden",    bootstyle=WARNING, command=self.do_load).pack(side=LEFT, padx=4)
        tb.Button(row2, text="🧨 Alles bereinigen",  bootstyle=DANGER,  command=self.do_wipe_all).pack(side=LEFT, padx=8)
        tb.Button(row2, text="Hilfe", bootstyle=INFO, command=self.help_save_load).pack(side=LEFT, padx=(6,0))

        # Vorfälle sammeln
        vcard = tb.Labelframe(self.tab_setup, text="Vorfälle sammeln (während der Schicht)", padding=10, bootstyle=PRIMARY)
        vcard.pack(fill=X, padx=6, pady=6)

        # Startmodus
        self.start_mode = tb.StringVar(value="BOX")  # BOX (Standard) oder TIME (Notfall)
        mode_row = tb.Frame(vcard); mode_row.pack(fill=X, pady=(0,6))
        tb.Label(mode_row, text="Startmodus:", width=20).pack(side=LEFT)
        tb.Radiobutton(mode_row, text="Erste Box-ID",   variable=self.start_mode, value="BOX").pack(side=LEFT, padx=(0,10))
        tb.Radiobutton(mode_row, text="Uhrzeit (Notfall)", variable=self.start_mode, value="TIME").pack(side=LEFT)
        tb.Button(mode_row, text="Hilfe", bootstyle=INFO, command=self.help_startmodus).pack(side=LEFT, padx=(12,0))

        self.aff = tb.StringVar()
        self.sub = tb.StringVar(value="ERSATZLOS")  # Freitext möglich!
        self.hhmm = tb.StringVar(value=TIME_)
        self.start_boxid = tb.StringVar(value="")

        # Präfixe: alle AUS (manuell wählen, Mehrfachauswahl bestätigen)
        self.chk_nordics = tb.BooleanVar(value=False)
        self.chk_tz = tb.BooleanVar(value=False)
        self.chk_tk = tb.BooleanVar(value=False)
        self.chk_tv = tb.BooleanVar(value=False)

        rowa = tb.Frame(vcard); rowa.pack(fill=X, pady=4)
        tb.Label(rowa, text="Betroffenes Rezept:", width=20).pack(side=LEFT)
        tb.Entry(rowa, textvariable=self.aff, width=12).pack(side=LEFT, padx=(0, 10))
        tb.Label(rowa, text="Substitut (Freitext o. ERSATZLOS):", width=28).pack(side=LEFT)
        tb.Entry(rowa, textvariable=self.sub, width=24).pack(side=LEFT, padx=(0, 10))
        tb.Button(rowa, text="Hilfe", bootstyle=INFO, command=self.help_rezept_sub).pack(side=LEFT, padx=(8,0))

        rowa2 = tb.Frame(vcard); rowa2.pack(fill=X, pady=4)
        tb.Label(rowa2, text="Erste Box-ID (Start):", width=20).pack(side=LEFT)
        tb.Entry(rowa2, textvariable=self.start_boxid, width=18).pack(side=LEFT, padx=(0, 10))
        tb.Label(rowa2, text=f"Startzeit ({TIME_}):", width=18).pack(side=LEFT)
        hhmm_entry = tb.Entry(rowa2, textvariable=self.hhmm, width=10); hhmm_entry.pack(side=LEFT)
        tb.Button(rowa2, text="Hilfe", bootstyle=INFO, command=self.help_startfelder).pack(side=LEFT, padx=(8,0))
        def on_focus_in(e):
            if self.hhmm.get().strip().upper() == TIME_:
                self.hhmm.set("")
        def on_focus_out(e):
            if not self.hhmm.get().strip():
                self.hhmm.set(TIME_)
        hhmm_entry.bind("<FocusIn>", on_focus_in)
        hhmm_entry.bind("<FocusOut>", on_focus_out)

        # Präfixe – Layout (TK/TV abseits)
        rowb = tb.Frame(vcard); rowb.pack(fill=X, pady=4)
        tb.Label(rowb, text="Kundenpräfixe:", width=20).pack(side=LEFT)

        grp_left = tb.Frame(rowb); grp_left.pack(side=LEFT)
        tb.Checkbutton(grp_left, text="Nordics (TK+TV)", variable=self.chk_nordics, bootstyle=INFO).pack(side=LEFT, padx=(0,8))
        tb.Checkbutton(grp_left, text="TZ", variable=self.chk_tz, bootstyle=INFO).pack(side=LEFT)

        spacer = tb.Frame(rowb, width=90); spacer.pack(side=LEFT)  # größerer Abstand

        grp_right = tb.Frame(rowb); grp_right.pack(side=LEFT)
        tb.Checkbutton(grp_right, text="TK", variable=self.chk_tk, bootstyle=INFO).pack(side=LEFT)
        tb.Checkbutton(grp_right, text="TV", variable=self.chk_tv, bootstyle=INFO).pack(side=LEFT, padx=(8,0))

        tb.Button(rowb, text="Hilfe", bootstyle=INFO, command=self.help_prefixe).pack(side=LEFT, padx=(16,0))
        tb.Button(rowb, text="➕ Vorfall hinzufügen", bootstyle=PRIMARY, command=self.add_incident).pack(side=RIGHT)

        cols = ("#", "start_mode", "start_boxid", "affected_recipe", "substitute_recipe", "start_time", "prefixes")
        self.inc_table = tb.Treeview(vcard, columns=cols, show="headings", height=8, bootstyle=INFO)
        for c, w in zip(cols, (50, 110, 160, 140, 220, 110, 280)):
            self.inc_table.heading(c, text=c)
            self.inc_table.column(c, width=w, anchor="w")
        self.inc_table.pack(fill=X, padx=6, pady=(6,2))

        # Edit-Funktionen
        rowc = tb.Frame(vcard); rowc.pack(fill=X, pady=6)
        tb.Button(rowc, text="⬅️ In Formular laden", bootstyle=WARNING, command=self.load_selected_to_form).pack(side=LEFT, padx=4)
        tb.Button(rowc, text="💾 Änderungen speichern", bootstyle=SUCCESS, command=self.update_selected).pack(side=LEFT, padx=4)
        tb.Button(rowc, text="🗑️ Ausgewählte entfernen", bootstyle=DANGER,    command=self.del_selected).pack(side=LEFT, padx=12)
        tb.Button(rowc, text="🧹 Alle löschen",          bootstyle=SECONDARY, command=self.clear_all).pack(side=LEFT, padx=4)
        tb.Button(rowc, text="Hilfe", bootstyle=INFO, command=self.help_vorfall_tabelle).pack(side=LEFT, padx=8)
        tb.Label(rowc, text="Fenster gelten pro Rezept bis zum nächsten Vorfall desselben Rezepts.", bootstyle=SECONDARY).pack(side=RIGHT)

        acard = tb.Labelframe(self.tab_setup, text="Aktionen", padding=10, bootstyle=SECONDARY)
        acard.pack(fill=X, padx=6, pady=6)
        tb.Button(acard, text="🔎 Nur testen (ohne CSV)", bootstyle=INFO,    command=self.dryrun).pack(side=LEFT, padx=6, pady=2)
        tb.Button(acard, text="🏁 Schichtabschluss berechnen", bootstyle=PRIMARY, command=self.run_compute).pack(side=LEFT, padx=8, pady=2)
        tb.Button(acard, text="Hilfe", bootstyle=INFO, command=self.help_berechnen).pack(side=LEFT, padx=8, pady=2)

    # ---------- Mapping Tab ----------
    def build_mapping_tab(self):
        mcard = tb.Labelframe(self.tab_mapping, text="FE-/Maitre-Mapping (Woche)", padding=10, bootstyle=SUCCESS)
        mcard.pack(fill=BOTH, expand=YES, padx=6, pady=6)

        row1 = tb.Frame(mcard); row1.pack(fill=X, pady=4)
        tb.Label(row1, text="Produktionsdatum (für Mapping-Datei lokal):", width=34).pack(side=LEFT)
        self.map_date_str = tb.StringVar(value=datetime.now().strftime("%Y-%m-%d"))
        tb.Entry(row1, textvariable=self.map_date_str, width=16).pack(side=LEFT)
        tb.Button(row1, text="📂 Mapping laden",    bootstyle=WARNING, command=self.do_load_mapping).pack(side=LEFT, padx=6)
        tb.Button(row1, text="💾 Mapping speichern", bootstyle=SUCCESS, command=self.do_save_mapping).pack(side=LEFT)
        tb.Button(row1, text="🗑️ Mapping löschen (Datum)", bootstyle=DANGER, command=self.do_delete_mapping).pack(side=LEFT, padx=8)
        tb.Button(row1, text="Hilfe", bootstyle=INFO, command=self.help_mapping).pack(side=LEFT, padx=8)

        # Quick-Loader für Picklisten
        rowPick = tb.Frame(mcard); rowPick.pack(fill=X, pady=(2,6))
        tb.Button(rowPick, text="📥 Pickliste (beste) scannen", bootstyle=PRIMARY,
                  command=lambda: self.load_mapping_from_latest_picklist(silent=False)).pack(side=LEFT)
        tb.Button(rowPick, text="📄 Pickliste manuell wählen…", bootstyle=SECONDARY,
                  command=self.load_mapping_from_manual_picklist).pack(side=LEFT, padx=8)
        tb.Button(rowPick, text="Hilfe", bootstyle=INFO,
                  command=lambda: messagebox.showinfo(
                      "Hilfe – Pickliste",
                      "Auto-Mapping aus Factor-Picklisten (Excel):\n"
                      "• Es wird die beste (meiste FE-Zeilen in Spalte H) der letzten Dateien gewählt.\n"
                      "• Spalte A enthält Rezeptwerte wie '301r-1p' – daraus werden die ersten 3 Ziffern extrahiert.\n"
                      "• Spalte H enthält den Maitre-/FE-Code.\n"
                      "• Letzte Vorkommen überschreiben frühere.\n"
                      "• Das Ergebnis wird ins Mapping-Feld eingefügt und lokal gespeichert."
                  )).pack(side=LEFT, padx=8)

        hint = ("Copy & Paste aus GSheet (z. B. Bereich B11:C28).\n"
                "Format je Zeile: '301,565123'  (Rezept-ID, Maitre-Code)\n"
                "Tab als Trenner möglich. Ohne Code → Rezept-ID als Fallback.")
        tb.Label(mcard, text=hint, bootstyle=INFO).pack(anchor="w", padx=4, pady=(6,2))

        self.map_text = ScrolledText(mcard, height=16, wrap="none")
        self.map_text.pack(fill=BOTH, expand=YES, padx=4, pady=6)

        row2 = tb.Frame(mcard); row2.pack(fill=X, pady=4)
        tb.Button(row2, text="📄 Vorlage 301–318 anlegen", bootstyle=SECONDARY, command=self.fill_template_301_318).pack(side=LEFT)

    # ---------- Results Tab ----------
    def build_results_tab(self):
        nb = tb.Notebook(self.tab_results, bootstyle="primary")
        nb.pack(fill=BOTH, expand=YES, padx=4, pady=6)

        self.tab_log = tb.Frame(nb); nb.add(self.tab_log, text="Incidents Log")
        self.tab_details = tb.Frame(nb); nb.add(self.tab_details, text="Details gesamt")

        log_cols = ("incident_id","affected_recipe","substitute_recipe","start_mode","start_anchor","start_time","end_time","count_boxes")
        self.log_table = tb.Treeview(self.tab_log, columns=log_cols, show="headings", height=12, bootstyle=SUCCESS)
        for c, w in zip(log_cols, (110,130,220,110,180,160,160,110)):
            self.log_table.heading(c, text=c); self.log_table.column(c, width=w, anchor="w")
        self.log_table.pack(fill=BOTH, expand=YES, padx=6, pady=6)
        tb.Button(self.tab_log, text="Hilfe", bootstyle=INFO, command=self.help_log).pack(anchor="e", padx=10, pady=(0,6))

        det_cols = ("incident_id","affected_recipe","substitute_recipe","boxid","customer_id","subs_for_this_box","recipes","inducted_ts")
        self.det_table = tb.Treeview(self.tab_details, columns=det_cols, show="headings", height=18, bootstyle=INFO)
        for c, w in zip(det_cols, (110,130,220,200,180,160,320,180)):
            self.det_table.heading(c, text=c)
            self.det_table.column(c, width=w, anchor="w")
        self.det_table.pack(fill=BOTH, expand=YES, padx=6, pady=6)
        tb.Button(self.tab_details, text="Hilfe", bootstyle=INFO, command=self.help_details).pack(anchor="e", padx=10, pady=(0,6))

        footer = tb.Labelframe(self.tab_results, text="Export", padding=10, bootstyle=SECONDARY)
        footer.pack(fill=X, padx=6, pady=6)

        self.export_btn_auto = tb.Button(footer, text="⬇️ Auto-Export → G:\\…\\Factor Incident Reports",
                                         bootstyle=SUCCESS, command=self.export_excel_auto, state=DISABLED)
        self.export_btn_auto.pack(side=LEFT, padx=(0,8))
        self.export_btn_saveas = tb.Button(footer, text="💾 Speichern unter… (manuell)",
                                           bootstyle=SECONDARY, command=self.export_excel_saveas, state=DISABLED)
        self.export_btn_saveas.pack(side=LEFT)
        tb.Button(footer, text="Hilfe", bootstyle=INFO, command=self.help_export).pack(side=LEFT, padx=8)

    # ---------- Status/Progress ----------
    def set_status(self, text: str, prog: Optional[int] = None):
        self.status_var.set(text)
        if prog is not None:
            self.progress_var.set(max(0, min(100, prog)))
        self.app.update_idletasks()

    # ---------- File Helpers ----------
    def browse_csv(self):
        p = filedialog.askopenfilename(
            title="Datenextrakt CSV wählen",
            filetypes=[("CSV","*.csv")],
            initialdir=self.settings.get("last_csv_dir", BASE_DIR)
        )
        if p:
            self.csv_path.set(p)
            self.settings["last_csv_dir"] = os.path.dirname(p)
            self.settings["csv_path"] = p
            save_settings(self.settings)

    def browse_custom_csv(self):
        p = filedialog.askopenfilename(
            title="Custom ID Datei wählen (CSV/Excel)",
            filetypes=[("CSV/Excel","*.csv;*.xlsx;*.xls")],
            initialdir=self.settings.get("last_custom_dir", BASE_DIR)
        )
        if p:
            self.custom_csv_path.set(p)
            self.settings["last_custom_dir"] = os.path.dirname(p)
            self.settings["custom_csv_path"] = p
            save_settings(self.settings)

    def current_date(self) -> Optional[date]:
        try:
            return datetime.strptime(self.date_str.get().strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    def mapping_date(self) -> Optional[date]:
        try:
            return datetime.strptime(self.map_date_str.get().strip(), "%Y-%m-%d").date()
        except Exception:
            return None

    def refresh_incident_table(self):
        for item in self.inc_table.get_children():
            self.inc_table.delete(item)
        for i, inc in enumerate(self.incidents, start=1):
            p = ",".join(inc.get("prefixes", VALID_PREFIXES))
            self.inc_table.insert("", "end", values=(
                i,
                inc.get("start_mode","BOX"),
                inc.get("start_boxid",""),
                inc["affected_recipe"],
                inc["substitute_recipe"],
                inc["start_hhmm"],
                p
            ))

    def auto_save_if_enabled(self):
        if not self.autosave.get():
            return
        d = self.current_date()
        if not d:
            return
        try:
            save_incidents(d, self.incidents)
        except Exception as e:
            messagebox.showerror("Auto-Save", f"Fehlgeschlagen:\n{e}")

    def do_save(self):
        d = self.current_date()
        if not d:
            messagebox.showerror("Speichern", "Bitte Produktionsdatum als JJJJ-MM-TT eingeben.")
            return
        try:
            save_incidents(d, self.incidents)
            self.settings["last_incident_date"] = d.strftime("%Y-%m-%d")
            save_settings(self.settings)
            messagebox.showinfo("Speichern", f"Gespeichert:\n{incidents_path_for(d)}")
        except Exception as e:
            messagebox.showerror("Speichern", f"Fehler:\n{e}")

    def do_load(self):
        d = self.current_date()
        if not d:
            messagebox.showerror("Laden", "Bitte Produktionsdatum als JJJJ-MM-TT eingeben.")
            return
        payload = load_incidents(d)
        if not payload:
            messagebox.showerror("Laden", "Kein gespeicherter Vorfallsatz für dieses Datum gefunden.")
            return
        self.incidents.clear()
        for inc in payload.get("incidents", []):
            self.incidents.append({
                "affected_recipe": inc.get("affected_recipe",""),
                "substitute_recipe": inc.get("substitute_recipe","ERSATZLOS"),
                "start_hhmm": inc.get("start_hhmm", TIME_),
                "start_boxid": inc.get("start_boxid",""),
                "start_mode": inc.get("start_mode","BOX"),
                "prefixes": inc.get("prefixes", VALID_PREFIXES)
            })
        self.refresh_incident_table()
        messagebox.showinfo("Laden", "Gespeicherte Vorfälle geladen.")

    def do_wipe_all(self):
        if messagebox.askyesno("Alles bereinigen", "Wirklich ALLE gespeicherten Daten (Vorfälle & FE-Mappings) löschen?"):
            n = delete_all_data()
            messagebox.showinfo("Bereinigt", f"{n} Datei(en) entfernt. Neustart empfohlen.")
            self.incidents.clear(); self.refresh_incident_table()
            self.mapping_cache.clear(); self.map_text_delete_all()

    # ---------- Vorfälle ----------
    def _confirm_multi_prefix(self) -> bool:
        selected_labels = []
        if self.chk_nordics.get(): selected_labels.append("Nordics (TK+TV)")
        if self.chk_tz.get(): selected_labels.append("TZ")
        if self.chk_tk.get(): selected_labels.append("TK")
        if self.chk_tv.get(): selected_labels.append("TV")

        if len(selected_labels) >= 2:
            redundant = (self.chk_nordics.get() and (self.chk_tk.get() or self.chk_tv.get()))
            note = "\n\nHinweis: 'Nordics' enthält bereits TK+TV." if redundant else ""
            confirm = messagebox.askyesno(
                "Mehrfachauswahl bestätigen",
                "Du hast mehrere Präfixe gewählt:\n- " + "\n- ".join(selected_labels) +
                "\n\nSicher, dass du das so willst?" + note
            )
            return bool(confirm)
        return True

    def selected_prefixes(self, start_box_upper: str = "", for_mode: str = "BOX") -> List[str]:
        prefs = set()
        if self.chk_nordics.get():
            prefs.update(["TK", "TV"])
        if self.chk_tz.get(): prefs.add("TZ")
        if self.chk_tk.get(): prefs.add("TK")
        if self.chk_tv.get(): prefs.add("TV")

        if not prefs:
            if for_mode == "BOX":
                if start_box_upper.startswith("TK") or start_box_upper.startswith("TV"):
                    prefs.update(["TK", "TV"])
                elif start_box_upper.startswith("TZ"):
                    prefs.add("TZ")
                else:
                    prefs.update(VALID_PREFIXES)
            else:
                prefs.update(VALID_PREFIXES)
        return sorted(prefs)

    def add_incident(self):
        aff = self.aff.get().strip()
        sub = (self.sub.get().strip() or "ERSATZLOS")  # Freitext erlaubt
        hhmm = self.hhmm.get().strip()
        start_box = self.start_boxid.get().strip()
        mode = self.start_mode.get()

        if not aff:
            messagebox.showerror("Vorfälle", "Bitte 'Betroffenes Rezept' ausfüllen (z. B. 301).")
            return

        if not self._confirm_multi_prefix():
            return

        if mode == "BOX":
            if not start_box:
                messagebox.showerror("Vorfälle", "Bitte die ERSTE Box-ID (Startpunkt) eingeben (z. B. TK12345).")
                return
            prefixes = self.selected_prefixes(start_box_upper=start_box.upper(), for_mode="BOX")
        else:
            if not is_valid_hhmm(hhmm):
                messagebox.showerror("Vorfälle", f"Bitte Startzeit korrekt eingeben (Format {TIME_}).")
                return
            prefixes = self.selected_prefixes(for_mode="TIME")

        self.incidents.append({
            "affected_recipe": aff,
            "substitute_recipe": sub,
            "start_hhmm": hhmm if mode == "TIME" else TIME_,
            "start_boxid": start_box if mode == "BOX" else "",
            "start_mode": mode,
            "prefixes": prefixes
        })
        self.refresh_incident_table()
        self.auto_save_if_enabled()
        if mode == "BOX":
            self.start_boxid.set("")
        else:
            self.hhmm.set(TIME_)

    def load_selected_to_form(self):
        sel = self.inc_table.selection()
        if not sel:
            messagebox.showwarning("Bearbeiten", "Bitte einen Vorfall in der Tabelle auswählen.")
            return
        item = sel[0]
        vals = self.inc_table.item(item, "values")
        if not vals:
            messagebox.showwarning("Bearbeiten", "Auswahl ungültig.")
            return
        idx = int(vals[0]) - 1
        if idx < 0 or idx >= len(self.incidents):
            messagebox.showwarning("Bearbeiten", "Index außerhalb der Liste.")
            return
        inc = self.incidents[idx]

        self.start_mode.set(inc.get("start_mode","BOX"))
        self.start_boxid.set(inc.get("start_boxid",""))
        self.hhmm.set(inc.get("start_hhmm", TIME_))
        self.aff.set(inc.get("affected_recipe",""))
        self.sub.set(inc.get("substitute_recipe","ERSATZLOS"))

        prefs = set(inc.get("prefixes", []))
        self.chk_nordics.set(("TK" in prefs and "TV" in prefs) and not ("TZ" in prefs))
        self.chk_tz.set("TZ" in prefs)
        self.chk_tk.set("TK" in prefs)
        self.chk_tv.set("TV" in prefs)

        messagebox.showinfo("Bearbeiten", "Werte ins Formular geladen. Ändere sie und klicke „Änderungen speichern“.")

    def update_selected(self):
        sel = self.inc_table.selection()
        if not sel:
            messagebox.showwarning("Änderungen speichern", "Bitte einen Vorfall in der Tabelle auswählen.")
            return
        item = sel[0]
        vals = self.inc_table.item(item, "values")
        if not vals:
            messagebox.showwarning("Änderungen speichern", "Auswahl ungültig.")
            return
        idx = int(vals[0]) - 1
        if idx < 0 or idx >= len(self.incidents):
            messagebox.showwarning("Änderungen speichern", "Index außerhalb der Liste.")
            return

        aff = self.aff.get().strip()
        sub = (self.sub.get().strip() or "ERSATZLOS")
        hhmm = self.hhmm.get().strip()
        start_box = self.start_boxid.get().strip()
        mode = self.start_mode.get()

        if not aff:
            messagebox.showerror("Änderungen speichern", "Bitte 'Betroffenes Rezept' ausfüllen (z. B. 301).")
            return

        if not self._confirm_multi_prefix():
            return

        if mode == "BOX":
            if not start_box:
                messagebox.showerror("Änderungen speichern", "Bitte die ERSTE Box-ID (Startpunkt) eingeben (z. B. TK12345).")
                return
            prefixes = self.selected_prefixes(start_box_upper=start_box.upper(), for_mode="BOX")
            start_hhmm = TIME_
        else:
            if not is_valid_hhmm(hhmm):
                messagebox.showerror("Änderungen speichern", f"Bitte Startzeit korrekt eingeben (Format {TIME_}).")
                return
            prefixes = self.selected_prefixes(for_mode="TIME")
            start_hhmm = hhmm

        self.incidents[idx] = {
            "affected_recipe": aff,
            "substitute_recipe": sub,
            "start_hhmm": start_hhmm,
            "start_boxid": start_box if mode == "BOX" else "",
            "start_mode": mode,
            "prefixes": prefixes
        }
        self.refresh_incident_table()
        self.auto_save_if_enabled()
        messagebox.showinfo("Änderungen speichern", "Vorfall wurde aktualisiert.")

    def del_selected(self):
        sel = self.inc_table.selection()
        if not sel:
            messagebox.showerror("Vorfälle", "Bitte in der Tabelle Zeilen auswählen.")
            return
        idxs = []
        for item in sel:
            vals = self.inc_table.item(item, "values")
            if vals:
                idxs.append(int(vals[0]) - 1)
        for idx in sorted(idxs, reverse=True):
            if 0 <= idx < len(self.incidents):
                self.incidents.pop(idx)
        self.refresh_incident_table()
        self.auto_save_if_enabled()

    def clear_all(self):
        self.incidents.clear()
        self.refresh_incident_table()
        self.auto_save_if_enabled()

    def dryrun(self):
        if not self._validate_basic(check_files=False):
            return
        d = self.current_date()
        modes = ", ".join(sorted({inc.get("start_mode","BOX") for inc in self.incidents}))
        messagebox.showinfo("Test", f"Vorfälle OK.\nDatum: {d}\nAnzahl Vorfälle: {len(self.incidents)}\nStartmodi: {modes}\n→ Am Schichtende CSV (+ Custom-ID) wählen und berechnen.")

    def _validate_basic(self, check_files: bool = True) -> bool:
        if not self.incidents:
            messagebox.showerror("Check", "Bitte mindestens einen Vorfall hinzufügen.")
            return False
        if not self.current_date():
            messagebox.showerror("Check", "Bitte Produktionsdatum als JJJJ-MM-TT eingeben.")
            return False
        if check_files:
            if not self.csv_path.get().strip():
                messagebox.showerror("Check", "Bitte Datenextrakt-CSV auswählen.")
                return False
            # Custom-ID ist optional
        for inc in self.incidents:
            mode = inc.get("start_mode","BOX")
            if not inc.get("affected_recipe"):
                messagebox.showerror("Check", "Leeres 'Betroffenes Rezept' gefunden.")
                return False
            if mode == "BOX":
                sb = (inc.get("start_boxid","") or "").strip()
                if not sb:
                    messagebox.showerror("Check", "Startmodus Box-ID gewählt, aber keine Start-Box eingegeben.")
                    return False
            else:
                hhmm = inc.get("start_hhmm","").strip()
                if not is_valid_hhmm(hhmm):
                    messagebox.showerror("Check", f"Ungültige Startzeit: '{hhmm}' – Format {TIME_}.")
                    return False
        return True

    # ---------- Mapping (manuell)
    def fill_template_301_318(self):
        lines = [f"{r}," for r in range(301, 319)]
        self.map_text_delete_all()
        self.map_text.insert(END, "\n".join(lines))

    def parse_mapping_from_text(self) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        raw = self.map_text.get("1.0", END).strip().splitlines()
        for line in raw:
            line = line.strip()
            if not line:
                continue
            sep = "," if "," in line else "\t"
            parts = line.split(sep, 1)
            if len(parts) < 2:
                messagebox.showerror("Mapping", f"Ungültige Zeile: {line}")
                return {}
            left, right = parts
            recipe = left.strip()
            fe = right.strip()
            if not recipe:
                messagebox.showerror("Mapping", f"Leere RezeptID in Zeile: {line}")
                return {}
            mapping[recipe] = fe if fe else recipe
        return mapping

    def do_save_mapping(self):
        d = self.mapping_date()
        if not d:
            messagebox.showerror("Mapping speichern", "Bitte Datum als JJJJ-MM-TT eingeben.")
            return
        mapping = self.parse_mapping_from_text()
        if mapping is None:
            return
        save_mapping(d, mapping)
        self.mapping_cache = mapping
        messagebox.showinfo("Mapping", f"FE-/Maitre-Mapping gespeichert:\n{mapping_path_for(d)}")

    def do_load_mapping(self):
        d = self.mapping_date()
        if not d:
            messagebox.showerror("Mapping laden", "Bitte Datum als JJJJ-MM-TT eingeben.")
            return
        mapping = load_mapping(d)
        self.mapping_cache = mapping
        self.map_text_delete_all()
        if not mapping:
            messagebox.showwarning("Mapping", "Kein Mapping gefunden. Erzeuge ggf. eine Vorlage 301–318 oder lade aus Pickliste.")
            return
        for k in sorted(mapping, key=lambda x: int(x) if str(x).isdigit() else str(x)):
            self.map_text.insert(END, f"{k},{mapping[k]}\n")
        messagebox.showinfo("Mapping", "Mapping geladen.")

    def do_delete_mapping(self):
        d = self.mapping_date()
        if not d:
            messagebox.showerror("Mapping löschen", "Bitte Datum als JJJJ-MM-TT eingeben.")
            return
        delete_mapping(d)
        self.mapping_cache.clear()
        self.map_text_delete_all()
        messagebox.showinfo("Mapping", "Mapping für dieses Datum gelöscht.")

    def map_text_delete_all(self):
        try:
            self.map_text.delete("1.0", END)
        except Exception:
            pass

    def map_label_numeric_or_text(self, value: str) -> str:
        """
        - 'ERSATZLOS' -> 'ERSATZLOS'
        - numerisch (z.B. '302') -> Mapping nutzen (falls vorhanden) sonst Rezept-ID
        - Freitext -> Freitext unverändert
        """
        if not value:
            return ""
        v = str(value).strip()
        if v.upper() == "ERSATZLOS":
            return "ERSATZLOS"
        if v.isdigit():
            return self.mapping_cache.get(v, v)
        return v  # Freitext unverändert

    # ---------- Auto-Mapping via Pickliste ----------
    def load_mapping_from_latest_picklist(self, silent: bool = False):
        """Picklisten-Ordner scannen, beste Datei wählen, Mapping bauen und ins UI/Caches übernehmen."""
        self.set_status("Scanne Picklisten…", 5)
        path = find_best_picklist_with_fe(PICKLIST_DIR, lookback=12, min_fe_rows=5)
        if not path:
            self.set_status("Keine passende Pickliste gefunden.", 0)
            if not silent:
                messagebox.showwarning(
                    "Pickliste",
                    "Es wurde keine geeignete Pickliste gefunden.\n"
                    "Prüfe den Ordner oder nutze 'Pickliste manuell wählen…'."
                )
            return
        self.set_status(f"Pickliste gefunden: {os.path.basename(path)}", 5)
        self._load_mapping_from_picklist_common(path, silent=silent)

    def load_mapping_from_manual_picklist(self):
        """Manuelle Dateiauswahl für Pickliste (Excel)."""
        p = filedialog.askopenfilename(
            title="Pickliste (Excel) wählen",
            filetypes=[("Excel", "*.xlsx;*.xls")],
            initialdir=self.settings.get("last_picklist_dir", PICKLIST_DIR)
        )
        if not p:
            return
        self.settings["last_picklist_dir"] = os.path.dirname(p)
        save_settings(self.settings)
        self._load_mapping_from_picklist_common(p, silent=False)

    def _load_mapping_from_picklist_common(self, path: str, silent: bool = False):
        """Gemeinsame Logik: Excel lesen, Mapping bauen, UI auffüllen und lokal speichern."""
        try:
            self.set_status("Lese Pickliste…", 15)
            mapping = build_mapping_from_picklist(path)
            if not mapping:
                self.set_status("Pickliste ohne verwertbares Mapping.", 0)
                if not silent:
                    messagebox.showwarning("Pickliste", "Keine Mapping-Einträge aus der Pickliste ermittelt.")
                return

            # UI-Editor befüllen
            self.map_text_delete_all()
            for k in sorted(mapping, key=lambda x: int(x) if str(x).isdigit() else str(x)):
                self.map_text.insert(END, f"{k},{mapping[k]}\n")

            # Cache + lokale Speicherung (auf Mapping-Datum oder Schichtdatum)
            d = self.mapping_date() or self.current_date() or datetime.now().date()
            save_mapping(d, mapping)
            self.mapping_cache = mapping

            self.set_status(f"Mapping geladen aus Pickliste: {os.path.basename(path)}", 40)
            if not silent:
                messagebox.showinfo("Pickliste", f"Mapping erfolgreich geladen aus:\n{path}")
        except Exception as e:
            self.set_status("Fehler beim Laden der Pickliste.", 0)
            if not silent:
                messagebox.showerror("Pickliste", f"{e}")

    # ---------- Berechnung / Export ----------
    def run_compute(self):
        if not self._validate_basic():
            return
        try:
            self.set_status("Lade Daten…", 5)
            df = read_csv_safely(self.csv_path.get().strip())
            if df is None:
                self.set_status("Fehler: Datenextrakt-CSV.", 0); return

            cust_df = None
            if self.custom_csv_path.get().strip():
                cust_df = read_custom_id_csv(self.custom_csv_path.get().strip())
                if cust_df is None:
                    self.set_status("Fehler: Custom-ID-Datei.", 0); return

            df_valid = df[df["__dt"].notna()].copy()
            if df_valid.empty:
                messagebox.showerror("Berechnen", "Keine gültigen Timestamps in der Datenextrakt-Datei.")
                self.set_status("Keine gültigen Timestamps.", 0); return

            # chronologisch stabil sortieren
            df_valid.sort_values("__dt", inplace=True, kind="mergesort")

            def start_dt_from_box(boxid: str) -> Optional[datetime]:
                if not boxid:
                    return None
                sub = df_valid[df_valid[COL_BOX].astype(str).str.upper() == boxid.upper()]
                if sub.empty:
                    return None
                return sub.iloc[0]["__dt"]

            # Mapping Cache sicherstellen
            if not self.mapping_cache:
                dmap = self.mapping_date() or self.current_date()
                if dmap:
                    self.mapping_cache = load_mapping(dmap)

            # Vorfälle nach Rezept gruppieren
            d = self.current_date(); self.set_status("Vorfälle vorbereiten…", 15)
            inc_by_recipe: Dict[str, List[Dict]] = {}
            for inc in self.incidents:
                mode = inc.get("start_mode","BOX")
                rec = str(inc["affected_recipe"]).strip()
                sub = inc["substitute_recipe"].strip() if inc["substitute_recipe"].strip() else "ERSATZLOS"
                if mode == "BOX":
                    start_anchor = inc.get("start_boxid","").strip()
                    start_dt = start_dt_from_box(start_anchor)
                    if start_dt is None:
                        messagebox.showerror("Berechnen", f"Start-Box '{start_anchor}' nicht in der CSV gefunden – Vorfall '{rec}' wird übersprungen.")
                        continue
                    prefixes = self.selected_prefixes(start_box_upper=start_anchor.upper(), for_mode="BOX")
                else:
                    start_anchor = inc.get("start_hhmm","").strip()
                    start_dt = normalize_incident_dt(d, start_anchor)
                    if start_dt is None:
                        messagebox.showerror("Berechnen", f"Ungültige Startzeit: {start_anchor} – Vorfall '{rec}' wird übersprungen.")
                        continue
                    prefixes = self.selected_prefixes(for_mode="TIME")

                inc_by_recipe.setdefault(rec, []).append({
                    "affected_recipe": rec,
                    "substitute_recipe": sub,
                    "start_dt": start_dt,
                    "start_mode": mode,
                    "start_anchor": start_anchor,
                    "prefixes": prefixes
                })

            if not any(inc_by_recipe.values()):
                self.set_status("Keine gültigen Startpunkte.", 0)
                messagebox.showerror("Berechnen", "Kein gültiger Vorfall (Startpunkt) gefunden.")
                return

            for r in inc_by_recipe:
                inc_by_recipe[r].sort(key=lambda x: x["start_dt"])

            # Fenster bilden & selektieren
            self.set_status("Zeitfenster bilden…", 25)
            details_rows, log_rows = [], []
            self.per_incident_details = {}
            inc_id_counter = 1

            for r, incs in inc_by_recipe.items():
                windows = compute_time_windows(incs)
                for i, (start, end, _) in enumerate(windows):
                    inc_obj = incs[i]
                    prefixes = inc_obj["prefixes"]

                    df_pref = df_valid[df_valid[COL_BOX].apply(lambda x: filter_prefix(x, prefixes))].copy()
                    mask_time = (df_pref["__dt"] >= start) & (df_pref["__dt"] < end)
                    mask_recipe = df_pref[COL_REC].apply(lambda s: has_recipe(s, r))
                    sub_df = df_pref[mask_time & mask_recipe][[COL_BOX, COL_TS, COL_REC, "__dt", "__box_upper"]].copy()

                    # Count pro Box (wie oft betroffenes Rezept in der Kombi)
                    sub_df["subs_for_this_box"] = sub_df[COL_REC].apply(lambda s: count_recipe(s, r))

                    # Customer-ID Mergen (optional)
                    if cust_df is not None:
                        sub_df = sub_df.merge(cust_df[["__box_upper", "customer_id"]], on="__box_upper", how="left")
                    else:
                        sub_df["customer_id"] = ""

                    sub_df = sub_df.drop_duplicates(subset=[COL_BOX])

                    incident_id = f"I{inc_id_counter:03d}"; inc_id_counter += 1
                    fe_aff = self.map_label_numeric_or_text(r if r.isdigit() else r)
                    sub_label = self.map_label_numeric_or_text(inc_obj["substitute_recipe"])
                    sheet_key = f"{incident_id}_{fe_aff}_zu_{sub_label}"

                    per_inc = sub_df.rename(columns={COL_BOX: "boxid", COL_TS: "inducted_ts", COL_REC: "recipes"})
                    per_inc = per_inc.drop(columns=["__dt", "__box_upper"], errors="ignore")
                    per_inc.insert(0, "incident_id", incident_id)
                    per_inc.insert(1, "affected_recipe", r)
                    per_inc.insert(2, "substitute_recipe", inc_obj["substitute_recipe"])
                    desired_cols = ["incident_id","affected_recipe","substitute_recipe","boxid","customer_id","subs_for_this_box","recipes","inducted_ts"]
                    per_inc = per_inc[[c for c in desired_cols if c in per_inc.columns]]
                    self.per_incident_details[sheet_key] = per_inc
                    for _, row in per_inc.iterrows():
                        details_rows.append(row.to_dict())

                    log_rows.append({
                        "incident_id": incident_id,
                        "affected_recipe": r,
                        "substitute_recipe": inc_obj["substitute_recipe"],
                        "start_mode": inc_obj["start_mode"],
                        "start_anchor": inc_obj["start_anchor"],
                        "start_time": start.strftime("%Y-%m-%d %H:%M:%S"),
                        "end_time": end.strftime("%Y-%m-%d %H:%M:%S"),
                        "count_boxes": len(per_inc)
                    })

            self.log_df = pd.DataFrame(log_rows)
            self.details_df = pd.DataFrame(details_rows)
            if not self.details_df.empty:
                desired_cols = ["incident_id","affected_recipe","substitute_recipe","boxid","customer_id","subs_for_this_box","recipes","inducted_ts"]
                self.details_df = self.details_df[[c for c in desired_cols if c in self.details_df.columns]]

            self._refresh_table(self.log_table, self.log_df)
            self._refresh_table(self.det_table, self.details_df)

            self.export_btn_auto.configure(state=NORMAL, bootstyle=SUCCESS)
            self.export_btn_saveas.configure(state=NORMAL, bootstyle=SECONDARY)

            messagebox.showinfo("Berechnung", "Abgeschlossen. Export ist bereit.")
            self.set_status("Fertig.", 100)
        except Exception as e:
            self.set_status("Fehler bei Berechnung.", 0)
            messagebox.showerror("Berechnen", f"{e}")

    def _refresh_table(self, tree: tb.Treeview, df: Optional[pd.DataFrame]):
        for item in tree.get_children():
            tree.delete(item)
        if df is None or df.empty:
            return
        for _, row in df.iterrows():
            tree.insert("", "end", values=[row[c] for c in df.columns])

    # ---------- Excel Export ----------
    def _ensure_export_dir(self) -> Optional[str]:
        try:
            os.makedirs(EXPORT_TARGET_DIR, exist_ok=True)
            testfile = os.path.join(EXPORT_TARGET_DIR, "~write_test.tmp")
            with open(testfile, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(testfile)
            return EXPORT_TARGET_DIR
        except Exception as e:
            messagebox.showwarning("Auto-Export",
                f"Zielordner nicht erreichbar:\n{EXPORT_TARGET_DIR}\n\nGrund: {e}\n\nEs wird 'Speichern unter…' angeboten.")
            return None

    def _make_unique_filename(self, dirpath: str, base_filename: str) -> str:
        root, ext = os.path.splitext(base_filename)
        candidate = os.path.join(dirpath, base_filename)
        if not os.path.exists(candidate):
            return candidate
        i = 2
        while True:
            cand = os.path.join(dirpath, f"{root}_v{i}{ext}")
            if not os.path.exists(cand):
                return cand
            i += 1

    def export_excel_auto(self):
        if self.log_df is None or self.log_df.empty:
            messagebox.showerror("Export", "Keine Daten zum Exportieren. Zuerst berechnen.")
            return
        target_dir = self._ensure_export_dir()
        if not target_dir:
            self.export_excel_saveas(); return
        d = self.current_date() or datetime.now().date()
        filename = f"Factor_Incident_Result_{d.strftime('%Y-%m-%d')}.xlsx"
        final_path = self._make_unique_filename(target_dir, filename)
        try:
            self.set_status("Exportiere Excel…", 75)
            xls_bytes = self.export_excel_bytes(self.log_df,
                                                self.details_df if self.details_df is not None else pd.DataFrame(),
                                                self.per_incident_details)
            with open(final_path, "wb") as f:
                f.write(xls_bytes)
            self.set_status("Export fertig.", 100)
            msg = f"Auto-Export erfolgreich:\n{final_path}\n\nIn Explorer öffnen?"
            if messagebox.askyesno("Auto-Export", msg):
                try: os.startfile(target_dir)
                except Exception: pass
        except Exception as e:
            self.set_status("Export fehlgeschlagen.", 0)
            messagebox.showerror("Export", f"Fehler beim Auto-Export:\n{e}")

    def export_excel_saveas(self):
        if self.log_df is None or self.log_df.empty:
            messagebox.showerror("Export", "Keine Daten zum Exportieren. Zuerst berechnen.")
            return
        d = self.current_date() or datetime.now().date()
        p = filedialog.asksaveasfilename(title="Excel speichern unter…",
                                         defaultextension=".xlsx",
                                         filetypes=[("Excel", "*.xlsx")],
                                         initialfile=f"Factor_Incident_Result_{d.strftime('%Y-%m-%d')}.xlsx",
                                         initialdir=self.settings.get("last_save_dir", BASE_DIR))
        if not p: return
        try:
            self.set_status("Exportiere Excel…", 75)
            xls_bytes = self.export_excel_bytes(self.log_df,
                                                self.details_df if self.details_df is not None else pd.DataFrame(),
                                                self.per_incident_details)
            with open(p, "wb") as f: f.write(xls_bytes)
            self.set_status("Export fertig.", 100)
            self.settings["last_save_dir"] = os.path.dirname(p); save_settings(self.settings)
            messagebox.showinfo("Export", f"Export erfolgreich:\n{p}")
        except Exception as e:
            self.set_status("Export fehlgeschlagen.", 0)
            messagebox.showerror("Export", f"Fehler beim Speichern:\n{e}")

    @staticmethod
    def export_excel_bytes(log_df: pd.DataFrame,
                           details_df: pd.DataFrame,
                           per_incident: Dict[str, pd.DataFrame]) -> bytes:
        output = io.BytesIO()
        engine = "xlsxwriter"
        try:
            __import__("xlsxwriter")
        except ImportError:
            engine = "openpyxl"
        with pd.ExcelWriter(output, engine=engine) as writer:
            log_df.to_excel(writer, index=False, sheet_name="Incidents_Log")
            details_df.to_excel(writer, index=False, sheet_name="Details")
            for sheet_key, df in per_incident.items():
                sheet = sanitize_sheet_name(sheet_key)
                df.to_excel(writer, index=False, sheet_name=sheet)
            # Auto-Width
            try:
                for sheet_name, df in [("Incidents_Log", log_df), ("Details", details_df)]:
                    ws = writer.sheets.get(sheet_name)
                    if not ws: continue
                    for i, col in enumerate(df.columns):
                        try:
                            width = min(max(df[col].astype(str).map(len).max() + 2, len(col) + 2), 60)
                            ws.set_column(i, i, width)
                        except Exception: pass
                for sheet_key, df in per_incident.items():
                    ws = writer.sheets.get(sanitize_sheet_name(sheet_key))
                    if not ws: continue
                    for i, col in enumerate(df.columns):
                        try:
                            width = min(max(df[col].astype(str).map(len).max() + 2, len(col) + 2), 60)
                            ws.set_column(i, i, width)
                        except Exception: pass
            except Exception:
                pass
        return output.getvalue()

    # ---------- Hilfen ----------
    def help_all(self):
        messagebox.showinfo(
            "Gesamthilfe – Incident Box Finder",
            "Zweck:\n"
            "• Bei Rezeptausfällen/Substitutionen alle betroffenen Box-IDs ab dem gemeldeten Startpunkt finden.\n"
            "• Startpunkt: ERSTE Box-ID (Standard) oder Uhrzeit (Notfall).\n"
            "• Substitut: numerisch (mit FE-/Maitre-Mapping) oder Freitext; 'ERSATZLOS' bleibt möglich.\n"
            "• Filter nach Kundenpräfix (TZ, TK, TV, Nordics=TK+TV).\n"
            "• Export als Excel: Übersicht, Details, und je Vorfall ein eigenes Sheet.\n\n"
            "Ablauf:\n"
            "1) Datenextrakt-CSV auswählen (+ optional Custom-ID-Datei für customer_id).\n"
            "2) Vorfälle erfassen oder bestehende per 'In Formular laden' bearbeiten und 'Änderungen speichern'.\n"
            "3) Berechnen → prüfen → Export."
        )

    def help_extrakt(self):
        messagebox.showinfo(
            "Hilfe – Datenextrakt CSV",
            "Erforderliche Spalten:\n"
            "• inducted_ts – Datum+Uhrzeit\n"
            "• boxid – Box-ID (TZ/TK/TV)\n"
            "• recipes – Rezeptkette\n"
            "Die CSV wird chronologisch sortiert."
        )

    def help_custom(self):
        messagebox.showinfo(
            "Hilfe – Custom ID Datei",
            "Optional: CSV/XLSX mit Zuordnung boxid → customer_id.\n"
            "Erkannte Spalten: boxid/box_id/box, customer_id/kundennummer/…\n"
            "Fehlt die Datei, bleibt 'customer_id' leer."
        )

    def help_datum(self):
        messagebox.showinfo(
            "Hilfe – Produktionsdatum",
            "Wird u. a. benötigt, um im Notfallmodus 'Uhrzeit' einen vollständigen Timestamp zu bilden."
        )

    def help_save_load(self):
        messagebox.showinfo(
            "Hilfe – Vorfälle speichern/laden",
            "Speichert/Lädt die Vorfälle datumsbezogen. 'Alles bereinigen' löscht alle Daten des Tools."
        )

    def help_startmodus(self):
        messagebox.showinfo(
            "Hilfe – Startmodus",
            "Erste Box-ID (Standard):\n"
            "• Du gibst die erste beobachtete Box-ID an – ab deren Zeitstempel wird ausgewertet.\n\n"
            "Uhrzeit (Notfall):\n"
            "• Du gibst HH:MM an; das Produktionsdatum + Uhrzeit sind der Startpunkt."
        )

    def help_rezept_sub(self):
        messagebox.showinfo(
            "Hilfe – Rezept & Substitut",
            "Betroffenes Rezept: z. B. 301\n"
            "Substitut: numerisch (z. B. 302), 'ERSATZLOS' oder Freitext (z. B. 'Sonderlabel').\n"
            "Beim Excel-Sheetnamen: numerisch → Mapping, Freitext → direkt."
        )

    def help_startfelder(self):
        messagebox.showinfo(
            "Hilfe – Startfelder",
            f"Erste Box-ID (Start) für Modus 'Erste Box-ID'.\nStartzeit ({TIME_}) für Modus 'Uhrzeit (Notfall)'."
        )

    def help_prefixe(self):
        messagebox.showinfo(
            "Hilfe – Kundenpräfixe",
            "Standard: nichts ausgewählt.\nNordics = TK+TV. TK/TV/TZ einzeln möglich.\nMehrfachauswahl wird vor dem Hinzufügen bestätigt."
        )

    def help_vorfall_tabelle(self):
        messagebox.showinfo(
            "Hilfe – Vorfalltabelle",
            "Markiere einen Vorfall und benutze:\n"
            "• '⬅️ In Formular laden' → Werte in die Eingabefelder übernehmen\n"
            "• '💾 Änderungen speichern' → aktuelle Formularwerte in markierte Zeile übernehmen\n"
            "• '🗑️ Ausgewählte entfernen' / '🧹 Alle löschen'"
        )

    def help_berechnen(self):
        messagebox.showinfo(
            "Hilfe – Berechnen",
            "CSV wird nach Zeit sortiert; Fenster pro Rezept bis zum nächsten Vorfall desselben Rezepts.\n"
            "'subs_for_this_box' zählt, wie oft das betroffene Rezept in der Box-Kombi vorkommt.\n"
            "customer_id wird gemappt, wenn eine Custom-ID-Datei geladen wurde."
        )

    def help_mapping(self):
        messagebox.showinfo(
            "Hilfe – FE-/Maitre-Mapping",
            "Optionen:\n"
            "• Aus Pickliste automatisch scannen (beste Datei) oder manuell wählen.\n"
            "• Copy & Paste möglich: '301,565123'. Ohne Code → Rezept-ID als Fallback.\n"
            "• Wöchentlich/Datumsbezogen speichern & laden."
        )

    def help_log(self):
        messagebox.showinfo(
            "Hilfe – Incidents Log",
            "Zeigt pro Vorfall: Startmodus, Anker (erste Box-ID oder Uhrzeit), Zeitfenster und Anzahl Boxen."
        )

    def help_details(self):
        messagebox.showinfo(
            "Hilfe – Details gesamt",
            "Alle Boxen über alle Vorfälle: boxid, customer_id, subs_for_this_box, recipes, inducted_ts."
        )

    def help_export(self):
        messagebox.showinfo(
            "Hilfe – Export",
            "Excel: Incidents_Log, Details, je Vorfall eigenes Blatt.\n"
            "Dateiname: Factor_Incident_Result_YYYY-MM-DD.xlsx.\n"
            "Auto-Export in das definierte Zielverzeichnis oder 'Speichern unter…'."
        )

    # ---------- Auto-Load ----------
    def auto_load_today(self):
        try:
            today = datetime.now().date()
            payload = load_incidents(today)
            if payload:
                self.date_str.set(payload.get("date", today.strftime("%Y-%m-%d")))
                self.incidents.clear()
                for inc in payload.get("incidents", []):
                    self.incidents.append({
                        "affected_recipe": inc.get("affected_recipe",""),
                        "substitute_recipe": inc.get("substitute_recipe","ERSATZLOS"),
                        "start_hhmm": inc.get("start_hhmm", TIME_),
                        "start_boxid": inc.get("start_boxid",""),
                        "start_mode": inc.get("start_mode","BOX"),
                        "prefixes": inc.get("prefixes", VALID_PREFIXES)
                    })
                self.refresh_incident_table()
            m = load_mapping(today)
            if m:
                self.mapping_cache = m
                self.map_date_str.set(today.strftime("%Y-%m-%d"))
                self.map_text_delete_all()
                for k in sorted(m, key=lambda x: int(x) if str(x).isdigit() else str(x)):
                    self.map_text.insert(END, f"{k},{m[k]}\n")
        except Exception:
            pass

# ============== Main ==============
if __name__ == "__main__":
    app = tb.Window(themename="flatly")
    IncidentToolApp(app)
    app.mainloop()
