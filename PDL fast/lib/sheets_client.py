"""Lightweight Google Sheets reader using the existing service account.

Read-only client, returns plain DataFrames. Cached for the Streamlit session.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError as exc:  # pragma: no cover - import guard
    raise ImportError(
        "google-api-python-client and google-auth are required. "
        "Install with: pip install google-api-python-client google-auth"
    ) from exc


SERVICE_ACCOUNT_KEY = (
    Path(__file__).resolve().parents[1] / "secrets" / "hellofresh-de-problem-solve-78fb952762cd.json"
)
SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]


def _build_service(key_path: Path = SERVICE_ACCOUNT_KEY):
    creds = service_account.Credentials.from_service_account_file(str(key_path), scopes=SCOPES)
    return build("sheets", "v4", credentials=creds, cache_discovery=False)


def list_tabs(spreadsheet_id: str) -> list[dict]:
    """Return [{'title','gid','rows','cols'}, ...]."""
    svc = _build_service()
    meta = svc.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    out = []
    for s in meta.get("sheets", []):
        p = s["properties"]
        gp = p.get("gridProperties", {})
        out.append(
            {
                "title": p["title"],
                "gid": p["sheetId"],
                "rows": gp.get("rowCount", 0),
                "cols": gp.get("columnCount", 0),
            }
        )
    return out


def read_range(spreadsheet_id: str, a1_range: str) -> list[list[str]]:
    """Raw values – missing trailing cells are *not* padded."""
    svc = _build_service()
    resp = (
        svc.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=a1_range, valueRenderOption="UNFORMATTED_VALUE")
        .execute()
    )
    return resp.get("values", [])


def read_tab_as_df(
    spreadsheet_id: str,
    tab_name: str,
    header_row: int = 1,
    end_col: str = "AZ",
    end_row: int | None = None,
) -> pd.DataFrame:
    """Read a tab into a DataFrame.

    header_row is 1-based row number that contains the column headers.
    Rows above header_row are dropped, rows below become data.
    """
    last = end_row if end_row else 5000
    rng = f"'{tab_name}'!A{header_row}:{end_col}{last}"
    raw = read_range(spreadsheet_id, rng)
    if not raw:
        return pd.DataFrame()
    headers = [str(h).strip() for h in raw[0]]
    width = len(headers)
    data_rows = []
    for row in raw[1:]:
        # pad short rows
        if len(row) < width:
            row = list(row) + [""] * (width - len(row))
        elif len(row) > width:
            row = row[:width]
        data_rows.append(row)
    df = pd.DataFrame(data_rows, columns=headers)
    # drop fully-empty rows
    df = df.replace("", pd.NA).dropna(how="all").reset_index(drop=True)
    return df


def to_number(series: pd.Series) -> pd.Series:
    """Convert localized numeric strings ('1,826.00', '3,495', '5.26 %') to floats."""
    if series.dtype.kind in "if":
        return series
    s = series.astype(str).str.strip()
    s = s.str.replace("%", "", regex=False).str.strip()
    # Heuristic: if both '.' and ',' present treat ',' as thousands sep
    has_both = s.str.contains(r"\.").any() and s.str.contains(",").any()
    if has_both:
        s = s.str.replace(",", "", regex=False)
    else:
        # German style? "3.495" → 3495 ; "3,495" → ambiguous, treat as thousands too
        s = s.str.replace(",", "", regex=False)
    return pd.to_numeric(s, errors="coerce")


def extract_codes(text: pd.Series, pattern: str = r"(FE\d{4}[A-Z]?)") -> pd.Series:
    """Extract the first matching code (e.g. FE0612) from a free-text column."""
    return text.astype(str).str.extract(pattern, expand=False)
