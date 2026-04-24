"""Storage abstraction — local parquet today, Firestore in Etappe 2.

Logic code uses only the `Storage` interface, so swapping backends is one line.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Protocol

import pandas as pd


class Storage(Protocol):
    def save(self, dataset: str, week: str, df: pd.DataFrame) -> Path: ...
    def load(self, dataset: str, week: str) -> pd.DataFrame: ...
    def list_weeks(self, dataset: str) -> list[str]: ...


class LocalParquetStorage:
    """Stores DataFrames under <root>/<dataset>/<week>.parquet (+ .meta.json)."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def _path(self, dataset: str, week: str) -> Path:
        d = self.root / dataset
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{week}.parquet"

    def save(self, dataset: str, week: str, df: pd.DataFrame) -> Path:
        path = self._path(dataset, week)
        # parquet needs string column names
        df = df.copy()
        df.columns = [str(c) for c in df.columns]
        try:
            df.to_parquet(path, index=False)
        except (ImportError, ValueError):
            # fallback if pyarrow/fastparquet missing
            path = path.with_suffix(".csv")
            df.to_csv(path, index=False)
        meta = path.with_suffix(path.suffix + ".meta.json")
        meta.write_text(
            f'{{"saved_at": "{datetime.utcnow().isoformat()}Z", "rows": {len(df)} }}',
            encoding="utf-8",
        )
        return path

    def load(self, dataset: str, week: str) -> pd.DataFrame:
        p = self._path(dataset, week)
        if p.exists():
            return pd.read_parquet(p)
        csv = p.with_suffix(".csv")
        if csv.exists():
            return pd.read_csv(csv)
        return pd.DataFrame()

    def list_weeks(self, dataset: str) -> list[str]:
        d = self.root / dataset
        if not d.exists():
            return []
        weeks = set()
        for f in d.iterdir():
            if f.suffix in {".parquet", ".csv"}:
                weeks.add(f.stem)
        return sorted(weeks)
