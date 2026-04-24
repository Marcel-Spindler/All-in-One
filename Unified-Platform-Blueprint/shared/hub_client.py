from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib import error, request

DEFAULT_HUB_BASE_URL = os.environ.get("UNIFIED_OPS_HUB_URL", "http://127.0.0.1:3020")
DEFAULT_TIMEOUT = 5


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _post_json(url: str, payload: dict[str, Any], method: str = "POST") -> Optional[dict[str, Any]]:
    body = json.dumps(payload, ensure_ascii=True).encode("utf-8")
    req = request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with request.urlopen(req, timeout=DEFAULT_TIMEOUT) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except (error.URLError, error.HTTPError, TimeoutError, json.JSONDecodeError):
        return None


def start_run(tool: str, input_files: Iterable[str], hub_base_url: str = DEFAULT_HUB_BASE_URL, started_at: Optional[str] = None) -> Optional[str]:
    payload = {
        "tool": tool,
        "startedAt": started_at or utc_now_iso(),
        "inputFiles": [str(item) for item in input_files],
    }
    response = _post_json(f"{hub_base_url}/api/v1/runs/start", payload, method="POST")
    return str(response.get("runId")) if response and response.get("runId") else None


def add_artifact(run_id: str, artifact_type: str, artifact_path: str | Path, content_type: str, hub_base_url: str = DEFAULT_HUB_BASE_URL) -> bool:
    path_value = Path(artifact_path)
    size_bytes = path_value.stat().st_size if path_value.exists() else 0
    payload = {
        "type": artifact_type,
        "path": str(path_value),
        "contentType": content_type,
        "sizeBytes": int(size_bytes),
    }
    response = _post_json(f"{hub_base_url}/api/v1/runs/{run_id}/artifact", payload, method="POST")
    return bool(response and response.get("ok"))


def finish_run(
    run_id: str,
    status: str,
    metrics: Optional[dict[str, Any]] = None,
    warnings: Optional[Iterable[str]] = None,
    errors: Optional[Iterable[str]] = None,
    hub_base_url: str = DEFAULT_HUB_BASE_URL,
    finished_at: Optional[str] = None,
) -> bool:
    payload = {
        "status": status,
        "finishedAt": finished_at or utc_now_iso(),
        "metrics": metrics or {},
        "warnings": [str(item) for item in (warnings or [])],
        "errors": [str(item) for item in (errors or [])],
    }
    response = _post_json(f"{hub_base_url}/api/v1/runs/{run_id}", payload, method="PATCH")
    return bool(response and response.get("runId"))
