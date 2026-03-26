from __future__ import annotations

import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def stable_hash(obj: Any) -> str:
    return hashlib.sha256(canonical_json(obj).encode("utf-8")).hexdigest()


def atomic_write_json(path: Path, data: Any) -> None:
    ensure_dir(path.parent)
    fd, tmp_name = tempfile.mkstemp(prefix=path.name, dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True, ensure_ascii=True)
            handle.write("\n")
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)


def read_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def chunked(items: List[Any], size: int) -> Iterator[List[Any]]:
    if size <= 0:
        raise ValueError("size must be positive")
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def is_deleted(entity: Optional[Dict[str, Any]]) -> bool:
    if not entity:
        return True

    # Primary YNAB tombstone marker.
    if entity.get("deleted") is True:
        return True

    # Defensive fallback for future "status"-style tombstones.
    status = entity.get("status")
    if isinstance(status, str) and status.lower() in {"deleted", "tombstone", "archived"}:
        return True

    return False


def clean_deleted(items: Optional[Iterable[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    if items is None:
        return []
    return [item for item in items if not is_deleted(item)]


def normalize_name(value: Optional[str]) -> str:
    return (value or "").strip().casefold()


def deterministic_import_id(source_plan_id: str, transaction_id: str) -> str:
    digest = hashlib.sha1(f"{source_plan_id}:{transaction_id}".encode("utf-8")).hexdigest()
    return f"MG:{digest[:33]}"


def summarize_exception(error: Exception) -> str:
    message = str(error).strip()
    if message:
        return f"{error.__class__.__name__}: {message}"
    return error.__class__.__name__
