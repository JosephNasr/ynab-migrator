from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .utils import now_utc_iso


class CheckpointStore:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS metadata (
              key TEXT PRIMARY KEY,
              value TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS mappings (
              entity TEXT NOT NULL,
              source_id TEXT NOT NULL,
              dest_id TEXT NOT NULL,
              PRIMARY KEY (entity, source_id)
            );

            CREATE TABLE IF NOT EXISTS cursors (
              name TEXT PRIMARY KEY,
              value INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS events (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              created_at TEXT NOT NULL,
              level TEXT NOT NULL,
              message TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def set_metadata(self, key: str, value: Any) -> None:
        payload = json.dumps(value, sort_keys=True, ensure_ascii=True)
        self.conn.execute(
            """
            INSERT INTO metadata (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, payload),
        )
        self.conn.commit()

    def get_metadata(self, key: str, default: Any = None) -> Any:
        row = self.conn.execute("SELECT value FROM metadata WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        return json.loads(row[0])

    def set_mapping(self, entity: str, source_id: str, dest_id: str) -> None:
        self.conn.execute(
            """
            INSERT INTO mappings (entity, source_id, dest_id)
            VALUES (?, ?, ?)
            ON CONFLICT(entity, source_id) DO UPDATE SET dest_id=excluded.dest_id
            """,
            (entity, source_id, dest_id),
        )
        self.conn.commit()

    def get_mapping(self, entity: str, source_id: str) -> Optional[str]:
        row = self.conn.execute(
            "SELECT dest_id FROM mappings WHERE entity = ? AND source_id = ?",
            (entity, source_id),
        ).fetchone()
        if not row:
            return None
        return str(row[0])

    def list_mappings(self, entity: str) -> List[Tuple[str, str]]:
        rows = self.conn.execute(
            "SELECT source_id, dest_id FROM mappings WHERE entity = ? ORDER BY source_id",
            (entity,),
        ).fetchall()
        return [(str(source), str(dest)) for source, dest in rows]

    def get_mapping_dict(self, entity: str) -> Dict[str, str]:
        return {source: dest for source, dest in self.list_mappings(entity)}

    def set_cursor(self, name: str, value: int) -> None:
        self.conn.execute(
            """
            INSERT INTO cursors (name, value) VALUES (?, ?)
            ON CONFLICT(name) DO UPDATE SET value=excluded.value
            """,
            (name, int(value)),
        )
        self.conn.commit()

    def get_cursor(self, name: str, default: int = 0) -> int:
        row = self.conn.execute("SELECT value FROM cursors WHERE name = ?", (name,)).fetchone()
        if not row:
            return default
        return int(row[0])

    def add_event(self, level: str, message: str) -> None:
        self.conn.execute(
            "INSERT INTO events (created_at, level, message) VALUES (?, ?, ?)",
            (now_utc_iso(), level, message),
        )
        self.conn.commit()

    def list_events(self, limit: int = 500) -> List[Dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT created_at, level, message
            FROM events
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
        return [
            {"created_at": str(created_at), "level": str(level), "message": str(message)}
            for created_at, level, message in reversed(rows)
        ]

    def close(self) -> None:
        self.conn.close()
