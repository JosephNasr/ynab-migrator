from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from .utils import now_utc_iso


class CheckpointStore:
    SCHEMA_VERSION = 2
    TERMINAL_STATUSES = {"succeeded", "excluded"}
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

            CREATE TABLE IF NOT EXISTS entity_status (
              entity TEXT NOT NULL,
              source_id TEXT NOT NULL,
              status TEXT NOT NULL,
              dest_id TEXT,
              operation_key TEXT,
              payload_hash TEXT,
              attempt_count INTEGER NOT NULL DEFAULT 0,
              last_error TEXT,
              updated_at TEXT NOT NULL,
              PRIMARY KEY (entity, source_id)
            );

            CREATE INDEX IF NOT EXISTS entity_status_state_idx
              ON entity_status (entity, status);
            """
        )
        self.conn.execute(f"PRAGMA user_version={self.SCHEMA_VERSION}")
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
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO mappings (entity, source_id, dest_id)
                VALUES (?, ?, ?)
                ON CONFLICT(entity, source_id) DO UPDATE SET dest_id=excluded.dest_id
                """,
                (entity, source_id, dest_id),
            )
            self.conn.execute(
                """
                INSERT INTO entity_status
                  (entity, source_id, status, dest_id, attempt_count, updated_at)
                VALUES (?, ?, 'succeeded', ?, 0, ?)
                ON CONFLICT(entity, source_id) DO UPDATE SET
                  status='succeeded', dest_id=excluded.dest_id,
                  last_error=NULL, updated_at=excluded.updated_at
                """,
                (entity, source_id, dest_id, now_utc_iso()),
            )

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

    def seed_entity_statuses(
        self,
        entity: str,
        source_ids: Iterable[str],
        *,
        excluded_ids: Iterable[str] = (),
    ) -> None:
        excluded = {str(item) for item in excluded_ids}
        mapped = self.get_mapping_dict(entity)
        timestamp = now_utc_iso()
        with self.conn:
            for raw_source_id in source_ids:
                source_id = str(raw_source_id)
                dest_id = mapped.get(source_id)
                status = "succeeded" if dest_id else ("excluded" if source_id in excluded else "pending")
                self.conn.execute(
                    """
                    INSERT INTO entity_status
                      (entity, source_id, status, dest_id, attempt_count, updated_at)
                    VALUES (?, ?, ?, ?, 0, ?)
                    ON CONFLICT(entity, source_id) DO NOTHING
                    """,
                    (entity, source_id, status, dest_id, timestamp),
                )

    def set_entity_status(
        self,
        entity: str,
        source_id: str,
        status: str,
        *,
        dest_id: Optional[str] = None,
        operation_key: Optional[str] = None,
        payload_hash: Optional[str] = None,
        error: Optional[str] = None,
        increment_attempt: bool = False,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT INTO entity_status
                  (entity, source_id, status, dest_id, operation_key, payload_hash,
                   attempt_count, last_error, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(entity, source_id) DO UPDATE SET
                  status=excluded.status,
                  dest_id=COALESCE(excluded.dest_id, entity_status.dest_id),
                  operation_key=COALESCE(excluded.operation_key, entity_status.operation_key),
                  payload_hash=COALESCE(excluded.payload_hash, entity_status.payload_hash),
                  attempt_count=entity_status.attempt_count + ?,
                  last_error=excluded.last_error,
                  updated_at=excluded.updated_at
                """,
                (
                    entity,
                    str(source_id),
                    status,
                    dest_id,
                    operation_key,
                    payload_hash,
                    1 if increment_attempt else 0,
                    error,
                    now_utc_iso(),
                    1 if increment_attempt else 0,
                ),
            )

    def get_entity_status(self, entity: str, source_id: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """
            SELECT status, dest_id, operation_key, payload_hash, attempt_count,
                   last_error, updated_at
            FROM entity_status WHERE entity=? AND source_id=?
            """,
            (entity, str(source_id)),
        ).fetchone()
        if not row:
            return None
        return {
            "status": str(row[0]),
            "dest_id": str(row[1]) if row[1] is not None else None,
            "operation_key": row[2],
            "payload_hash": row[3],
            "attempt_count": int(row[4]),
            "last_error": row[5],
            "updated_at": str(row[6]),
        }

    def list_entity_statuses(self, entity: Optional[str] = None) -> List[Dict[str, Any]]:
        sql = "SELECT entity, source_id, status, dest_id, attempt_count, last_error FROM entity_status"
        params: Tuple[Any, ...] = ()
        if entity is not None:
            sql += " WHERE entity=?"
            params = (entity,)
        sql += " ORDER BY entity, source_id"
        rows = self.conn.execute(sql, params).fetchall()
        return [
            {
                "entity": str(row[0]), "source_id": str(row[1]), "status": str(row[2]),
                "dest_id": str(row[3]) if row[3] is not None else None,
                "attempt_count": int(row[4]), "last_error": row[5],
            }
            for row in rows
        ]

    def entity_status_counts(self, entity: Optional[str] = None) -> Dict[str, int]:
        sql = "SELECT status, COUNT(*) FROM entity_status"
        params: Tuple[Any, ...] = ()
        if entity is not None:
            sql += " WHERE entity=?"
            params = (entity,)
        sql += " GROUP BY status"
        return {str(status): int(count) for status, count in self.conn.execute(sql, params)}

    def recover_interrupted_operations(self) -> int:
        with self.conn:
            cursor = self.conn.execute(
                """
                UPDATE entity_status
                SET status='ambiguous_commit',
                    last_error=COALESCE(last_error, 'process stopped while API write was in progress'),
                    updated_at=?
                WHERE status='in_progress'
                """,
                (now_utc_iso(),),
            )
        return int(cursor.rowcount)

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

    def list_cursors(self) -> Dict[str, int]:
        return {
            str(name): int(value)
            for name, value in self.conn.execute("SELECT name, value FROM cursors ORDER BY name")
        }

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
