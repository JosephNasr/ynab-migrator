from __future__ import annotations

import json
import os
import socket
from pathlib import Path
from typing import IO, Optional

import fcntl

from .utils import now_utc_iso


class WorkdirLock:
    """Advisory process lock preventing concurrent mutating runs in one workdir."""

    def __init__(self, path: Path, command: str):
        self.path = path
        self.command = command
        self._handle: Optional[IO[str]] = None

    def __enter__(self) -> "WorkdirLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            handle.seek(0)
            owner = handle.read().strip() or "unknown owner"
            handle.close()
            raise RuntimeError(f"another mutating migration is active ({owner})") from error
        handle.seek(0)
        handle.truncate()
        json.dump(
            {
                "pid": os.getpid(),
                "hostname": socket.gethostname(),
                "command": self.command,
                "started_at": now_utc_iso(),
            },
            handle,
            sort_keys=True,
        )
        handle.flush()
        os.fsync(handle.fileno())
        self._handle = handle
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._handle is None:
            return
        fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
        self._handle.close()
        self._handle = None
