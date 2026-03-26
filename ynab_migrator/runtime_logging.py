from __future__ import annotations

import logging
import re
from pathlib import Path

from .utils import ensure_dir


_BEARER_PATTERN = re.compile(r"(?i)(\bBearer\s+)([A-Za-z0-9._~+/\-]+=*)")
_TOKEN_ARG_PATTERN = re.compile(r"(--(?:source|dest)-token\s+)(\S+)")
_LONG_TOKEN_PATTERN = re.compile(r"\b[A-Za-z0-9_\-]{40,}\b")


def _redact_text(value: str) -> str:
    redacted = _BEARER_PATTERN.sub(r"\1[REDACTED]", value)
    redacted = _TOKEN_ARG_PATTERN.sub(r"\1[REDACTED]", redacted)
    redacted = _LONG_TOKEN_PATTERN.sub("[REDACTED]", redacted)
    return redacted


class _ContextFilter(logging.Filter):
    def __init__(self, command: str):
        super().__init__()
        self.command = command

    def filter(self, record: logging.LogRecord) -> bool:
        record.command = getattr(record, "command", self.command)
        if not hasattr(record, "component"):
            logger_name = record.name or "ynab_migrator"
            if logger_name == "ynab_migrator":
                component = "main"
            elif logger_name.startswith("ynab_migrator."):
                component = logger_name[len("ynab_migrator.") :]
            else:
                component = logger_name
            record.component = component
        return True


class _RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        rendered = record.getMessage()
        record.msg = _redact_text(rendered)
        record.args = ()
        return True


def command_log_path(workdir: Path, command: str) -> Path:
    normalized = (command or "run").strip().lower()
    if not normalized:
        normalized = "run"
    return workdir / f"{normalized}.log"


def build_runtime_logger(workdir: Path, command: str, verbose: bool = False) -> logging.Logger:
    ensure_dir(workdir)
    logger = logging.getLogger("ynab_migrator")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.propagate = False

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    if verbose:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s [%(command)s] [%(component)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    context_filter = _ContextFilter(command=command)
    redaction_filter = _RedactionFilter()

    stream_handler = logging.StreamHandler()
    file_handler = logging.FileHandler(command_log_path(workdir, command), mode="w", encoding="utf-8")

    for handler in (stream_handler, file_handler):
        handler.setLevel(logging.DEBUG if verbose else logging.INFO)
        handler.setFormatter(formatter)
        handler.addFilter(context_filter)
        handler.addFilter(redaction_filter)
        logger.addHandler(handler)

    return logger
