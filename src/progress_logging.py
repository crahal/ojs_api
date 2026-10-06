"""Small, content-free progress events for long-running local operations.

Heartbeats indicate that the Python monitor is alive, not that SQL has made
forward progress. Only callers with an actual counter or completed SQL marker
advance ``idle_seconds``. Nothing is persisted here: systemd owns retention.
"""

from __future__ import annotations

import json
import math
import os
import re
import shutil
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TextIO

DEFAULT_INTERVAL = 30.0
_OUTPUT_LOCK = threading.Lock()
_NAME = re.compile(r"[a-z][a-z0-9_]{0,47}\Z")
_LABEL = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:/ -]{0,159}\Z")
_SENSITIVE = re.compile(r"password|secret|credential|token|sql|query|xml|command|url", re.I)
_RESERVED = {
    "event", "stage", "timestamp", "elapsed_seconds", "idle_seconds",
    "process_pid", "process_cpu_seconds", "process_rss_bytes", "disk_free_bytes",
}


def progress_interval(value: str | float | None = None) -> float:
    """Validate the user-facing heartbeat setting without echoing its value."""
    if value is None:
        value = os.environ.get("OJS_PROGRESS_SECONDS", str(DEFAULT_INTERVAL))
    try:
        interval = float(value)
    except (TypeError, ValueError):
        raise ValueError("OJS_PROGRESS_SECONDS must be a number from 1 to 3600") from None
    if not math.isfinite(interval) or not 1 <= interval <= 3600:
        raise ValueError("OJS_PROGRESS_SECONDS must be a number from 1 to 3600")
    return interval


def _safe_label(value: str) -> str:
    if not isinstance(value, str) or _LABEL.fullmatch(value) is None:
        raise ValueError("progress labels must be short, plain-text identifiers")
    return value


def _safe_fields(fields: dict) -> dict:
    if len(fields) > 32:
        raise ValueError("too many progress fields")
    for key, value in fields.items():
        if not isinstance(key, str) or not _NAME.fullmatch(key) or key in _RESERVED or _SENSITIVE.search(key):
            raise ValueError("unsupported progress field name")
        if value is None or isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            try:
                finite = math.isfinite(value)
            except OverflowError:
                finite = False
            if not finite:
                raise ValueError("progress counters must be finite")
        elif isinstance(value, str):
            _safe_label(value)
        else:
            raise ValueError("progress values must be scalar counters or identifiers")
    return fields


def _process_metrics(pid: int) -> dict:
    """Read one known process, never scan a database tree or inspect arguments."""
    try:
        # comm is parenthesized and may itself contain spaces or parentheses.
        stat = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
        fields = stat[stat.rindex(")") + 2:].split()
        ticks = os.sysconf("SC_CLK_TCK")
        pages = os.sysconf("SC_PAGE_SIZE")
        return {
            "process_cpu_seconds": round((int(fields[11]) + int(fields[12])) / ticks, 2),
            "process_rss_bytes": int(fields[21]) * pages,
        }
    except (OSError, ValueError, IndexError):
        return {}


class Progress:
    """Log bounded JSON events independently of blocking work in the caller.

    Use static labels and numeric counters, never URLs, SQL, XML, command lines,
    or exception messages. ``process_pid`` identifies the measured process;
    resource use is not the total of a process tree or a systemd service.
    """

    def __init__(self, label: str, *, interval: float | None = None,
                 process_pid: int | None = None, disk_path: Path | None = None,
                 stream: TextIO | None = None, **fields):
        self.label = _safe_label(label)
        self.interval = progress_interval() if interval is None else float(interval)
        if not math.isfinite(self.interval) or self.interval <= 0:
            raise ValueError("progress interval must be positive and finite")
        if process_pid is not None and (not isinstance(process_pid, int) or process_pid <= 0):
            raise ValueError("progress process ID must be positive")
        self.process_pid = process_pid
        self.disk_path = disk_path
        self.stream = sys.stderr if stream is None else stream
        self._fields = dict(_safe_fields(fields))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._started = 0.0
        self._advanced = 0.0
        self._active = False
        self._used = False

    def __enter__(self):
        if self._used:
            raise RuntimeError("a progress context cannot be reused")
        self._used = True
        self._started = self._advanced = time.monotonic()
        self._active = True
        self._emit("started")
        self._thread = threading.Thread(target=self._heartbeat,
                                        name="ojs-progress", daemon=True)
        self._thread.start()
        return self

    def update(self, **fields) -> None:
        values = _safe_fields(fields)
        with self._lock:
            if len(self._fields.keys() | values.keys()) > 32:
                raise ValueError("too many progress fields")
            if any(key not in self._fields or self._fields[key] != value
                   for key, value in values.items()):
                self._fields.update(values)
                self._advanced = time.monotonic()

    def event(self, event: str, **fields) -> None:
        """Emit a real milestone, resetting idle time even for equal counters."""
        _safe_label(event)
        self.update(**fields)
        with self._lock:
            self._advanced = time.monotonic()
        self._emit(event)

    def _heartbeat(self) -> None:
        while not self._stop.wait(self.interval):
            self._emit("heartbeat")

    def _emit(self, event: str) -> None:
        if not self._active:
            return
        now = time.monotonic()
        with self._lock:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                "event": event,
                "stage": self.label,
                "elapsed_seconds": round(max(0, now - self._started), 1),
                "idle_seconds": round(max(0, now - self._advanced), 1),
                **self._fields,
            }
        if self.process_pid is not None:
            payload["process_pid"] = self.process_pid
            payload.update(_process_metrics(self.process_pid))
        if self.disk_path is not None:
            try:
                payload["disk_free_bytes"] = shutil.disk_usage(self.disk_path).free
            except OSError:
                pass
        try:
            line = "[progress] " + json.dumps(payload, sort_keys=True, allow_nan=False)
            with _OUTPUT_LOCK:
                print(line, file=self.stream, flush=True)
        except (OSError, ValueError):
            # A closed log destination must not turn a completed build into a
            # failed release. Do not retry or buffer unbounded log messages.
            pass

    def __exit__(self, exc_type, exc_value, traceback):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._emit("completed" if exc_type is None else "failed")
        self._active = False
        return False
