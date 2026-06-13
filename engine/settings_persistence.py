"""Coalesced settings persistence for dashboard configuration changes."""

from __future__ import annotations

import atexit
import threading
from typing import Any, Optional

from loguru import logger


class AsyncSettingsPersister:
    """Batch rapid settings changes so .env is not rewritten per click."""

    def __init__(self, delay_seconds: float = 0.75) -> None:
        self.delay_seconds = max(0.05, float(delay_seconds or 0.75))
        self._lock = threading.RLock()
        self._timer: Optional[threading.Timer] = None
        self._pending: Any = None
        self.save_count = 0
        self.last_error = ""

    def schedule(self, settings: Any, *, immediate: bool = False) -> None:
        with self._lock:
            self._pending = settings
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None
            if immediate:
                self._flush_locked()
                return
            self._timer = threading.Timer(self.delay_seconds, self.flush)
            self._timer.daemon = True
            self._timer.start()

    def flush(self) -> None:
        with self._lock:
            self._flush_locked()

    def _flush_locked(self) -> None:
        settings = self._pending
        self._pending = None
        self._timer = None
        if settings is None:
            return
        try:
            settings.save_to_env()
            self.save_count += 1
            self.last_error = ""
        except Exception as exc:
            self.last_error = str(exc)
            logger.exception(f"Settings persistence failed: {exc}")

    def status(self) -> dict:
        with self._lock:
            return {
                "pending": self._pending is not None,
                "save_count": self.save_count,
                "last_error": self.last_error,
            }


_PERSISTER = AsyncSettingsPersister()
atexit.register(_PERSISTER.flush)


def schedule_settings_save(settings: Any, *, immediate: bool = False) -> None:
    _PERSISTER.schedule(settings, immediate=immediate)


def flush_settings_save() -> None:
    _PERSISTER.flush()


def settings_persistence_status() -> dict:
    return _PERSISTER.status()
