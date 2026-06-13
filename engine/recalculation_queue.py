"""Single-flight queue for expensive scanner recalculation work."""

from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Optional

from loguru import logger


class RecalculationQueue:
    """Serialize backtest/recalculation requests and collapse duplicates."""

    def __init__(self, runner: Callable[[], Awaitable[object]], debounce_seconds: float = 2.0) -> None:
        self._runner = runner
        self._debounce_seconds = max(0.0, float(debounce_seconds or 0.0))
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._generation = 0
        self.superseded_count = 0
        self._pending_reason: Optional[str] = None
        self.last_status = "idle"
        self.last_reason = ""

    async def request(self, reason: str = "manual") -> dict:
        """Queue a recalculation and return immediately.

        Settings changes are user intent, not a FIFO workload. If a settings
        refresh is already running and another one arrives, cancel the stale run
        so the scanner can move directly to the newest saved configuration.
        """
        async with self._lock:
            self._pending_reason = reason or "manual"
            self.last_reason = self._pending_reason
            if self._task and not self._task.done():
                if self._supersedes_running(self._pending_reason):
                    old_task = self._task
                    old_task.cancel()
                    self.superseded_count += 1
                    self._generation += 1
                    self._task = asyncio.create_task(
                        self._worker(self._generation),
                        name="scanner-recalculation-queue",
                    )
                    self.last_status = "superseded"
                    return {"status": "superseded", "reason": self._pending_reason}
                self.last_status = "queued"
                return {"status": "queued", "reason": self._pending_reason}
            self._generation += 1
            self._task = asyncio.create_task(
                self._worker(self._generation),
                name="scanner-recalculation-queue",
            )
            self.last_status = "started"
            return {"status": "started", "reason": self._pending_reason}

    async def drain(self) -> object:
        """Wait for the active queued work. Useful for tests."""
        task = self._task
        if task:
            return await task
        return None

    def _debounce_for(self, reason: str) -> float:
        if reason == "settings-refresh":
            return self._debounce_seconds
        return 0.0

    def _supersedes_running(self, reason: str) -> bool:
        return reason == "settings-refresh"

    async def _worker(self, generation: int) -> None:
        while True:
            async with self._lock:
                reason = self._pending_reason or "manual"
                self._pending_reason = None
                debounce_seconds = self._debounce_for(reason)
                self.last_status = "debouncing" if debounce_seconds > 0 else "running"
                self.last_reason = reason

            try:
                if debounce_seconds > 0:
                    await asyncio.sleep(debounce_seconds)
                    async with self._lock:
                        if self._pending_reason is not None:
                            reason = self._pending_reason
                            self._pending_reason = None
                            self.last_reason = reason
                        self.last_status = "running"
                logger.info(f"Recalculation queue running: {reason}")
                await self._runner()
            except asyncio.CancelledError:
                logger.info(f"Recalculation queue superseded: {reason}")
                raise
            except Exception as exc:
                logger.exception(f"Recalculation queue failed: {exc}")
            finally:
                async with self._lock:
                    if generation != self._generation:
                        return
                    if self._pending_reason is None:
                        self.last_status = "idle"
                        self._task = None
                        return
                    self.last_status = "queued"
