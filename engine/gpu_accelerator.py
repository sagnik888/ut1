"""Optional GPU acceleration helpers.

The trading engine must stay deterministic and safe if CUDA is unavailable, so
all helpers fall back to CPU-compatible outputs and never own trading decisions.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

import numpy as np


@dataclass
class GpuVectorStats:
    mean: float
    std: float
    sum: float
    wins: int
    losses: int
    max_drawdown: float
    backend: str


class GpuAccelerator:
    """Small CuPy-backed accelerator for heavy vector summaries/telemetry."""

    def __init__(self) -> None:
        self._cp = None
        self._status_cache: Optional[Dict[str, Any]] = None
        self._status_at = 0.0
        self._bench_cache: Optional[Dict[str, Any]] = None
        self._bench_at = 0.0

    def _load_cupy(self):
        if self._cp is not None:
            return self._cp
        import cupy as cp

        if cp.cuda.runtime.getDeviceCount() < 1:
            raise RuntimeError("No CUDA device available")
        self._cp = cp
        return cp

    def status(self, ttl_seconds: float = 20.0) -> Dict[str, Any]:
        now = time.time()
        if self._status_cache and now - self._status_at < ttl_seconds:
            return dict(self._status_cache)

        payload: Dict[str, Any] = {
            "compute_available": False,
            "compute_backend": "none",
            "acceleration_mode": "telemetry_only",
            "decision_acceleration": "disabled",
            "deterministic_parity_required": True,
            "error": "",
        }
        try:
            cp = self._load_cupy()
            with cp.cuda.Device(0):
                free, total = cp.cuda.runtime.memGetInfo()
            payload.update(
                {
                    "compute_available": True,
                    "compute_backend": "cupy_cuda",
                    "acceleration_mode": "vector_accel_ready",
                    "decision_acceleration": "disabled",
                    "deterministic_parity_required": True,
                    "cuda_total_mb": round(total / (1024 * 1024), 1),
                    "cuda_free_mb": round(free / (1024 * 1024), 1),
                }
            )
        except Exception as exc:
            payload["error"] = f"{type(exc).__name__}: {exc}"

        self._status_cache = dict(payload)
        self._status_at = now
        return payload

    def benchmark(self, n: int = 1_000_000, ttl_seconds: float = 120.0) -> Dict[str, Any]:
        now = time.time()
        if self._bench_cache and now - self._bench_at < ttl_seconds:
            return dict(self._bench_cache)

        status = self.status(ttl_seconds=0.0)
        payload: Dict[str, Any] = {
            "available": bool(status.get("compute_available")),
            "backend": status.get("compute_backend", "none"),
            "sample_size": int(n),
            "gpu_ms": 0.0,
            "cpu_ms": 0.0,
            "speedup": 0.0,
            "memory_delta_mb": 0.0,
            "parity_ok": False,
            "error": status.get("error", ""),
        }
        if not status.get("compute_available"):
            self._bench_cache = dict(payload)
            self._bench_at = now
            return payload

        try:
            cp = self._load_cupy()
            cpu_arr = np.linspace(0.0, 1.0, n, dtype=np.float32)
            cpu_start = time.perf_counter()
            cpu_result = float(np.sum(np.sin(cpu_arr) + np.cos(cpu_arr * 0.5)))
            cpu_ms = (time.perf_counter() - cpu_start) * 1000.0

            with cp.cuda.Device(0):
                warm = cp.asarray(np.arange(32, dtype=np.float32))
                float(cp.sum(cp.sin(warm) + cp.cos(warm * 0.5)).get())
                cp.cuda.Stream.null.synchronize()
                free0, _ = cp.cuda.runtime.memGetInfo()
                gpu_start = time.perf_counter()
                gpu_arr = cp.asarray(cpu_arr)
                gpu_result = float(cp.sum(cp.sin(gpu_arr) + cp.cos(gpu_arr * 0.5)).get())
                cp.cuda.Stream.null.synchronize()
                gpu_ms = (time.perf_counter() - gpu_start) * 1000.0
                free1, _ = cp.cuda.runtime.memGetInfo()

            parity_ok = abs(cpu_result - gpu_result) <= max(0.5, abs(cpu_result) * 1e-5)
            payload.update(
                {
                    "gpu_ms": round(gpu_ms, 2),
                    "cpu_ms": round(cpu_ms, 2),
                    "speedup": round(cpu_ms / gpu_ms, 2) if gpu_ms > 0 else 0.0,
                    "memory_delta_mb": round(max(0.0, (free0 - free1) / (1024 * 1024)), 1),
                    "parity_ok": parity_ok,
                    "error": "" if parity_ok else "GPU benchmark parity drift",
                }
            )
        except Exception as exc:
            payload.update({"available": False, "error": f"{type(exc).__name__}: {exc}"})

        self._bench_cache = dict(payload)
        self._bench_at = now
        return payload

    def vector_stats(self, values: Iterable[float], min_gpu_size: int = 1_000_000) -> GpuVectorStats:
        arr = np.asarray(list(values), dtype=np.float64)
        if arr.size == 0:
            return GpuVectorStats(0.0, 0.0, 0.0, 0, 0, 0.0, "cpu")
        if arr.size < min_gpu_size or not self.status().get("compute_available"):
            running = np.cumsum(arr)
            peak = np.maximum.accumulate(running)
            drawdown = peak - running
            return GpuVectorStats(
                mean=float(np.mean(arr)),
                std=float(np.std(arr)),
                sum=float(np.sum(arr)),
                wins=int(np.sum(arr > 0)),
                losses=int(np.sum(arr <= 0)),
                max_drawdown=float(np.max(drawdown)) if drawdown.size else 0.0,
                backend="cpu",
            )

        try:
            cp = self._load_cupy()
            gpu_arr = cp.asarray(arr)
            running = cp.cumsum(gpu_arr)
            peak = cp.maximum.accumulate(running)
            drawdown = peak - running
            stats = GpuVectorStats(
                mean=float(cp.mean(gpu_arr).get()),
                std=float(cp.std(gpu_arr).get()),
                sum=float(cp.sum(gpu_arr).get()),
                wins=int(cp.sum(gpu_arr > 0).get()),
                losses=int(cp.sum(gpu_arr <= 0).get()),
                max_drawdown=float(cp.max(drawdown).get()) if arr.size else 0.0,
                backend="cupy_cuda",
            )
            cp.cuda.Stream.null.synchronize()
            return stats
        except Exception:
            running = np.cumsum(arr)
            peak = np.maximum.accumulate(running)
            drawdown = peak - running
            return GpuVectorStats(
                mean=float(np.mean(arr)),
                std=float(np.std(arr)),
                sum=float(np.sum(arr)),
                wins=int(np.sum(arr > 0)),
                losses=int(np.sum(arr <= 0)),
                max_drawdown=float(np.max(drawdown)) if drawdown.size else 0.0,
                backend="cpu_fallback",
            )


_ACCELERATOR = GpuAccelerator()


def get_gpu_accelerator() -> GpuAccelerator:
    return _ACCELERATOR
