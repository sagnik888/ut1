# GPU Acceleration Audit and Restorepoint 23 Summary

Date: 2026-06-07 05:15 IST
Workspace: `C:\Users\sagnik\Desktop\ut index 2`

## GPU Audit Result

- NVIDIA driver detects the RTX 4060 Laptop GPU correctly.
- `nvidia-smi` reports CUDA driver support: CUDA 13.2.
- Initial environment issue: Python venv had no GPU runtime (`cupy`, `torch`, `onnxruntime`) and `numba.cuda` kernel execution failed with an access violation.
- Installed and verified CuPy CUDA 13 runtime:
  - `cupy-cuda13x==14.1.1`
  - `nvidia-cuda-runtime==13.3.29`
  - `nvidia-cuda-nvrtc==13.3.33`
  - `nvidia-cuda-cccl==13.3.3.3.1`

## Integration Added

- Added `engine/gpu_accelerator.py`.
- Added optional CuPy CUDA status, benchmark, and large-vector summary helpers.
- Scanner diagnostics now report:
  - GPU telemetry from `nvidia-smi`.
  - CUDA compute availability.
  - GPU backend name.
  - Cached vector benchmark.
  - CUDA memory/free memory.
- Dashboard UT1 hardware panel now shows compute backend and benchmark speed when available.
- Dashboard summary vector stats can use GPU for very large closed-trade vectors, with CPU fallback for normal/small live workloads.

## Performance Reality

Measured standalone benchmark after CUDA warm-up:

- 100k vector sample: GPU slower due to transfer/kernel overhead.
- 1M vector sample: GPU about 2.97x faster.
- 5M vector sample: GPU about 3.81x faster.

Conclusion:

- GPU should not be forced onto small live scanner calculations.
- GPU can reduce CPU stress for large vector/batch analytics and future large backtest/indicator batches.
- Accuracy is not improved by GPU; the benefit is throughput for suitable math workloads.
- Full UT Bot indicator migration to GPU should be a separate parity-tested phase because live signal decisions must remain deterministic.

## Related Fixes Captured Since Restorepoint 22

- One-step REAL mode confirmation.
- Historical/live mode isolation hardening.
- 15-minute warm restart memory engine.
- Settings refresh persistence for costs, capital, strike selection, UT preset, and concurrency guard.
- Historical P&L deterministic restart drift fix from restorepoint 22 preserved.
- Signal panel historical row cap raised toward 1000.
- Chart panel toggle and refresh controls.
- Hardware monitor panel for RAM, process CPU/RAM, disk, GPU.
- Dynamic RR model:
  - minimum initial RR: 1.6
  - quality-scaled RR ceiling: 12.0
  - runner unlock ratio: 0.95
  - runner gain lock: 0.90
- Concurrency guard OFF now literally allows same-index/cross-timeframe overlap in historical display and live/REAL candidate preparation.

## Verification

- Focused GPU/RR/concurrency tests passed.
- Full automated suite passed: 149 passed, 4 deselected, 1 warning.
- Localhost verified running in HISTORICAL mode.
- `/api/diagnostics` verified:
  - `gpu.compute_available = true`
  - `gpu.compute_backend = cupy_cuda`
  - `gpu.acceleration_mode = vector_accel_ready`
  - process RAM reported correctly instead of 0 MB.

## Caveats

- `venv` is not copied into restorepoint folders, following restorepoint 22 policy.
- GPU runtime dependencies are captured in `requirements.txt`.
- Restorepoint contains sensitive local runtime files such as `.env` and broker tokens; keep private.
