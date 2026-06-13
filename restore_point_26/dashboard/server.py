"""
Dashboard Server — FastAPI + WebSocket for Real-Time Dashboard
═══════════════════════════════════════════════════════════════
"""
import asyncio
import hashlib
import inspect
import json
import os
import secrets
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from datetime import datetime
from decimal import Decimal
from typing import Set, Dict, Any, Optional, Callable
from urllib.parse import urlparse

from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from config.settings import get_settings
from engine.settings_persistence import schedule_settings_save

try:
    import orjson
except Exception:  # pragma: no cover - optional fast path
    orjson = None

# WebSocket connections
active_connections: Set[WebSocket] = set()

# Reference to scanner (set by main.py)
scanner_ref = None

# Periodic dashboard task managed by FastAPI lifespan.
_periodic_trades_task: Optional[asyncio.Task] = None
_ipv6_localhost_proxy: Optional[asyncio.base_events.Server] = None
startup_hook: Optional[Callable[[], Any]] = None
_dashboard_runtime_token = secrets.token_urlsafe(32)
_last_periodic_trades_hash: Optional[str] = None
_DASHBOARD_STATE_TRADE_LIMIT = 80


def _dashboard_token() -> str:
    return str(getattr(get_settings(), "dashboard_auth_token", "") or _dashboard_runtime_token)


def _allowed_origins() -> Set[str]:
    settings = get_settings()
    host = str(getattr(settings, "dashboard_host", "127.0.0.1") or "127.0.0.1")
    port = int(getattr(settings, "dashboard_port", 7000) or 7000)
    origins = {
        f"http://{host}:{port}",
        f"http://127.0.0.1:{port}",
        f"http://localhost:{port}",
    }
    origins.update(str(o).rstrip("/") for o in getattr(settings, "dashboard_allowed_origins", []) or [] if o)
    return origins


def _is_allowed_origin(origin: Optional[str]) -> bool:
    if not origin:
        return True
    parsed = urlparse(origin)
    if parsed.hostname in {"127.0.0.1", "localhost"}:
        return True
    return origin.rstrip("/") in _allowed_origins()


def _token_matches(token: Optional[str]) -> bool:
    expected = _dashboard_token()
    return bool(token) and secrets.compare_digest(str(token), expected)


def _request_authorized(request: Request) -> bool:
    origin = request.headers.get("origin") or request.headers.get("referer")
    token = request.headers.get("x-dashboard-token") or request.query_params.get("token")
    return _is_allowed_origin(origin) and _token_matches(token)


def _valid_command_payload(msg: Dict[str, Any]) -> Optional[str]:
    cmd = msg.get("cmd", "")
    allowed = {
        "get_state", "configure", "set_mode", "reset_pnl", "set_power",
        "set_regime_adaptation", "set_sl_mode", "set_concurrency_guard",
        "close_trade", "kill_all", "update_fyers_token", "subscribe_chart",
        "set_chart_stream", "system_recalibrate",
    }
    if cmd not in allowed:
        return "unknown_command"
    if cmd in {"set_power", "set_regime_adaptation", "set_concurrency_guard"} and msg.get("state") not in {"ON", "OFF"}:
        return "invalid_state"
    if cmd == "close_trade":
        if not str(msg.get("trade_id", "")).strip() or len(str(msg.get("trade_id", ""))) > 120:
            return "invalid_trade_id"
        try:
            float(msg.get("price", 0))
        except (TypeError, ValueError):
            return "invalid_price"
    if cmd == "update_fyers_token" and len(str(msg.get("auth_code", ""))) > 4096:
        return "invalid_auth_code"
    return None


def _command_response(msg: Dict[str, Any], msg_type: str, data: Dict[str, Any]) -> Dict[str, Any]:
    payload = {"type": msg_type, "data": data}
    request_id = msg.get("request_id")
    if request_id is not None:
        payload["request_id"] = request_id
    return payload


async def _send_command_response(ws: WebSocket, msg: Dict[str, Any], msg_type: str, data: Dict[str, Any]) -> None:
    await ws.send_json(_command_response(msg, msg_type, data))


async def shutdown_scanner():
    logger.info("👋 Graceful Shutdown Triggered...")
    if scanner_ref:
        try:
            scanner_ref.stop()
            logger.info("✅ Scanner loop stopped.")
        except Exception as e:
            logger.error(f"Error stopping scanner: {e}")
        try:
            scanner_ref.trades.save_state()
            logger.info("✅ Trade state saved.")
        except Exception as e:
            logger.error(f"Error saving trade state during shutdown: {e}")


async def _relay_stream(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while not reader.at_eof():
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def _handle_ipv6_localhost_proxy(
    client_reader: asyncio.StreamReader,
    client_writer: asyncio.StreamWriter,
) -> None:
    settings = get_settings()
    port = int(getattr(settings, "dashboard_port", 7000) or 7000)
    try:
        upstream_reader, upstream_writer = await asyncio.open_connection("127.0.0.1", port)
    except Exception:
        client_writer.close()
        await client_writer.wait_closed()
        return
    await asyncio.gather(
        _relay_stream(client_reader, upstream_writer),
        _relay_stream(upstream_reader, client_writer),
        return_exceptions=True,
    )


async def start_ipv6_localhost_proxy() -> None:
    """Bridge Windows localhost (::1) to the IPv4 Uvicorn listener."""
    global _ipv6_localhost_proxy
    if _ipv6_localhost_proxy is not None:
        return
    settings = get_settings()
    host = str(getattr(settings, "dashboard_host", "127.0.0.1") or "127.0.0.1")
    if host not in {"127.0.0.1", "localhost"}:
        return
    port = int(getattr(settings, "dashboard_port", 7000) or 7000)
    try:
        _ipv6_localhost_proxy = await asyncio.start_server(
            _handle_ipv6_localhost_proxy,
            host="::1",
            port=port,
        )
        logger.info(f"IPv6 localhost bridge active on [::1]:{port} -> 127.0.0.1:{port}")
    except OSError as exc:
        logger.debug(f"IPv6 localhost bridge not started: {exc}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _periodic_trades_task, _ipv6_localhost_proxy
    _periodic_trades_task = asyncio.create_task(periodic_trades_updater())
    await start_ipv6_localhost_proxy()
    if startup_hook:
        result = startup_hook()
        if inspect.iscoroutine(result):
            await result
    try:
        yield
    finally:
        if _periodic_trades_task:
            _periodic_trades_task.cancel()
            try:
                await _periodic_trades_task
            except asyncio.CancelledError:
                pass
            _periodic_trades_task = None
        if _ipv6_localhost_proxy is not None:
            _ipv6_localhost_proxy.close()
            await _ipv6_localhost_proxy.wait_closed()
            _ipv6_localhost_proxy = None
        await shutdown_scanner()


app = FastAPI(title="UT Bot Index Trading System", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=sorted(_allowed_origins()),
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-Dashboard-Token"],
)

# Static files
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = static_dir / "index.html"
    if html_path.exists():
        return FileResponse(str(html_path), headers={"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"})
    return HTMLResponse("<h1>UT Bot Index Trading System</h1><p>Dashboard loading...</p>")


@app.get("/api/client_config")
async def api_client_config(response: Response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return {
        "dashboard_token": _dashboard_token(),
        "allowed_origins": sorted(_allowed_origins()),
    }


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    origin = ws.headers.get("origin")
    token = ws.query_params.get("token")
    if not _is_allowed_origin(origin) or not _token_matches(token):
        await ws.close(code=1008)
        return
    await ws.accept()
    active_connections.add(ws)
    logger.info(f"📡 Dashboard connected ({len(active_connections)} clients)")
    try:
        while True:
            # Receive commands from dashboard
            data = await ws.receive_text()
            try:
                msg = json.loads(data)
                await handle_command(ws, msg)
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        active_connections.discard(ws)
        dashboard_state.last_sent_states.pop(id(ws), None)
        logger.info(f"📡 Dashboard disconnected ({len(active_connections)} clients)")
    except Exception as e:
        active_connections.discard(ws)
        dashboard_state.last_sent_states.pop(id(ws), None)


async def handle_command(ws: WebSocket, msg: Dict):
    """Handle commands from the dashboard"""
    if not isinstance(msg, dict):
        await ws.send_json({"type": "command_rejected", "data": {"reason": "invalid_payload"}})
        return
    cmd = msg.get("cmd", "")
    scanner_required = {
        "get_state",
        "configure",
        "set_mode",
        "reset_pnl",
        "set_power",
        "set_regime_adaptation",
        "set_sl_mode",
        "set_concurrency_guard",
        "close_trade",
        "kill_all",
        "update_fyers_token",
        "subscribe_chart",
        "set_chart_stream",
        "system_recalibrate",
    }
    if cmd in scanner_required and not scanner_ref:
        await _send_command_response(ws, msg, "system_unavailable", {"status": "starting", "cmd": cmd})
        return

    validation_error = _valid_command_payload(msg)
    if validation_error:
        await _send_command_response(ws, msg, "command_rejected", {"cmd": cmd, "reason": validation_error})
        return

    if cmd == "get_state":
        if scanner_ref:
            data = scanner_ref.dashboard_cache.state() if hasattr(scanner_ref, "dashboard_cache") else scanner_ref.latest_results
            if not data and hasattr(scanner_ref, "get_latest_results"):
                data = scanner_ref.get_latest_results(include_full_charts=False) or {}
            payload = dict(data or {})
            payload["dashboard_heartbeat"] = _dashboard_heartbeat()
            await _send_command_response(ws, msg, "full_update", make_serializable(_lighten_state_payload(payload)))

    elif cmd == "configure":
        if scanner_ref:
            configured = scanner_ref.configure(
                capital_total=msg.get("capital_total"),
                capital_fut=msg.get("capital_fut"),
                risk_fut_pct=msg.get("risk_fut_pct"),
                capital_opt=msg.get("capital_opt"),
                risk_opt_pct=msg.get("risk_opt_pct"),
                lots=msg.get("lots"),
                lots_fut=msg.get("lots_fut"),
                futures_sl_pct=msg.get("futures_sl_pct"),
                options_sl_pct=msg.get("options_sl_pct"),
                fut_cost=msg.get("fut_cost"),
                opt_cost=msg.get("opt_cost"),
                backtest_days=msg.get("backtest_days"),
                auto_mode=msg.get("auto_mode"),
                inst_pref=msg.get("inst_pref"),
                strike_selection=msg.get("strike_selection"),
                grade_preference=msg.get("grade_preference"),
                ut_preset=msg.get("ut_preset"),
                active_indices=msg.get("active_indices"),
                mode=msg.get("mode"),
                reset=msg.get("reset", False),
                confirm_real_mode=msg.get("confirm_real_mode", False),
                real_mode_verification=msg.get("real_mode_verification", ""),
                timeframe_entry_policy=msg.get("timeframe_entry_policy"),
                max_trades_per_index=msg.get("max_trades_per_index"),
                max_consecutive_losses=msg.get("max_consecutive_losses"),
                index_cooldown_minutes=msg.get("index_cooldown_minutes"),
            )
            await _send_command_response(ws, msg, "config_ack", {"status": "ok" if configured is not False else "blocked"})

    elif cmd == "set_mode":
        if scanner_ref:
            mode = msg.get("mode", "paper")
            reset = msg.get("reset", False)
            configured = scanner_ref.configure(
                mode=mode,
                reset=reset,
                confirm_real_mode=msg.get("confirm_real_mode", False),
                real_mode_verification=msg.get("real_mode_verification", ""),
            )
            await _send_command_response(ws, msg, "mode_updated", {
                "mode": scanner_ref.mode,
                "status": "ok" if configured is not False else "blocked",
            })

    elif cmd == "reset_pnl":
        if scanner_ref:
            scanner_ref.configure(reset=True)
            await _send_command_response(ws, msg, "pnl_reset", {"status": "ok"})

    elif cmd == "set_power":
        if scanner_ref:
            state = msg.get("state", "OFF")
            scanner_ref.system_power = state
            await _send_command_response(ws, msg, "power_updated", {"state": state})

    elif cmd == "set_regime_adaptation":
        if scanner_ref:
            from config.settings import get_settings
            state = msg.get("state", "OFF")
            is_on = state == "ON"
            settings = get_settings()
            settings.ut_regime_adaptation = is_on
            schedule_settings_save(settings)
            if hasattr(getattr(scanner_ref, "mtf", None), "apply_engine_params"):
                scanner_ref.mtf.apply_engine_params(settings.get_ut_engine_params())
            await _send_command_response(ws, msg, "config_ack", {"status": "ok"})

    elif cmd == "set_sl_mode":
        if scanner_ref:
            from config.settings import get_settings
            state = str(msg.get("state", "NATURAL") or "NATURAL").upper()
            if state not in {"NATURAL", "HARDCODED"}:
                await _send_command_response(ws, msg, "config_ack", {"status": "blocked", "reason": "invalid_sl_mode"})
                return
            settings = get_settings()
            settings.sl_mode = state
            schedule_settings_save(settings)
            await _send_command_response(ws, msg, "config_ack", {"status": "ok"})

    elif cmd == "set_concurrency_guard":
        if scanner_ref:
            state = msg.get("state", "ON")
            is_on = state == "ON"
            scanner_ref.configure(concurrency_guard=is_on)
            await _send_command_response(ws, msg, "config_ack", {"status": "ok"})

    elif cmd == "close_trade":
        if scanner_ref:
            tid = msg.get("trade_id", "")
            price = float(msg.get("price", 0) or 0)
            scanner_ref.trades.close_trade(tid, price, "MANUAL")
            scanner_ref.log_event(f"👤 Manual Exit: {tid} | Auto-Mode Adapted", "info")
            await _send_command_response(ws, msg, "trade_closed", {"trade_id": tid})

    elif cmd == "kill_all":
        if scanner_ref:
            open_tids = list(scanner_ref.trades.open_trades.keys())
            for tid in open_tids:
                trade = scanner_ref.trades.open_trades[tid]
                price = getattr(trade, 'current_price', trade.entry_price)
                scanner_ref.trades.close_trade(tid, price, "HARDBREAKER")
            scanner_ref.log_event("🚨 HARDBREAKER: All trades closed manually!", "warning")
            await _send_command_response(ws, msg, "all_trades_killed", {"status": "ok"})

    elif cmd == "update_fyers_token":
        if scanner_ref:
            result = scanner_ref.update_fyers_token(msg.get("auth_code", ""))
            await _send_command_response(ws, msg, "fyers_auth_updated", make_serializable(result))

    elif cmd == "subscribe_chart":
        if scanner_ref:
            instrument = str(msg.get("instrument") or "NIFTY").upper()
            tf = str(msg.get("tf") or "5min")
            loop = asyncio.get_running_loop()
            snapshot = await loop.run_in_executor(
                None,
                lambda: scanner_ref.get_chart_snapshot(instrument=instrument, timeframe=tf),
            )
            await _send_command_response(ws, msg, "chart_snapshot", make_serializable(snapshot))

    elif cmd == "set_chart_stream":
        if scanner_ref:
            enabled = msg.get("enabled", True)
            if hasattr(scanner_ref, "set_chart_stream_enabled"):
                enabled = scanner_ref.set_chart_stream_enabled(enabled)
            else:
                scanner_ref.chart_stream_enabled = bool(enabled)
            await _send_command_response(ws, msg, "chart_stream_updated", {"enabled": bool(enabled)})

    elif cmd == "system_recalibrate":
        if scanner_ref:
            asyncio.create_task(scanner_ref.perform_system_recalibration())
            await _send_command_response(ws, msg, "config_ack", {"status": "ok"})


# Cache to track last sent state to clients
import asyncio

class DashboardState:
    def __init__(self):
        self.lock = asyncio.Lock()
        self.latest_safe_state: Dict[str, Any] = {}
        self.last_sent_states: Dict[int, Any] = {}

dashboard_state = DashboardState()

def build_trades_payload() -> Dict[str, Any]:
    if not scanner_ref:
        return {"open": [], "closed": [], "summary": {}, "signals": []}
    if hasattr(scanner_ref, "dashboard_cache"):
        cached = scanner_ref.dashboard_cache.trades()
        if cached and _payload_matches_scanner_mode({"trades": cached}):
            return cached
    if hasattr(scanner_ref, "_build_dashboard_trade_payload"):
        return make_serializable(scanner_ref._build_dashboard_trade_payload())
    return make_serializable(scanner_ref.trades.get_dashboard_payload(
        is_historical=(scanner_ref.mode == "HISTORICAL"),
        backtest_days=scanner_ref.backtest_days,
    ))


def _payload_matches_scanner_mode(payload: Dict[str, Any]) -> bool:
    if not scanner_ref:
        return True
    expected_mode = str(getattr(scanner_ref, "mode", "") or "").upper()
    if not expected_mode:
        return True
    payload_mode = str((payload or {}).get("mode") or "").upper()
    trades = (payload or {}).get("trades")
    trade_meta = trades.get("meta") if isinstance(trades, dict) else {}
    trade_mode = str((trade_meta or {}).get("mode") or "").upper()
    if payload_mode and payload_mode != expected_mode:
        return False
    if trade_mode and trade_mode != expected_mode:
        return False
    return True

async def broadcast(data: Dict):
    """Broadcast delta payloads to all connected WebSocket clients"""
    payload = dict(data or {})
    payload["dashboard_heartbeat"] = _dashboard_heartbeat()
    safe_data = make_serializable(_lighten_state_payload(payload))
    
    async with dashboard_state.lock:
        dashboard_state.latest_safe_state = safe_data

        if not active_connections:
            return
        
        disconnected = set()
        
        for ws in list(active_connections):
            try:
                ws_id = id(ws)
                old_state = dashboard_state.last_sent_states.get(ws_id)
                
                if old_state is None:
                    # First time: Send full update
                    message = json.dumps({"type": "full_update", "data": safe_data}, default=str)
                    await ws.send_text(message)
                    dashboard_state.last_sent_states[ws_id] = safe_data
                else:
                    # Delta Update
                    delta = compute_delta(old_state, safe_data)
                    if delta:
                        message = json.dumps({"type": "delta_update", "data": delta}, default=str)
                        await ws.send_text(message)
                        dashboard_state.last_sent_states[ws_id] = safe_data
                        
            except Exception:
                disconnected.add(ws)
                
        # Cleanup disconnected
        for ws in disconnected:
            active_connections.discard(ws)
            dashboard_state.last_sent_states.pop(id(ws), None)

def compute_delta(old, new):
    """
    Computes a sparse delta dictionary. If applied to 'old' via Object.assign deep-merge, it yields 'new'.
    """
    if not isinstance(old, dict) or not isinstance(new, dict):
        return new if old != new else None

    delta = {}
    for k, v in new.items():
        if k not in old:
            delta[k] = v
        elif k == "instruments":
            inst_delta = {}
            for inst_k, inst_v in v.items():
                if inst_k not in old[k]:
                    inst_delta[inst_k] = inst_v
                else:
                    inst_old = old[k][inst_k]
                    inst_diff = {}
                    for prop_k, prop_v in inst_v.items():
                        if prop_k not in inst_old or inst_old[prop_k] != prop_v:
                            inst_diff[prop_k] = prop_v
                    if inst_diff:
                        inst_delta[inst_k] = inst_diff
            if inst_delta:
                delta[k] = inst_delta
        elif k == "trades":
            trade_delta = {}
            for tk, tv in v.items():
                if tk not in old[k] or old[k][tk] != tv:
                    trade_delta[tk] = tv
            if trade_delta:
                delta[k] = trade_delta
        elif old[k] != v:
            delta[k] = v
            
    return delta if delta else None


def make_serializable(obj: Any) -> Any:
    """High-performance serialization for large trading data"""
    if hasattr(obj, "item") and callable(getattr(obj, "item", None)):
        try:
            return make_serializable(obj.item())
        except Exception:
            pass
    if isinstance(obj, (int, str, bool, type(None))):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, float):
        if obj != obj or obj == float('inf') or obj == float('-inf'): return 0
        return obj
    if isinstance(obj, dict):
        return {str(k): make_serializable(v) for k, v in list(obj.items())}
    if isinstance(obj, (list, tuple, set, deque)):
        return [make_serializable(i) for i in list(obj)]
    if isinstance(obj, datetime):
        return obj.isoformat()
    return str(obj)


def _encode_json_bytes(payload: Dict[str, Any]) -> bytes:
    safe = make_serializable(payload)
    if orjson is not None:
        return orjson.dumps(safe)
    return json.dumps(safe, default=str, separators=(",", ":")).encode("utf-8")


def _api_state_json_response(payload: Dict[str, Any]) -> Response:
    """Serve a pre-lightened JSON response without FastAPI re-walking it."""
    light_payload = _lighten_state_payload(payload)
    return Response(content=_encode_json_bytes(light_payload), media_type="application/json")


@app.get("/api/status")
async def api_status():
    if scanner_ref and hasattr(scanner_ref, "dashboard_cache"):
        return scanner_ref.dashboard_cache.status(scanner_ref, len(active_connections))
    return {"status": "starting", "connected_clients": len(active_connections), "pid": os.getpid()}


@app.get("/api/state")
async def api_state():
    if not scanner_ref:
        return {"status": "starting"}
    if hasattr(scanner_ref, "dashboard_cache"):
        cached_state = scanner_ref.dashboard_cache.state()
        if cached_state and _payload_matches_scanner_mode(cached_state):
            payload = dict(cached_state)
            if not getattr(scanner_ref, "chart_stream_enabled", True) and payload.get("instruments"):
                instruments = {}
                for name, ui_data in payload.get("instruments", {}).items():
                    if isinstance(ui_data, dict):
                        hydrated_ui = dict(ui_data)
                        hydrated_ui["chart"] = {}
                        instruments[name] = hydrated_ui
                    else:
                        instruments[name] = ui_data
                payload["instruments"] = instruments
            payload["dashboard_heartbeat"] = _dashboard_heartbeat()
            return _api_state_json_response(payload)
    if not getattr(scanner_ref, "chart_stream_enabled", True):
        payload = scanner_ref.get_latest_results(include_full_charts=False) or {}
        payload = dict(payload)
        payload["dashboard_heartbeat"] = _dashboard_heartbeat()
        return _api_state_json_response(payload)
    if dashboard_state.latest_safe_state and _payload_matches_scanner_mode(dashboard_state.latest_safe_state):
        payload = dict(dashboard_state.latest_safe_state)
        payload["dashboard_heartbeat"] = _dashboard_heartbeat()
        return _api_state_json_response(payload)
    payload = scanner_ref.get_latest_results(include_full_charts=False) or {}
    payload = dict(payload)
    payload["dashboard_heartbeat"] = _dashboard_heartbeat()
    return _api_state_json_response(payload)


@app.get("/api/diagnostics")
async def api_diagnostics():
    if not scanner_ref:
        return {"status": "starting", "diagnostics": {}}
    if hasattr(scanner_ref, "dashboard_cache"):
        cached = scanner_ref.dashboard_cache.diagnostics()
        if cached:
            return cached
    return make_serializable(scanner_ref._get_diagnostics_payload())


@app.get("/api/signal-report")
async def api_signal_report(date: str | None = None):
    if not scanner_ref:
        return {"status": "starting", "report": {}}
    loop = asyncio.get_running_loop()
    report = await loop.run_in_executor(
        None,
        lambda: scanner_ref.generate_daily_signal_report(date),
    )
    return make_serializable({"status": "ok", "report": report})


@app.get("/api/chart")
async def api_chart(instrument: str = "NIFTY", tf: str = "5min", bars: int | None = None):
    if not scanner_ref:
        return {"status": "starting", "instrument": instrument, "tf": tf, "chart": {}}
    settings = get_settings()
    max_bars = max(1, int(getattr(settings, "api_max_chart_bars", 1000) or 1000))
    default_bars = max(1, int(getattr(settings, "api_default_chart_bars", 300) or 300))
    requested_bars = max(1, min(max_bars, int(bars or default_bars)))
    loop = asyncio.get_running_loop()
    snapshot = await loop.run_in_executor(
        None,
        lambda: scanner_ref.get_chart_snapshot(instrument=instrument, timeframe=tf),
    )
    chart = snapshot.get("chart") if isinstance(snapshot, dict) else None
    if isinstance(chart, dict):
        for tf_key, payload in list(chart.items()):
            if not isinstance(payload, dict):
                continue
            trimmed = dict(payload)
            for key in ("candles", "trailing_stop", "markers"):
                value = trimmed.get(key)
                if isinstance(value, list) and len(value) > requested_bars:
                    trimmed[key] = value[-requested_bars:]
            chart[tf_key] = trimmed
        snapshot["window"] = {"bars": requested_bars, "max_bars": max_bars}
    return make_serializable(snapshot)


@app.post("/api/reset_cache")
async def api_reset_cache(request: Request):
    if not _request_authorized(request):
        return {"status": "error", "message": "Unauthorized"}
    if not scanner_ref:
        return {"status": "error", "message": "Scanner not initialized"}
    if hasattr(scanner_ref, "queue_full_recalculation"):
        scanner_ref.queue_full_recalculation("api-reset-cache")
    else:
        asyncio.create_task(scanner_ref._perform_full_recalculation())
    return {"status": "ok", "message": "Cache reset initiated"}


@app.post("/api/recalibrate")
async def api_recalibrate(request: Request):
    if not _request_authorized(request):
        return {"status": "error", "message": "Unauthorized"}
    if not scanner_ref:
        return {"status": "error", "message": "Scanner not initialized"}
    asyncio.create_task(scanner_ref.perform_system_recalibration())
    return {"status": "ok", "message": "System recalibration initiated"}


@app.get("/api/fyers_auth")
async def api_fyers_auth_start(request: Request):
    if not _request_authorized(request):
        return {"status": "error", "message": "Unauthorized"}
    if not scanner_ref:
        return {"status": "error", "message": "Scanner is still starting. Try again in a few seconds."}

    try:
        auth = scanner_ref.data.get_fyers_auth_status()
        auth_url = scanner_ref.data.get_fyers_auth_url()
        return {"status": "ok", "auth": auth, "auth_url": auth_url}
    except Exception as exc:
        logger.exception(f"Unable to start Fyers login: {exc}")
        return {"status": "error", "message": f"Unable to start Fyers login: {exc}"}


@app.get("/api/fyers_login")
async def api_fyers_login_redirect(request: Request):
    if not _request_authorized(request):
        return HTMLResponse("Unauthorized", status_code=401)
    if not scanner_ref:
        return HTMLResponse("System not ready", status_code=503)
    try:
        auth_url = scanner_ref.data.get_fyers_auth_url()
        return RedirectResponse(auth_url)
    except Exception as exc:
        return HTMLResponse(f"Error generating Fyers URL: {exc}", status_code=500)

@app.post("/api/fyers_auth")
async def api_fyers_auth_complete(request: Request):
    if not _request_authorized(request):
        return {"status": "error", "message": "Unauthorized"}
    if not scanner_ref:
        return {"status": "error", "message": "Scanner is still starting. Try again in a few seconds."}

    try:
        payload = await request.json()
    except Exception:
        payload = {}

    auth_code = payload.get("auth_code", "")
    try:
        return make_serializable(scanner_ref.update_fyers_token(auth_code))
    except Exception as exc:
        logger.exception(f"Fyers token update failed: {exc}")
        return {"status": "error", "message": f"Fyers token update failed: {exc}"}


def _window_trades_payload(payload: Dict[str, Any], limit: int | None, offset: int = 0) -> Dict[str, Any]:
    settings = get_settings()
    is_hist = "scanner_ref" in globals() and scanner_ref and getattr(scanner_ref, "mode", None) == "HISTORICAL"
    if is_hist:
        max_limit = 50000
        default_limit = 50000
        resolved_limit = max(1, min(max_limit, int(limit or default_limit)))
    else:
        max_limit = max(1, int(getattr(settings, "api_max_trade_limit", 1000) or 1000))
        default_limit = max(1, int(getattr(settings, "api_default_trade_limit", 250) or 250))
        resolved_limit = max(1, min(max_limit, int(limit or default_limit)))
    resolved_offset = max(0, int(offset or 0))
    windowed = dict(payload or {})
    for key in ("closed", "signals"):
        rows = list(windowed.get(key) or [])
        windowed[key] = rows[resolved_offset: resolved_offset + resolved_limit]
        windowed[f"{key}_total"] = len(rows)
    windowed["window"] = {
        "limit": resolved_limit,
        "offset": resolved_offset,
        "max_limit": max_limit,
    }
    return windowed


def _limit_dashboard_trades(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Keep live dashboard payloads small; full history remains available via /api/trades."""
    windowed = dict(payload or {})
    trades = windowed.get("trades")
    if isinstance(trades, dict):
        windowed["trades"] = _window_trades_payload(trades, _DASHBOARD_STATE_TRADE_LIMIT, 0)
    return windowed


def _lighten_state_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Remove heavy chart arrays from state/WS payloads; /api/chart serves charts."""
    light = _limit_dashboard_trades(payload)
    instruments = light.get("instruments")
    if not isinstance(instruments, dict):
        return light

    light_instruments: Dict[str, Any] = {}
    for name, ui_data in list(instruments.items()):
        if not isinstance(ui_data, dict):
            light_instruments[name] = ui_data
            continue

        ui_light = dict(ui_data)
        chart = ui_light.get("chart")
        if isinstance(chart, dict):
            chart_state = {}
            for tf, tf_payload in list(chart.items()):
                if isinstance(tf_payload, dict):
                    chart_state[tf] = {"state": tf_payload.get("state", {})}
            ui_light["chart"] = chart_state
        light_instruments[name] = ui_light

    light["instruments"] = light_instruments
    return light


@app.get("/api/trades")
async def api_trades(limit: int | None = None, offset: int = 0):
    if not scanner_ref:
        return {"open": [], "closed": [], "signals": [], "summary": {}}
    if hasattr(scanner_ref, "dashboard_cache"):
        cached = scanner_ref.dashboard_cache.trades()
        if cached and _payload_matches_scanner_mode({"trades": cached}):
            return _window_trades_payload(cached, limit, offset)
    if (
        dashboard_state.latest_safe_state
        and _payload_matches_scanner_mode(dashboard_state.latest_safe_state)
        and isinstance(dashboard_state.latest_safe_state.get("trades"), dict)
    ):
        return _window_trades_payload(dashboard_state.latest_safe_state["trades"], limit, offset)
    return _window_trades_payload(build_trades_payload(), limit, offset)


async def periodic_trades_updater():
    """Low-cost heartbeat so live open/closed trade rows stay fresh between scans."""
    global _last_periodic_trades_hash
    logger.info("Periodic trades updater background task started (change-driven loop)")
    while True:
        try:
            await asyncio.sleep(1.0)
            if scanner_ref and active_connections:
                if hasattr(scanner_ref, "dashboard_cache"):
                    trades = scanner_ref.dashboard_cache.trades()
                else:
                    trades = build_trades_payload()
                trades = _window_trades_payload(trades, _DASHBOARD_STATE_TRADE_LIMIT, 0)
                trade_hash = hashlib.blake2b(
                    json.dumps(make_serializable(trades), sort_keys=True, default=str).encode("utf-8"),
                    digest_size=12,
                ).hexdigest()
                if trade_hash == _last_periodic_trades_hash:
                    continue
                _last_periodic_trades_hash = trade_hash
                await broadcast({
                    "trades": trades,
                    "trade_update_id": trade_hash,
                    "timestamp": datetime.now().isoformat(),
                })
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.error(f"Error in periodic trades updater: {exc}")


def _dashboard_heartbeat() -> Dict[str, Any]:
    now = datetime.now()
    heartbeat: Dict[str, Any] = {"timestamp": now.isoformat()}
    if not scanner_ref:
        return heartbeat
    last_scan = getattr(scanner_ref, "last_scan_time", None)
    if last_scan:
        try:
            last_scan_naive = last_scan.replace(tzinfo=None) if getattr(last_scan, "tzinfo", None) else last_scan
            heartbeat["last_scan"] = last_scan.isoformat()
            heartbeat["scan_age_ms"] = round(max(0.0, (now - last_scan_naive).total_seconds() * 1000.0))
        except Exception:
            heartbeat["scan_age_ms"] = None
    else:
        heartbeat["scan_age_ms"] = None
    heartbeat["scan_count"] = getattr(scanner_ref, "scan_count", 0)
    heartbeat["is_calculating"] = bool(getattr(scanner_ref, "is_calculating", False))
    heartbeat["calculation_lock"] = bool(getattr(scanner_ref, "_calculation_lock", False))
    heartbeat["mode"] = getattr(scanner_ref, "mode", None)
    return heartbeat

