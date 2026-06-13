from datetime import datetime, timedelta
import asyncio

from config.settings import get_settings
from dashboard import server
from engine.signal_processor import SignalProcessor
from scanner import Scanner
from trading.trade_manager import Trade, TradeManager, IST


def test_historical_state_uses_first_trade_page(monkeypatch):
    class DummyScanner:
        mode = "HISTORICAL"

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())
    payload = {
        "trades": {
            "closed": [{"id": f"c{i}"} for i in range(120)],
            "signals": [{"id": f"s{i}"} for i in range(120)],
        }
    }

    limited = server._limit_dashboard_trades(payload)

    assert len(limited["trades"]["closed"]) == 100
    assert len(limited["trades"]["signals"]) == 100
    assert limited["trades"]["window"]["limit"] == 100
    assert limited["trades"]["window"]["offset"] == 0
    assert limited["trades"]["window"]["page"] == 1
    assert limited["trades"]["window"]["page_count"] == 2
    assert limited["trades"]["window"]["total"] == 120
    assert limited["trades"]["closed_total"] == 120
    assert limited["trades"]["signals_total"] == 120


def test_live_state_still_caps_trade_rows(monkeypatch):
    class DummyScanner:
        mode = "REAL"

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())
    payload = {
        "trades": {
            "closed": [{"id": f"c{i}"} for i in range(120)],
            "signals": [{"id": f"s{i}"} for i in range(120)],
        }
    }

    limited = server._limit_dashboard_trades(payload)

    assert len(limited["trades"]["closed"]) == 80
    assert len(limited["trades"]["signals"]) == 80
    assert limited["trades"]["window"]["limit"] == 80


def test_trade_window_caps_requested_limit_to_2000(monkeypatch):
    class DummyScanner:
        mode = "HISTORICAL"

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())
    payload = {
        "closed": [{"id": f"c{i}"} for i in range(2500)],
        "signals": [{"id": f"s{i}"} for i in range(2500)],
        "summary": {"total_trades": 2500},
    }

    windowed = server._window_trades_payload(payload, limit=50000, offset=0)

    assert len(windowed["closed"]) == 2000
    assert len(windowed["signals"]) == 2000
    assert windowed["window"]["limit"] == 2000
    assert windowed["window"]["max_limit"] == 2000
    assert windowed["window"]["page_count"] == 2
    assert windowed["closed_total"] == 2500


def test_trade_window_clamps_offset_to_last_page(monkeypatch):
    class DummyScanner:
        mode = "HISTORICAL"

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())
    payload = {
        "closed": [{"id": f"c{i}"} for i in range(250)],
        "signals": [{"id": f"s{i}"} for i in range(250)],
        "summary": {"total_trades": 250},
    }

    windowed = server._window_trades_payload(payload, limit=100, offset=900)

    assert windowed["window"]["offset"] == 200
    assert windowed["window"]["page"] == 3
    assert windowed["window"]["page_count"] == 3
    assert len(windowed["closed"]) == 50
    assert windowed["closed"][0]["id"] == "c200"


def test_legacy_historical_trade_cache_without_context_is_rejected(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "ut_concurrency_guard", False)
    monkeypatch.setattr(settings, "signal_grade_preference", "B")
    monkeypatch.setattr(settings, "ut_timeframe_entry_policy", "INCLUDE_5MIN")

    class DummyScanner:
        mode = "HISTORICAL"
        backtest_days = 30
        inst_pref = "AUTO"

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())

    assert not server._cached_trades_match_current_context({
        "closed": [],
        "signals": [],
        "meta": {"mode": "HISTORICAL", "source": "historical_trade_manager"},
    })
    assert server._cached_trades_match_current_context({
        "closed": [],
        "signals": [],
        "meta": {
            "mode": "HISTORICAL",
            "backtest_days": 30,
            "complete": True,
            "inst_pref": "AUTO",
            "ut_concurrency_guard": False,
            "grade_preference": "B",
            "timeframe_entry_policy": "INCLUDE_5MIN",
        },
    })


def test_api_trades_prefers_latest_safe_state_over_matching_cache(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "ut_concurrency_guard", False)
    monkeypatch.setattr(settings, "signal_grade_preference", "B")
    monkeypatch.setattr(settings, "ut_timeframe_entry_policy", "INCLUDE_5MIN")

    meta = {
        "mode": "HISTORICAL",
        "backtest_days": 30,
        "complete": True,
        "inst_pref": "AUTO",
        "ut_concurrency_guard": False,
        "grade_preference": "B",
        "timeframe_entry_policy": "INCLUDE_5MIN",
    }

    class DummyCache:
        def trades(self):
            return {
                "closed": [{"id": "cache"}],
                "signals": [{"id": "cache"}],
                "summary": {"total_trades": 1},
                "meta": meta,
            }

    class DummyScanner:
        mode = "HISTORICAL"
        backtest_days = 30
        inst_pref = "AUTO"
        dashboard_cache = DummyCache()

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())
    server.dashboard_state.latest_safe_state = {
        "mode": "HISTORICAL",
        "trades": {
            "closed": [{"id": "state-1"}, {"id": "state-2"}],
            "signals": [{"id": "state-1"}, {"id": "state-2"}],
            "summary": {"total_trades": 2},
            "meta": meta,
        },
    }

    payload = asyncio.run(server.api_trades(limit=50000, offset=0))

    assert payload["closed_total"] == 2
    assert [row["id"] for row in payload["closed"]] == ["state-1", "state-2"]


def test_api_trades_skips_windowed_safe_state_for_later_pages(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "ut_concurrency_guard", False)
    monkeypatch.setattr(settings, "signal_grade_preference", "B")
    monkeypatch.setattr(settings, "ut_timeframe_entry_policy", "INCLUDE_5MIN")

    meta = {
        "mode": "HISTORICAL",
        "backtest_days": 30,
        "complete": True,
        "inst_pref": "AUTO",
        "ut_concurrency_guard": False,
        "grade_preference": "B",
        "timeframe_entry_policy": "INCLUDE_5MIN",
    }
    full_rows = [{"id": f"full-{idx}"} for idx in range(300)]

    class DummyCache:
        def trades(self):
            return {
                "closed": full_rows,
                "signals": full_rows,
                "summary": {"total_trades": 300},
                "meta": meta,
            }

    class DummyScanner:
        mode = "HISTORICAL"
        backtest_days = 30
        inst_pref = "AUTO"
        dashboard_cache = DummyCache()

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())
    server.dashboard_state.latest_safe_state = {
        "mode": "HISTORICAL",
        "trades": {
            "closed": full_rows[:100],
            "signals": full_rows[:100],
            "closed_total": 300,
            "signals_total": 300,
            "summary": {"total_trades": 300},
            "meta": meta,
            "window": {"limit": 100, "offset": 0, "total": 300, "page_count": 3},
        },
    }

    payload = asyncio.run(server.api_trades(limit=100, offset=200))

    assert len(payload["closed"]) == 100
    assert payload["closed"][0]["id"] == "full-200"
    assert payload["window"]["offset"] == 200
    assert payload["window"]["page"] == 3


def test_incomplete_historical_trade_cache_is_rejected(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "ut_concurrency_guard", False)
    monkeypatch.setattr(settings, "signal_grade_preference", "B")
    monkeypatch.setattr(settings, "ut_timeframe_entry_policy", "INCLUDE_5MIN")

    class DummyScanner:
        mode = "HISTORICAL"
        backtest_days = 30
        inst_pref = "AUTO"

    monkeypatch.setattr(server, "scanner_ref", DummyScanner())

    assert not server._cached_trades_match_current_context({
        "closed": [{"id": "partial"}],
        "signals": [{"id": "partial"}],
        "meta": {
            "mode": "HISTORICAL",
            "backtest_days": 30,
            "complete": False,
            "inst_pref": "AUTO",
            "ut_concurrency_guard": False,
            "grade_preference": "B",
            "timeframe_entry_policy": "INCLUDE_5MIN",
        },
    })


def test_concurrency_guard_false_string_does_not_filter_historical_rows(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "ut_concurrency_guard", "False")
    manager = TradeManager.__new__(TradeManager)

    start = IST.localize(datetime(2026, 6, 10, 10, 0, 0))
    trades = [
        Trade(
            id=f"H_NIFTY_15min_{idx}",
            instrument="NIFTY",
            timeframe="15min",
            direction="LONG",
            entry_price=100.0 + idx,
            entry_time=start + timedelta(minutes=idx),
            trailing_stop=99.0,
            current_stop=99.0,
            lots=1,
            lot_size=50,
            grade="B+ (Hist)",
            status="CLOSED",
            exit_price=101.0 + idx,
            exit_time=start + timedelta(minutes=30),
            pnl=50.0,
        )
        for idx in range(3)
    ]

    filtered = manager._filter_concurrency_compliant_closed(trades)

    assert [trade.id for trade in filtered] == [
        "H_NIFTY_15min_2",
        "H_NIFTY_15min_1",
        "H_NIFTY_15min_0",
    ]


def test_scanner_bool_parser_treats_false_string_as_disabled():
    assert Scanner._setting_enabled("False", True) is False
    assert Scanner._setting_enabled("0", True) is False
    assert Scanner._setting_enabled("true", False) is True


def test_signal_processor_false_string_disables_regime_gate(monkeypatch):
    settings = get_settings()
    monkeypatch.setattr(settings, "ut_regime_adaptation", "False")
    monkeypatch.setattr(settings, "ut_backtest_more_results", True)
    monkeypatch.setattr(settings, "live_filter_leniency_pct", 0.0)

    class DummyScanner:
        mode = "HISTORICAL"

    processor = SignalProcessor.__new__(SignalProcessor)
    processor.scanner = DummyScanner()

    assert processor._passes_quality_gate(
        "15min",
        "B",
        0.55,
        "CHOPPY",
        "OPT",
        settings,
        adx_value=0.0,
    )
