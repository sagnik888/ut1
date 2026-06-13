import asyncio

from scanner import Scanner


def test_fetch_all_data_forwards_days_back_override():
    scanner = Scanner.__new__(Scanner)
    scanner._last_data_fetch = {}
    scanner._data_fetch_interval = 60
    calls = []

    async def fake_fetch_one(name, cfg, tf, days_back_override=None):
        calls.append((name, cfg, tf, days_back_override))

    scanner._fetch_one = fake_fetch_one

    asyncio.run(
        Scanner._fetch_all_data(
            scanner,
            {"NIFTY": {"token": "99926000", "exchange": "NSE"}},
            force=True,
            timeframes=["1min", "5min"],
            days_back_override=7,
        )
    )

    assert calls == [
        ("NIFTY", {"token": "99926000", "exchange": "NSE"}, "1min", 7),
        ("NIFTY", {"token": "99926000", "exchange": "NSE"}, "5min", 7),
    ]
