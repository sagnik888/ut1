"""Pytest collection hygiene for local audit/probe files.

These files are kept in the workspace as manual diagnostics or restore copies,
but they are not maintained regression tests and should not run in the default
suite.
"""

collect_ignore = [
    "test_backtest_head.py",
    "test_db.py",
    "test_db2.py",
    "test_db3.py",
    "test_db4.py",
    "test_db5.py",
    "test_db6.py",
    "test_warm_startup.py",
    "ut1-index-final3",
]
