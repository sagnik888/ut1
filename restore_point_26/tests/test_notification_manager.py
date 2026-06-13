import subprocess
import sys
import textwrap


def test_notification_manager_imports_without_windows_toasts():
    code = textwrap.dedent(
        """
        import builtins
        real_import = builtins.__import__

        def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "windows_toasts":
                raise ModuleNotFoundError("No module named 'windows_toasts'")
            return real_import(name, globals, locals, fromlist, level)

        builtins.__import__ = guarded_import
        import engine.notification_manager as notifications

        manager = notifications.NotificationManager()
        assert manager.toaster is None
        manager.send_trade_notification(
            "BUY",
            "NIFTY",
            entry_price=100.0,
            target_price=110.0,
            sl_price=95.0,
            rr_ratio=2.0,
        )
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=".",
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
