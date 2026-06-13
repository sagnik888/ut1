"""
Desktop Notifications — Windows Toast Notifications
═══════════════════════════════════════════════════════════════
"""

import threading
from loguru import logger

_notifier = None


def _get_notifier():
    global _notifier
    if _notifier is None:
        try:
            from plyer import notification
            _notifier = notification
            logger.debug("Desktop notifications: plyer ready")
        except ImportError:
            logger.warning("plyer not installed — desktop notifications disabled")
    return _notifier


def send_desktop_notification(message: str, msg_type: str = "info"):
    """
    Send a Windows desktop toast notification.
    Runs in a separate thread to avoid blocking the event loop.
    """
    def _send():
        notifier = _get_notifier()
        if notifier is None:
            return
        try:
            titles = {
                "buy": "🟢 UT Bot — Trade Signal",
                "sell": "🔴 UT Bot — Trade Signal",
                "info": "📊 UT Bot — Info",
            }
            notifier.notify(
                title=titles.get(msg_type, "📊 UT Bot"),
                message=message[:256],  # Windows toast limit
                app_name="UT Bot Pro",
                timeout=8,
            )
        except Exception as e:
            logger.debug(f"Notification error: {e}")

    thread = threading.Thread(target=_send, daemon=True)
    thread.start()
