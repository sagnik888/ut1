import logging
import asyncio
import webbrowser
from typing import Callable, Optional
from windows_toasts import Toast, InteractableWindowsToaster, ToastButton, ToastAudio, AudioSource

logger = logging.getLogger(__name__)

class NotificationManager:
    def __init__(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        self.toaster = InteractableWindowsToaster('UT Bot Pro')
        self.loop = loop

    def send_trade_notification(
        self,
        signal_type: str, 
        instrument: str,
        entry_price: float,
        target_price: float,
        sl_price: float,
        rr_ratio: float,
        profit: float = 0.0,
        grade: str = "",
        confidence: float = 0.0,
        timeframe: str = "",
        exit_reason: str = "",
        on_execute: Optional[Callable] = None,
        on_watch: Optional[Callable] = None,
        dashboard_url: str = "http://localhost:7000"
    ):
        """
        Sends an interactable toast notification for a trade signal.
        signal_type should be "BUY", "SELL", or "EXIT".
        """
        toast = Toast()
        
        # Audio cues
        toast.audio = ToastAudio(AudioSource.IM)
        
        # Signal extras text
        extras_text = f"TF: {timeframe} | {grade}" if timeframe or grade else ""
        if confidence:
            extras_text += f" ({confidence:.0f}%)"
            
        # Format the text
        if signal_type == "BUY":
            icon = "🟢"
            toast.text_fields = [
                f"{icon} BUY SIGNAL: {instrument}",
                f"Entry: ₹{entry_price:.2f} | {extras_text}" if extras_text else f"Entry: ₹{entry_price:.2f}",
                f"Target: ₹{target_price:.2f} | SL: ₹{sl_price:.2f} | R:R: 1:{rr_ratio:.2f}"
            ]
        elif signal_type == "SELL":
            icon = "🔴"
            toast.text_fields = [
                f"{icon} SELL SIGNAL: {instrument}",
                f"Entry: ₹{entry_price:.2f} | {extras_text}" if extras_text else f"Entry: ₹{entry_price:.2f}",
                f"Target: ₹{target_price:.2f} | SL: ₹{sl_price:.2f} | R:R: 1:{rr_ratio:.2f}"
            ]
        elif signal_type == "EXIT":
            icon = "⚪"
            profit_str = f"+₹{profit:.2f}" if profit >= 0 else f"-₹{abs(profit):.2f}"
            reason_str = f" ({exit_reason.replace('_', ' ').title()})" if exit_reason else ""
            toast.text_fields = [
                f"{icon} EXIT SIGNAL: {instrument}",
                f"Exit Price: ₹{entry_price:.2f}{reason_str}",
                f"P&L: {profit_str} per qty"
            ]
        else:
            return

        def handle_click(args):
            if args.arguments == 'execute':
                logger.info("Execute button clicked on toast.")
                if on_execute:
                    if self.loop and not self.loop.is_closed():
                        self.loop.call_soon_threadsafe(on_execute)
                    else:
                        on_execute()
            elif args.arguments == 'watch':
                logger.info("Watch button clicked on toast.")
                if on_watch:
                    if self.loop and not self.loop.is_closed():
                        self.loop.call_soon_threadsafe(on_watch)
                    else:
                        on_watch()
            elif args.arguments == 'dashboard':
                logger.info("Dashboard button clicked on toast.")
                webbrowser.open(dashboard_url)
                
        if on_execute:
            btn_text = "Execute Exit" if signal_type == "EXIT" else "Execute Trade"
            toast.AddAction(ToastButton(btn_text, arguments='execute'))
            
        if on_watch and signal_type != "EXIT":
            toast.AddAction(ToastButton('Watch [Ghost]', arguments='watch'))
            
        toast.AddAction(ToastButton('View Dashboard', arguments='dashboard'))
        toast.AddAction(ToastButton('Dismiss', arguments='dismiss'))
        
        toast.on_activated = handle_click
        
        try:
            self.toaster.show_toast(toast)
        except Exception as e:
            logger.error(f"Failed to show toast notification: {e}")

# Global instance for easier import if needed
_global_notification_manager = None

def get_notification_manager(loop=None):
    global _global_notification_manager
    if _global_notification_manager is None:
        _global_notification_manager = NotificationManager(loop)
    elif loop is not None:
        _global_notification_manager.loop = loop
    return _global_notification_manager
