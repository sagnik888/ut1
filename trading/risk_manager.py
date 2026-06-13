"""Compatibility shim for the unified risk manager.

The active implementation lives in engine.risk_manager. Keeping this import
path prevents older scripts from accidentally reintroducing divergent risk
rules.
"""

from engine.risk_manager import RiskManager

__all__ = ["RiskManager"]

