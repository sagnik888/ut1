"""Canonical intelligence-score units used by live and historical paths."""

from __future__ import annotations


def normalize_intelligence_score(value: float) -> float:
    """Normalize legacy -100..100 scores and canonical -1..1 scores to -1..1."""
    score = float(value or 0.0)
    if abs(score) > 1.0:
        score /= 100.0
    return max(-1.0, min(1.0, score))

