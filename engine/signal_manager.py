"""
Signal Manager — Signal Lifecycle & Grading
═══════════════════════════════════════════════════════════════

Manages signal generation, deduplication, grading, and history.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional
from dataclasses import dataclass, field
from loguru import logger
from engine.intelligence_score import normalize_intelligence_score

from engine.ut_bot_core import UTBotSignal


@dataclass
class GradedSignal:
    """A signal with quality grading"""
    signal: UTBotSignal
    grade: str = "B"           # A+, A, B+, B, C
    confidence: float = 0.5    # 0.0 - 1.0
    reasons: List[str] = field(default_factory=list)
    intelligence_score: float = 0.0
    confluence_score: float = 0.0
    is_actionable: bool = True
    created_at: datetime = field(default_factory=datetime.now)


class SignalManager:
    """
    Manages signal lifecycle:
    1. Receives raw signals from UT Bot engines
    2. Deduplicates (same instrument, same direction within N bars)
    3. Grades based on confluence + intelligence
    4. Maintains history for audit trail
    """

    def __init__(self, dedup_window_seconds: int = 300):
        self.dedup_window = timedelta(seconds=dedup_window_seconds)
        self.active_signals: Dict[str, GradedSignal] = {}  # key: instrument
        self.signal_history: List[GradedSignal] = []
        self.max_history = 500

    def process_signal(
        self,
        signal: UTBotSignal,
        confluence_score: float = 0.0,
        intelligence_score: float = 0.0,
        regime: str = "UNKNOWN"
    ) -> Optional[GradedSignal]:
        """
        Process a new signal through grading and deduplication.

        Returns GradedSignal if actionable, None if filtered out.
        """
        # Deduplication: skip if same instrument+direction within window
        key = f"{signal.instrument}_{signal.timeframe}"
        existing = self.active_signals.get(key)
        if existing:
            s_ts = signal.timestamp.replace(tzinfo=None) if signal.timestamp.tzinfo else signal.timestamp
            e_ts = existing.signal.timestamp.replace(tzinfo=None) if existing.signal.timestamp.tzinfo else existing.signal.timestamp
            time_diff = s_ts - e_ts
            if time_diff <= self.dedup_window:
                if existing.signal.signal_type != signal.signal_type:
                    logger.warning(f"Whipsaw deduplication: blocked {signal.signal_type} immediately after {existing.signal.signal_type} on {signal.instrument}")
                return None  # Duplicate or whipsaw within cooldown

        # ── Grading ──
        grade, confidence, reasons = self._grade_signal(
            signal, confluence_score, intelligence_score, regime
        )

        graded = GradedSignal(
            signal=signal,
            grade=grade,
            confidence=confidence,
            reasons=reasons,
            intelligence_score=intelligence_score,
            confluence_score=confluence_score,
            is_actionable=grade in ["A+", "A", "B+", "B"],  # Only C is blocked
        )

        # Update active and history
        self.active_signals[key] = graded
        self.signal_history.append(graded)
        if len(self.signal_history) > self.max_history:
            self.signal_history = self.signal_history[-self.max_history:]

        logger.info(
            f"📊 Signal: {signal.signal_type} {signal.instrument} "
            f"@ {signal.price:.2f} | Grade: {grade} ({confidence:.0%}) "
            f"| ADX: {signal.adx_value:.1f}"
        )

        return graded

    def _grade_signal(
        self,
        signal: UTBotSignal,
        confluence_score: float,
        intelligence_score: float,
        regime: str = "UNKNOWN",
        vix_value: float = 15.0
    ) -> tuple:
        """
        Mixed-Intelligence Grading Engine:
        Combines 3 Pillars:
        1. TECHNICAL (Price/Trend/Momentum)
        2. INTELLIGENCE (Volume/OI/PCR)
        3. REGIME (Market Structure/Context)
        """
        tech_score = 0.0
        
        # ── VIX Patch ──
        vix_penalty = 0.0
        reasons = []
        if vix_value > 20.0:
            vix_penalty = 0.10
            reasons.append(f"VIX High ({vix_value:.1f})")
        elif vix_value < 12.0:
            vix_penalty = -0.05
            reasons.append(f"VIX Low ({vix_value:.1f})")
        intel_score_part = 0.0
        regime_score = 0.0

        # ── Pillar 1: TECHNICAL (40% Weight) ──
        # ADX Momentum
        adx = min(50.0, max(15.0, signal.adx_value))
        if adx > 20:
            adx_score = ((adx - 20) / 30.0) * 0.15
            tech_score += adx_score
            
            # ── ADX Penalization Patch ──
            if adx > 40:
                from config.settings import get_settings
                settings = get_settings()
                if getattr(settings, "ut_dynamic_confidence", False):
                    tech_score -= 0.10
                    reasons.append(f"Momentum: Extreme (ADX: {signal.adx_value:.1f}) - Risk of exhaustion")
                    
            if adx > 30 and adx <= 40:
                reasons.append(f"Momentum: Strong (ADX: {signal.adx_value:.1f})")
            elif adx <= 30:
                reasons.append(f"Momentum: Healthy (ADX: {signal.adx_value:.1f})")
        
        # MTF Confluence
        direction_sign = 1.0 if signal.signal_type == "BUY" else -1.0
        aligned_conf = max(-1.0, min(1.0, float(confluence_score or 0.0) * direction_sign))
        if abs(aligned_conf) > 0.2:
            mtf_score = ((abs(aligned_conf) - 0.2) / 0.8) * 0.20
            tech_score += mtf_score if aligned_conf > 0 else -mtf_score
            if aligned_conf > 0.6:
                reasons.append("Technical: Strong Multi-TF Confluence")
            elif aligned_conf > 0:
                reasons.append("Technical: Moderate Multi-TF Confluence")
            else:
                reasons.append("Technical: Multi-TF trend CONTRADICTS signal")
            
        # Stop Distance
        if signal.stop_distance > 0 and signal.atr_value > 0:
            atr_ratio = signal.stop_distance / signal.atr_value
            if atr_ratio < 1.5:
                stop_score = min(0.05, ((1.5 - atr_ratio) / 1.0) * 0.05)
                tech_score += stop_score
                reasons.append(f"Technical: Tight Risk-Window ({atr_ratio:.1f}x ATR)")

        # ── Pillar 2: INTELLIGENCE (30% Weight) ──
        # Align intelligence score with signal direction
        intelligence_score = normalize_intelligence_score(intelligence_score)
        aligned_intel = intelligence_score if signal.signal_type == "BUY" else -intelligence_score
        
        intel_score_part = max(-0.20, min(0.30, aligned_intel * 0.30))
        
        if aligned_intel > 0.5:
            reasons.append("Intelligence: Institutional Volume/OI Confirmed")
        elif aligned_intel > 0.2:
            reasons.append("Intelligence: Moderate Data Support")
        elif aligned_intel < -0.3:
            reasons.append("Intelligence: Data CONTRADICTS signal")

        # ── Pillar 3: REGIME (30% Weight) ──
        # This is the 'Context' layer requested in Audit
        if regime == "TRENDING_UP":
            if signal.signal_type == "BUY":
                regime_score += 0.30
                reasons.append("Regime: Trend-Following alignment")
            else:
                regime_score -= 0.25
                reasons.append("Regime: COUNTER-TREND (High Risk)")
        elif regime == "TRENDING_DOWN":
            if signal.signal_type == "SELL":
                regime_score += 0.30
                reasons.append("Regime: Trend-Following alignment")
            else:
                regime_score -= 0.25
                reasons.append("Regime: COUNTER-TREND (High Risk)")
        elif regime in ["RANGING", "SIDEWAYS", "VOLATILE", "MEAN_REVERTING"]:
            regime_score -= 0.10
            reasons.append(f"Regime: Range-bound noise ({regime})")
        elif regime == "UNKNOWN":
            regime_score -= 0.30
            reasons.append(f"Regime: UNKNOWN (High Risk Penalty)")

        # ── Pillar 4: MANIPULATION FILTER (Global Penalty) ──
        if hasattr(signal, 'raw_candle'):
            c = signal.raw_candle
            body = abs(c['close'] - c['open'])
            total = c['high'] - c['low']
            if total > 0:
                wick_top = c['high'] - max(c['open'], c['close'])
                wick_bottom = min(c['open'], c['close']) - c['low']
                # Penalize BUY if it has a massive TOP wick (rejection of higher prices)
                if signal.signal_type == "BUY" and wick_top > (body * 2.5):
                    tech_score -= 0.4 
                    reasons.append("⚠️ ALERT: Supply-Trap Wick Detected")
                # Penalize SELL if it has a massive BOTTOM wick (rejection of lower prices)
                elif signal.signal_type == "SELL" and wick_bottom > (body * 2.5):
                    tech_score -= 0.4
                    reasons.append("⚠️ ALERT: SL-Hunt Wick Detected")

        # Final Mixed Score Calculation
        total_score = tech_score + intel_score_part + regime_score - vix_penalty
        confidence = max(0.0, min(1.0, 0.5 + total_score))
        
        # Grading Map (Mixed logic: Need High Tech + High Regime for A+)
        if confidence >= 0.85:
            grade = "A+"
        elif confidence >= 0.70:
            grade = "A"
        elif confidence >= 0.55:
            grade = "B+"
        elif confidence >= 0.40:
            grade = "B"
        else:
            grade = "C"

        return grade, confidence, reasons

    def get_active_signals(self, mode: str = "HISTORICAL", instrument: str = None) -> List[Dict]:
        """Get all current active signals as dicts"""
        result = []
        
        import datetime
        from zoneinfo import ZoneInfo
        IST = ZoneInfo("Asia/Kolkata")
        today = datetime.datetime.now(IST).date()
        
        for key, gs in self.active_signals.items():
            if instrument and gs.signal.instrument != instrument:
                continue
            # In REAL mode, only show signals from today
            if mode == "REAL" and gs.signal.timestamp:
                sig_date = gs.signal.timestamp.date()
                if sig_date != today:
                    continue
                    
            result.append({
                "instrument": gs.signal.instrument,
                "timeframe": gs.signal.timeframe,
                "type": gs.signal.signal_type,
                "price": gs.signal.price,
                "trailing_stop": gs.signal.trailing_stop,
                "stop_distance": gs.signal.stop_distance,
                "grade": gs.grade,
                "confidence": gs.confidence,
                "reasons": gs.reasons,
                "timestamp": gs.signal.timestamp.isoformat() if gs.signal.timestamp else "",
                "is_actionable": gs.is_actionable,
            })
        return result

    def get_signal_stats(self) -> Dict:
        """Get signal statistics"""
        if not self.signal_history:
            return {"total": 0, "buys": 0, "sells": 0, "grade_dist": {}}

        buys = sum(1 for s in self.signal_history if s.signal.signal_type == "BUY")
        sells = sum(1 for s in self.signal_history if s.signal.signal_type == "SELL")
        grades = {}
        for s in self.signal_history:
            grades[s.grade] = grades.get(s.grade, 0) + 1

        return {
            "total": len(self.signal_history),
            "buys": buys,
            "sells": sells,
            "grade_distribution": grades,
            "avg_confidence": sum(s.confidence for s in self.signal_history) / len(self.signal_history),
        }
