import sqlite3
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import json
from loguru import logger
import pytz

IST = pytz.timezone('Asia/Kolkata')

class DatabaseManager:
    """Thread-safe SQLite database manager for trade state and history."""
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, db_path: str = "data_store/utbot_data.db"):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(DatabaseManager, cls).__new__(cls)
                cls._instance._init_db(db_path)
            return cls._instance

    def _init_db(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # We use check_same_thread=False because multiple threads (websocket, scanner, trade sync) might access the DB.
        # But we must serialize writes.
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.write_lock = threading.Lock()
        
        self._create_tables()
        self.cleanup_old_records()


    def cleanup_old_records(self, days=30):
        with self.write_lock:
            try:
                cursor = self.conn.cursor()
                cursor.execute(f"DELETE FROM session_signals WHERE date < date('now', '-{days} days')")
                cursor.execute(f"DELETE FROM signal_decisions WHERE date < date('now', '-{days} days')")
                cursor.execute(
                    f"DELETE FROM trades "
                    f"WHERE COALESCE(exit_timestamp, exit_time) < datetime('now', '-{days} days') "
                    f"AND status = 'CLOSED'"
                )
                self.conn.commit()
                cursor.execute("VACUUM")
                logger.info("Database cleanup completed and vacuumed.")
            except Exception as e:
                logger.error(f"Error during DB cleanup: {e}")
                
    def _create_tables(self):
        with self.write_lock:
            cursor = self.conn.cursor()
            
            # 1. Trades Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id TEXT PRIMARY KEY,
                    instrument TEXT,
                    timeframe TEXT,
                    direction TEXT,
                    entry_price REAL,
                    entry_time TEXT,
                    trailing_stop REAL,
                    current_stop REAL,
                    lots INTEGER,
                    lot_size INTEGER,
                    grade TEXT,
                    entry_timestamp REAL,
                    broker_order_id TEXT,
                    exit_order_id TEXT,
                    execution_status TEXT,
                    execution_error TEXT,
                    pending_execution INTEGER,
                    pending_exit INTEGER,
                    broker_quantity INTEGER,
                    trading_symbol TEXT,
                    symbol_token TEXT,
                    atm_strike REAL,
                    option_type TEXT,
                    target REAL,
                    rr_ratio REAL,
                    confidence REAL,
                    inst_type TEXT,
                    instrument_multiplier REAL,
                    exec_type TEXT,
                    status TEXT,
                    current_price REAL,
                    entry_spot REAL,
                    spot_stop REAL,
                    spot_target REAL,
                    exit_price REAL,
                    exit_time TEXT,
                    exit_timestamp TEXT,
                    exit_reason TEXT,
                    pnl REAL,
                    charges REAL,
                    peak_pnl REAL,
                    max_drawdown REAL
                )
            ''')
            
            self._ensure_columns(
                cursor,
                "trades",
                {
                    "entry_timestamp": "REAL",
                    "exit_timestamp": "TEXT",
                    "exit_order_id": "TEXT",
                    "execution_status": "TEXT",
                    "execution_error": "TEXT",
                    "pending_execution": "INTEGER",
                    "pending_exit": "INTEGER",
                    "broker_quantity": "INTEGER",
                    "is_ghost": "INTEGER",
                },
            )
            
            # 2. Session Signals Table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS session_signals (
                    id TEXT PRIMARY KEY,
                    date TEXT,
                    timestamp TEXT,
                    instrument TEXT,
                    timeframe TEXT,
                    direction TEXT,
                    price REAL,
                    confidence REAL,
                    grade TEXT,
                    executed INTEGER,
                    payload TEXT
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS signal_decisions (
                    id TEXT PRIMARY KEY,
                    date TEXT,
                    timestamp TEXT,
                    instrument TEXT,
                    timeframe TEXT,
                    direction TEXT,
                    status TEXT,
                    reason TEXT,
                    message TEXT,
                    source TEXT,
                    payload TEXT
                )
            ''')
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_signal_decisions_date "
                "ON signal_decisions(date, timestamp)"
            )
            
            self.conn.commit()

    def _ensure_columns(self, cursor: sqlite3.Cursor, table: str, columns: Dict[str, str]) -> None:
        """Add missing columns for DBs created by older restore points."""
        existing = {row[1] for row in cursor.execute(f"PRAGMA table_info({table})").fetchall()}
        for name, column_type in columns.items():
            if name not in existing:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}")
                logger.info(f"Added missing DB column {table}.{name}")

    def save_trade(self, trade_dict: dict):
        """Insert or replace a trade record."""
        # SQLite doesn't natively support datetime objects without adapters, 
        # so we ensure entry_time and exit_time are strings.
        VALID_COLUMNS = {"id", "instrument", "timeframe", "direction", "entry_price", "entry_time", "trailing_stop", "current_stop", "lots", "lot_size", "grade", "entry_timestamp", "exit_timestamp", "broker_order_id", "exit_order_id", "execution_status", "execution_error", "pending_execution", "pending_exit", "broker_quantity", "trading_symbol", "symbol_token", "atm_strike", "option_type", "target", "rr_ratio", "confidence", "inst_type", "instrument_multiplier", "exec_type", "status", "current_price", "entry_spot", "spot_stop", "spot_target", "exit_price", "exit_time", "exit_reason", "pnl", "charges", "peak_pnl", "max_drawdown", "is_ghost"}
        filtered_dict = {k: v for k, v in trade_dict.items() if k in VALID_COLUMNS}

        with self.write_lock:
            cursor = self.conn.cursor()
            
            columns = ', '.join(filtered_dict.keys())
            placeholders = ', '.join(['?'] * len(filtered_dict))
            
            # Handle datetime serialization if they are passed as raw datetime objects
            # However, trade.to_dict() mostly formats them. Let's make sure.
            values = []
            for k, v in filtered_dict.items():
                if isinstance(v, datetime):
                    values.append(v.isoformat())
                elif v == "--" and k in ('entry_time', 'exit_time'):
                    values.append(None)
                else:
                    values.append(v)

            sql = f'''
                INSERT OR REPLACE INTO trades ({columns}) 
                VALUES ({placeholders})
            '''
            try:
                cursor.execute(sql, values)
                self.conn.commit()
            except Exception as e:
                logger.error(f"Error saving trade to DB: {e}")

    def save_trades(self, trade_dicts: List[Dict]) -> None:
        """Persist a trade snapshot in one transaction instead of one commit per row."""
        valid_columns = {
            "id", "instrument", "timeframe", "direction", "entry_price", "entry_time",
            "trailing_stop", "current_stop", "lots", "lot_size", "grade",
            "entry_timestamp", "exit_timestamp", "broker_order_id", "exit_order_id",
            "execution_status", "execution_error", "pending_execution", "pending_exit",
            "broker_quantity", "trading_symbol", "symbol_token",
            "atm_strike", "option_type", "target", "rr_ratio", "confidence",
            "inst_type", "instrument_multiplier", "exec_type", "status",
            "current_price", "entry_spot", "spot_stop", "spot_target", "exit_price",
            "exit_time", "exit_reason", "pnl", "charges", "peak_pnl", "max_drawdown", "is_ghost",
        }
        grouped: Dict[tuple, List[tuple]] = {}
        for trade_dict in trade_dicts:
            filtered = {key: value for key, value in trade_dict.items() if key in valid_columns}
            if not filtered:
                continue
            columns = tuple(filtered)
            values = []
            for key in columns:
                value = filtered[key]
                if isinstance(value, datetime):
                    value = value.isoformat()
                elif value == "--" and key in ("entry_time", "exit_time"):
                    value = None
                values.append(value)
            grouped.setdefault(columns, []).append(tuple(values))

        if not grouped:
            return
        with self.write_lock:
            cursor = self.conn.cursor()
            try:
                for columns, rows in grouped.items():
                    column_sql = ", ".join(columns)
                    placeholders = ", ".join("?" for _ in columns)
                    cursor.executemany(
                        f"INSERT OR REPLACE INTO trades ({column_sql}) VALUES ({placeholders})",
                        rows,
                    )
                self.conn.commit()
            except Exception as exc:
                self.conn.rollback()
                logger.error(f"Error saving trade snapshot to DB: {exc}")

    def load_all_trades(self) -> List[Dict]:
        """Load all trades from the database."""
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM trades")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def load_open_trades(self) -> List[Dict]:
        """Load only open trades from the database."""
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM trades WHERE status = 'OPEN'")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]
            
    def load_closed_trades(self) -> List[Dict]:
        """Load only closed trades from the database."""
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM trades WHERE status = 'CLOSED'")
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def save_session_signals(self, date_str: str, signals: List[Dict]):
        """Replace the saved session candidates for one trading date."""
        with self.write_lock:
            cursor = self.conn.cursor()
            rows_to_save = []
            for signal_dict in signals:
                payload_day = self._payload_session_date(signal_dict)
                if payload_day and payload_day != date_str:
                    logger.warning(
                        f"Skipping stale session signal payload for {payload_day}; active ledger date is {date_str}"
                    )
                    continue
                sig_id = str(signal_dict.get("id") or signal_dict.get("trade_id") or "")
                if not sig_id:
                    sig_id = "|".join(
                        [
                            "SESSION",
                            str(signal_dict.get("instrument") or ""),
                            str(signal_dict.get("timeframe") or ""),
                            str(signal_dict.get("action") or "ENTRY"),
                            str(signal_dict.get("direction") or signal_dict.get("type") or ""),
                            str(signal_dict.get("timestamp") or signal_dict.get("signal_timestamp") or ""),
                        ]
                    )
                    signal_dict = dict(signal_dict)
                    signal_dict["id"] = sig_id
                rows_to_save.append((sig_id, signal_dict))

            cursor.execute("SELECT COUNT(*) AS row_count FROM session_signals WHERE date = ?", (date_str,))
            existing = int(cursor.fetchone()["row_count"] or 0)
            if existing and not rows_to_save:
                logger.warning(
                    f"Refusing to replace {existing} persisted session signal row(s) "
                    f"for {date_str} with an empty/stale transient ledger"
                )
                return
            cursor.execute("DELETE FROM session_signals WHERE date = ?", (date_str,))
            for sig_id, signal_dict in rows_to_save:
                payload_str = json.dumps(signal_dict)
                cursor.execute('''
                    INSERT OR REPLACE INTO session_signals 
                    (id, date, timestamp, instrument, timeframe, direction, price, confidence, grade, executed, payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    sig_id,
                    date_str,
                    signal_dict.get('timestamp'),
                    signal_dict.get('instrument'),
                    signal_dict.get('timeframe'),
                    signal_dict.get('direction', signal_dict.get('type')),
                    signal_dict.get('price', 0.0),
                    signal_dict.get('confidence', 0.0),
                    signal_dict.get('grade', ''),
                    1 if signal_dict.get('executed') else 0,
                    payload_str
                ))
            self.conn.commit()

    def load_session_signals(self, date_str: str) -> List[Dict]:
        """Retrieve all session candidates for a given date."""
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, payload FROM session_signals WHERE date = ?", (date_str,))
            rows = cursor.fetchall()
            signals = []
            stale_ids = []
            for row in rows:
                try:
                    payload = json.loads(row['payload'])
                except json.JSONDecodeError:
                    stale_ids.append(row['id'])
                    continue
                payload_day = self._payload_session_date(payload)
                if payload_day and payload_day != date_str:
                    stale_ids.append(row['id'])
                    continue
                signals.append(payload)
            if stale_ids:
                cursor.executemany("DELETE FROM session_signals WHERE id = ?", [(sid,) for sid in stale_ids])
                self.conn.commit()
                logger.warning(f"Purged {len(stale_ids)} stale/corrupt session signal rows for {date_str}")
            return signals

    def list_session_signal_dates(self) -> List[str]:
        """Return persisted session ledger dates in ascending order."""
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT DISTINCT date FROM session_signals ORDER BY date")
            return [str(row["date"]) for row in cursor.fetchall() if row["date"]]

    def save_signal_decision(self, decision: Dict[str, Any]) -> None:
        """Persist one live rejection/block decision for daily reporting."""
        timestamp = str(decision.get("timestamp") or datetime.now(IST).isoformat())
        date_str = str(decision.get("date") or timestamp[:10])
        decision_id = str(decision.get("id") or "")
        if not decision_id:
            decision_id = "|".join(
                [
                    date_str,
                    timestamp,
                    str(decision.get("instrument") or ""),
                    str(decision.get("timeframe") or ""),
                    str(decision.get("direction") or ""),
                    str(decision.get("message") or ""),
                ]
            )
        payload = dict(decision)
        payload["id"] = decision_id
        payload["date"] = date_str
        payload["timestamp"] = timestamp
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute(
                '''
                INSERT OR IGNORE INTO signal_decisions
                (id, date, timestamp, instrument, timeframe, direction, status, reason, message, source, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (
                    decision_id,
                    date_str,
                    timestamp,
                    decision.get("instrument", ""),
                    decision.get("timeframe", ""),
                    decision.get("direction", ""),
                    decision.get("status", "REJECTED"),
                    decision.get("reason", ""),
                    decision.get("message", ""),
                    decision.get("source", "runtime"),
                    json.dumps(payload),
                ),
            )
            self.conn.commit()

    def load_signal_decisions(self, date_str: str) -> List[Dict[str, Any]]:
        """Load persisted rejection/block decisions for one session date."""
        with self.write_lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT * FROM signal_decisions WHERE date = ? ORDER BY timestamp",
                (date_str,),
            )
            rows = []
            for row in cursor.fetchall():
                item = dict(row)
                try:
                    payload = json.loads(item.get("payload") or "{}")
                    if isinstance(payload, dict):
                        item.update(payload)
                except Exception:
                    pass
                rows.append(item)
            return rows

    @staticmethod
    def _payload_session_date(payload: Dict) -> Optional[str]:
        """Return the IST session date encoded in a persisted signal payload."""
        raw_ts = payload.get("signal_timestamp") or payload.get("timestamp")
        if not raw_ts:
            return None
        if isinstance(raw_ts, datetime):
            ts = raw_ts
        else:
            raw = str(raw_ts).strip()
            if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
                try:
                    ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except ValueError:
                    return raw[:10]
            else:
                return None
        if ts.tzinfo is not None:
            return ts.astimezone(IST).date().isoformat()
        return ts.date().isoformat()

# Global instance
db = DatabaseManager()
