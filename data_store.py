import time
import sqlite3
import logging
import threading
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import config as cfg


@dataclass
class RepeaterState:
    name: str = ""
    pubkey: str = ""
    battery_mv: int = 0
    battery_voltage: float = 0.0
    rssi: int = 0
    snr: float = 0.0
    noise_floor: int = 0
    temperature: float = 0.0
    uptime_seconds: int = 0
    packets_recv: int = 0
    packets_sent: int = 0
    packets_recv_direct: int = 0
    packets_recv_flood: int = 0
    packets_sent_direct: int = 0
    packets_sent_flood: int = 0
    hops: int = 0
    route_path: str = ""
    lat: float = 0.0
    lon: float = 0.0
    fw_version: str = ""
    last_seen_epoch: float = 0.0
    last_poll_ok: Optional[bool] = None
    show_public: bool = False

    def to_dict(self) -> dict:
        d = asdict(self)
        d["online"] = self.is_online
        d["poll_ok"] = self.last_poll_ok
        d["pubkey_short"] = self.pubkey[:12] if self.pubkey else ""
        return d

    @property
    def is_online(self) -> bool:
        return self.last_poll_ok is True


class SQLiteLogHandler(logging.Handler):
    def __init__(self, db_path: str, username: str):
        super().__init__()
        self.db_path = db_path
        self.username = username

    def emit(self, record: logging.LogRecord):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO activity_log (timestamp, level, logger_name, message, username) "
                "VALUES (?, ?, ?, ?, ?)",
                (record.created, record.levelname, record.name, self.format(record), self.username),
            )
            conn.commit()
            conn.close()
        except Exception:
            self.handleError(record)


class DataStore:
    """Per-user data store, namespaced by username in the shared SQLite DB."""

    def __init__(self, username: str):
        self.username = username
        self._lock = threading.Lock()
        self._repeaters: Dict[str, RepeaterState] = {}
        self._db_path = cfg.HISTORY_DB if cfg.ENABLE_HISTORY else None
        if self._db_path:
            self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self._db_path)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS telemetry_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                pubkey TEXT NOT NULL,
                name TEXT,
                battery_mv INTEGER,
                battery_voltage REAL,
                temperature REAL,
                rssi INTEGER,
                snr REAL,
                noise_floor INTEGER,
                uptime_seconds INTEGER,
                packets_recv INTEGER,
                packets_sent INTEGER,
                packets_recv_direct INTEGER,
                packets_recv_flood INTEGER,
                packets_sent_direct INTEGER,
                packets_sent_flood INTEGER
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_telemetry_user_pubkey_ts
            ON telemetry_log (username, pubkey, timestamp)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                level TEXT NOT NULL,
                logger_name TEXT NOT NULL,
                message TEXT NOT NULL,
                username TEXT NOT NULL DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_activity_log_ts
            ON activity_log (timestamp)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                direction TEXT NOT NULL,
                channel_idx INTEGER,
                sender_pubkey TEXT,
                sender_name TEXT,
                text TEXT NOT NULL,
                hops INTEGER DEFAULT -1,
                path TEXT DEFAULT '',
                ack_code TEXT DEFAULT '',
                acks INTEGER DEFAULT 0
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (timestamp)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS node_names (
                username TEXT NOT NULL DEFAULT '',
                node_id TEXT NOT NULL,
                name TEXT NOT NULL,
                updated REAL NOT NULL,
                PRIMARY KEY (username, node_id)
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS advert_nodes (
                username TEXT NOT NULL DEFAULT '',
                pubkey TEXT NOT NULL,
                name TEXT NOT NULL,
                lat REAL,
                lon REAL,
                last_seen REAL NOT NULL,
                PRIMARY KEY (username, pubkey)
            )
        """)

        # Migration: add username column to older tables if missing
        for table in ("telemetry_log", "activity_log", "messages"):
            try:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN username TEXT NOT NULL DEFAULT ''")
            except Exception:
                pass

        # Migration: add extended packet/telemetry columns
        extra_cols = [
            ("temperature", "REAL"),
            ("noise_floor", "INTEGER"),
            ("packets_recv", "INTEGER"),
            ("packets_sent", "INTEGER"),
            ("packets_recv_direct", "INTEGER"),
            ("packets_recv_flood", "INTEGER"),
            ("packets_sent_direct", "INTEGER"),
            ("packets_sent_flood", "INTEGER"),
        ]
        for col, typ in extra_cols:
            try:
                conn.execute(f"ALTER TABLE telemetry_log ADD COLUMN {col} {typ}")
            except Exception:
                pass

        # Migration: messages extra columns
        for col, definition in [
            ("hops", "INTEGER DEFAULT -1"),
            ("path", "TEXT DEFAULT ''"),
            ("ack_code", "TEXT DEFAULT ''"),
            ("acks", "INTEGER DEFAULT 0"),
        ]:
            try:
                conn.execute(f"ALTER TABLE messages ADD COLUMN {col} {definition}")
            except Exception:
                pass

        conn.commit()
        conn.close()

    # ---- Repeater CRUD ----

    def init_repeater(self, pubkey: str, name: str, show_public: bool = False):
        with self._lock:
            if pubkey not in self._repeaters:
                self._repeaters[pubkey] = RepeaterState(name=name, pubkey=pubkey, show_public=show_public)
            else:
                self._repeaters[pubkey].name = name
                self._repeaters[pubkey].show_public = show_public

    def remove_repeater(self, pubkey: str):
        with self._lock:
            self._repeaters.pop(pubkey, None)

    def sync_repeaters(self, configured: list):
        configured_keys = {r["pubkey"] for r in configured}
        with self._lock:
            for pk in list(self._repeaters.keys()):
                if pk not in configured_keys:
                    del self._repeaters[pk]
        for r in configured:
            self.init_repeater(r["pubkey"], r["name"], r.get("show_public", False))

    def reorder(self, pubkeys: list):
        with self._lock:
            ordered = {pk: self._repeaters[pk] for pk in pubkeys if pk in self._repeaters}
            for pk, v in self._repeaters.items():
                if pk not in ordered:
                    ordered[pk] = v
            self._repeaters = ordered

    def update_hops(self, pubkey: str, hops: int):
        with self._lock:
            if pubkey in self._repeaters:
                self._repeaters[pubkey].hops = hops

    def update_route(self, pubkey: str, hops: int, route_path: str):
        with self._lock:
            if pubkey in self._repeaters:
                self._repeaters[pubkey].hops = hops
                self._repeaters[pubkey].route_path = route_path

    def get_route_by_prefix(self, pubkey_prefix: str) -> tuple:
        if not pubkey_prefix:
            return (-1, "")
        pre = pubkey_prefix.lower()
        with self._lock:
            for pk, state in self._repeaters.items():
                if pk.lower().startswith(pre) or pre.startswith(pk.lower()):
                    if state.route_path or state.hops >= 0:
                        return (state.hops, state.route_path)
        return (-1, "")

    def update_location(self, pubkey: str, lat: float, lon: float):
        with self._lock:
            if pubkey in self._repeaters:
                self._repeaters[pubkey].lat = lat
                self._repeaters[pubkey].lon = lon

    def mark_poll_failed(self, pubkey: str):
        with self._lock:
            if pubkey in self._repeaters:
                self._repeaters[pubkey].last_poll_ok = False

    def update_repeater(self, pubkey: str, **kwargs):
        with self._lock:
            if pubkey not in self._repeaters:
                self._repeaters[pubkey] = RepeaterState(pubkey=pubkey)
            r = self._repeaters[pubkey]
            for k, v in kwargs.items():
                if hasattr(r, k) and v is not None:
                    setattr(r, k, v)
            r.last_seen_epoch = time.time()
            r.last_poll_ok = True

        if self._db_path:
            self._log_to_db(pubkey)

    def _log_to_db(self, pubkey: str):
        with self._lock:
            r = self._repeaters.get(pubkey)
            if not r:
                return
            row = (
                time.time(), self.username, r.pubkey, r.name,
                r.battery_mv, r.battery_voltage, r.temperature,
                r.rssi, r.snr, r.noise_floor, r.uptime_seconds,
                r.packets_recv, r.packets_sent,
                r.packets_recv_direct, r.packets_recv_flood,
                r.packets_sent_direct, r.packets_sent_flood,
            )
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute(
                "INSERT INTO telemetry_log "
                "(timestamp, username, pubkey, name, battery_mv, battery_voltage, temperature, "
                "rssi, snr, noise_floor, uptime_seconds, packets_recv, packets_sent, "
                "packets_recv_direct, packets_recv_flood, packets_sent_direct, packets_sent_flood) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                row,
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DataStore] DB write error: {e}")

    def get_all(self) -> List[dict]:
        with self._lock:
            return [r.to_dict() for r in self._repeaters.values()]

    def get_history(self, pubkey: str, hours: int = 168) -> List[dict]:
        """Return historical telemetry for a repeater over the last N hours (default 7 days)."""
        if not self._db_path:
            return []
        since = time.time() - (hours * 3600)
        try:
            conn = sqlite3.connect(self._db_path)
            rows = conn.execute(
                "SELECT timestamp, battery_mv, battery_voltage, temperature, rssi, snr, "
                "noise_floor, uptime_seconds, packets_recv, packets_sent, "
                "packets_recv_direct, packets_recv_flood, packets_sent_direct, packets_sent_flood "
                "FROM telemetry_log "
                "WHERE username = ? AND pubkey = ? AND timestamp > ? "
                "ORDER BY timestamp",
                (self.username, pubkey, since),
            ).fetchall()
            conn.close()
            return [
                {
                    "ts": row[0],
                    "battery_mv": row[1],
                    "battery_v": row[2],
                    "temperature": row[3],
                    "rssi": row[4],
                    "snr": row[5],
                    "noise_floor": row[6],
                    "uptime": row[7],
                    "packets_recv": row[8],
                    "packets_sent": row[9],
                    "packets_recv_direct": row[10],
                    "packets_recv_flood": row[11],
                    "packets_sent_direct": row[12],
                    "packets_sent_flood": row[13],
                }
                for row in rows
            ]
        except Exception as e:
            print(f"[DataStore] DB read error: {e}")
            return []

    def get_log_handler(self) -> logging.Handler:
        if not self._db_path:
            return logging.NullHandler()
        handler = SQLiteLogHandler(self._db_path, self.username)
        handler.setFormatter(logging.Formatter("%(message)s"))
        return handler

    def get_activity_logs(self, hours: int = 24, level: str = None, search: str = None, limit: int = 500) -> list:
        if not self._db_path:
            return []
        since = time.time() - (hours * 3600)
        try:
            conn = sqlite3.connect(self._db_path)
            where = "WHERE username = ? AND timestamp > ?"
            params: list = [self.username, since]
            if level:
                where += " AND level = ?"
                params.append(level.upper())
            if search:
                where += " AND message LIKE ?"
                params.append(f"%{search}%")
            params.append(limit)
            rows = conn.execute(
                f"SELECT id, timestamp, level, logger_name, message "
                f"FROM activity_log {where} "
                f"ORDER BY timestamp DESC LIMIT ?",
                params,
            ).fetchall()
            conn.close()
            return [{"id": r[0], "ts": r[1], "level": r[2], "logger": r[3], "message": r[4]} for r in rows]
        except Exception as e:
            print(f"[DataStore] Activity log read error: {e}")
            return []

    def store_message(self, direction: str, channel_idx, sender_pubkey: str, sender_name: str, text: str,
                      hops: int = -1, path: str = "", ack_code: str = "") -> bool:
        if not self._db_path:
            return True
        try:
            conn = sqlite3.connect(self._db_path)
            since = time.time() - 300
            existing = conn.execute(
                "SELECT id FROM messages WHERE username=? AND direction=? AND channel_idx IS ? "
                "AND sender_pubkey=? AND text=? AND timestamp > ?",
                (self.username, direction, channel_idx, sender_pubkey or "", text, since),
            ).fetchone()
            if existing:
                conn.close()
                return False
            conn.execute(
                "INSERT INTO messages "
                "(timestamp, username, direction, channel_idx, sender_pubkey, sender_name, text, hops, path, ack_code) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (time.time(), self.username, direction, channel_idx, sender_pubkey or "", sender_name or "", text,
                 hops, path or "", ack_code or ""),
            )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            print(f"[DataStore] Message store error: {e}")
            return True

    def increment_message_acks(self, ack_code: str) -> int:
        if not self._db_path or not ack_code:
            return 0
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute(
                "UPDATE messages SET acks = acks + 1 "
                "WHERE username = ? AND ack_code = ? AND direction = 'out'",
                (self.username, ack_code),
            )
            row = conn.execute(
                "SELECT acks FROM messages WHERE username = ? AND ack_code = ? AND direction = 'out'",
                (self.username, ack_code),
            ).fetchone()
            conn.commit()
            conn.close()
            return row[0] if row else 0
        except Exception as e:
            print(f"[DataStore] ACK update error: {e}")
            return 0

    def get_messages(self, channel_idx=None, hours: int = 48, limit: int = 200) -> list:
        if not self._db_path:
            return []
        since = time.time() - (hours * 3600)
        try:
            conn = sqlite3.connect(self._db_path)
            if channel_idx is not None:
                rows = conn.execute(
                    "SELECT id, timestamp, direction, channel_idx, sender_pubkey, sender_name, "
                    "text, hops, path, acks, ack_code "
                    "FROM messages WHERE username=? AND timestamp > ? AND channel_idx = ? "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (self.username, since, channel_idx, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, timestamp, direction, channel_idx, sender_pubkey, sender_name, "
                    "text, hops, path, acks, ack_code "
                    "FROM messages WHERE username=? AND timestamp > ? "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (self.username, since, limit),
                ).fetchall()
            conn.close()
            return [
                {
                    "id": row[0], "ts": row[1], "direction": row[2],
                    "channel_idx": row[3], "sender_pubkey": row[4],
                    "sender_name": row[5], "text": row[6],
                    "hops": row[7] if row[7] is not None else -1,
                    "path": row[8] or "", "acks": row[9] or 0, "ack_code": row[10] or "",
                }
                for row in rows
            ]
        except Exception as e:
            print(f"[DataStore] Message read error: {e}")
            return []

    def upsert_advert_node(self, pubkey: str, name: str, lat: float = None, lon: float = None):
        if not self._db_path or not pubkey or not name:
            return
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute(
                """INSERT INTO advert_nodes (username, pubkey, name, lat, lon, last_seen)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(username, pubkey) DO UPDATE SET
                     name=excluded.name,
                     lat=COALESCE(excluded.lat, advert_nodes.lat),
                     lon=COALESCE(excluded.lon, advert_nodes.lon),
                     last_seen=excluded.last_seen""",
                (self.username, pubkey, name, lat, lon, time.time())
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DataStore] Advert node upsert error: {e}")

    def get_advert_nodes(self) -> list:
        if not self._db_path:
            return []
        try:
            conn = sqlite3.connect(self._db_path)
            rows = conn.execute(
                "SELECT pubkey, name, lat, lon, last_seen FROM advert_nodes "
                "WHERE username = ? ORDER BY last_seen DESC",
                (self.username,),
            ).fetchall()
            conn.close()
            return [{"pubkey": r[0], "name": r[1], "lat": r[2], "lon": r[3], "last_seen": r[4]} for r in rows]
        except Exception as e:
            print(f"[DataStore] Advert nodes read error: {e}")
            return []

    def load_node_names(self) -> dict:
        if not self._db_path:
            return {}
        try:
            conn = sqlite3.connect(self._db_path)
            rows = conn.execute(
                "SELECT node_id, name FROM node_names WHERE username = ?", (self.username,)
            ).fetchall()
            conn.close()
            return {row[0]: row[1] for row in rows}
        except Exception as e:
            print(f"[DataStore] Node names load error: {e}")
            return {}

    def save_node_names(self, cache: dict):
        if not self._db_path or not cache:
            return
        try:
            now = time.time()
            conn = sqlite3.connect(self._db_path)
            conn.executemany(
                "INSERT OR REPLACE INTO node_names (username, node_id, name, updated) VALUES (?, ?, ?, ?)",
                [(self.username, node_id, name, now) for node_id, name in cache.items()]
            )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DataStore] Node names save error: {e}")

    def prune_activity_logs(self, retention_hours: int):
        if not self._db_path:
            return
        cutoff = time.time() - (retention_hours * 3600)
        try:
            conn = sqlite3.connect(self._db_path)
            conn.execute("DELETE FROM activity_log WHERE username = ? AND timestamp < ?", (self.username, cutoff))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[DataStore] Activity log prune error: {e}")
