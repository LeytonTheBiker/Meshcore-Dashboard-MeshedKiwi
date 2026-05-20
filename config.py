# ============================================================
# MeshCore Repeater Dashboard - Multi-Tenant Configuration
# ============================================================

import json
import hashlib
import secrets
import time
from pathlib import Path

BASE_DIR = Path(__file__).parent
_USERS_FILE = BASE_DIR / "users.json"
_SESSIONS_FILE = BASE_DIR / "sessions.json"

# --- History ---
ENABLE_HISTORY = True
HISTORY_DB = str(BASE_DIR / "repeater_history.db")


# ---- Defaults ----

def _default_settings() -> dict:
    return {
        "companion_host": "192.168.1.100",
        "companion_port": 5000,
        "repeaters": [],
        "poll_interval_seconds": 3600,
        "stagger_delay_seconds": 30,
        "stale_threshold_seconds": 900,
        "low_battery_percent": 20,
        "log_retention_hours": 24,
        "map_path_max_km": 300,
        "node_id_chars": 2,
        "channels": [{"name": "Primary", "idx": 0}],
        "ntfy_topic": "",
        "ntfy_server": "https://ntfy.sh",
        "ntfy_enabled": True,
        "dashboard_url": "",
        "home_lat": 0.0,
        "home_lon": 0.0,
    }


# ---- User Management ----

def _load_users() -> dict:
    if _USERS_FILE.exists():
        try:
            with open(_USERS_FILE, "r") as f:
                data = json.load(f)
            # Validate it looks like a users dict (has at least one user entry with password_hash)
            if isinstance(data, dict) and any(
                isinstance(v, dict) and "password_hash" in v for v in data.values()
            ):
                return data
        except Exception:
            pass

    # Check for legacy single-user settings.json from the original dashboard
    legacy_settings = _default_settings()
    legacy_file = BASE_DIR / "settings.json"
    if legacy_file.exists():
        try:
            with open(legacy_file, "r") as f:
                legacy = json.load(f)
            # Old format has companion_host, repeaters etc directly at the top level
            if isinstance(legacy, dict) and (
                "companion_host" in legacy or "repeaters" in legacy
            ):
                # Merge legacy settings over defaults
                merged = {**_default_settings(), **legacy}
                legacy_settings = merged
                import logging as _log
                _log.getLogger("config").info(
                    f"Imported legacy settings.json into admin account "
                    f"({len(legacy.get('repeaters', []))} repeater(s))"
                )
        except Exception as e:
            import logging as _log
            _log.getLogger("config").warning(f"Could not read legacy settings.json: {e}")

    default_admin = {
        "admin": {
            "password_hash": _hash_password("admin"),
            "settings": legacy_settings,
        }
    }
    _save_users(default_admin)
    return default_admin


def _save_users(users: dict):
    with open(_USERS_FILE, "w") as f:
        json.dump(users, f, indent=2)


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


# ---- Session Management ----

def _load_sessions() -> dict:
    if _SESSIONS_FILE.exists():
        try:
            with open(_SESSIONS_FILE, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_sessions(sessions: dict):
    with open(_SESSIONS_FILE, "w") as f:
        json.dump(sessions, f, indent=2)


def create_session(username: str) -> str:
    token = secrets.token_hex(32)
    sessions = _load_sessions()
    sessions[token] = {"username": username, "created": time.time()}
    _save_sessions(sessions)
    return token


def validate_session(token: str):
    """Returns username if valid session, else None."""
    if not token:
        return None
    sessions = _load_sessions()
    session = sessions.get(token)
    if not session:
        return None
    if time.time() - session.get("created", 0) > 7 * 86400:
        del sessions[token]
        _save_sessions(sessions)
        return None
    return session["username"]


def invalidate_session(token: str):
    sessions = _load_sessions()
    sessions.pop(token, None)
    _save_sessions(sessions)


def authenticate_user(username: str, password: str) -> bool:
    users = _load_users()
    user = users.get(username)
    if not user:
        return False
    return user["password_hash"] == _hash_password(password)


def change_password(username: str, new_password: str) -> bool:
    users = _load_users()
    if username not in users:
        return False
    users[username]["password_hash"] = _hash_password(new_password)
    _save_users(users)
    return True


def list_users() -> list:
    users = _load_users()
    return list(users.keys())


def create_user(username: str, password: str) -> bool:
    users = _load_users()
    if username in users:
        return False
    users[username] = {
        "password_hash": _hash_password(password),
        "settings": _default_settings(),
    }
    _save_users(users)
    return True


def delete_user(username: str) -> bool:
    users = _load_users()
    if username not in users or username == "admin":
        return False
    del users[username]
    _save_users(users)
    return True


# ---- Per-User Settings ----

def get_settings(username: str) -> dict:
    users = _load_users()
    user = users.get(username, {})
    saved = user.get("settings", {})
    return {**_default_settings(), **saved}


def save_settings(username: str, settings: dict):
    users = _load_users()
    if username not in users:
        return
    users[username]["settings"] = settings
    _save_users(users)


def get_companion_host(username: str) -> str:
    return get_settings(username)["companion_host"]

def get_companion_port(username: str) -> int:
    return get_settings(username)["companion_port"]

def get_repeaters(username: str) -> list:
    return get_settings(username)["repeaters"]

def get_poll_interval(username: str) -> int:
    return get_settings(username)["poll_interval_seconds"]

def get_stagger_delay(username: str) -> int:
    return get_settings(username)["stagger_delay_seconds"]

def get_stale_threshold(username: str) -> int:
    return get_settings(username)["stale_threshold_seconds"]

def get_low_battery_percent(username: str) -> int:
    return get_settings(username).get("low_battery_percent", 20)

def get_log_retention_hours(username: str) -> int:
    return get_settings(username).get("log_retention_hours", 24)

def get_channels(username: str) -> list:
    return get_settings(username).get("channels", [{"name": "Primary", "idx": 0}])


def get_public_repeaters() -> list:
    """Return all repeaters across all users that have show_public=True."""
    users = _load_users()
    result = []
    for username, user_data in users.items():
        settings = user_data.get("settings", {})
        for r in settings.get("repeaters", []):
            if r.get("show_public", False):
                result.append({**r, "owner": username})
    return result
