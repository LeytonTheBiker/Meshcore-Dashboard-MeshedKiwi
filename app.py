import asyncio
import json
import logging
import os
import signal
import tempfile
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, File, Request, UploadFile, Response, Cookie
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import config as cfg
from data_store import DataStore
from meshcore_poller import MeshcorePoller

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("app")

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# ---- Per-user state ----
# { username: {"store": DataStore, "poller": MeshcorePoller, "task": asyncio.Task} }
_user_instances: dict = {}
_instances_lock = asyncio.Lock()


async def get_or_create_instance(username: str) -> dict:
    async with _instances_lock:
        if username not in _user_instances:
            store = DataStore(username)
            # Pre-populate store from config so dashboard shows nodes immediately,
            # even before the poller connects to the companion.
            for r in cfg.get_repeaters(username):
                if r.get("pubkey") and r.get("name"):
                    store.init_repeater(r["pubkey"], r["name"], r.get("show_public", False))
            poller = MeshcorePoller(store, username)
            log_handler = store.get_log_handler()
            logging.getLogger().addHandler(log_handler)
            task = asyncio.create_task(poller.start())
            _user_instances[username] = {
                "store": store,
                "poller": poller,
                "task": task,
            }
            logger.info(f"[{username}] Poller started")
        return _user_instances[username]


async def stop_instance(username: str):
    async with _instances_lock:
        inst = _user_instances.pop(username, None)
        if inst:
            await inst["poller"].stop()
            inst["task"].cancel()
            try:
                await inst["task"]
            except asyncio.CancelledError:
                pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start pollers for all existing users
    for username in cfg.list_users():
        await get_or_create_instance(username)

    async def prune_logs_periodically():
        while True:
            await asyncio.sleep(3600)
            async with _instances_lock:
                for username, inst in _user_instances.items():
                    retention = cfg.get_log_retention_hours(username)
                    inst["store"].prune_activity_logs(retention)

    prune_task = asyncio.create_task(prune_logs_periodically())
    yield
    prune_task.cancel()
    for username in list(_user_instances.keys()):
        await stop_instance(username)


app = FastAPI(title="MeshCore Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


# ---- Auth helpers ----

def get_current_user(request: Request):
    token = request.cookies.get("session")
    if not token:
        return None
    return cfg.validate_session(token)


def require_auth(request: Request):
    """Returns username or raises redirect."""
    user = get_current_user(request)
    return user


# ---- Auth Routes ----

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    html_path = BASE_DIR / "templates" / "login.html"
    return html_path.read_text()


@app.post("/login")
async def login(request: Request):
    body = await request.json()
    username = body.get("username", "").strip()
    password = body.get("password", "")
    if not cfg.authenticate_user(username, password):
        return JSONResponse({"ok": False, "error": "Invalid username or password"})
    token = cfg.create_session(username)
    response = JSONResponse({"ok": True})
    response.set_cookie("session", token, httponly=True, max_age=7 * 86400)
    return response


@app.post("/logout")
async def logout(request: Request):
    token = request.cookies.get("session")
    if token:
        cfg.invalidate_session(token)
    response = RedirectResponse("/login", status_code=303)
    response.delete_cookie("session")
    return response


# ---- Dashboard ----

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    html_path = BASE_DIR / "templates" / "dashboard.html"
    return html_path.read_text()


@app.get("/poll-queue", response_class=HTMLResponse)
async def poll_queue_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    html_path = BASE_DIR / "templates" / "poll_queue.html"
    return html_path.read_text()


@app.get("/public", response_class=HTMLResponse)
async def public_dashboard():
    html_path = BASE_DIR / "templates" / "public.html"
    return html_path.read_text()


@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login")
    return templates.TemplateResponse(request, "settings.html")



@app.get("/api/public/repeaters")
async def get_public_repeaters():
    """Return live data for all nodes marked show_public=True across all users."""
    public_configs = cfg.get_public_repeaters()
    result = []
    for r_cfg in public_configs:
        username = r_cfg["owner"]
        pubkey = r_cfg["pubkey"]
        inst = _user_instances.get(username)
        if not inst:
            continue
        store: DataStore = inst["store"]
        all_repeaters = store.get_all()
        match = next((r for r in all_repeaters if r["pubkey"] == pubkey), None)
        if match:
            result.append({**match, "owner": username})
    return result


@app.get("/api/public/history/{pubkey}")
async def get_public_history(pubkey: str, hours: int = 168):
    """Return 7-day history for a public node."""
    public_configs = cfg.get_public_repeaters()
    match_cfg = next((r for r in public_configs if r["pubkey"] == pubkey), None)
    if not match_cfg:
        return []
    username = match_cfg["owner"]
    inst = _user_instances.get(username)
    if not inst:
        return []
    return inst["store"].get_history(pubkey, hours)


# ---- Authenticated Repeater API ----

@app.get("/api/repeaters")
async def get_repeaters(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    return inst["store"].get_all()




@app.get("/api/history/{pubkey}")
async def get_history(pubkey: str, request: Request, hours: int = 168):
    hours = min(hours, 720)  # cap at 30 days
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    return inst["store"].get_history(pubkey, hours)


@app.get("/api/poll-queue")
async def get_poll_queue(request: Request):
    """Return the current poll queue state for the user's poller."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    poller = inst["poller"]
    return poller.get_poll_queue_state()


@app.get("/api/stream")
async def event_stream(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)

    async def generate():
        inst = await get_or_create_instance(user)
        last_non_empty = None
        while True:
            current = inst["store"].get_all()
            # Never push an empty list if we previously had nodes —
            # this prevents a transient empty store from blanking the dashboard
            if current:
                last_non_empty = current
                data = json.dumps(current)
            elif last_non_empty is not None:
                data = json.dumps(last_non_empty)
            else:
                data = json.dumps(current)
            yield f"event: update\ndata: {data}\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/node-names")
async def get_node_names(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    poller = inst["poller"]
    return poller._node_id_name_cache if poller else {}


@app.get("/api/contact-routes")
async def get_contact_routes(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    poller = inst["poller"]
    if not poller:
        return {}
    return {k: {"hops": v[0], "path": v[1]} for k, v in poller._contact_routes.items()}


@app.get("/api/message-paths")
async def get_message_paths(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    messages = inst["store"].get_messages(hours=48, limit=500)
    seen = set()
    result = []
    for m in messages:
        if m["direction"] == "in" and m.get("path") and m.get("sender_pubkey"):
            key = (m["sender_pubkey"][:4].lower(), m["path"])
            if key not in seen:
                seen.add(key)
                result.append({
                    "sender_pubkey": m["sender_pubkey"],
                    "sender_name": m["sender_name"],
                    "path": m["path"],
                    "hops": m["hops"],
                })
    return result


@app.get("/api/map")
async def get_map_data(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    store = inst["store"]
    poller = inst["poller"]
    repeaters = store.get_all()
    home = {"lat": 0.0, "lon": 0.0, "name": "Gateway"}
    if poller and poller.mc and hasattr(poller.mc, "self_info") and poller.mc.self_info:
        si = poller.mc.self_info
        home["lat"] = si.get("adv_lat", 0.0) or 0.0
        home["lon"] = si.get("adv_lon", 0.0) or 0.0
    if not home["lat"] and not home["lon"]:
        s = cfg.get_settings(user)
        home["lat"] = s.get("home_lat", 0.0) or 0.0
        home["lon"] = s.get("home_lon", 0.0) or 0.0
    mesh_contacts = []
    if poller:
        try:
            mesh_contacts = poller.get_mesh_contacts()
        except Exception:
            pass
    configured_pubkeys = {r["pubkey"] for r in repeaters}
    for c in mesh_contacts:
        c["configured"] = c["pubkey"] in configured_pubkeys
    advert_nodes = store.get_advert_nodes()
    for n in advert_nodes:
        n["configured"] = any(
            n["pubkey"] == pk or n["pubkey"].startswith(pk) or pk.startswith(n["pubkey"])
            for pk in configured_pubkeys
        )
    return {"home": home, "repeaters": repeaters, "contacts": mesh_contacts, "advert_nodes": advert_nodes}


@app.post("/api/home")
async def set_home_location(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    try:
        lat = float(body.get("lat", 0.0))
        lon = float(body.get("lon", 0.0))
    except (TypeError, ValueError):
        return {"ok": False, "error": "lat and lon must be numbers"}
    s = cfg.get_settings(user)
    s["home_lat"] = lat
    s["home_lon"] = lon
    cfg.save_settings(user, s)
    return {"ok": True}


@app.get("/api/logs")
async def get_logs(request: Request, hours: int = 24, level: str = None, search: str = None, limit: int = 500):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    return inst["store"].get_activity_logs(hours=hours, level=level, search=search, limit=limit)



@app.get("/api/connection")
async def get_connection(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    poller = inst["poller"]
    return {
        "connected": poller.is_connected,
        "host": cfg.get_companion_host(user),
        "port": cfg.get_companion_port(user),
        "auto_reconnect": poller.auto_reconnect_enabled,
    }


@app.post("/api/connection/auto-reconnect")
async def toggle_auto_reconnect(request: Request):
    """Enable or disable auto-reconnect for the user poller."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    enabled = body.get("enabled", True)
    inst = await get_or_create_instance(user)
    poller = inst["poller"]
    if enabled:
        poller.enable_auto_reconnect()
    else:
        poller.disable_auto_reconnect()
    return {"ok": True, "auto_reconnect": poller.auto_reconnect_enabled}





@app.post("/api/connection/test")
async def test_connection(request: Request):
    """Test TCP connectivity to a companion host/port without affecting the live poller."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    host = str(body.get("host", "")).strip()
    port = int(body.get("port", 5000))
    if not host:
        return {"ok": False, "error": "Host is required"}
    import asyncio as _asyncio
    try:
        reader, writer = await _asyncio.wait_for(
            _asyncio.open_connection(host, port),
            timeout=8.0
        )
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        logger.info(f"[{user}] Connection test to {host}:{port} succeeded")
        return {"ok": True}
    except _asyncio.TimeoutError:
        return {"ok": False, "error": f"Timed out connecting to {host}:{port}"}
    except ConnectionRefusedError:
        return {"ok": False, "error": f"Connection refused at {host}:{port}"}
    except OSError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}



@app.get("/api/debug")
async def debug_state(request: Request):
    """Debug endpoint — shows raw config and store state."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    import os

    # What users.json contains
    users_file = BASE_DIR / "users.json"
    users_raw = None
    try:
        with open(users_file) as f:
            import json as _json
            raw = _json.load(f)
            # Scrub password hashes
            users_raw = {
                u: {"has_settings": "settings" in data,
                    "repeater_count": len(data.get("settings", {}).get("repeaters", [])),
                    "repeaters": [{"name": r.get("name"), "pubkey": r.get("pubkey", "")[:16]}
                                  for r in data.get("settings", {}).get("repeaters", [])],
                    "companion_host": data.get("settings", {}).get("companion_host"),
                   }
                for u, data in raw.items()
            }
    except Exception as e:
        users_raw = {"error": str(e)}

    # What cfg.get_repeaters returns for this user
    cfg_repeaters = [{"name": r.get("name"), "pubkey": r.get("pubkey", "")[:16]}
                     for r in cfg.get_repeaters(user)]

    # What the live store contains
    store_nodes = []
    if user in _user_instances:
        store_nodes = [{"name": r["name"], "pubkey": r["pubkey"][:16], "online": r["online"]}
                       for r in _user_instances[user]["store"].get_all()]

    return {
        "user": user,
        "users_file_exists": users_file.exists(),
        "users_file_path": str(users_file),
        "users_file_content": users_raw,
        "cfg_get_repeaters": cfg_repeaters,
        "store_nodes": store_nodes,
        "instance_exists": user in _user_instances,
    }

# ---- Settings API ----

@app.get("/api/settings")
async def get_settings(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    return cfg.get_settings(user)


@app.post("/api/settings")
async def save_settings(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()

    # Companion host/port required only for admin (non-admins don't submit these fields)
    if user == "admin":
        if "companion_host" not in body or not body["companion_host"]:
            return {"ok": False, "error": "Companion host IP is required"}
    if "companion_port" not in body:
        body["companion_port"] = 5000
    try:
        body["companion_port"] = int(body["companion_port"])
    except (ValueError, TypeError):
        body["companion_port"] = 5000

    repeaters = body.get("repeaters", [])
    for r in repeaters:
        if not r.get("name") or not r.get("pubkey"):
            return {"ok": False, "error": "Each repeater needs a name and public key"}

    body.setdefault("poll_interval_seconds", 120)
    body.setdefault("stagger_delay_seconds", 15)
    body.setdefault("stale_threshold_seconds", 900)
    try:
        body["poll_interval_seconds"] = max(30, int(body["poll_interval_seconds"]))
        body["stagger_delay_seconds"] = max(5, int(body["stagger_delay_seconds"]))
        body["stale_threshold_seconds"] = max(60, int(body["stale_threshold_seconds"]))
    except (ValueError, TypeError):
        return {"ok": False, "error": "Timing values must be numbers"}

    body.setdefault("log_retention_hours", 24)
    try:
        body["log_retention_hours"] = max(1, int(body["log_retention_hours"]))
    except (ValueError, TypeError):
        body["log_retention_hours"] = 24

    body.setdefault("map_path_max_km", 300)
    body.setdefault("node_id_chars", 2)
    try:
        body["map_path_max_km"] = max(10, int(body["map_path_max_km"]))
        body["node_id_chars"] = max(2, min(6, int(body["node_id_chars"])))
    except (ValueError, TypeError):
        body["map_path_max_km"] = 300
        body["node_id_chars"] = 2

    existing = cfg.get_settings(user)
    for key in ("ntfy_enabled",):
        if key not in body:
            body[key] = existing.get(key, True)

    # Non-admin users cannot change companion host/port or polling timing
    if user != "admin":
        existing = cfg.get_settings(user)
        for protected in ("companion_host", "companion_port", "poll_interval_seconds",
                          "stagger_delay_seconds", "stale_threshold_seconds"):
            body[protected] = existing.get(protected)

    cfg.save_settings(user, body)
    logger.info(f"[{user}] Settings saved: {body['companion_host']}:{body['companion_port']}, "
                f"{len(repeaters)} repeaters")

    inst = await get_or_create_instance(user)
    inst["store"].sync_repeaters(repeaters)
    # Force disconnect so the loop restarts with new host/port immediately
    poller = inst["poller"]
    if poller.mc:
        try:
            await poller.mc.disconnect()
        except Exception:
            pass
        poller.mc = None
    poller._stay_disconnected = False
    poller._needs_reconnect = True
    return {"ok": True}


# ---- Reorder & Poll APIs ----

@app.post("/api/reorder")
async def reorder_repeaters(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    pubkeys = body.get("pubkeys", [])
    if not pubkeys:
        return {"ok": False, "error": "No pubkeys provided"}
    settings = cfg.get_settings(user)
    existing = {r["pubkey"]: r for r in settings.get("repeaters", [])}
    settings["repeaters"] = [existing[pk] for pk in pubkeys if pk in existing]
    for pk, r in existing.items():
        if pk not in pubkeys:
            settings["repeaters"].append(r)
    cfg.save_settings(user, settings)
    inst = await get_or_create_instance(user)
    inst["store"].reorder(pubkeys)
    return {"ok": True}


@app.post("/api/ping/{pubkey}")
async def ping_repeater(pubkey: str, request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    return await inst["poller"].ping_repeater(pubkey)


@app.post("/api/advert/{pubkey}")
async def send_advert(pubkey: str, request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    inst = await get_or_create_instance(user)
    return await inst["poller"].send_advert(pubkey)


@app.post("/api/ntfy/test")
async def test_ntfy(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    topic = str(body.get("topic", "")).strip()
    server = str(body.get("server", "https://ntfy.sh")).strip().rstrip("/")
    click_url = str(body.get("click_url", "")).strip()
    if not topic:
        return {"ok": False, "error": "No topic provided"}
    inst = await get_or_create_instance(user)
    await inst["poller"]._send_ntfy_to(server, topic, "MeshCore Test", "Push notifications are working!", click_url)
    return {"ok": True}


@app.post("/api/ntfy/toggle")
async def toggle_ntfy(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    s = cfg.get_settings(user)
    s["ntfy_enabled"] = not s.get("ntfy_enabled", True)
    cfg.save_settings(user, s)
    return {"ok": True, "enabled": s["ntfy_enabled"]}


# ---- User Management API (admin only) ----

@app.get("/api/users")
async def list_users_api(request: Request):
    user = get_current_user(request)
    if user != "admin":
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    return {"users": cfg.list_users()}


@app.post("/api/users")
async def create_user_api(request: Request):
    user = get_current_user(request)
    if user != "admin":
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    body = await request.json()
    username = body.get("username", "").strip()
    password = body.get("password", "")
    if not username or not password:
        return {"ok": False, "error": "Username and password required"}
    if not cfg.create_user(username, password):
        return {"ok": False, "error": "User already exists"}
    await get_or_create_instance(username)
    return {"ok": True}


@app.delete("/api/users/{username}")
async def delete_user_api(username: str, request: Request):
    user = get_current_user(request)
    if user != "admin":
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    if not cfg.delete_user(username):
        return {"ok": False, "error": "Cannot delete user"}
    await stop_instance(username)
    return {"ok": True}


@app.post("/api/users/password")
async def change_password_api(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    body = await request.json()
    new_password = body.get("new_password", "")
    if not new_password or len(new_password) < 6:
        return {"ok": False, "error": "Password must be at least 6 characters"}
    cfg.change_password(user, new_password)
    return {"ok": True}


@app.get("/api/me")
async def get_me(request: Request):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"error": "Unauthorized"}, status_code=401)
    return {"username": user, "is_admin": user == "admin"}


# ---- Update API ----

_ALLOWED_UPDATE_PATHS = {
    "app.py", "config.py", "data_store.py", "meshcore_poller.py", "requirements.txt",
    "docker-compose.yml",
}
_ALLOWED_UPDATE_PREFIXES = ("templates/", "static/")
_KNOWN_TOP_DIRS = {"templates", "static"}


def _is_allowed_path(name: str) -> bool:
    if name in _ALLOWED_UPDATE_PATHS:
        return True
    return any(name.startswith(p) for p in _ALLOWED_UPDATE_PREFIXES)


@app.post("/api/update")
async def apply_update(request: Request, file: UploadFile = File(...)):
    user = get_current_user(request)
    if user != "admin":
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    if not file.filename.endswith(".zip"):
        return {"ok": False, "error": "File must be a .zip archive"}
    data = await file.read()
    if len(data) > 20 * 1024 * 1024:
        return {"ok": False, "error": "Upload too large (max 20 MB)"}
    with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
        tmp.write(data)
        tmp_path = tmp.name
    try:
        with zipfile.ZipFile(tmp_path) as zf:
            names = zf.namelist()
            def _normalise(name: str) -> str:
                parts = name.split("/", 1)
                if (len(parts) == 2 and "." not in parts[0]
                        and parts[0] not in _KNOWN_TOP_DIRS and parts[1]):
                    return parts[1]
                return name
            normalised = [_normalise(n) for n in names]
            bad = [n for n in normalised if n and not n.endswith("/") and not _is_allowed_path(n)]
            if bad:
                return {"ok": False, "error": f"Zip contains unexpected paths: {bad[:5]}"}
            for zip_name, norm_name in zip(names, normalised):
                if not norm_name or norm_name.endswith("/"):
                    continue
                dest = BASE_DIR / norm_name
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_bytes(zf.read(zip_name))
        logger.info(f"Update applied: {len([n for n in normalised if n and not n.endswith('/')])} files")
        return {"ok": True, "files": [n for n in normalised if n and not n.endswith("/")]}
    except zipfile.BadZipFile:
        return {"ok": False, "error": "Invalid zip file"}
    finally:
        os.unlink(tmp_path)


@app.post("/api/restart")
async def restart_app(request: Request):
    user = get_current_user(request)
    if user != "admin":
        return JSONResponse({"error": "Forbidden"}, status_code=403)
    logger.info("Restart requested via /api/restart")
    async def _delayed_kill():
        await asyncio.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)
    asyncio.create_task(_delayed_kill())
    return {"ok": True}
