"""
Pre- and post-login flow (from simple login + dm auth.py).
Pre: sync_launcher before login. Post: reels tray then timeline (same order as simple login_flow).
"""
import uuid as _uuid


def _get_client_uuid(client):
    """Get uuid from client (settings or attribute)."""
    u = getattr(client, "uuid", None)
    if u is not None:
        return str(u)
    if getattr(client, "settings", None) and isinstance(client.settings, dict):
        return str(client.settings.get("uuid", _uuid.uuid4()))
    return str(_uuid.uuid4())


def pre_login_flow(client) -> bool:
    """Emulate mobile app behavior before login (sync_launcher)."""
    try:
        uid = _get_client_uuid(client)
        data = {"id": uid, "server_config_retrieval": "1"}
        client.private_request("launcher/sync/", data, login=True)
        return True
    except Exception:
        return False


def post_login_flow(client) -> bool:
    """Emulate mobile app behavior after login (same order as simple login + dm login_flow: reels then timeline)."""
    # Simple system: check_flow.append(get_reels_tray_feed("cold_start")); check_flow.append(get_timeline_feed(["cold_start_fetch"]))
    try:
        if hasattr(client, "get_reels_tray_feed"):
            client.get_reels_tray_feed()
    except Exception:
        pass
    try:
        if hasattr(client, "get_timeline_feed"):
            client.get_timeline_feed()
            return True
    except Exception:
        pass
    return False
