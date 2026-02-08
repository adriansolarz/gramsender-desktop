"""
Reply detection + tracking: poll DMs for unread replies, append to replies.csv, broadcast to UI.
Uses same session/login as workers (InstagramLoginHelper). Run in background thread when REPLY_MONITOR_ENABLED.
"""
import csv
import json
import os
import threading
import time
import requests
from datetime import datetime
from typing import Callable, Dict, List, Optional

from .config import (
    ACCOUNTS_FILE,
    CAMPAIGNS_FILE,
    KEY_FILE,
    REPLIES_CSV,
    REPLY_POLL_INTERVAL,
    SESSIONS_DIR,
    STORAGE_MODE,
)

REPLIES_CSV_HEADER = (
    "timestamp",
    "account_username",
    "account_name",
    "campaign_id",
    "thread_id",
    "thread_title",
    "replier_user_id",
    "replier_username",
    "reply_text",
    "replied_to_message_text",
    "message_id",
    "message_type",  # "reply" = replied to a message; "inbound" = new message from lead (not a reply)
)

_replies_csv_lock = threading.Lock()
_sent_dms_lock = threading.Lock()
SENT_DMS_FILE = os.path.join(os.path.dirname(__file__), "sent_dms.json")


def _get_accounts_for_monitor() -> List[Dict]:
    """Return list of account dicts with username, password, proxy, session_cookies, account_name (for reply polling)."""
    accounts_list = []
    if STORAGE_MODE == "supabase":
        try:
            from .services.database import DatabaseService
            db = DatabaseService.get_instance()
            # get_accounts() returns minimal; we need full account with password/session for login
            rows = db.client.table("accounts").select("username").execute()
            for row in (rows.data or []):
                username = row.get("username")
                if not username:
                    continue
                acc = db.get_account(username)
                if acc and (acc.get("password") or acc.get("session_cookies")):
                    accounts_list.append(acc)
        except Exception as e:
            print(f"[ReplyMonitor] Supabase get_accounts error: {e}")
        return accounts_list
    # JSON: load and decrypt
    if not os.path.exists(ACCOUNTS_FILE):
        return []
    try:
        with open(ACCOUNTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return []
    except Exception:
        return []
    if not os.path.exists(KEY_FILE):
        return []
    try:
        from cryptography.fernet import Fernet
        import base64
        with open(KEY_FILE, "rb") as f:
            key = f.read()
        fernet = Fernet(key)
        for username, raw in data.items():
            password = None
            if raw.get("password"):
                try:
                    password = fernet.decrypt(base64.b64decode(raw["password"])).decode()
                except Exception:
                    pass
            proxy = None
            if raw.get("proxy"):
                try:
                    proxy = fernet.decrypt(base64.b64decode(raw["proxy"])).decode()
                except Exception:
                    pass
            session_cookies = None
            if raw.get("session_cookies"):
                try:
                    session_cookies = fernet.decrypt(base64.b64decode(raw["session_cookies"])).decode()
                except Exception:
                    pass
            if password or session_cookies:
                accounts_list.append({
                    "username": username,
                    "account_name": raw.get("account_name", username),
                    "password": password or "",
                    "proxy": proxy or "",
                    "session_cookies": session_cookies or "",
                })
    except Exception as e:
        print(f"[ReplyMonitor] JSON decrypt error: {e}")
    return accounts_list


def _append_reply(
    account_username: str,
    account_name: str,
    campaign_id: str,
    thread_id: str,
    thread_title: str,
    replier_user_id: str,
    replier_username: str,
    reply_text: str,
    replied_to_text: str,
    message_id: str,
    message_type: str = "reply",  # "reply" or "inbound"
) -> None:
    # Write to Supabase if enabled
    if STORAGE_MODE == "supabase":
        try:
            from .services.database import DatabaseService
            db = DatabaseService.get_instance()
            db.record_reply(
                account_username=account_username,
                sender_username=replier_username,
                sender_user_id=replier_user_id,
                message_preview=reply_text,
                is_inbound=(message_type == "inbound"),
            )
        except Exception as e:
            print(f"[ReplyMonitor] Failed to write to Supabase: {e}")
    
    # Also write to CSV as backup
    with _replies_csv_lock:
        try:
            file_exists = os.path.isfile(REPLIES_CSV)
            with open(REPLIES_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
                if not file_exists:
                    w.writerow(REPLIES_CSV_HEADER)
                w.writerow((
                    datetime.now().isoformat(),
                    account_username,
                    account_name,
                    campaign_id or "",
                    thread_id,
                    (thread_title or "")[:200],
                    replier_user_id,
                    replier_username or "",
                    (reply_text or "")[:2000],
                    (replied_to_text or "")[:2000],
                    message_id or "",
                    message_type or "reply",
                ))
        except Exception as e:
            print(f"[ReplyMonitor] Failed to append replies.csv: {e}")


def _find_campaign_for_recipient(replier_username: str) -> Optional[str]:
    """Find the most recent campaign_id for a recipient_username from sent_dms.json."""
    with _sent_dms_lock:
        try:
            with open(SENT_DMS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            return None
    matching = [d for d in data if d.get("recipient_username") == replier_username]
    if not matching:
        return None
    # Sort by sent_at descending
    matching.sort(key=lambda x: x.get("sent_at", ""), reverse=True)
    return matching[0].get("campaign_id")


def _get_global_settings():
    """Get global settings from settings.json."""
    try:
        with open(os.path.join(os.path.dirname(__file__), "..", "settings.json"), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _process_unread_replies_for_account(
    account: Dict,
    broadcast_sync: Optional[Callable[[dict], None]],
) -> None:
    """One pass: login (session/password), fetch unread threads, detect new replies, append + broadcast."""
    username = account.get("username") or ""
    account_name = account.get("account_name") or username
    password = account.get("password") or ""
    proxy = account.get("proxy") or None
    session_cookies = account.get("session_cookies") or None
    sessionid = None
    if session_cookies:
        try:
            parsed = json.loads(session_cookies)
            if isinstance(parsed, dict) and parsed.get("sessionid"):
                sessionid = parsed["sessionid"]
            elif isinstance(parsed, str):
                sessionid = parsed
        except Exception:
            pass
    from .instagram_login import InstagramLoginHelper
    helper = InstagramLoginHelper(
        username=username,
        password=password,
        sessions_dir=SESSIONS_DIR,
        proxy=proxy or None,
        sessionid=sessionid,
        challenge_code_callback=None,
    )
    try:
        if not helper.login():
            return
    except Exception as e:
        print(f"[ReplyMonitor] Login failed for @{username}: {e}")
        return
    cl = helper.client
    my_user_id = getattr(cl, "user_id", None)
    if my_user_id is None:
        try:
            my_user_id = cl.user_id
        except Exception:
            pass
    if my_user_id is None:
        return
    my_user_id_str = str(my_user_id)
    try:
        # Fetch threads with unread messages
        threads = cl.direct_threads(amount=10, selected_filter="unread")
    except Exception as e:
        print(f"[ReplyMonitor] direct_threads failed for @{username}: {e}")
        return
    for thread in (threads or []):
        thread_id = getattr(thread, "id", None) or getattr(thread, "thread_id", None)
        thread_title = getattr(thread, "thread_title", None) or getattr(thread, "title", "") or ""
        last_read_ts = 0
        last_seen = getattr(thread, "last_seen_at", None)
        if last_seen:
            if isinstance(last_seen, dict) and my_user_id_str in last_seen:
                t = last_seen[my_user_id_str]
                if isinstance(t, dict) and "timestamp" in t:
                    last_read_ts = int(t["timestamp"])
                elif isinstance(t, (int, float)):
                    last_read_ts = int(t)
            elif hasattr(last_seen, "get") and last_seen.get(my_user_id_str):
                t = last_seen[my_user_id_str]
                last_read_ts = int(t.get("timestamp", t) if isinstance(t, dict) else t)
        try:
            messages = cl.direct_messages(thread_id, amount=10)
        except Exception:
            continue
        for msg in (messages or []):
            msg_ts = getattr(msg, "timestamp", None) or getattr(msg, "created_at", 0)
            if isinstance(msg_ts, str):
                try:
                    msg_ts = int(msg_ts)
                except ValueError:
                    msg_ts = 0
            elif hasattr(msg_ts, "timestamp"):
                # datetime.datetime from instagrapi
                msg_ts = int(msg_ts.timestamp())
            elif not isinstance(msg_ts, (int, float)):
                msg_ts = 0
            msg_ts = int(msg_ts)
            msg_user_id = str(getattr(msg, "user_id", "") or "")
            is_new = msg_ts > int(last_read_ts)
            # Only process new messages from the other person (not from us)
            if not is_new or msg_user_id == my_user_id_str:
                continue
            replied_to = getattr(msg, "replied_to_message", None)
            message_type = "reply" if replied_to is not None else "inbound"
            orig_text = ""
            if replied_to is not None:
                orig_text = getattr(replied_to, "text", None) or getattr(replied_to, "message", "") or ""
            reply_text = getattr(msg, "text", None) or getattr(msg, "message", "") or ""
            msg_id = str(getattr(msg, "id", "") or getattr(msg, "message_id", "") or "")
            replier_username = getattr(msg, "username", None) or ""
            campaign_id = _find_campaign_for_recipient(replier_username)
            _append_reply(
                account_username=username,
                account_name=account_name,
                campaign_id=campaign_id,
                thread_id=str(thread_id or ""),
                thread_title=thread_title,
                replier_user_id=msg_user_id,
                replier_username=replier_username,
                reply_text=reply_text,
                replied_to_text=orig_text,
                message_id=msg_id,
                message_type=message_type,
            )
            if broadcast_sync:
                try:
                    broadcast_sync({
                        "type": "new_reply",
                        "account_username": username,
                        "account_name": account_name,
                        "thread_title": thread_title,
                        "replier_username": replier_username,
                        "reply_text": (reply_text or "")[:200],
                        "message_type": message_type,
                        "timestamp": datetime.now().isoformat(),
                    })
                except Exception:
                    pass
            # Send webhook if linked to campaign or global
            url = None
            secret = ""
            if campaign_id:
                # Load campaign
                if STORAGE_MODE == "supabase":
                    try:
                        from .services.database import DatabaseService
                        db = DatabaseService.get_instance()
                        campaign = db.get_campaign(campaign_id)
                    except Exception:
                        campaign = None
                else:
                    try:
                        with open(CAMPAIGNS_FILE, "r", encoding="utf-8") as f:
                            campaigns = json.load(f)
                        campaign = campaigns.get(campaign_id)
                    except Exception:
                        campaign = None
                if campaign and campaign.get("webhook_url"):
                    url = campaign["webhook_url"]
                else:
                    # Fallback to global
                    global_settings = _get_global_settings()
                    if global_settings.get("global_webhook_url") and "new_lead" in global_settings.get("webhook_events", []):
                        url = global_settings["global_webhook_url"]
                        secret = global_settings.get("webhook_secret", "")
            if url:
                payload = {
                    "event": "new_lead",
                    "campaign_id": campaign_id or "",
                    "account_username": username,
                    "replier_username": replier_username,
                    "reply_text": (reply_text or "")[:1000],
                    "replied_to_message_text": (orig_text or "")[:1000],
                    "message_type": message_type,
                    "thread_id": str(thread_id or ""),
                    "timestamp": datetime.now().isoformat(),
                    "app_version": "1.0",
                    "secret": secret
                }
                try:
                    requests.post(url, json=payload, timeout=10)
                except Exception as e:
                    print(f"[Webhook] Failed to send for campaign {campaign_id or 'global'}: {e}")
    # Small delay between accounts to avoid hammering
    time.sleep(2)


def run_reply_monitor_loop(broadcast_sync: Optional[Callable[[dict], None]] = None) -> None:
    """Run forever: get accounts, process unread replies for each, sleep REPLY_POLL_INTERVAL.
    Skips polling when workers are active to avoid API rate limit conflicts."""
    print("[ReplyMonitor] Started reply monitor loop.")
    while True:
        try:
            # Skip reply monitoring when workers are actively running to avoid rate limits
            try:
                from .worker_manager import WorkerManager
                wm = WorkerManager.get_instance()
                active = wm.get_all_workers()
                running_workers = [w for w in active.values() if w.get("status") in ("starting", "running")]
                if running_workers:
                    # Workers are active - skip to avoid Instagram API conflicts
                    time.sleep(REPLY_POLL_INTERVAL)
                    continue
            except Exception:
                pass
            
            accounts = _get_accounts_for_monitor()
            if not accounts:
                time.sleep(REPLY_POLL_INTERVAL)
                continue
            for acc in accounts:
                try:
                    _process_unread_replies_for_account(acc, broadcast_sync)
                except Exception as e:
                    print(f"[ReplyMonitor] Error processing @{acc.get('username', '?')}: {e}")
                time.sleep(1)
        except Exception as e:
            print(f"[ReplyMonitor] Loop error: {e}")
        time.sleep(REPLY_POLL_INTERVAL)
