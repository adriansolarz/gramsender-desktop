"""
Local SQLite storage for sends, replies, and conversation history.
Data stays on the user's device - never uploaded to cloud.
"""
import os
import sqlite3
import threading
from datetime import datetime
from typing import Dict, List, Optional, Tuple


# Default DB path - can be overridden via init()
_DB_PATH: Optional[str] = None
_lock = threading.Lock()


def _get_db_path() -> str:
    global _DB_PATH
    if _DB_PATH:
        return _DB_PATH
    app_data = os.path.join(os.path.expanduser("~"), ".gramsender", "data")
    os.makedirs(app_data, exist_ok=True)
    _DB_PATH = os.path.join(app_data, "gramsender.db")
    return _DB_PATH


def init(db_path: Optional[str] = None):
    """Initialize the SQLite database and create tables if they don't exist."""
    global _DB_PATH
    if db_path:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        _DB_PATH = db_path
    path = _get_db_path()
    print(f"[LocalStorage] Initializing SQLite database at: {path}")
    with _lock:
        conn = sqlite3.connect(path)
        try:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS sends (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    account_username TEXT NOT NULL,
                    account_name TEXT DEFAULT '',
                    recipient_username TEXT NOT NULL,
                    recipient_user_id TEXT DEFAULT '',
                    campaign_id TEXT DEFAULT '',
                    campaign_name TEXT DEFAULT '',
                    lead_source TEXT DEFAULT '',
                    lead_target TEXT DEFAULT '',
                    message_preview TEXT DEFAULT '',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS replies (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    account_username TEXT NOT NULL,
                    account_name TEXT DEFAULT '',
                    sender_username TEXT NOT NULL,
                    sender_user_id TEXT DEFAULT '',
                    campaign_id TEXT DEFAULT '',
                    thread_id TEXT DEFAULT '',
                    thread_title TEXT DEFAULT '',
                    message_preview TEXT DEFAULT '',
                    replied_to_text TEXT DEFAULT '',
                    message_id TEXT DEFAULT '',
                    message_type TEXT DEFAULT 'reply',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_username TEXT NOT NULL,
                    recipient_username TEXT NOT NULL,
                    campaign_id TEXT DEFAULT '',
                    direction TEXT NOT NULL DEFAULT 'outbound',
                    message_text TEXT NOT NULL,
                    thread_id TEXT DEFAULT '',
                    message_id TEXT DEFAULT '',
                    timestamp TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                );

                CREATE INDEX IF NOT EXISTS idx_sends_recipient ON sends(recipient_username);
                CREATE INDEX IF NOT EXISTS idx_sends_timestamp ON sends(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_sends_campaign ON sends(campaign_id);
                CREATE INDEX IF NOT EXISTS idx_replies_sender ON replies(sender_username);
                CREATE INDEX IF NOT EXISTS idx_replies_timestamp ON replies(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_replies_campaign ON replies(campaign_id);
                CREATE INDEX IF NOT EXISTS idx_convos_recipient ON conversations(recipient_username);
                CREATE INDEX IF NOT EXISTS idx_convos_timestamp ON conversations(timestamp DESC);
                CREATE INDEX IF NOT EXISTS idx_convos_campaign ON conversations(campaign_id);
                CREATE INDEX IF NOT EXISTS idx_convos_direction ON conversations(direction);
            """)
            conn.commit()
            print("[LocalStorage] SQLite tables ready.")
        finally:
            conn.close()


def _connect() -> sqlite3.Connection:
    """Get a connection with row_factory set for dict-like access."""
    conn = sqlite3.connect(_get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


# ─── SENDS ────────────────────────────────────────────────────────────────────

def record_send(
    account_username: str,
    recipient_username: str,
    account_name: str = "",
    campaign_id: str = "",
    campaign_name: str = "",
    lead_source: str = "",
    lead_target: str = "",
    recipient_user_id: str = "",
    message_preview: str = "",
) -> Optional[Dict]:
    """Record a sent DM to local SQLite."""
    ts = datetime.now().isoformat()
    with _lock:
        try:
            conn = _connect()
            cur = conn.execute(
                """INSERT INTO sends 
                   (timestamp, account_username, account_name, recipient_username, 
                    recipient_user_id, campaign_id, campaign_name, lead_source,
                    lead_target, message_preview)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, account_username, account_name or "", recipient_username,
                 str(recipient_user_id or ""), campaign_id or "", campaign_name or "",
                 lead_source or "", lead_target or "", (message_preview or "")[:500]),
            )
            conn.commit()
            row_id = cur.lastrowid
            conn.close()
            return {"id": row_id, "timestamp": ts, "recipient_username": recipient_username}
        except Exception as e:
            print(f"[LocalStorage] Error recording send: {e}")
            return None


def get_sends(limit: int = 100, campaign_id: str = None, account: str = None, since: str = None) -> List[Dict]:
    """Get recent sends from local SQLite, newest first."""
    try:
        conn = _connect()
        query = "SELECT * FROM sends"
        params = []
        conditions = []
        if campaign_id:
            conditions.append("campaign_id = ?")
            params.append(campaign_id)
        if account:
            conditions.append("account_username = ?")
            params.append(account)
        if since:
            conditions.append("timestamp >= ?")
            params.append(since)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[LocalStorage] Error getting sends: {e}")
        return []


def get_outreach_recipients(limit: int = 5000) -> set:
    """Get set of all recipient usernames we've sent DMs to."""
    try:
        conn = _connect()
        rows = conn.execute(
            "SELECT DISTINCT LOWER(recipient_username) as u FROM sends LIMIT ?", (limit,)
        ).fetchall()
        conn.close()
        return {r["u"] for r in rows if r["u"]}
    except Exception as e:
        print(f"[LocalStorage] Error getting outreach recipients: {e}")
        return set()


def find_campaign_for_recipient(recipient_username: str) -> Optional[str]:
    """Find the most recent campaign_id for a recipient from sends table."""
    try:
        conn = _connect()
        row = conn.execute(
            """SELECT campaign_id FROM sends 
               WHERE LOWER(recipient_username) = LOWER(?)
               AND campaign_id != ''
               ORDER BY timestamp DESC LIMIT 1""",
            (recipient_username,)
        ).fetchone()
        conn.close()
        return dict(row)["campaign_id"] if row else None
    except Exception as e:
        print(f"[LocalStorage] Error finding campaign for recipient: {e}")
        return None


def count_sends_in_range(start_dt: datetime, end_dt: datetime) -> int:
    """Count sends within a date range."""
    try:
        conn = _connect()
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM sends WHERE timestamp >= ? AND timestamp <= ?",
            (start_dt.isoformat(), end_dt.isoformat())
        ).fetchone()
        conn.close()
        return dict(row)["cnt"] if row else 0
    except Exception as e:
        print(f"[LocalStorage] Error counting sends: {e}")
        return 0


def count_total_sends() -> int:
    """Count all sends."""
    try:
        conn = _connect()
        row = conn.execute("SELECT COUNT(*) as cnt FROM sends").fetchone()
        conn.close()
        return dict(row)["cnt"] if row else 0
    except Exception as e:
        print(f"[LocalStorage] Error counting total sends: {e}")
        return 0


# ─── REPLIES ──────────────────────────────────────────────────────────────────

def record_reply(
    account_username: str,
    sender_username: str,
    account_name: str = "",
    sender_user_id: str = "",
    campaign_id: str = "",
    thread_id: str = "",
    thread_title: str = "",
    message_preview: str = "",
    replied_to_text: str = "",
    message_id: str = "",
    message_type: str = "reply",
) -> Optional[Dict]:
    """Record an incoming reply to local SQLite."""
    ts = datetime.now().isoformat()
    with _lock:
        try:
            conn = _connect()
            cur = conn.execute(
                """INSERT INTO replies 
                   (timestamp, account_username, account_name, sender_username,
                    sender_user_id, campaign_id, thread_id, thread_title,
                    message_preview, replied_to_text, message_id, message_type)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (ts, account_username, account_name or "", sender_username,
                 str(sender_user_id or ""), campaign_id or "", thread_id or "",
                 (thread_title or "")[:200], (message_preview or "")[:2000],
                 (replied_to_text or "")[:2000], message_id or "", message_type or "reply"),
            )
            conn.commit()
            row_id = cur.lastrowid
            conn.close()
            return {"id": row_id, "timestamp": ts, "sender_username": sender_username}
        except Exception as e:
            print(f"[LocalStorage] Error recording reply: {e}")
            return None


def get_replies(limit: int = 100, account: str = None, campaign_id: str = None, since: str = None) -> List[Dict]:
    """Get recent replies from local SQLite, newest first."""
    try:
        conn = _connect()
        query = "SELECT * FROM replies"
        params = []
        conditions = []
        if account:
            conditions.append("account_username = ?")
            params.append(account)
        if campaign_id:
            conditions.append("campaign_id = ?")
            params.append(campaign_id)
        if since:
            conditions.append("timestamp >= ?")
            params.append(since)
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        rows = conn.execute(query, params).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[LocalStorage] Error getting replies: {e}")
        return []


def count_replies_and_inbounds_in_range(
    start: Optional[str] = None, end: Optional[str] = None
) -> Tuple[int, int]:
    """Count replies vs inbounds in a date range. If no range, count today."""
    try:
        conn = _connect()
        if start and end:
            start_iso = start.replace("Z", "+00:00")
            end_iso = end.replace("Z", "+00:00")
            rows = conn.execute(
                """SELECT message_type, COUNT(*) as cnt FROM replies 
                   WHERE timestamp >= ? AND timestamp <= ?
                   GROUP BY message_type""",
                (start_iso, end_iso)
            ).fetchall()
        else:
            today = datetime.now().date().isoformat()
            rows = conn.execute(
                """SELECT message_type, COUNT(*) as cnt FROM replies 
                   WHERE timestamp >= ?
                   GROUP BY message_type""",
                (today,)
            ).fetchall()
        conn.close()
        replies_count = 0
        inbounds_count = 0
        for r in rows:
            d = dict(r)
            mt = (d.get("message_type") or "").strip().lower()
            if mt == "inbound":
                inbounds_count = d["cnt"]
            else:
                replies_count = d["cnt"]
        return replies_count, inbounds_count
    except Exception as e:
        print(f"[LocalStorage] Error counting replies: {e}")
        return 0, 0


# ─── CONVERSATIONS ────────────────────────────────────────────────────────────

def record_conversation(
    account_username: str,
    recipient_username: str,
    direction: str,
    message_text: str,
    campaign_id: str = "",
    thread_id: str = "",
    message_id: str = "",
) -> Optional[Dict]:
    """Record a conversation message (outbound or inbound) to local SQLite."""
    ts = datetime.now().isoformat()
    with _lock:
        try:
            conn = _connect()
            cur = conn.execute(
                """INSERT INTO conversations 
                   (account_username, recipient_username, campaign_id, direction,
                    message_text, thread_id, message_id, timestamp)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (account_username, recipient_username, campaign_id or "",
                 direction, (message_text or "")[:5000], thread_id or "",
                 message_id or "", ts),
            )
            conn.commit()
            row_id = cur.lastrowid
            conn.close()
            return {"id": row_id, "timestamp": ts}
        except Exception as e:
            print(f"[LocalStorage] Error recording conversation: {e}")
            return None


def get_conversation(recipient_username: str, limit: int = 50) -> List[Dict]:
    """Get conversation history with a specific user, ordered chronologically."""
    try:
        conn = _connect()
        rows = conn.execute(
            """SELECT * FROM conversations 
               WHERE LOWER(recipient_username) = LOWER(?)
               ORDER BY timestamp ASC LIMIT ?""",
            (recipient_username, limit)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[LocalStorage] Error getting conversation: {e}")
        return []


def get_all_conversations(limit: int = 100) -> List[Dict]:
    """Get recent conversation messages across all recipients."""
    try:
        conn = _connect()
        rows = conn.execute(
            "SELECT * FROM conversations ORDER BY timestamp DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        print(f"[LocalStorage] Error getting all conversations: {e}")
        return []


# ─── STATS ────────────────────────────────────────────────────────────────────

def get_stats() -> Dict:
    """Get aggregate statistics from local storage."""
    try:
        conn = _connect()
        total_sends = conn.execute("SELECT COUNT(*) as cnt FROM sends").fetchone()
        total_replies = conn.execute("SELECT COUNT(*) as cnt FROM replies WHERE message_type = 'reply'").fetchone()
        total_inbounds = conn.execute("SELECT COUNT(*) as cnt FROM replies WHERE message_type = 'inbound'").fetchone()
        conn.close()
        return {
            "total_sends": dict(total_sends)["cnt"] if total_sends else 0,
            "total_replies": dict(total_replies)["cnt"] if total_replies else 0,
            "total_inbounds": dict(total_inbounds)["cnt"] if total_inbounds else 0,
        }
    except Exception as e:
        print(f"[LocalStorage] Error getting stats: {e}")
        return {"total_sends": 0, "total_replies": 0, "total_inbounds": 0}
