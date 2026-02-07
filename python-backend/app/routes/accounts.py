from fastapi import APIRouter, HTTPException, File, UploadFile
from typing import List, Dict, Optional, Tuple
import csv
import io
import json
import os
import base64
import uuid
import threading
import time
import tempfile
from datetime import datetime
from cryptography.fernet import Fernet

from ..config import ACCOUNTS_FILE, KEY_FILE, STORAGE_MODE, SESSIONS_DIR
from ..instagram_login import InstagramLoginHelper

router = APIRouter()

# Account verification: 2FA and result state (keyed by verification_id)
_verification_pending: Dict[str, dict] = {}
_verification_results: Dict[str, dict] = {}
_verification_lock = threading.Lock()

# Initialize database service if using Supabase
db_service = None
DatabaseService = None
if STORAGE_MODE == "supabase":
    try:
        from ..services.database import DatabaseService
        db_service = DatabaseService.get_instance()
    except Exception as e:
        print(f"Warning: Could not initialize Supabase: {e}")
        print("Falling back to JSON storage")

def get_or_create_key():
    """Get or create encryption key"""
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            return f.read()
    else:
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(key)
        return key

def load_accounts() -> Dict:
    """Load accounts from JSON file"""
    if not os.path.exists(ACCOUNTS_FILE):
        return {}
    try:
        with open(ACCOUNTS_FILE, "r", encoding='utf-8') as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def save_accounts(accounts: Dict):
    """Save accounts to JSON file"""
    with open(ACCOUNTS_FILE, "w", encoding='utf-8') as f:
        json.dump(accounts, f, indent=2)


def _create_one_account(
    username: str,
    password: str,
    account_name: str,
    proxy: str,
    session_cookies: str,
    accounts_dict: Optional[Dict] = None,
) -> Tuple[bool, Optional[str]]:
    """Create a single account (used by single create and bulk import). Returns (success, error_message)."""
    username = (username or "").strip()
    if not username:
        return (False, "Username is required")
    if not password and not session_cookies:
        return (False, "Either password or session cookies are required")
    if session_cookies:
        try:
            json.loads(session_cookies)
        except json.JSONDecodeError:
            return (False, "Session cookies must be valid JSON")
    account_name = (account_name or username).strip() or username
    proxy = (proxy or "").strip() or ""

    if STORAGE_MODE == "supabase" and db_service:
        try:
            db_service.create_account(
                username=username,
                account_name=account_name,
                password=password if password else None,
                proxy=proxy if proxy else None,
                session_cookies=session_cookies if session_cookies else None,
            )
            return (True, None)
        except Exception as e:
            return (False, str(e))
    else:
        accounts = accounts_dict if accounts_dict is not None else load_accounts()
        if username in accounts and accounts_dict is None:
            return (False, "Username already exists")
        key = get_or_create_key()
        fernet = Fernet(key)
        encrypted_password = None
        if password:
            encrypted_password = fernet.encrypt(password.encode())
        encrypted_session_cookies = None
        if session_cookies:
            encrypted_session_cookies = fernet.encrypt(session_cookies.encode())
        accounts[username] = {
            "account_name": account_name or username,
            "created_at": datetime.now().timestamp(),
        }
        if encrypted_password:
            accounts[username]["password"] = base64.b64encode(encrypted_password).decode()
        if encrypted_session_cookies:
            accounts[username]["session_cookies"] = base64.b64encode(encrypted_session_cookies).decode()
        if proxy:
            encrypted_proxy = fernet.encrypt(proxy.encode())
            accounts[username]["proxy"] = base64.b64encode(encrypted_proxy).decode()
        if accounts_dict is None:
            save_accounts(accounts)
        return (True, None)


def _get_or_create_pending(verification_id: str) -> dict:
    with _verification_lock:
        if verification_id not in _verification_pending:
            _verification_pending[verification_id] = {"event": threading.Event(), "code": None}
        return _verification_pending[verification_id]


def _set_challenge_code(verification_id: str, code: str):
    with _verification_lock:
        if verification_id in _verification_pending:
            _verification_pending[verification_id]["code"] = code
            _verification_pending[verification_id]["event"].set()


def _run_verify_login(verification_id: str, username: str, password: str, proxy: Optional[str]):
    """Run in background thread: login via InstagramLoginHelper (session -> sessionid -> username/password + 2FA)."""
    try:
        with _verification_lock:
            _verification_results[verification_id] = {"status": "pending"}

        def challenge_callback(u: str, method: str) -> str:
            pending = _get_or_create_pending(verification_id)
            with _verification_lock:
                _verification_results[verification_id]["status"] = "need_2fa"
            pending["event"].wait(timeout=300)
            return pending.get("code") or ""

        helper = InstagramLoginHelper(
            username=username,
            password=password,
            sessions_dir=SESSIONS_DIR,
            proxy=proxy or None,
            sessionid=None,
            challenge_code_callback=challenge_callback,
        )
        if not helper.login():
            with _verification_lock:
                _verification_results[verification_id] = {"status": "error", "error": "Login failed"}
            return

        # Success: extract session (sessionid, ds_user_id) from helper.client
        try:
            fd, path = tempfile.mkstemp(suffix=".json")
            os.close(fd)
            try:
                helper.client.dump_settings(path)
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                auth = data.get("authorization_data") or {}
                sessionid = auth.get("sessionid", "")
                ds_user_id = auth.get("ds_user_id", "")
                if not sessionid and data.get("cookies"):
                    sessionid = data["cookies"].get("sessionid", "")
                if not ds_user_id and data.get("cookies"):
                    ds_user_id = data["cookies"].get("ds_user_id", "")
                session_cookies = json.dumps({"sessionid": sessionid, "ds_user_id": ds_user_id})
                with _verification_lock:
                    _verification_results[verification_id] = {
                        "status": "success",
                        "session_cookies": session_cookies,
                    }
            finally:
                try:
                    os.unlink(path)
                except OSError:
                    pass
        except Exception as e:
            with _verification_lock:
                _verification_results[verification_id] = {"status": "error", "error": f"Failed to save session: {e}"}
    except Exception as e:
        with _verification_lock:
            _verification_results[verification_id] = {"status": "error", "error": str(e)}
    finally:
        with _verification_lock:
            _verification_pending.pop(verification_id, None)


@router.get("")
async def get_accounts():
    """Get all accounts (without passwords)"""
    if STORAGE_MODE == "supabase" and db_service:
        accounts = db_service.get_accounts()
        return {"accounts": accounts}
    else:
        # Legacy JSON implementation
        accounts = load_accounts()
        if not isinstance(accounts, dict):
            accounts = {}
        result = {}
        for username, data in accounts.items():
            result[username] = {
                "username": username,
                "account_name": data.get("account_name", username),
                "created_at": data.get("created_at")
            }
        return {"accounts": result}

@router.post("")
async def create_account(account_data: dict):
    """Create a new account"""
    username = account_data.get("username")
    password = account_data.get("password", "")
    account_name = account_data.get("account_name", username)
    proxy = account_data.get("proxy", "")
    session_cookies = account_data.get("session_cookies", "")
    
    if not username:
        raise HTTPException(status_code=400, detail="Username is required")
    
    # Either password or session_cookies must be provided
    if not password and not session_cookies:
        raise HTTPException(status_code=400, detail="Either password or session cookies are required")
    
    # Validate session cookies JSON if provided
    if session_cookies:
        try:
            json.loads(session_cookies)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Session cookies must be valid JSON")
    
    if STORAGE_MODE == "supabase" and db_service:
        account = db_service.create_account(
            username=username,
            account_name=account_name,
            password=password if password else None,
            proxy=proxy if proxy else None,
            session_cookies=session_cookies if session_cookies else None
        )
        return {
            "username": account["username"],
            "account_name": account["account_name"],
            "created_at": account["created_at"]
        }
    else:
        # Legacy JSON implementation
        accounts = load_accounts()
        
        # Encrypt password if provided
        key = get_or_create_key()
        fernet = Fernet(key)
        
        encrypted_password = None
        if password:
            encrypted_password = fernet.encrypt(password.encode())
        
        # Encrypt session cookies if provided
        encrypted_session_cookies = None
        if session_cookies:
            encrypted_session_cookies = fernet.encrypt(session_cookies.encode())
        
        accounts[username] = {
            "account_name": account_name or username,
            "created_at": datetime.now().timestamp()
        }
        
        if encrypted_password:
            accounts[username]["password"] = base64.b64encode(encrypted_password).decode()
        
        if encrypted_session_cookies:
            accounts[username]["session_cookies"] = base64.b64encode(encrypted_session_cookies).decode()
        
        if proxy:
            # Encrypt proxy as well for security
            encrypted_proxy = fernet.encrypt(proxy.encode())
            accounts[username]["proxy"] = base64.b64encode(encrypted_proxy).decode()
        
        save_accounts(accounts)
        
        return {
            "username": username,
            "account_name": account_name,
            "created_at": accounts[username]["created_at"]
        }


@router.post("/import")
async def import_accounts(file: UploadFile = File(...)):
    """Bulk import accounts from a CSV file. Columns: username (required), password, account_name, sessionid, proxy. Either password or sessionid required per row."""
    if not file.filename or not file.filename.lower().endswith((".csv", ".txt")):
        raise HTTPException(status_code=400, detail="Upload a CSV or TXT file")
    content = await file.read()
    try:
        text = content.decode("utf-8")
    except UnicodeDecodeError:
        try:
            text = content.decode("utf-8-sig")
        except Exception:
            raise HTTPException(status_code=400, detail="File must be UTF-8 encoded")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV has no header row")
    # Normalize column names (case-insensitive)
    col_map = {f.strip().lower(): f for f in reader.fieldnames}
    def get_col(row: dict, *names: str) -> str:
        for n in names:
            key = col_map.get(n.strip().lower())
            if key is not None:
                return (row.get(key) or "").strip()
        return ""
    imported = 0
    errors: List[dict] = []
    accounts_dict = load_accounts() if STORAGE_MODE != "supabase" or not db_service else None
    for i, row in enumerate(reader):
        username = get_col(row, "username", "user", "instagram", "handle")
        password = get_col(row, "password", "pass")
        account_name = get_col(row, "account_name", "display_name", "name")
        sessionid = get_col(row, "sessionid", "session_id", "session")
        proxy = get_col(row, "proxy")
        session_cookies = json.dumps({"sessionid": sessionid}) if sessionid else ""
        if not username:
            errors.append({"row": i + 2, "username": username or "(empty)", "error": "Username is required"})
            continue
        if STORAGE_MODE == "supabase" and db_service:
            ok, err = _create_one_account(username, password, account_name or username, proxy, session_cookies)
        else:
            ok, err = _create_one_account(
                username, password, account_name or username, proxy, session_cookies, accounts_dict=accounts_dict
            )
        if ok:
            imported += 1
        else:
            errors.append({"row": i + 2, "username": username, "error": err or "Unknown error"})
    if accounts_dict is not None:
        save_accounts(accounts_dict)
    return {"imported": imported, "errors": errors}


@router.post("/verify-login")
async def verify_login(payload: dict):
    """Start login verification (username/password). Returns need_2fa + verification_id, or success/session_cookies, or error."""
    username = payload.get("username", "").strip()
    password = payload.get("password", "")
    proxy = (payload.get("proxy") or "").strip() or None
    if not username or not password:
        raise HTTPException(status_code=400, detail="Username and password are required for verification")
    verification_id = str(uuid.uuid4())
    with _verification_lock:
        _verification_results[verification_id] = {"status": "pending"}
    thread = threading.Thread(
        target=_run_verify_login,
        args=(verification_id, username, password, proxy),
        daemon=True,
    )
    thread.start()
    # Poll for up to 20s for initial outcome (success, error, or need_2fa)
    for _ in range(100):
        time.sleep(0.2)
        with _verification_lock:
            r = _verification_results.get(verification_id, {})
        status = r.get("status", "pending")
        if status == "need_2fa":
            return {"need_2fa": True, "verification_id": verification_id, "username": username}
        if status == "success":
            return {
                "success": True,
                "session_cookies": r.get("session_cookies"),
            }
        if status == "error":
            return {"success": False, "error": r.get("error", "Login failed")}
    # Timeout: assume 2FA will be required
    with _verification_lock:
        if _verification_results.get(verification_id, {}).get("status") == "pending":
            _verification_results[verification_id]["status"] = "need_2fa"
    return {"need_2fa": True, "verification_id": verification_id, "username": username}


@router.post("/verify-login/challenge-code")
async def verify_login_challenge_code(payload: dict):
    """Submit 2FA code for account verification. Returns success + session_cookies or error."""
    verification_id = payload.get("verification_id")
    code = (payload.get("code") or "").strip()
    if not verification_id or not code:
        raise HTTPException(status_code=400, detail="verification_id and code are required")
    _set_challenge_code(verification_id, code)
    # Wait for thread to finish (up to 120s)
    for _ in range(240):
        time.sleep(0.5)
        with _verification_lock:
            r = _verification_results.get(verification_id, {})
        status = r.get("status", "pending")
        if status == "success":
            session_cookies = r.get("session_cookies")
            with _verification_lock:
                _verification_results.pop(verification_id, None)
            return {"success": True, "session_cookies": session_cookies}
        if status == "error":
            err = r.get("error", "Verification failed")
            with _verification_lock:
                _verification_results.pop(verification_id, None)
            return {"success": False, "error": err}
    with _verification_lock:
        _verification_results.pop(verification_id, None)
    return {"success": False, "error": "Verification timed out"}


@router.delete("/{username}")
async def delete_account(username: str):
    """Delete an account"""
    if STORAGE_MODE == "supabase" and db_service:
        deleted = db_service.delete_account(username)
        if not deleted:
            raise HTTPException(status_code=404, detail="Account not found")
        return {"message": "Account deleted successfully"}
    else:
        # Legacy JSON implementation
        accounts = load_accounts()
        if username not in accounts:
            raise HTTPException(status_code=404, detail="Account not found")
        
        del accounts[username]
        save_accounts(accounts)
        return {"message": "Account deleted successfully"}
