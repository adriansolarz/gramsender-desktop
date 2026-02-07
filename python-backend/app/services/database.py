"""Supabase database service layer with multi-tenant support"""
import os
from typing import Dict, List, Optional
try:
    from supabase import create_client, Client
    SUPABASE_AVAILABLE = True
except ImportError:
    SUPABASE_AVAILABLE = False
    create_client = None
    Client = None
from cryptography.fernet import Fernet
import base64
import json
from datetime import datetime

from ..config import KEY_FILE, STORAGE_MODE, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_ANON_KEY, SUPABASE_USER_ID

class DatabaseService:
    """Service layer for Supabase database operations with multi-tenant support"""
    
    _instance = None
    _client: Optional[Client] = None
    _user_id: Optional[str] = None  # Default user_id for local mode
    
    def __init__(self, user_id: Optional[str] = None):
        if DatabaseService._instance is not None:
            raise Exception("DatabaseService is a singleton")
        
        # Check if supabase is available and configured
        if not SUPABASE_AVAILABLE or STORAGE_MODE != "supabase":
            raise ValueError("Supabase not available or STORAGE_MODE is not 'supabase'. Use JSON storage instead.")
        
        # Use hardcoded values from config (more reliable) or fall back to env vars
        supabase_url = SUPABASE_URL or os.getenv("SUPABASE_URL")
        supabase_key = SUPABASE_SERVICE_ROLE_KEY or os.getenv("SUPABASE_SERVICE_ROLE_KEY") or SUPABASE_ANON_KEY or os.getenv("SUPABASE_ANON_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY or SUPABASE_ANON_KEY must be set")
        
        print(f"[DatabaseService] Connecting to Supabase: {supabase_url[:30]}...")
        self.client = create_client(supabase_url, supabase_key)
        DatabaseService._client = self.client
        DatabaseService._instance = self
        
        # Set user_id from parameter, config, or environment
        self._user_id = user_id or SUPABASE_USER_ID or os.getenv("SUPABASE_USER_ID")
        print(f"[DatabaseService] User ID: {self._user_id[:8] if self._user_id else 'not set'}...")
    
    @classmethod
    def get_instance(cls, user_id: Optional[str] = None):
        """Get singleton instance"""
        if cls._instance is None:
            cls._instance = cls(user_id)
        return cls._instance
    
    def set_user_id(self, user_id: str):
        """Set the current user_id for multi-tenant operations"""
        self._user_id = user_id
    
    def get_user_id(self) -> Optional[str]:
        """Get the current user_id"""
        return self._user_id
    
    def get_encryption_key(self) -> bytes:
        """Get or create encryption key"""
        if os.path.exists(KEY_FILE):
            with open(KEY_FILE, "rb") as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(KEY_FILE, "wb") as f:
                f.write(key)
            return key
    
    def encrypt(self, data: str) -> str:
        """Encrypt sensitive data"""
        if not data:
            return None
        key = self.get_encryption_key()
        fernet = Fernet(key)
        encrypted = fernet.encrypt(data.encode())
        return base64.b64encode(encrypted).decode()
    
    def decrypt(self, encrypted_data: str) -> Optional[str]:
        """Decrypt sensitive data"""
        if not encrypted_data:
            return None
        try:
            key = self.get_encryption_key()
            fernet = Fernet(key)
            encrypted = base64.b64decode(encrypted_data)
            return fernet.decrypt(encrypted).decode()
        except Exception:
            return None
    
    # Accounts methods
    def get_accounts(self, user_id: Optional[str] = None) -> Dict:
        """Get all accounts (without sensitive data)"""
        try:
            query = self.client.table("accounts").select("id, username, account_name, created_at")
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            result = {}
            for row in response.data:
                result[row["username"]] = {
                    "id": row.get("id"),
                    "username": row["username"],
                    "account_name": row.get("account_name", row["username"]),
                    "created_at": row.get("created_at")
                }
            return result
        except Exception as e:
            print(f"Error getting accounts: {e}")
            return {}
    
    def get_account(self, username: str, user_id: Optional[str] = None) -> Optional[Dict]:
        """Get account with decrypted sensitive data"""
        try:
            query = self.client.table("accounts").select("*").eq("username", username)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            if not response.data:
                return None
            
            account = response.data[0]
            result = {
                "id": account.get("id"),
                "username": account["username"],
                "account_name": account.get("account_name", account["username"]),
                "created_at": account.get("created_at")
            }
            
            # Decrypt sensitive fields
            if account.get("password"):
                result["password"] = self.decrypt(account["password"])
            if account.get("proxy"):
                result["proxy"] = self.decrypt(account["proxy"])
            if account.get("session_cookies"):
                result["session_cookies"] = self.decrypt(account["session_cookies"])
            
            return result
        except Exception as e:
            print(f"Error getting account {username}: {e}")
            return None
    
    def create_account(self, username: str, account_name: str, password: Optional[str] = None,
                       proxy: Optional[str] = None, session_cookies: Optional[str] = None,
                       user_id: Optional[str] = None) -> Dict:
        """Create a new account"""
        uid = user_id or self._user_id
        data = {
            "username": username,
            "account_name": account_name or username,
            "created_at": datetime.now().isoformat()
        }
        
        if uid:
            data["user_id"] = uid
        
        if password:
            data["password"] = self.encrypt(password)
        if proxy:
            data["proxy"] = self.encrypt(proxy)
        if session_cookies:
            data["session_cookies"] = self.encrypt(session_cookies)
        
        response = self.client.table("accounts").insert(data).execute()
        return response.data[0] if response.data else {}
    
    def update_account(self, username: str, user_id: Optional[str] = None, **kwargs) -> Optional[Dict]:
        """Update account"""
        data = {}
        if "password" in kwargs and kwargs["password"]:
            data["password"] = self.encrypt(kwargs["password"])
        if "proxy" in kwargs and kwargs["proxy"]:
            data["proxy"] = self.encrypt(kwargs["proxy"])
        if "session_cookies" in kwargs and kwargs["session_cookies"]:
            data["session_cookies"] = self.encrypt(kwargs["session_cookies"])
        if "account_name" in kwargs:
            data["account_name"] = kwargs["account_name"]
        
        if not data:
            return None
        
        query = self.client.table("accounts").update(data).eq("username", username)
        uid = user_id or self._user_id
        if uid:
            query = query.eq("user_id", uid)
        response = query.execute()
        return response.data[0] if response.data else None
    
    def delete_account(self, username: str, user_id: Optional[str] = None) -> bool:
        """Delete account"""
        try:
            query = self.client.table("accounts").delete().eq("username", username)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Error deleting account {username}: {e}")
            return False
    
    # Campaigns methods
    def get_campaigns(self, user_id: Optional[str] = None) -> Dict:
        """Get all campaigns"""
        try:
            query = self.client.table("campaigns").select("*")
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            result = {}
            for row in response.data:
                result[row["id"]] = row
            return result
        except Exception as e:
            print(f"Error getting campaigns: {e}")
            return {}
    
    def get_campaign(self, campaign_id: str, user_id: Optional[str] = None) -> Optional[Dict]:
        """Get a specific campaign"""
        try:
            query = self.client.table("campaigns").select("*").eq("id", campaign_id)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            return response.data[0] if response.data else None
        except Exception as e:
            print(f"Error getting campaign {campaign_id}: {e}")
            return None
    
    def create_campaign(self, campaign_id: str, campaign_data: Dict, user_id: Optional[str] = None) -> Dict:
        """Create a new campaign"""
        uid = user_id or self._user_id
        data = {
            "id": campaign_id,
            **campaign_data,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        if uid:
            data["user_id"] = uid
        response = self.client.table("campaigns").insert(data).execute()
        return response.data[0] if response.data else {}
    
    def update_campaign(self, campaign_id: str, campaign_data: Dict, user_id: Optional[str] = None) -> Optional[Dict]:
        """Update campaign"""
        data = {**campaign_data, "updated_at": datetime.now().isoformat()}
        query = self.client.table("campaigns").update(data).eq("id", campaign_id)
        uid = user_id or self._user_id
        if uid:
            query = query.eq("user_id", uid)
        response = query.execute()
        return response.data[0] if response.data else None
    
    def delete_campaign(self, campaign_id: str, user_id: Optional[str] = None) -> bool:
        """Delete campaign"""
        try:
            query = self.client.table("campaigns").delete().eq("id", campaign_id)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Error deleting campaign {campaign_id}: {e}")
            return False
    
    # Assignments methods
    def get_assignments(self, user_id: Optional[str] = None) -> Dict:
        """Get all assignments"""
        try:
            query = self.client.table("assignments").select("*, accounts(username)")
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            result = {}
            for row in response.data:
                # Support both old (username) and new (account_id) schemas
                username = row.get("username") or (row.get("accounts", {}) or {}).get("username")
                if username:
                    result[username] = row["campaign_id"]
            return result
        except Exception as e:
            print(f"Error getting assignments: {e}")
            return {}
    
    def create_assignment(self, username: str, campaign_id: str, user_id: Optional[str] = None,
                         account_id: Optional[str] = None) -> Optional[Dict]:
        """Create assignment"""
        uid = user_id or self._user_id
        data = {
            "campaign_id": campaign_id
        }
        if uid:
            data["user_id"] = uid
        if account_id:
            data["account_id"] = account_id
        else:
            data["username"] = username
        
        # Use upsert to handle existing assignments
        response = self.client.table("assignments").upsert(data, on_conflict="account_id" if account_id else "username").execute()
        return response.data[0] if response.data else None
    
    def delete_assignment(self, username: str, user_id: Optional[str] = None) -> bool:
        """Delete assignment"""
        try:
            query = self.client.table("assignments").delete().eq("username", username)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            return len(response.data) > 0
        except Exception as e:
            print(f"Error deleting assignment {username}: {e}")
            return False
    
    # Sends methods (message tracking)
    def record_send(self, account_username: str, recipient_username: str, 
                   account_name: str = None, campaign_id: str = None,
                   campaign_name: str = None, lead_source: str = None,
                   lead_target: str = None, recipient_user_id: str = None,
                   message_preview: str = None, user_id: Optional[str] = None,
                   account_id: Optional[str] = None) -> Optional[Dict]:
        """Record a sent message"""
        try:
            uid = user_id or self._user_id
            data = {
                "account_username": account_username,
                "recipient_username": recipient_username,
                "timestamp": datetime.now().isoformat(),
            }
            if uid:
                data["user_id"] = uid
            if account_id:
                data["account_id"] = account_id
            if account_name:
                data["account_name"] = account_name
            if campaign_id:
                data["campaign_id"] = campaign_id
            if campaign_name:
                data["campaign_name"] = campaign_name
            if lead_source:
                data["lead_source"] = lead_source
            if lead_target:
                data["lead_target"] = lead_target
            if recipient_user_id:
                data["recipient_user_id"] = str(recipient_user_id)
            if message_preview:
                data["message_preview"] = message_preview[:500]  # Limit preview length
            
            response = self.client.table("sends").insert(data).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            print(f"Error recording send: {e}")
            return None
    
    def get_sends(self, limit: int = 100, user_id: Optional[str] = None) -> List[Dict]:
        """Get recent sends"""
        try:
            query = self.client.table("sends").select("*").order("timestamp", desc=True).limit(limit)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            return response.data or []
        except Exception as e:
            print(f"Error getting sends: {e}")
            return []
    
    # Replies methods
    def record_reply(self, account_username: str, sender_username: str,
                    sender_user_id: str = None, message_preview: str = None,
                    is_inbound: bool = False, user_id: Optional[str] = None,
                    account_id: Optional[str] = None) -> Optional[Dict]:
        """Record a reply or inbound message"""
        try:
            uid = user_id or self._user_id
            data = {
                "account_username": account_username,
                "sender_username": sender_username,
                "timestamp": datetime.now().isoformat(),
                "is_inbound": is_inbound,
            }
            if uid:
                data["user_id"] = uid
            if account_id:
                data["account_id"] = account_id
            if sender_user_id:
                data["sender_user_id"] = str(sender_user_id)
            if message_preview:
                data["message_preview"] = message_preview[:500]
            
            response = self.client.table("replies").insert(data).execute()
            return response.data[0] if response.data else None
        except Exception as e:
            print(f"Error recording reply: {e}")
            return None
    
    def get_replies(self, limit: int = 500, user_id: Optional[str] = None) -> List[Dict]:
        """Get recent replies"""
        try:
            query = self.client.table("replies").select("*").order("timestamp", desc=True).limit(limit)
            uid = user_id or self._user_id
            if uid:
                query = query.eq("user_id", uid)
            response = query.execute()
            return response.data or []
        except Exception as e:
            print(f"Error getting replies: {e}")
            return []