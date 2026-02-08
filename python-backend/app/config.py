"""
GramSender Desktop - Configuration
Hardcoded Supabase credentials for security. No .env file needed.
"""
import os

# Storage mode: always use supabase for desktop app
STORAGE_MODE = os.getenv("STORAGE_MODE", "supabase")

# Supabase configuration - HARDCODED for security
# Users cannot modify these values
SUPABASE_URL = "https://hwrxnbvntqcxsaoxfdfg.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh3cnhuYnZudHFjeHNhb3hmZGZnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzAzNTY1MTUsImV4cCI6MjA4NTkzMjUxNX0.7uLU_K8yahphqLkVtJFzQHypK7Q5ySxx4I1U-p1zYB8"
# Service role key - needed for database operations
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imh3cnhuYnZudHFjeHNhb3hmZGZnIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MDM1NjUxNSwiZXhwIjoyMDg1OTMyNTE1fQ.HNJ2xkLH49-j8DOp5E4PNy0hbWD4YNOPeTq5_HL3t8g")

# User ID - set by Electron after login
# Note: This is read dynamically in database.py since it may be set after import
SUPABASE_USER_ID = os.getenv("SUPABASE_USER_ID", "")

def get_user_id():
    """Get user ID - reads from environment each time for dynamic updates"""
    return os.getenv("SUPABASE_USER_ID", "") or SUPABASE_USER_ID

# File paths (relative to app location)
_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_APP_DATA = os.path.join(os.path.expanduser("~"), ".gramsender")

# Create app data directory if it doesn't exist
os.makedirs(_APP_DATA, exist_ok=True)

# Sessions directory - in user's app data
SESSIONS_DIR = os.path.join(_APP_DATA, "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)

# Legacy JSON file paths (fallback only)
CAMPAIGNS_FILE = os.path.join(_APP_DATA, "campaigns.json")
ACCOUNTS_FILE = os.path.join(_APP_DATA, "saved_accounts.json")
KEY_FILE = os.path.join(_APP_DATA, "encryption.key")
ASSIGNMENTS_FILE = os.path.join(_APP_DATA, "campaign_assignments.json")
LEADS_DIR = os.path.join(_APP_DATA, "leads")
SENDS_CSV = os.path.join(_APP_DATA, "sends.csv")
REPLIES_CSV = os.path.join(_APP_DATA, "replies.csv")

# Reply monitor settings
REPLY_MONITOR_ENABLED = os.getenv("REPLY_MONITOR_ENABLED", "true").lower() == "true"
REPLY_POLL_INTERVAL = int(os.getenv("REPLY_POLL_INTERVAL", "45"))

# Grok API Configuration (optional)
GROK_API_KEY = os.getenv("GROK_API_KEY", "")
GROK_API_BASE = os.getenv("GROK_API_BASE", "https://api.x.ai/v1")

# Anti-detection (always enabled for desktop app)
ANTI_DETECTION_ENABLED = os.getenv("ANTI_DETECTION_ENABLED", "true").lower() == "true"

# Lead lookup rate limit (seconds between each lead lookup)
LEAD_LOOKUP_DELAY_MIN = float(os.getenv("LEAD_LOOKUP_DELAY_MIN", "8"))
LEAD_LOOKUP_DELAY_MAX = float(os.getenv("LEAD_LOOKUP_DELAY_MAX", "15"))

# HttpCloak settings
HTTPCLOAK_ENABLED = os.getenv("HTTPCLOAK_ENABLED", "false").lower() == "true"
HTTPCLOAK_PRESET = os.getenv("HTTPCLOAK_PRESET", "chrome-143")
HTTPCLOAK_USE_FOR_DM = os.getenv("HTTPCLOAK_USE_FOR_DM", "true").lower() == "true"
HTTPCLOAK_USE_FOR_LOGIN = os.getenv("HTTPCLOAK_USE_FOR_LOGIN", "false").lower() == "true"

# Two-factor code
TWO_FACTOR_CODE = (os.getenv("TWO_FACTOR_CODE") or "").strip() or None

# Fallback proxies
_FALLBACK_PROXIES_RAW = os.getenv("FALLBACK_PROXIES", "")
FALLBACK_PROXIES = [p.strip() for p in _FALLBACK_PROXIES_RAW.split(",") if p.strip()]
