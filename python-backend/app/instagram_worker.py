"""Refactored Instagram Worker for FastAPI backend"""
import os
import time
import random
import threading
import json
from datetime import datetime

SENT_DMS_FILE = os.path.join(os.path.dirname(__file__), "sent_dms.json")
_sent_dms_lock = threading.Lock()

# Monkey-patch to fix instagrapi 2.0.0 compatibility with Pydantic 2.x
# MUST patch BEFORE importing Client, as Client import triggers mixin imports
try:
    # Patch extract_user_gql to accept update_headers parameter
    from instagrapi import extractors
    import inspect
    
    original_extract_user_gql = extractors.extract_user_gql
    sig = inspect.signature(original_extract_user_gql)
    has_update_headers = 'update_headers' in sig.parameters
    
    if not has_update_headers:
        def patched_extract_user_gql(data, update_headers=None, **kwargs):
            """Patched version that accepts but ignores update_headers parameter"""
            return original_extract_user_gql(data, **kwargs)
        
        extractors.extract_user_gql = patched_extract_user_gql
    
    # Also patch the user_info_by_username_gql method directly to handle the call
    from instagrapi.mixins import user as user_mixin
    original_user_info_by_username_gql = user_mixin.UserMixin.user_info_by_username_gql
    
    def patched_user_info_by_username_gql(self, username, update_headers=True):
        """Patched version that safely calls extract_user_gql"""
        import json
        from instagrapi.extractors import extract_user_gql
        
        temporary_public_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        response = self.public_request(
            f'https://www.instagram.com/api/v1/users/web_profile_info/?username={username}',
            headers=temporary_public_headers
        )
        
        user_data = json.loads(response)['data']['user']
        
        # Call with or without update_headers based on function signature
        sig = inspect.signature(extract_user_gql)
        if 'update_headers' in sig.parameters:
            return extract_user_gql(user_data, update_headers=update_headers)
        else:
            return extract_user_gql(user_data)
    
    user_mixin.UserMixin.user_info_by_username_gql = patched_user_info_by_username_gql
    
except Exception as e:
    import sys
    print(f"Warning: Could not patch instagrapi: {e}", file=sys.stderr)

# Now import Client after patching
from instagrapi import Client
from instagrapi.exceptions import ClientError, LoginRequired, PleaseWaitFewMinutes
from requests.exceptions import TooManyRedirects

from .config import SESSIONS_DIR, LEADS_DIR, LEAD_LOOKUP_DELAY_MIN, LEAD_LOOKUP_DELAY_MAX, HTTPCLOAK_USE_FOR_DM
from .instagram_login import InstagramLoginHelper
from .services.grok_gender_detector import GrokGenderDetector

# #region agent log
def _agent_log(location: str, message: str, data: dict, hypothesis_id: str = ""):
    try:
        p = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", ".cursor", "debug.log"))
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({"location": location, "message": message, "data": data, "hypothesisId": hypothesis_id, "timestamp": datetime.now().isoformat(), "sessionId": "debug-session"}) + "\n")
    except Exception:
        pass
# #endregion

# Import utility functions - copy them here to avoid import issues
def get_human_delay(base_delay, variation=0.3):
    """Get a human-like delay with natural variation"""
    variation_amount = base_delay * variation
    min_delay = max(0.1, base_delay - variation_amount)
    max_delay = base_delay + variation_amount
    return random.uniform(min_delay, max_delay)

def get_random_human_delay():
    """Get a random delay that mimics human behavior"""
    common_delays = [1.2, 2.3, 3.7, 5.1, 8.4, 12.6, 18.9, 25.3]
    base_delay = random.choice(common_delays)
    return get_human_delay(base_delay, 0.4)

def apply_spintax(text: str, firstname: str = "") -> str:
    """Expand simple spintax expressions using [opt1|opt2|...] syntax."""
    import re
    if firstname:
        text = text.replace("{firstname}", firstname)
    pattern = re.compile(r"\[([^\[\]]*?\|[^\[\]]*?)\]")
    while True:
        match = pattern.search(text)
        if not match:
            break
        options = match.group(1).split("|")
        replacement = random.choice(options).strip()
        text = text[: match.start()] + replacement + text[match.end():]
    return text


def follow_up_delay_seconds(fu: dict) -> int:
    """Convert follow-up delay_value + delay_unit to seconds."""
    try:
        val = int(fu.get("delay_value", 0))
        val = max(0, val)
    except (TypeError, ValueError):
        val = 0
    unit = (fu.get("delay_unit") or "hours").lower()
    if unit == "minutes":
        return val * 60
    if unit == "hours":
        return val * 3600
    if unit == "days":
        return val * 86400
    return val * 3600


FIRST_WORLD_COUNTRIES = {
    'US', 'USA', 'United States', 'United States of America',
    'CA', 'Canada', 'GB', 'UK', 'United Kingdom', 'England', 'Scotland', 'Wales',
    'AU', 'Australia', 'NZ', 'New Zealand', 'DE', 'Germany', 'FR', 'France',
    'IT', 'Italy', 'ES', 'Spain', 'NL', 'Netherlands', 'BE', 'Belgium',
    'CH', 'Switzerland', 'AT', 'Austria', 'SE', 'Sweden', 'NO', 'Norway',
    'DK', 'Denmark', 'FI', 'Finland', 'IE', 'Ireland', 'JP', 'Japan',
    'KR', 'South Korea', 'SG', 'Singapore', 'HK', 'Hong Kong',
}

def is_first_world_country(country_name: str) -> bool:
    """Check if a country is considered first world/developed"""
    if not country_name:
        return False
    normalized = country_name.strip().title()
    if normalized in FIRST_WORLD_COUNTRIES:
        return True
    for country in FIRST_WORLD_COUNTRIES:
        if normalized in country or country in normalized:
            return True
    return False

def bio_contains_keywords(bio_text: str, keywords: list) -> bool:
    """Check if bio contains any of the specified keywords"""
    if not bio_text or not keywords:
        return True
    bio_lower = bio_text.lower().strip()
    for keyword in keywords:
        keyword_clean = keyword.strip().lower()
        if keyword_clean and keyword_clean in bio_lower:
            return True
    return False

def detect_gender_from_name(full_name: str, first_name: str = "") -> str:
    """Attempt to detect gender from name using common patterns"""
    if not full_name and not first_name:
        return "unknown"

    name = (first_name or full_name).strip().lower()
    if not name:
        return "unknown"

    # Common female name endings/patterns
    female_indicators = ['a', 'ia', 'ella', 'ette', 'ina', 'elle', 'anna', 'ella', 'sophia', 'emma', 'olivia']
    # Common male name endings/patterns
    male_indicators = ['o', 'er', 'on', 'en', 'an', 'el', 'ian', 'io']

    # Check if name ends with common patterns
    for indicator in female_indicators:
        if name.endswith(indicator) and len(name) > 3:
            return "female"

    for indicator in male_indicators:
        if name.endswith(indicator) and len(name) > 3:
            return "male"

    return "unknown"


def _append_sent_dm(campaign_id, account_username, recipient_username, thread_id, sent_at, initial_message, message_type="initial", follow_up_index=0):
    """Append sent DM record to sent_dms.json for linking replies to campaigns."""
    with _sent_dms_lock:
        try:
            with open(SENT_DMS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = []
        data.append({
            "campaign_id": campaign_id,
            "account_username": account_username,
            "recipient_username": recipient_username,
            "thread_id": str(thread_id or ""),
            "sent_at": sent_at,
            "initial_message": initial_message,
            "message_type": message_type,
            "follow_up_index": follow_up_index
        })
        with open(SENT_DMS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)


def _get_global_settings():
    """Get global settings from settings.json."""
    try:
        with open(os.path.join(os.path.dirname(__file__), "..", "settings.json"), "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _send_webhook(campaign_id, event, payload):
    """Send webhook for campaign performance events."""
    from .config import CAMPAIGNS_FILE, STORAGE_MODE
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
    url = None
    secret = ""
    if campaign and campaign.get("webhook_url"):
        url = campaign["webhook_url"]
    else:
        # Fallback to global
        global_settings = _get_global_settings()
        if global_settings.get("global_webhook_url") and event in global_settings.get("webhook_events", []):
            url = global_settings["global_webhook_url"]
            secret = global_settings.get("webhook_secret", "")
    if url:
        full_payload = {
            "event": event,
            "campaign_id": campaign_id,
            "timestamp": datetime.now().isoformat(),
            "app_version": "1.0",
            "secret": secret,
            **payload
        }
        try:
            requests.post(url, json=full_payload, timeout=10)
        except Exception as e:
            print(f"[Webhook] Failed to send {event} for campaign {campaign_id}: {e}")

class InstagramWorkerThread(threading.Thread):
    """Thread-based Instagram worker that uses callbacks instead of PyQt5 signals"""
    
    # Shared tracking across all workers
    _shared_processed_users = set()
    _shared_lock = threading.Lock()
    
    def __init__(self, worker_id="", username=None, password=None, target_mode=0, target_input="", followers_threshold=0,
                 message_templates=None, message_count=0, country_filter_enabled=False, bio_filter_enabled=False,
                 bio_keywords=None, gender_filter="all", account_name="", proxy=None, session_cookies=None,
                 debug_mode=False, min_delay=3, max_delay=8,
                 min_message_delay=2, max_message_delay=5, daily_limit=50, enable_rotation=True,
                 enable_sessions=True, human_behavior=True, on_update=None, on_progress=None, on_error=None,
                 on_complete=None, on_message_sent=None, on_request_challenge_code=None,
                 campaign_id=None, lead_count=0, follow_ups=None):
        super().__init__(daemon=True)
        self.worker_id = worker_id
        self.username = username
        self.password = password
        self.account_name = account_name or username
        self.proxy = proxy
        self.session_cookies = session_cookies
        self.on_request_challenge_code = on_request_challenge_code
        self.target_mode = target_mode
        self.target_input = target_input
        self.campaign_id = campaign_id
        self.lead_count = lead_count
        self.followers_threshold = followers_threshold
        self.message_templates = message_templates
        self.message_count = message_count
        self.follow_ups = follow_ups if follow_ups is not None else []
        self.country_filter_enabled = country_filter_enabled
        self.bio_filter_enabled = bio_filter_enabled
        self.bio_keywords = bio_keywords
        self.gender_filter = gender_filter
        self.grok_detector = GrokGenderDetector()  # Initialize Grok detector
        self.debug_mode = debug_mode
        self.running = False
        self.client = None
        self.login_attempts = 0
        self.max_login_attempts = 3
        
        # Callbacks
        self.on_update = on_update or (lambda x: None)
        self.on_progress = on_progress or (lambda x: None)
        self.on_error = on_error or (lambda x: None)
        self.on_complete = on_complete or (lambda *a, **kw: None)
        self.on_message_sent = on_message_sent or (lambda *a, **kw: None)
        if message_templates is None:
            message_templates = []
        if bio_keywords is None:
            bio_keywords = []
        
        # Settings
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.min_message_delay = min_message_delay
        self.max_message_delay = max_message_delay
        self.daily_limit = daily_limit
        self.enable_rotation = enable_rotation
        self.enable_sessions = enable_sessions
        self.fingerprint_rotation_enabled = enable_rotation
        self.human_behavior_enabled = human_behavior
        
        # Debug stats
        self.debug_stats = {
            'users_found': 0,
            'users_filtered_country': 0,
            'users_filtered_bio': 0,
            'users_filtered_followers': 0,
            'users_filtered_gender': 0,
            'users_already_processed': 0,
            'messages_sent': 0,
            'errors': 0,
            'login_attempts': 0,
            'fingerprint_changes': 0
        }
        # Set when we exit early due to auth/session failure (so UI shows failed, not completed)
        self._auth_failure = False
        # When user submits code via 2FA modal (ChallengeResolve/submit_phone), use for next login
        self._verification_code = ""
        
        self.debug_log("Worker initialized", f"Account: {self.account_name}, Target: {self.target_input}, Mode: {self.target_mode}")
    
    def create_client_with_fingerprint(self):
        """Create a new client with basic setup"""
        client = Client(proxy=self.proxy) if self.proxy else Client()
        self.debug_log("Created basic client", "Skipping fingerprint rotation due to API limitations")
        self.debug_stats['fingerprint_changes'] += 1
        return client
    
    def attempt_login_with_retry(self):
        """Attempt login using shared InstagramLoginHelper (session file -> sessionid -> username/password + 2FA)."""
        # #region agent log
        _agent_log("instagram_worker.py:attempt_login_with_retry", "entry", {"thread_id": threading.current_thread().ident}, "H3")
        # #endregion
        sessionid = None
        if self.session_cookies:
            try:
                cookies = json.loads(self.session_cookies)
                sessionid = (cookies.get("sessionid") or "").strip()
                if sessionid:
                    self.debug_log("Using sessionid from cookies", "Will try login_by_sessionid")
            except Exception as e:
                self.debug_log("Could not parse session_cookies", str(e))
        if not self.password and not sessionid:
            self.debug_log("Login failed", "Password or session cookies required")
            _agent_log("instagram_worker.py:attempt_login_with_retry", "exit", {"result": False, "reason": "no_creds"}, "H3")
            return False
        challenge_callback = self.on_request_challenge_code if self.on_request_challenge_code else None
        sessions_dir = SESSIONS_DIR
        if not self.enable_sessions:
            import tempfile
            sessions_dir = tempfile.mkdtemp(prefix="insta_worker_")
        helper = InstagramLoginHelper(
            username=self.username,
            password=self.password or "",
            sessions_dir=sessions_dir,
            proxy=self.proxy,
            sessionid=sessionid or None,
            challenge_code_callback=challenge_callback,
        )
        try:
            if helper.login():
                self.client = helper.client
                self.debug_stats["login_attempts"] += 1
                self.debug_log("Login successful", "Via InstagramLoginHelper")
                _agent_log("instagram_worker.py:attempt_login_with_retry", "exit", {"result": True, "reason": "helper_login"}, "H3")
                return True
        except Exception as e:
            self.debug_log("Login failed", str(e))
        _agent_log("instagram_worker.py:attempt_login_with_retry", "exit", {"result": False, "reason": "helper_failed"}, "H3")
        return False
    
    def _delete_stale_session_file(self):
        """Remove saved session file so next login is fresh (username/password)."""
        os.makedirs(SESSIONS_DIR, exist_ok=True)
        session_file = os.path.join(SESSIONS_DIR, f"{self.username}.json")
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
                self.debug_log("Deleted stale session file", session_file)
            except OSError as e:
                self.debug_log("Could not delete session file", str(e))

    def _is_rate_limit_error(self, error) -> bool:
        """Check if error is rate limit or restriction (like simple login + dm)."""
        s = str(error).lower()
        if "1545041" in s or ("error_code" in s and "403" in s):
            return True
        if "403" in s or "rate limit" in s or "too many" in s:
            return True
        return False

    def ensure_session_valid(self) -> bool:
        """Proactive session check (like simple login + dm). Try get_timeline_feed; on LoginRequired re-login."""
        try:
            if hasattr(self.client, "get_timeline_feed"):
                self.client.get_timeline_feed()
                return True
        except LoginRequired:
            pass
        except Exception:
            pass
        self.debug_log("Session invalid or expired", "Re-authenticating...")
        if self.password and hasattr(self.client, "relogin"):
            try:
                self.client.relogin()
                self.debug_log("Re-authenticated", "via relogin()")
                return True
            except Exception:
                pass
        self._delete_stale_session_file()
        return self.attempt_login_with_retry()

    def retry_on_login_required(self, func, *args, max_retries=2, operation_name="operation", **kwargs):
        """
        Retry a function call if it raises LoginRequired, attempting re-login first.
        
        Args:
            func: The function to call
            *args: Positional arguments for the function
            max_retries: Maximum number of retries after re-login
            operation_name: Name of the operation for logging
            **kwargs: Keyword arguments for the function
        
        Returns:
            The result of the function call
        
        Raises:
            The original exception if retries are exhausted
        """
        last_exception = None
        
        for attempt in range(max_retries + 1):
            try:
                return func(*args, **kwargs)
            except LoginRequired as e:
                last_exception = e
                if attempt < max_retries:
                    self.debug_log("Login required detected", f"{operation_name} - Attempt {attempt + 1}/{max_retries + 1}")
                    self.on_error(f"[{self.account_name}] Session expired during {operation_name}, attempting re-login...")
                    # Try relogin() first (like simple login + dm), then full login
                    re_logged_in = False
                    if self.password and hasattr(self.client, "relogin"):
                        try:
                            self.client.relogin()
                            re_logged_in = True
                            self.debug_log("Re-authenticated", "via relogin()")
                        except Exception:
                            pass
                    if not re_logged_in:
                        self._delete_stale_session_file()
                        re_logged_in = self.attempt_login_with_retry()
                    if re_logged_in:
                        self.debug_log("Re-login successful", f"Retrying {operation_name}")
                        # Like simple login + dm: space requests after re-auth before retry
                        time.sleep(random.uniform(3, 6))
                        continue
                    else:
                        self.on_error(f"[{self.account_name}] Failed to re-authenticate during {operation_name}")
                        raise
                else:
                    # Max retries reached
                    self.on_error(f"[{self.account_name}] Max retries reached for {operation_name} after re-login attempts")
                    raise
            except PleaseWaitFewMinutes as e:
                last_exception = e
                wait_time = random.uniform(60, 120)
                self.on_update(f"[{self.account_name}] Instagram says wait. Pausing {int(wait_time)}s...")
                time.sleep(wait_time)
                if attempt < max_retries:
                    continue
                raise
            except Exception as e:
                error_str = str(e).lower()
                if "429" in error_str or "too many" in error_str:
                    last_exception = e
                    wait_time = random.uniform(45, 90)
                    self.on_update(f"[{self.account_name}] Rate limited (429). Waiting {int(wait_time)}s...")
                    time.sleep(wait_time)
                    if attempt < max_retries:
                        continue
                # For other exceptions, don't retry
                raise
        
        # Should never reach here, but just in case
        if last_exception:
            raise last_exception
        raise Exception(f"Unexpected error in retry_on_login_required for {operation_name}")
    
    def simulate_human_behavior(self, action_type="general"):
        """Simulate human-like behavior patterns"""
        if not self.human_behavior_enabled:
            return
        
        if action_type == "login":
            delay = get_human_delay(random.uniform(3, 8), 0.5)
        elif action_type == "message":
            delay = get_human_delay(random.uniform(2, 6), 0.4)
        elif action_type == "browse":
            delay = get_random_human_delay()
        else:
            delay = get_human_delay(random.uniform(1, 4), 0.3)
        
        self.debug_log("Human behavior simulation", f"Action: {action_type}, Delay: {delay:.2f}s")
        time.sleep(delay)
    
    def simulate_reading_time(self, text_length):
        """Simulate realistic reading time based on text length"""
        if not self.human_behavior_enabled:
            return
        
        words = text_length / 5
        reading_time = (words / 250) * 60
        actual_time = max(2, get_human_delay(reading_time, 0.6))
        self.debug_log("Reading simulation", f"Text length: {text_length}, Reading time: {actual_time:.2f}s")
        time.sleep(actual_time)
    
    def simulate_profile_browsing(self):
        """Simulate human browsing behavior on profiles"""
        if not self.human_behavior_enabled:
            return
        
        behaviors = [
            {"action": "scroll_down", "delay": (1, 3)},
            {"action": "pause", "delay": (2, 5)},
            {"action": "scroll_up", "delay": (0.5, 2)},
            {"action": "pause", "delay": (1, 4)},
            {"action": "scroll_down", "delay": (2, 4)},
            {"action": "pause", "delay": (3, 8)}
        ]
        
        num_behaviors = random.randint(2, 4)
        selected_behaviors = random.sample(behaviors, num_behaviors)
        
        for behavior in selected_behaviors:
            delay = random.uniform(behavior["delay"][0], behavior["delay"][1])
            self.debug_log("Profile browsing", f"Action: {behavior['action']}, Delay: {delay:.2f}s")
            time.sleep(delay)
    
    def simulate_message_composition(self, message):
        """Simulate realistic message composition time"""
        if not self.human_behavior_enabled:
            return
        
        composition_time = len(message) * 0.1
        thinking_time = random.uniform(2, 8)
        total_time = composition_time + thinking_time
        actual_time = get_human_delay(total_time, 0.5)
        self.debug_log("Message composition", f"Message length: {len(message)}, Composition time: {actual_time:.2f}s")
        time.sleep(actual_time)
    
    def add_random_human_delays(self):
        """Add random human-like delays throughout the process"""
        if not self.human_behavior_enabled:
            return
        
        if random.random() < 0.3:
            pause = random.uniform(0.5, 2.0)
            self.debug_log("Micro-pause", f"Duration: {pause:.2f}s")
            time.sleep(pause)
        
        if random.random() < 0.1:
            pause = random.uniform(5, 15)
            self.debug_log("Distraction pause", f"Duration: {pause:.2f}s")
            time.sleep(pause)
    
    def debug_log(self, action, details=""):
        """Log debug information"""
        if self.debug_mode:
            timestamp = datetime.now().strftime("%H:%M:%S")
            debug_msg = f"[DEBUG {timestamp}] [{self.account_name}] {action}"
            if details:
                debug_msg += f" - {details}"
            self.on_update(debug_msg)
    
    def get_users_from_hashtag(self):
        """Get users from hashtag posts"""
        # Validate session before proceeding
        try:
            self.client.account_info()
        except (LoginRequired, ClientError) as e:
            self.on_error(f"[{self.account_name}] Session expired, attempting re-login...")
            self._delete_stale_session_file()
            if not self.attempt_login_with_retry():
                self.on_error(f"[{self.account_name}] Failed to re-authenticate")
                self._auth_failure = True
                # #region agent log
                _agent_log("instagram_worker.py:get_users_from_hashtag", "return_empty_auth_failure", {"branch": "initial_reauth_failed"}, "H4")
                # #endregion
                return []
        
        retry_with_code = True
        while retry_with_code:
            retry_with_code = False
            try:
                # Use retry logic for hashtag_medias_recent
                medias = self.retry_on_login_required(
                    self.client.hashtag_medias_recent,
                    self.target_input,
                    50,  # amount
                    operation_name=f"get hashtag posts for #{self.target_input}",
                    max_retries=2
                )
                
                if not medias:
                    self.on_update(f"[{self.account_name}] No posts found with hashtag #{self.target_input}")
                    return []
                
                users = []
                seen_users = set()
                for media in medias:
                    user = media.user
                    if user.pk not in seen_users:
                        users.append(user)
                        seen_users.add(user.pk)
                
                return users
            except LoginRequired as e:
                self.on_error(f"[{self.account_name}] Login required for hashtag #{self.target_input} after all retries: {str(e)}")
                self._auth_failure = True
                # #region agent log
                _agent_log("instagram_worker.py:get_users_from_hashtag", "return_empty_auth_failure", {"branch": "LoginRequired_after_retries"}, "H4")
                # #endregion
                return []
            except Exception as e:
                error_msg = str(e)
                self.on_error(f"[{self.account_name}] Error getting hashtag users: {error_msg}")
                is_challenge = "ChallengeResolve" in error_msg or "challenge" in error_msg.lower() or "submit_phone" in error_msg
                if is_challenge and self.on_request_challenge_code:
                    self.on_update(f"[{self.account_name}] Verification code required (SMS/email). Please enter the code in the modal.")
                    code = self.on_request_challenge_code(self.username, "SMS")
                    if code:
                        self._verification_code = code
                        self._delete_stale_session_file()
                        if self.attempt_login_with_retry():
                            retry_with_code = True
                            continue
                if is_challenge:
                    self._auth_failure = True
                return []
    
    def get_user_followers(self):
        """Get followers of specific users"""
        all_followers = []
        usernames = [username.strip() for username in self.target_input.split(',')]
        
        # Validate session before proceeding
        try:
            self.client.account_info()
        except (LoginRequired, ClientError) as e:
            self.on_error(f"[{self.account_name}] Session expired, attempting re-login...")
            self._delete_stale_session_file()
            if not self.attempt_login_with_retry():
                self.on_error(f"[{self.account_name}] Failed to re-authenticate")
                self._auth_failure = True
                return []
        
        for username in usernames:
            if not username:
                continue
            
            retry_with_code = True
            while retry_with_code:
                retry_with_code = False
                try:
                    self.on_update(f"[{self.account_name}] Getting followers of @{username}...")
                    
                    # Use retry logic for user_info_by_username
                    user_info = self.retry_on_login_required(
                        self.client.user_info_by_username,
                        username,
                        operation_name=f"get user info for @{username}",
                        max_retries=2
                    )
                    
                    if not user_info:
                        self.on_update(f"[{self.account_name}] User @{username} not found")
                        break
                    
                    # Use retry logic for user_followers
                    followers = self.retry_on_login_required(
                        self.client.user_followers,
                        user_info.pk,
                        100,  # amount
                        operation_name=f"get followers of @{username}",
                        max_retries=2
                    )
                    
                    followers_list = list(followers.values())
                    all_followers.extend(followers_list)
                    
                    self.on_update(f"[{self.account_name}] Found {len(followers_list)} followers from @{username}")
                    
                    if len(usernames) > 1:
                        time.sleep(random.uniform(5, 10))
                    break
                        
                except TooManyRedirects as e:
                    self.on_error(f"[{self.account_name}] Instagram is rate-limiting this account. Please wait a few minutes before trying again. Error: {str(e)}")
                    time.sleep(random.uniform(30, 60))
                    break
                except LoginRequired as e:
                    self.on_error(f"[{self.account_name}] Login required for @{username} after all retries: {str(e)}")
                    break
                except Exception as e:
                    error_msg = str(e)
                    if "Please wait" in error_msg or "rate limit" in error_msg.lower():
                        self.on_error(f"[{self.account_name}] Instagram rate limit detected. Please wait before trying again. Error: {error_msg}")
                        time.sleep(random.uniform(30, 60))
                        break
                    self.on_error(f"[{self.account_name}] Error getting followers from @{username}: {error_msg}")
                    is_challenge = "ChallengeResolve" in error_msg or "challenge" in error_msg.lower() or "submit_phone" in error_msg
                    if is_challenge and self.on_request_challenge_code:
                        self.on_update(f"[{self.account_name}] Verification code required (SMS/email). Please enter the code in the modal.")
                        code = self.on_request_challenge_code(self.username, "SMS")
                        if code:
                            self._verification_code = code
                            self._delete_stale_session_file()
                            if self.attempt_login_with_retry():
                                retry_with_code = True
                                continue
                    if is_challenge:
                        self._auth_failure = True
                    break
        
        self.on_update(f"[{self.account_name}] Total followers collected: {len(all_followers)}")
        return all_followers
    
    def get_user_following(self):
        """Get following list of specific users"""
        all_following = []
        usernames = [username.strip() for username in self.target_input.split(',')]
        
        # Validate session before proceeding
        try:
            self.client.account_info()
        except (LoginRequired, ClientError) as e:
            self.on_error(f"[{self.account_name}] Session expired, attempting re-login...")
            self._delete_stale_session_file()
            if not self.attempt_login_with_retry():
                self.on_error(f"[{self.account_name}] Failed to re-authenticate")
                self._auth_failure = True
                return []
        
        for username in usernames:
            if not username:
                continue
            
            retry_with_code = True
            while retry_with_code:
                retry_with_code = False
                try:
                    self.on_update(f"[{self.account_name}] Getting following list of @{username}...")
                    
                    # Use retry logic for user_info_by_username
                    user_info = self.retry_on_login_required(
                        self.client.user_info_by_username,
                        username,
                        operation_name=f"get user info for @{username}",
                        max_retries=2
                    )
                    
                    if not user_info:
                        self.on_update(f"[{self.account_name}] User @{username} not found")
                        break
                    
                    # Use retry logic for user_following
                    following = self.retry_on_login_required(
                        self.client.user_following,
                        user_info.pk,
                        100,  # amount
                        operation_name=f"get following of @{username}",
                        max_retries=2
                    )
                    
                    following_list = list(following.values())
                    all_following.extend(following_list)
                    
                    self.on_update(f"[{self.account_name}] Found {len(following_list)} following from @{username}")
                    
                    if len(usernames) > 1:
                        time.sleep(random.uniform(5, 10))
                    break
                    
                except TooManyRedirects as e:
                    self.on_error(f"[{self.account_name}] Instagram is rate-limiting this account. Please wait a few minutes before trying again. Error: {str(e)}")
                    time.sleep(random.uniform(30, 60))
                    break
                except LoginRequired as e:
                    self.on_error(f"[{self.account_name}] Login required for @{username} after all retries: {str(e)}")
                    break
                except Exception as e:
                    error_msg = str(e)
                    if "Please wait" in error_msg or "rate limit" in error_msg.lower():
                        self.on_error(f"[{self.account_name}] Instagram rate limit detected. Please wait before trying again. Error: {error_msg}")
                        time.sleep(random.uniform(30, 60))
                        break
                    self.on_error(f"[{self.account_name}] Error getting following from @{username}: {error_msg}")
                    is_challenge = "ChallengeResolve" in error_msg or "challenge" in error_msg.lower() or "submit_phone" in error_msg
                    if is_challenge and self.on_request_challenge_code:
                        self.on_update(f"[{self.account_name}] Verification code required (SMS/email). Please enter the code in the modal.")
                        code = self.on_request_challenge_code(self.username, "SMS")
                        if code:
                            self._verification_code = code
                            self._delete_stale_session_file()
                            if self.attempt_login_with_retry():
                                retry_with_code = True
                                continue
                    if is_challenge:
                        self._auth_failure = True
                    break
        
        self.on_update(f"[{self.account_name}] Total following collected: {len(all_following)}")
        return all_following
    
    def get_users_from_custom_list(self):
        """Stream leads from LEADS_DIR: .jsonl (with fullname/firstname mapping) or .txt (username per line). 
        Falls back to target_input if no file exists. Yields user_info with optional lead_row."""
        if not self.campaign_id:
            return
        import json as _json
        leads_txt = os.path.join(LEADS_DIR, f"{self.campaign_id}.txt")
        leads_jsonl = os.path.join(LEADS_DIR, f"{self.campaign_id}.jsonl")
        
        # If no files exist, try to create from target_input
        if not os.path.exists(leads_txt) and not os.path.exists(leads_jsonl):
            if self.target_input:
                os.makedirs(LEADS_DIR, exist_ok=True)
                usernames_list = [u.strip() for u in self.target_input.split(',') if u.strip()]
                if usernames_list:
                    with open(leads_txt, 'w', encoding='utf-8') as f:
                        for u in usernames_list:
                            f.write(u + '\n')
                    self.on_update(f"[{self.account_name}] Created leads list with {len(usernames_list)} usernames")
                else:
                    self.on_update(f"[{self.account_name}] No leads found in campaign data")
                    return
            else:
                self.on_update(f"[{self.account_name}] No leads file found for this campaign")
                return
        try:
            first_lead = True
            self.ensure_session_valid()
            def iterate_rows():
                if os.path.exists(leads_jsonl):
                    with open(leads_jsonl, "r", encoding="utf-8") as f:
                        for line in f:
                            line = line.strip()
                            if not line:
                                continue
                            try:
                                lead = _json.loads(line)
                                u = (lead.get("username") or "").strip()
                                if u:
                                    yield u, lead
                            except Exception:
                                continue
                else:
                    with open(leads_txt, "r", encoding="utf-8") as f:
                        for line in f:
                            u = line.strip()
                            if u and not u.startswith("#"):
                                yield u, None
            consecutive_errors = 0
            for username, lead_row in iterate_rows():
                if not self.running:
                    return
                if first_lead:
                    time.sleep(random.uniform(2.0, 4.0))
                    first_lead = False
                else:
                    # Increase delay if we're getting errors (backoff)
                    base_min = LEAD_LOOKUP_DELAY_MIN + (consecutive_errors * 5)
                    base_max = LEAD_LOOKUP_DELAY_MAX + (consecutive_errors * 10)
                    delay = random.uniform(base_min, base_max)
                    self.debug_log("Lead rate limit", f"Waiting {delay:.1f}s before next lookup (errors: {consecutive_errors})")
                    time.sleep(delay)
                try:
                    user_info = self.retry_on_login_required(
                        self.client.user_info_by_username,
                        username,
                        operation_name=f"get user info for @{username} (lead)",
                        max_retries=2
                    )
                    if user_info:
                        consecutive_errors = 0  # Reset on success
                        if lead_row is not None:
                            user_info.lead_row = lead_row
                        else:
                            user_info.lead_row = {}
                        yield user_info
                except Exception as e:
                    consecutive_errors += 1
                    error_str = str(e).lower()
                    if "429" in error_str or "too many" in error_str or "rate limit" in error_str or "please wait" in error_str:
                        backoff = min(60 + (consecutive_errors * 30), 300)  # Max 5 min backoff
                        self.on_update(f"[{self.account_name}] Rate limited by Instagram. Waiting {backoff}s before continuing...")
                        time.sleep(backoff)
                    else:
                        self.debug_log("Lead skip", f"@{username}: {e}")
                    continue
        except Exception as e:
            self.on_error(f"[{self.account_name}] Error reading leads file: {e}")
    
    def stop(self):
        """Stop the worker"""
        self.running = False
    
    def run(self):
        """Main worker execution"""
        start_time = time.time()
        try:
            self.running = True
            self.debug_log("Starting worker", f"Target mode: {self.target_mode}, Input: {self.target_input}")
            self.on_update(f"[{self.account_name}] Logging in to Instagram...")
            
            if not self.attempt_login_with_retry():
                self.on_error(f"[{self.account_name}] Login failed after {self.max_login_attempts} attempts")
                self._auth_failure = True
                self.on_complete(success=False)
                return
            
            self.on_update(f"[{self.account_name}] Login successful!")
            self.simulate_human_behavior("login")
            # Send webhook for campaign started
            if self.campaign_id:
                _send_webhook(self.campaign_id, "campaign_started", {"account_username": self.username, "target_mode": self.target_mode, "message_count": self.message_count})
            
            # Get users based on target mode
            if self.target_mode == 0:  # Hashtag
                self.on_update(f"[{self.account_name}] Searching for users with hashtag #{self.target_input}...")
                users = self.get_users_from_hashtag()
                total_users = len(users) if users else 0
            elif self.target_mode == 1:  # Followers
                usernames = [u.strip() for u in self.target_input.split(',')]
                self.on_update(f"[{self.account_name}] Getting followers of {len(usernames)} users...")
                users = self.get_user_followers()
                total_users = len(users) if users else 0
            elif self.target_mode == 2:  # Following
                usernames = [u.strip() for u in self.target_input.split(',')]
                self.on_update(f"[{self.account_name}] Getting following list of {len(usernames)} users...")
                users = self.get_user_following()
                total_users = len(users) if users else 0
            elif self.target_mode == 3:  # Custom list (CSV leads)
                leads_path = os.path.join(LEADS_DIR, f"{self.campaign_id}.txt") if self.campaign_id else None
                leads_jsonl = os.path.join(LEADS_DIR, f"{self.campaign_id}.jsonl") if self.campaign_id else None
                
                # If no local file exists but target_input has usernames (from web frontend), create the file
                if self.campaign_id and self.target_input and leads_path and not os.path.exists(leads_path) and not (leads_jsonl and os.path.exists(leads_jsonl)):
                    os.makedirs(LEADS_DIR, exist_ok=True)
                    usernames_list = [u.strip() for u in self.target_input.split(',') if u.strip()]
                    if usernames_list:
                        with open(leads_path, 'w', encoding='utf-8') as f:
                            for u in usernames_list:
                                f.write(u + '\n')
                        self.lead_count = len(usernames_list)
                        self.on_update(f"[{self.account_name}] Created leads file with {len(usernames_list)} usernames from campaign data")
                
                if not self.campaign_id or not (leads_path and os.path.exists(leads_path)) and not (leads_jsonl and os.path.exists(leads_jsonl)):
                    self.on_update(f"[{self.account_name}] No leads found. Add usernames to the campaign or upload a CSV.")
                    self.on_complete(success=False)
                    return
                total_users = self.lead_count or 0
                self.on_update(f"[{self.account_name}] Processing {total_users} leads...")
                users = self.get_users_from_custom_list()  # generator
            else:
                self.on_error(f"[{self.account_name}] Invalid targeting mode")
                self.on_complete(success=False)
                return
            
            if self.target_mode != 3 and (not users or total_users == 0):
                self.on_update(f"[{self.account_name}] No users found")
                self.on_complete(success=not getattr(self, "_auth_failure", False))
                return
            
            if self.target_mode != 3:
                self.debug_stats['users_found'] = total_users
                self.on_update(f"[{self.account_name}] Found {total_users} users to process")
            
            # Process users
            successful_messages = 0
            processed = 0
            
            for i, user in enumerate(users):
                if not self.running:
                    self.on_update(f"[{self.account_name}] Operation cancelled by user.")
                    self.on_complete()
                    return
                
                user_id = user.pk
                username = user.username
                
                # Check if already processed
                with InstagramWorkerThread._shared_lock:
                    if user_id in InstagramWorkerThread._shared_processed_users:
                        self.debug_stats['users_already_processed'] += 1
                        continue
                    InstagramWorkerThread._shared_processed_users.add(user_id)
                
                # Update progress (total_users set per target_mode; for mode 3 it's lead_count)
                progress = int((i / total_users) * 100) if total_users > 0 else 0
                self.on_progress(progress)
                
                retry_with_code = True
                while retry_with_code:
                    retry_with_code = False
                    try:
                        self.simulate_human_behavior("browse")
                        
                        # Use retry logic for user_info
                        user_info = self.retry_on_login_required(
                            self.client.user_info,
                            user_id,
                            operation_name=f"get user info for @{username}",
                            max_retries=2
                        )
                        
                        follower_count = user_info.follower_count
                        
                        self.on_update(f"[{self.account_name}] User: {username} - Followers: {follower_count}")
                        
                        bio_text = getattr(user_info, 'biography', None) or ""
                        self.simulate_reading_time(len(bio_text))
                        self.simulate_profile_browsing()
                        
                        # Check filters
                        if follower_count < self.followers_threshold:
                            self.debug_stats['users_filtered_followers'] += 1
                            self.on_update(f"[{self.account_name}] Skipping {username} - Followers too low ({follower_count})")
                            continue
                        
                        if self.country_filter_enabled:
                            country = getattr(user_info, 'country', None) or getattr(user_info, 'location', None)
                            if country and not is_first_world_country(country):
                                self.debug_stats['users_filtered_country'] += 1
                                self.on_update(f"[{self.account_name}] Skipping {username} - Not in first world country")
                                continue
                        
                        if self.bio_filter_enabled and self.bio_keywords:
                            if not bio_contains_keywords(bio_text, self.bio_keywords):
                                self.debug_stats['users_filtered_bio'] += 1
                                self.on_update(f"[{self.account_name}] Skipping {username} - Bio doesn't contain keywords")
                                continue
                        
                        # Gender filter with Grok AI detection
                        if self.gender_filter and self.gender_filter != "all":
                            full_name = getattr(user_info, 'full_name', None) or ""
                            first_name = getattr(user_info, 'first_name', None) or ""
                            bio_text = getattr(user_info, 'biography', None) or ""
                            
                            # Get profile picture URL
                            profile_pic_url = (
                                getattr(user_info, 'profile_pic_url_hd', None) or 
                                getattr(user_info, 'profile_pic_url', None) or
                                None
                            )
                            
                            # Use Grok for comprehensive gender detection
                            gender_result = self.grok_detector.detect_gender(
                                profile_pic_url=profile_pic_url,
                                full_name=full_name,
                                first_name=first_name,
                                bio_text=bio_text
                            )
                            
                            detected_gender = gender_result.get("gender", "unknown")
                            confidence = gender_result.get("confidence", 0.0)
                            source = gender_result.get("source", "unknown")
                            
                            if detected_gender != "unknown" and detected_gender != self.gender_filter:
                                self.debug_stats['users_filtered_gender'] += 1
                                self.on_update(
                                    f"[{self.account_name}] Skipping {username} - Gender doesn't match filter "
                                    f"(detected: {detected_gender}, confidence: {confidence:.2f}, source: {source})"
                                )
                                continue
                            elif detected_gender == "unknown" and confidence < 0.3:
                                # Low confidence unknown - allow through to avoid false filtering
                                self.on_update(
                                    f"[{self.account_name}] Gender detection uncertain for {username} "
                                    f"(confidence: {confidence:.2f}), allowing through"
                                )
                                pass
                            elif detected_gender == self.gender_filter:
                                self.on_update(
                                    f"[{self.account_name}] Gender match for {username} "
                                    f"(detected: {detected_gender}, confidence: {confidence:.2f}, source: {source})"
                                )
                        
                        # Send message: firstname from CSV lead_row, then Grok, then Instagram, then username fallback
                        lead_row = getattr(user_info, 'lead_row', None) or {}
                        firstname = (lead_row.get("firstname") or "").strip() if isinstance(lead_row, dict) else ""
                        if not firstname and self.grok_detector.enabled:
                            full_name = (lead_row.get("fullname") or "").strip() if isinstance(lead_row, dict) else ""
                            if not full_name:
                                full_name = getattr(user_info, 'full_name', None) or ""
                            firstname = self.grok_detector.extract_first_name(full_name=full_name, username=username)
                        if not firstname:
                            firstname = getattr(user_info, 'first_name', None) or ""
                            firstname = (firstname or "").strip()
                        if not firstname:
                            firstname = username.split('.')[0].split('_')[0].split('-')[0].title()
                        
                        template = random.choice(self.message_templates)
                        message = apply_spintax(template, firstname)
                        
                        self.on_update(f"[{self.account_name}] 📝 Preparing to send DM to @{username} (User ID: {user_id})")
                        self.on_update(f"[{self.account_name}] 📋 Message template: {template[:50]}...")
                        self.on_update(f"[{self.account_name}] ✨ Final message (with spintax): {message[:100]}...")
                        
                        self.simulate_message_composition(message)
                        
                        self.on_update(f"[{self.account_name}] 📤 Sending DM to @{username}...")
                        # Proactive session check before DM (like simple login + dm)
                        self.ensure_session_valid()
                        # Like simple login + dm: human-like delay before DM (anti_detection)
                        ad = getattr(self.client, "_anti_detection", None)
                        if ad:
                            ad.wait_for_request("dm")
                        # Try httpcloak first if enabled (browser-identical TLS fingerprinting)
                        httpcloak_ok = False
                        if HTTPCLOAK_USE_FOR_DM and getattr(self.client, "_httpcloak_client", None):
                            try:
                                if self.client._httpcloak_client.send_dm(message, [str(user_id)], self.client):
                                    httpcloak_ok = True
                                    self.on_update(f"[{self.account_name}] ✅ SUCCESS! DM sent to @{username} (via httpcloak)")
                            except Exception as hc_err:
                                self.debug_log("HttpCloak DM failed, falling back to direct_send", str(hc_err))
                        if not httpcloak_ok:
                            dm_sent = False
                            methods_tried = []
                            rate_limit_detected = False
                            last_dm_error = None
                            # Method 1: direct_send (with retry on LoginRequired)
                            try:
                                methods_tried.append("direct_send")
                                result = self.retry_on_login_required(
                                    self.client.direct_send,
                                    message,
                                    user_ids=[user_id],
                                    operation_name=f"send DM to @{username}",
                                    max_retries=2
                                )
                                dm_sent = True
                                self.on_update(f"[{self.account_name}] ✅ SUCCESS! DM sent to @{username} (Result: {result})")
                                # Track sent DM for reply linking
                                thread_id = getattr(result, "thread_id", None) or getattr(result, "id", None)
                                if thread_id:
                                    _append_sent_dm(self.campaign_id, self.username, username, thread_id, datetime.now().isoformat(), message, "initial", 0)
                            except LoginRequired:
                                raise
                            except Exception as e:
                                last_dm_error = e
                                if self._is_rate_limit_error(e):
                                    rate_limit_detected = True
                            # Method 2: direct_send_text (if available)
                            if not dm_sent and hasattr(self.client, "direct_send_text"):
                                try:
                                    methods_tried.append("direct_send_text")
                                    self.retry_on_login_required(
                                        (lambda: self.client.direct_send_text(message, user_ids=[user_id])),
                                        operation_name=f"send DM (text) to @{username}",
                                        max_retries=2
                                    )
                                    dm_sent = True
                                    self.on_update(f"[{self.account_name}] ✅ SUCCESS! DM sent to @{username} (via direct_send_text)")
                                except Exception as e:
                                    last_dm_error = e
                                    if self._is_rate_limit_error(e):
                                        rate_limit_detected = True
                            # Method 3: manual direct_v1 API
                            if not dm_sent:
                                try:
                                    methods_tried.append("manual_api")
                                    data = {
                                        "recipient_users": json.dumps([[int(user_id)]]),
                                        "client_context": getattr(self.client, "generate_uuid", lambda: str(__import__("uuid").uuid4()))(),
                                        "message": message,
                                        "action": "send_item",
                                    }
                                    self.client.private_request("direct_v1/threads/broadcast/text/", data=data)
                                    dm_sent = True
                                    self.on_update(f"[{self.account_name}] ✅ SUCCESS! DM sent to @{username} (via manual API)")
                                except Exception as e:
                                    last_dm_error = e
                                    if self._is_rate_limit_error(e):
                                        rate_limit_detected = True
                            # Method 4: thread first then items
                            if not dm_sent:
                                try:
                                    methods_tried.append("thread_first")
                                    thread_data = {
                                        "recipient_users": json.dumps([[int(user_id)]]),
                                        "client_context": getattr(self.client, "generate_uuid", lambda: str(__import__("uuid").uuid4()))(),
                                    }
                                    thread_resp = self.client.private_request("direct_v1/threads/", data=thread_data)
                                    if thread_resp and thread_resp.get("thread_id"):
                                        msg_data = {
                                            "text": message,
                                            "client_context": getattr(self.client, "generate_uuid", lambda: str(__import__("uuid").uuid4()))(),
                                            "action": "send_item",
                                        }
                                        self.client.private_request(
                                            f"direct_v1/threads/{thread_resp['thread_id']}/items/",
                                            data=msg_data
                                        )
                                        dm_sent = True
                                        self.on_update(f"[{self.account_name}] ✅ SUCCESS! DM sent to @{username} (via thread)")
                                except Exception as e:
                                    last_dm_error = e
                                    if self._is_rate_limit_error(e):
                                        rate_limit_detected = True
                            if not dm_sent:
                                if rate_limit_detected:
                                    self.on_error(
                                        f"[{self.account_name}] RATE LIMIT / RESTRICTION: All DM methods failed. "
                                        "Try increasing delays, wait 24-48h, or check account/recipient settings."
                                    )
                                err = last_dm_error or Exception("DM send failed")
                                self.on_update(f"[{self.account_name}] ❌ FAILED to send DM to @{username}: {err}")
                                raise err
                        
                        successful_messages += 1
                        self.debug_stats['messages_sent'] += 1
                        self.on_message_sent(username, user_id, message)
                        # Persist session after successful DM (like simple login + dm)
                        try:
                            sp = os.path.join(SESSIONS_DIR, f"{self.username}.json")
                            if hasattr(self.client, "dump_settings") and sp:
                                self.client.dump_settings(sp)
                        except Exception:
                            pass
                        self.on_update(f"[{self.account_name}] ✅ Message #{successful_messages} sent successfully to @{username}")
                        # Send webhook for message sent
                        if self.campaign_id:
                            _send_webhook(self.campaign_id, "message_sent", {"account_username": self.username, "recipient_username": username, "message_type": "initial", "cumulative_sent": successful_messages})

                        # Send follow-ups (wait delay, then send each)
                        if self.follow_ups:
                            for i, fu in enumerate(self.follow_ups):
                                delay_sec = follow_up_delay_seconds(fu)
                                if delay_sec > 0:
                                    self.on_update(f"[{self.account_name}] ⏳ Waiting {delay_sec}s before follow-up {i+1} to @{username}")
                                    time.sleep(delay_sec)
                                fu_message = apply_spintax(fu.get("message", ""), firstname or "")
                                self.ensure_session_valid()
                                ad = getattr(self.client, "_anti_detection", None)
                                if ad:
                                    ad.wait_for_request("dm")
                                try:
                                    self.retry_on_login_required(
                                        self.client.direct_send,
                                        fu_message,
                                        user_ids=[user_id],
                                        operation_name=f"send follow-up {i+1} to @{username}",
                                        max_retries=2
                                    )
                                    successful_messages += 1
                                    self.debug_stats['messages_sent'] += 1
                                    self.on_message_sent(username, user_id, fu_message)
                                    try:
                                        sp = os.path.join(SESSIONS_DIR, f"{self.username}.json")
                                        if hasattr(self.client, "dump_settings") and sp:
                                            self.client.dump_settings(sp)
                                    except Exception:
                                        pass
                                    self.on_update(f"[{self.account_name}] ✅ Follow-up {i+1} sent to @{username}")
                                    # Track follow-up DM
                                    if thread_id:
                                        _append_sent_dm(self.campaign_id, self.username, username, thread_id, datetime.now().isoformat(), fu_message, "follow_up", i+1)
                                    # Send webhook for follow-up message sent
                                    if self.campaign_id:
                                        _send_webhook(self.campaign_id, "message_sent", {"account_username": self.username, "recipient_username": username, "message_type": "follow_up", "cumulative_sent": successful_messages})
                                except Exception as e:
                                    self.on_update(f"[{self.account_name}] ⚠️ Follow-up {i+1} to @{username} failed: {e}")
                        
                        if successful_messages >= self.message_count:
                            self.on_update(f"[{self.account_name}] 🎯 Reached target of {self.message_count} messages!")
                            break
                        
                        self.simulate_human_behavior("message")
                        self.add_random_human_delays()
                        break
                        
                    except LoginRequired as e:
                        self.on_update(f"[{self.account_name}] ⚠️ Login required for {username} after all retries: {str(e)}")
                        self.debug_stats['errors'] += 1
                        break
                    except ClientError as e:
                        self.on_update(f"[{self.account_name}] ⚠️ Error with {username}: {str(e)}")
                        self.debug_stats['errors'] += 1
                        break
                    except PleaseWaitFewMinutes:
                        self.on_update(f"[{self.account_name}] ⚠️ Rate limited! Waiting 5 minutes...")
                        time.sleep(300)
                        break
                    except Exception as e:
                        error_msg = str(e)
                        self.on_update(f"[{self.account_name}] ⚠️ Unexpected error with {username}: {error_msg}")
                        self.debug_stats['errors'] += 1
                        is_challenge = "ChallengeResolve" in error_msg or "challenge" in error_msg.lower() or "submit_phone" in error_msg
                        if is_challenge and self.on_request_challenge_code:
                            self.on_update(f"[{self.account_name}] Verification code required (SMS/email). Please enter the code in the modal.")
                            code = self.on_request_challenge_code(self.username, "SMS")
                            if code:
                                self._verification_code = code
                                self._delete_stale_session_file()
                                if self.attempt_login_with_retry():
                                    retry_with_code = True
                                    continue
                        if is_challenge:
                            self._auth_failure = True
                        break
                
                time.sleep(random.uniform(2, 5))
            
            self.on_update(f"[{self.account_name}] Completed! Sent {successful_messages} messages.")
            self.on_progress(100)
            # Send webhook for worker completed
            if self.campaign_id:
                _send_webhook(self.campaign_id, "worker_completed", {"account_username": self.username, "messages_sent": successful_messages, "target_reached": successful_messages >= self.message_count, "duration_seconds": time.time() - start_time, "errors": self.debug_stats['errors']})
            self.on_complete()
            
        except Exception as e:
            self.on_error(f"[{self.account_name}] Critical error: {str(e)}")
            self.on_complete(success=False)
