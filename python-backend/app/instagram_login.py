"""
Shared Instagram login logic (mirrors upload_video.py + simple login + dm).
Flow: saved session (validate with get_timeline_feed) -> sessionid (multi-format) -> username/password with retries and 2FA.
Pre-login: sync_launcher. Post-login: get_timeline_feed. Uses per-account user-agent and device IDs.
"""
import os
import json
import random
import time
import uuid
from urllib.parse import unquote
from typing import Optional, Callable

try:
    from .patch_instagrapi import patch_instagrapi
    patch_instagrapi()
except Exception:
    pass

from instagrapi import Client
from instagrapi.exceptions import LoginRequired, ChallengeRequired, ClientError
from requests.exceptions import HTTPError

from .instagram_auth_flow import pre_login_flow, post_login_flow
from .config import (
    ANTI_DETECTION_ENABLED,
    HTTPCLOAK_ENABLED,
    HTTPCLOAK_PRESET,
    TWO_FACTOR_CODE,
    FALLBACK_PROXIES,
)


MOBILE_USER_AGENTS = {
    "ios": [
        "Instagram 319.0.0.0.0 (iPhone15,2; iOS 17_1; en_US; en-US; scale=3.00; 1290x2796; 543123456)",
        "Instagram 319.0.0.0.0 (iPhone14,2; iOS 17_0; en_US; en-US; scale=3.00; 1170x2532; 542987654)",
        "Instagram 318.0.0.0.0 (iPhone15,1; iOS 17_2; en_US; en-US; scale=3.00; 1290x2796; 543234567)",
        "Instagram 319.0.0.0.0 (iPhone13,2; iOS 16_7; en_US; en-US; scale=3.00; 1170x2532; 541876543)",
        "Instagram 318.0.0.0.0 (iPhone14,3; iOS 17_0; en_US; en-US; scale=3.00; 1284x2778; 542345678)",
        "Instagram 319.0.0.0.0 (iPhone15,3; iOS 17_1; en_US; en-US; scale=3.00; 1290x2796; 543456789)",
        "Instagram 318.0.0.0.0 (iPhone13,1; iOS 16_6; en_US; en-US; scale=2.00; 750x1624; 540123456)",
        "Instagram 319.0.0.0.0 (iPhone14,1; iOS 17_0; en_US; en-US; scale=3.00; 1170x2532; 542654321)",
    ],
    "android": [
        "Instagram 319.0.0.0.0 Android (33/13; 420dpi; 1080x2400; samsung; SM-S918B; dm3q; exynos2200; en_US; 543123456)",
        "Instagram 319.0.0.0.0 Android (33/13; 420dpi; 1080x2400; samsung; SM-S911B; o1s; exynos2200; en_US; 542987654)",
        "Instagram 318.0.0.0.0 Android (33/13; 420dpi; 1080x2400; samsung; SM-A546B; a54x; exynos1380; en_US; 543234567)",
        "Instagram 319.0.0.0.0 Android (33/13; 480dpi; 1440x3200; samsung; SM-S928B; dm4x; snapdragon; en_US; 543456789)",
        "Instagram 318.0.0.0.0 Android (33/13; 420dpi; 1080x2400; Google; Pixel 8 Pro; cheetah; cheetah; en_US; 542345678)",
        "Instagram 319.0.0.0.0 Android (33/13; 420dpi; 1080x2400; Google; Pixel 7; panther; panther; en_US; 541876543)",
        "Instagram 318.0.0.0.0 Android (33/13; 420dpi; 1080x2400; OnePlus; CPH2451; OP591BL1; taro; en_US; 542654321)",
        "Instagram 319.0.0.0.0 Android (33/13; 420dpi; 1080x2400; Xiaomi; 23013RK75G; corot; taro; en_US; 543567890)",
    ],
}


def human_delay(min_seconds: float = 0.5, max_seconds: float = 2.0) -> None:
    """Human-like random delay."""
    time.sleep(random.uniform(min_seconds, max_seconds))


class UserAgentManager:
    """Per-account user-agent and device IDs (persisted to sessions_dir)."""

    def __init__(self, sessions_dir: str) -> None:
        self.sessions_dir = sessions_dir
        os.makedirs(sessions_dir, exist_ok=True)

    def get_user_agent(self, username: str) -> tuple:
        """(user_agent, device_type)."""
        ua_file = os.path.join(self.sessions_dir, f"{username}_user_agent.json")
        if os.path.exists(ua_file):
            try:
                with open(ua_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    return data["user_agent"], data["device_type"]
            except Exception:
                pass
        device_type = random.choice(["ios", "android"])
        user_agent = random.choice(MOBILE_USER_AGENTS[device_type])
        try:
            with open(ua_file, "w", encoding="utf-8") as f:
                json.dump({"user_agent": user_agent, "device_type": device_type}, f)
        except Exception:
            pass
        return user_agent, device_type

    def get_device_ids(self, username: str) -> dict:
        """device_id, phone_id, uuid, adid."""
        device_file = os.path.join(self.sessions_dir, f"{username}_device.json")
        if os.path.exists(device_file):
            try:
                with open(device_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        device_ids = {
            "device_id": f"android-{uuid.uuid4().hex[:16]}",
            "phone_id": str(uuid.uuid4()),
            "uuid": str(uuid.uuid4()),
            "adid": str(uuid.uuid4()),
        }
        try:
            with open(device_file, "w", encoding="utf-8") as f:
                json.dump(device_ids, f)
        except Exception:
            pass
        return device_ids

    def rotate_fingerprint(self, username: str) -> None:
        """Remove saved UA and device files so next run gets fresh fingerprint."""
        for name in (f"{username}_user_agent.json", f"{username}_device.json"):
            path = os.path.join(self.sessions_dir, name)
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass


class InstagramLoginHelper:
    """
    Login flow matching upload_video.py:
    1. Load saved session (session_path)
    2. Else login_by_sessionid if provided
    3. Else username/password with retries; on ChallengeRequired use callback
    """

    def __init__(
        self,
        username: str,
        password: str,
        sessions_dir: str,
        proxy: Optional[str] = None,
        sessionid: Optional[str] = None,
        challenge_code_callback: Optional[Callable[[str, str], str]] = None,
    ) -> None:
        self.username = username
        self.password = password
        raw = (sessionid or "").strip()
        self.sessionid = unquote(raw) if raw else None
        self.sessions_dir = sessions_dir
        self.session_path = os.path.join(sessions_dir, f"{username}.json")
        self.proxy = (proxy or "").strip() or None
        self.challenge_code_callback = challenge_code_callback
        os.makedirs(sessions_dir, exist_ok=True)
        self.ua_manager = UserAgentManager(sessions_dir)
        self.user_agent, _ = self.ua_manager.get_user_agent(username)
        self.device_ids = self.ua_manager.get_device_ids(username)
        self.client = Client()
        if self.proxy:
            try:
                self.client.set_proxy(self.proxy)
            except Exception:
                pass
        if ANTI_DETECTION_ENABLED:
            try:
                from .core.anti_detection import apply_anti_detection
                apply_anti_detection(self.client)
            except Exception:
                pass
        else:
            # Default device when no anti_detection (like simple login + dm _setup_device)
            try:
                if hasattr(self.client, "set_device"):
                    self.client.set_device({
                        "app_version": "269.0.0.18.75",
                        "android_version": 26,
                        "android_release": "8.0.0",
                        "dpi": "480dpi",
                        "resolution": "1080x1920",
                        "manufacturer": "samsung",
                        "device": "SM-G930F",
                        "model": "herolte",
                        "cpu": "samsungexynos8890",
                        "version_code": "314665256",
                    })
            except Exception:
                pass
        if HTTPCLOAK_ENABLED:
            try:
                from .core.httpcloak_client import HttpCloakClient, HTTPCLOAK_AVAILABLE
                if HTTPCLOAK_AVAILABLE:
                    self.client._httpcloak_client = HttpCloakClient(
                        preset=HTTPCLOAK_PRESET,
                        proxy=self.proxy,
                    )
                else:
                    self.client._httpcloak_client = None
            except Exception:
                self.client._httpcloak_client = None
        else:
            self.client._httpcloak_client = None
        if hasattr(self.client, "settings"):
            self.client.settings["user_agent"] = self.user_agent
            self.client.settings["device_id"] = self.device_ids["device_id"]
            self.client.settings["phone_id"] = self.device_ids["phone_id"]
            self.client.settings["uuid"] = self.device_ids["uuid"]
            self.client.settings["adid"] = self.device_ids["adid"]
        else:
            if hasattr(self.client, "user_agent"):
                self.client.user_agent = self.user_agent
            for k, v in self.device_ids.items():
                if hasattr(self.client, k):
                    setattr(self.client, k, v)
        self.client.delay_range = [1.5, 3.5]

    def _validate_session(self) -> bool:
        """Validate current session with get_timeline_feed (like simple login + dm)."""
        try:
            if hasattr(self.client, "get_timeline_feed"):
                self.client.get_timeline_feed()
                return True
        except (LoginRequired, Exception):
            pass
        return False

    def _load_session(self) -> bool:
        if not os.path.exists(self.session_path):
            return False
        try:
            self.client.load_settings(self.session_path)
            if hasattr(self.client, "settings"):
                self.client.settings["user_agent"] = self.user_agent
            elif hasattr(self.client, "user_agent"):
                self.client.user_agent = self.user_agent
            human_delay(0.5, 1.5)
            if self._validate_session():
                return True
        except Exception:
            pass
        return False

    def _login_by_sessionid_multi(self) -> bool:
        """Try sessionid: full string, then token-only, then manual cookies (user_id:session_id:version:csrf)."""
        raw = (self.sessionid or "").strip()
        if not raw or len(raw) < 30:
            return False
        session_id_decoded = unquote(raw)
        # Method 1: full sessionid string
        try:
            human_delay(1.0, 2.0)
            self.client.login_by_sessionid(session_id_decoded)
            human_delay(0.5, 1.0)
            if self._validate_session():
                return True
        except Exception:
            pass
        # Method 2: parse user_id:session_id:version:csrf
        parts = session_id_decoded.split(":")
        if len(parts) >= 4:
            user_id, session_token, _version, csrf_token = parts[0], parts[1], parts[2], parts[3]
            try:
                self.client.login_by_sessionid(session_token)
                human_delay(0.5, 1.0)
                if self._validate_session():
                    return True
            except Exception:
                pass
            # Method 3: manual cookies
            try:
                for domain in (".instagram.com", "www.instagram.com"):
                    self.client.private.cookies.set("sessionid", session_token, domain=domain, path="/")
                    self.client.private.cookies.set("csrftoken", csrf_token, domain=domain, path="/")
                    self.client.private.cookies.set("ds_user_id", user_id, domain=domain, path="/")
                if hasattr(self.client, "set_settings"):
                    self.client.set_settings({
                        "authorization_data": {"sessionid": session_token, "ds_user_id": user_id},
                    })
                if hasattr(self.client, "inject_sessionid_to_public"):
                    self.client.inject_sessionid_to_public()
                if self._validate_session():
                    return True
            except Exception:
                pass
        return False

    def _save_session(self) -> bool:
        try:
            self.client.dump_settings(self.session_path)
            return True
        except Exception:
            return False

    def _handle_challenge(self, method: str = "email") -> bool:
        """Request code, get from callback, submit. Used when ChallengeRequired is raised."""
        if not self.challenge_code_callback:
            return False
        try:
            self.client.challenge_code_handler(self.username, method)
        except Exception:
            pass
        code = self.challenge_code_callback(self.username, method)
        if not code:
            return False
        try:
            self.client.challenge_code_handler(self.username, method, code)
            return True
        except Exception:
            return False

    def login(self) -> bool:
        if self._load_session():
            post_login_flow(self.client)
            return True
        if self.sessionid:
            if self._login_by_sessionid_multi():
                self._save_session()
                post_login_flow(self.client)
                return True
        max_retries = 3
        backoff = [30, 60, 120]
        last_error = None
        if self.challenge_code_callback:
            def _handler(username, choice):
                method = "email" if choice and "email" in str(choice).lower() else "sms"
                return self.challenge_code_callback(username, method) or ""
            self.client.challenge_code_handler = _handler
        for attempt in range(max_retries):
            human_delay(1.0, 3.0)
            try:
                # Like simple login + dm: human-like delay before password login (or use anti_detection)
                ad = getattr(self.client, "_anti_detection", None)
                if ad:
                    ad.wait_for_request("login")
                else:
                    time.sleep(2)
                pre_login_flow(self.client)
                login_kwargs = {"username": self.username, "password": self.password}
                if TWO_FACTOR_CODE:
                    login_kwargs["verification_code"] = TWO_FACTOR_CODE
                self.client.login(**login_kwargs)
                human_delay(0.5, 1.5)
                post_login_flow(self.client)
                self._save_session()
                # Reload session to sync state (like simple login + dm)
                try:
                    self.client.load_settings(self.session_path)
                except Exception:
                    pass
                return True
            except ChallengeRequired:
                if self._handle_challenge("email") or self._handle_challenge("sms"):
                    self._save_session()
                    try:
                        self.client.load_settings(self.session_path)
                    except Exception:
                        pass
                    return True
                self.ua_manager.rotate_fingerprint(self.username)
                if os.path.exists(self.session_path):
                    try:
                        os.remove(self.session_path)
                    except Exception:
                        pass
                return False
            except LoginRequired:
                return False
            except (ClientError, HTTPError, Exception) as e:
                last_error = e
                err_str = str(e).lower()
                if ("572" in err_str or "500" in err_str or "Server Error" in err_str) and attempt < max_retries - 1:
                    time.sleep(backoff[min(attempt, len(backoff) - 1)])
                    continue
                # Fallback proxies on blacklist / account lookup blocked (like simple login + dm)
                is_blacklist = "blacklist" in err_str or "ip address" in err_str
                is_account_blocked = "can't find an account" in err_str or "account with" in err_str
                if (is_blacklist or is_account_blocked) and FALLBACK_PROXIES:
                    try:
                        if self._try_login_with_proxies():
                            return True
                    except ChallengeRequired:
                        raise
                break
        self.ua_manager.rotate_fingerprint(self.username)
        if os.path.exists(self.session_path):
            try:
                os.remove(self.session_path)
            except Exception:
                pass
        if last_error:
            raise last_error
        raise RuntimeError("Login failed")

    def _format_proxy(self, proxy_string: str, protocol: str = "http") -> str:
        """Format proxy string to full URL (ip:port or ip:port:user:pass)."""
        parts = proxy_string.split(":")
        if len(parts) == 4:
            ip, port, user, pw = parts
            return f"{protocol}://{user}:{pw}@{ip}:{port}"
        if len(parts) == 2:
            ip, port = parts
            return f"{protocol}://{ip}:{port}"
        raise ValueError(f"Invalid proxy format: {proxy_string}")

    def _try_proxy_login(self, proxy_url: str) -> bool:
        """Try login with a specific proxy. Returns True if success."""
        try:
            from instagrapi import Client
            c = Client()
            if self.proxy:
                try:
                    c.set_proxy(self.proxy)
                except Exception:
                    pass
            c.set_proxy(proxy_url)
            time.sleep(2)
            kwargs = {"username": self.username, "password": self.password}
            if TWO_FACTOR_CODE:
                kwargs["verification_code"] = TWO_FACTOR_CODE
            if c.login(**kwargs):
                self.client = c
                self._save_session()
                if hasattr(self.client, "inject_sessionid_to_public"):
                    try:
                        self.client.inject_sessionid_to_public()
                    except Exception:
                        pass
                post_login_flow(self.client)
                # Re-apply anti_detection and httpcloak so client stays consistent
                if ANTI_DETECTION_ENABLED:
                    try:
                        from .core.anti_detection import apply_anti_detection
                        apply_anti_detection(self.client)
                    except Exception:
                        pass
                if HTTPCLOAK_ENABLED:
                    try:
                        from .core.httpcloak_client import HttpCloakClient, HTTPCLOAK_AVAILABLE
                        if HTTPCLOAK_AVAILABLE:
                            self.client._httpcloak_client = HttpCloakClient(
                                preset=HTTPCLOAK_PRESET, proxy=self.proxy
                            )
                        else:
                            self.client._httpcloak_client = None
                    except Exception:
                        self.client._httpcloak_client = None
                # Reload session to sync state (like simple login + dm)
                try:
                    self.client.load_settings(self.session_path)
                except Exception:
                    pass
                return True
        except Exception:
            pass
        return False

    def _try_login_with_proxies(self) -> bool:
        """Try each FALLBACK_PROXIES on blacklist/account-blocked. Returns True if any succeeds."""
        for i, proxy_string in enumerate(FALLBACK_PROXIES):
            try:
                if i > 0:
                    time.sleep(1)
                if proxy_string.startswith(("http://", "https://", "socks5://", "socks4://")):
                    if self._try_proxy_login(proxy_string):
                        return True
                else:
                    for protocol in ("socks5", "http"):
                        try:
                            proxy_url = self._format_proxy(proxy_string, protocol)
                            if self._try_proxy_login(proxy_url):
                                return True
                        except Exception:
                            continue
            except ChallengeRequired:
                raise
            except Exception:
                continue
        return False
