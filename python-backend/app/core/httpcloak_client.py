"""
HttpCloak client for bot detection bypass (from simple login + dm).
Uses browser-identical TLS/HTTP fingerprinting. Optional - pip install httpcloak.
"""
import json
import logging
import urllib.parse
import uuid
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import httpcloak
    HTTPCLOAK_AVAILABLE = True
except ImportError:
    HTTPCLOAK_AVAILABLE = False
    httpcloak = None


class HttpCloakClient:
    """Wrapper for httpcloak Session with Instagram DM send (from simple login + dm)."""

    def __init__(self, preset: str = "chrome-143", proxy: Optional[str] = None):
        if not HTTPCLOAK_AVAILABLE:
            raise ImportError("httpcloak not available. Install with: pip install httpcloak")
        self.preset = preset
        self.proxy = proxy
        self.session = None
        self._initialize_session()

    def _initialize_session(self) -> None:
        try:
            self.session = httpcloak.Session(
                preset=self.preset,
                proxy=self.proxy,
                timeout=30,
                http_version="h1",
            )
            logger.debug("HttpCloak session initialized with preset=%s", self.preset)
        except Exception as e:
            logger.warning("HttpCloak init failed: %s", e)
            raise

    def get_headers_from_instagrapi(self, instagrapi_client) -> Dict[str, str]:
        """Build headers from instagrapi client for httpcloak requests."""
        headers: Dict[str, str] = {}
        try:
            if hasattr(instagrapi_client, "private"):
                h = getattr(instagrapi_client.private, "headers", None)
                if h is not None:
                    headers.update(dict(h))
            ua = getattr(instagrapi_client, "user_agent", None) or "Instagram"
            headers["User-Agent"] = ua
            headers["X-IG-App-ID"] = "936619743392459"
            headers["X-IG-Device-ID"] = getattr(instagrapi_client, "device_id", "") or ""
            headers["X-IG-Android-ID"] = getattr(instagrapi_client, "android_id", "") or ""
            if hasattr(instagrapi_client, "private") and hasattr(instagrapi_client.private, "cookies"):
                cookie_str = "; ".join(
                    f"{name}={value}" for name, value in instagrapi_client.private.cookies.items()
                )
                if cookie_str:
                    headers["Cookie"] = cookie_str
            # Merge anti_detection X-IG-* headers if present
            ad = getattr(instagrapi_client, "_anti_detection", None)
            if ad and hasattr(ad, "get_headers"):
                headers.update(ad.get_headers())
        except Exception as e:
            logger.warning("get_headers_from_instagrapi: %s", e)
        return headers

    def send_dm(self, message: str, user_ids: List[str], instagrapi_client) -> bool:
        """Send DM via httpcloak. Returns True if successful."""
        if not self.session:
            return False
        try:
            headers = self.get_headers_from_instagrapi(instagrapi_client)
            headers["Content-Type"] = "application/x-www-form-urlencoded; charset=UTF-8"
            gen_uuid = getattr(instagrapi_client, "generate_uuid", None) or (lambda: str(uuid.uuid4()))
            data = {
                "recipient_users": json.dumps([[int(uid)] for uid in user_ids]),
                "client_context": gen_uuid() if callable(gen_uuid) else str(uuid.uuid4()),
                "thread": "",
                "text": message,
            }
            url = "https://i.instagram.com/api/v1/direct_v2/threads/broadcast/text/"
            try:
                response = self.session.post(url, data=data, headers=headers)
            except (TypeError, AttributeError):
                form_data = urllib.parse.urlencode(data)
                response = self.session.post(
                    url, data=form_data.encode("utf-8"), headers=headers
                )
            ok = getattr(response, "ok", None)
            if ok:
                logger.info("DM sent via httpcloak to %s user(s)", len(user_ids))
                return True
            status = getattr(response, "status_code", "?")
            logger.warning("HttpCloak DM failed: status=%s", status)
            return False
        except Exception as e:
            logger.warning("HttpCloak send_dm error: %s", e)
            return False

    def close(self) -> None:
        if self.session:
            try:
                self.session.close()
            except Exception:
                pass
