"""
Full anti-detection from simple login + dm: device, region, battery/charging/dark mode,
network simulation, request-type-aware delays (wait_for_request), and optional X-IG-* headers.
Use when ANTI_DETECTION_ENABLED=true.
"""
import random
import time
import uuid
import hashlib
import logging
from typing import Dict

from .device_profiles import get_random_device
from .geographic import get_random_region, get_current_time_in_region

logger = logging.getLogger(__name__)


class AntiDetection:
    """Advanced anti-detection (from simple login + dm): device, region, timing, headers."""

    def __init__(self):
        self.device = get_random_device()
        self.region = get_random_region()
        # Backend geographic may not have "language"; derive from locale
        if "language" not in self.region:
            self.region["language"] = (self.region.get("locale") or "en_US").split("_")[0]
        self.device_id = self._generate_device_id()
        self.android_id = self._generate_android_id()
        self._last_request_time = None
        logger.debug(
            "Anti-detection: %s %s in %s",
            self.device.get("manufacturer"),
            self.device.get("device"),
            self.region.get("country_code"),
        )

    def _generate_device_id(self) -> str:
        entropy = f"{self.device['manufacturer']}{self.device['model']}{int(time.time())}{uuid.uuid4()}"
        return hashlib.md5(entropy.encode()).hexdigest()[:16]

    def _generate_android_id(self) -> str:
        entropy = f"{self.device['manufacturer']}{self.device['device']}{self.device['model']}{int(time.time())}"
        return hashlib.md5(entropy.encode()).hexdigest()[:16]

    def get_device_config(self) -> Dict:
        """Device config for instagrapi set_device()."""
        return {
            "app_version": self.device["app_version"],
            "android_version": self.device["android_version"],
            "android_release": self.device["android_release"],
            "dpi": self.device["dpi"],
            "resolution": self.device["resolution"],
            "manufacturer": self.device["manufacturer"],
            "device": self.device["device"],
            "model": self.device["model"],
            "cpu": self.device["cpu"],
            "version_code": self.device["version_code"],
        }

    def get_region_config(self) -> Dict:
        """Region (country_code, locale, timezone_offset) for client."""
        offset_sec = 0
        try:
            t = get_current_time_in_region(self.region)
            if t.utcoffset() is not None:
                offset_sec = int(t.utcoffset().total_seconds())
        except Exception:
            pass
        return {
            "country_code": self.region["country_code"],
            "locale": self.region["locale"],
            "timezone_offset": offset_sec,
        }

    def get_battery_level(self) -> int:
        """Realistic battery level by time of day."""
        try:
            current_time = get_current_time_in_region(self.region)
        except Exception:
            current_time = __import__("datetime").datetime.utcnow()
        hour = current_time.hour
        if 0 <= hour < 6:
            return random.randint(85, 100)
        elif 6 <= hour < 12:
            return random.randint(70, 95)
        elif 12 <= hour < 18:
            return random.randint(40, 80)
        else:
            return random.randint(20, 60)

    def is_charging(self) -> bool:
        battery = self.get_battery_level()
        try:
            current_time = get_current_time_in_region(self.region)
        except Exception:
            current_time = __import__("datetime").datetime.utcnow()
        hour = current_time.hour
        if battery < 30:
            return random.random() < 0.70
        if 0 <= hour < 6:
            return random.random() < 0.60
        return random.random() < 0.15

    def is_dark_mode(self) -> bool:
        try:
            current_time = get_current_time_in_region(self.region)
        except Exception:
            current_time = __import__("datetime").datetime.utcnow()
        hour = current_time.hour
        if 18 <= hour < 24 or 0 <= hour < 8:
            return random.random() < 0.67
        return random.random() < 0.33

    def get_network_info(self) -> Dict:
        connection_type = random.choices(["wifi", "4g", "5g"], weights=[0.70, 0.25, 0.05])[0]
        if connection_type == "wifi":
            bandwidth = random.randint(15, 100)
            latency = random.randint(1, 15)
        elif connection_type == "4g":
            bandwidth = random.randint(5, 50)
            latency = random.randint(20, 80)
        else:
            bandwidth = random.randint(50, 300)
            latency = random.randint(1, 10)
        return {
            "connection_type": connection_type,
            "bandwidth_mbps": bandwidth,
            "latency_ms": latency,
        }

    def get_request_delay(self, request_type: str = "default") -> float:
        """Human-like delay by time of day and request type."""
        try:
            current_time = get_current_time_in_region(self.region)
        except Exception:
            current_time = __import__("datetime").datetime.utcnow()
        hour = current_time.hour
        if 0 <= hour < 6:
            base_delay = random.uniform(1.5, 3.0)
        elif 6 <= hour < 12:
            base_delay = random.uniform(0.8, 2.0)
        elif 12 <= hour < 18:
            base_delay = random.uniform(0.5, 1.5)
        else:
            base_delay = random.uniform(1.0, 2.5)
        if random.random() < 0.15:
            base_delay += random.uniform(2.0, 5.0)
        if request_type == "dm":
            base_delay += random.uniform(0.5, 1.5)
        elif request_type == "login":
            base_delay += random.uniform(1.0, 2.0)
        return base_delay

    def wait_for_request(self, request_type: str = "default") -> None:
        """Wait with human-like timing before making a request (like simple login + dm)."""
        if self._last_request_time is not None:
            delay = self.get_request_delay(request_type)
            time.sleep(delay)
        self._last_request_time = time.time()

    def get_headers(self) -> Dict[str, str]:
        """Optional X-IG-* headers for requests."""
        network = self.get_network_info()
        battery = self.get_battery_level()
        return {
            "X-IG-Device-ID": self.device_id,
            "X-IG-Android-ID": self.android_id,
            "X-IG-Connection-Type": network["connection_type"].upper(),
            "X-IG-Bandwidth": str(network["bandwidth_mbps"]),
            "X-IG-Latency": str(network["latency_ms"]),
            "X-IG-Battery-Level": str(battery),
            "X-IG-Is-Charging": "1" if self.is_charging() else "0",
            "X-IG-Dark-Mode": "1" if self.is_dark_mode() else "0",
            "X-IG-Country-Code": self.region["country_code"],
            "X-IG-Locale": self.region["locale"],
            "X-IG-Language": self.region.get("language", "en"),
        }


def apply_anti_detection(client) -> bool:
    """
    Set device + region on client and attach AntiDetection for wait_for_request/get_headers.
    Returns True if applied. Client will have client._anti_detection set.
    """
    try:
        ad = AntiDetection()
        cfg = ad.get_device_config()
        if hasattr(client, "set_device"):
            client.set_device(cfg)
        region = ad.get_region_config()
        if hasattr(client, "set_country_code"):
            try:
                code = {"US": 1, "GB": 44, "DE": 49, "FR": 33, "CA": 1, "AU": 61}.get(
                    region["country_code"], 1
                )
                client.set_country_code(code)
            except Exception:
                pass
        if hasattr(client, "set_locale"):
            try:
                client.set_locale(region["locale"])
            except Exception:
                pass
        if hasattr(client, "set_timezone_offset") and region.get("timezone_offset") is not None:
            try:
                client.set_timezone_offset(region["timezone_offset"])
            except Exception:
                pass
        client._anti_detection = ad
        # Inject get_headers() into every private_request (like simple login + dm)
        if hasattr(client, "private_request"):
            _orig_private_request = client.private_request

            def _wrapped_private_request(endpoint, data=None, *args, **kwargs):
                ad = getattr(client, "_anti_detection", None)
                if ad and hasattr(client, "private") and getattr(client.private, "headers", None):
                    for k, v in ad.get_headers().items():
                        client.private.headers[k] = v
                return _orig_private_request(endpoint, data, *args, **kwargs)

            client.private_request = _wrapped_private_request
        return True
    except Exception:
        return False
