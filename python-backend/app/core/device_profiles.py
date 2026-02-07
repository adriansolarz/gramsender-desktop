"""
Realistic device profiles pool (from simple login + dm).
"""
import random
from typing import Dict

DEVICE_PROFILES = [
    {"manufacturer": "samsung", "device": "SM-G973F", "model": "beyond1", "android_version": 29, "android_release": "10", "dpi": "420dpi", "resolution": "1080x2280", "cpu": "exynos9820", "app_version": "269.0.0.18.75", "version_code": "314665256"},
    {"manufacturer": "google", "device": "Pixel 4", "model": "flame", "android_version": 30, "android_release": "11", "dpi": "560dpi", "resolution": "1080x2280", "cpu": "msmnile", "app_version": "269.0.0.18.75", "version_code": "314665256"},
    {"manufacturer": "oneplus", "device": "GM1913", "model": "OnePlus7Pro", "android_version": 29, "android_release": "10", "dpi": "560dpi", "resolution": "1440x3120", "cpu": "msmnile", "app_version": "269.0.0.18.75", "version_code": "314665256"},
    {"manufacturer": "xiaomi", "device": "Mi 9", "model": "cepheus", "android_version": 28, "android_release": "9", "dpi": "480dpi", "resolution": "1080x2340", "cpu": "msmnile", "app_version": "269.0.0.18.75", "version_code": "314665256"},
    {"manufacturer": "samsung", "device": "SM-A505F", "model": "a50", "android_version": 28, "android_release": "9", "dpi": "420dpi", "resolution": "1080x2340", "cpu": "exynos9610", "app_version": "269.0.0.18.75", "version_code": "314665256"},
    {"manufacturer": "google", "device": "Pixel 3", "model": "blueline", "android_version": 29, "android_release": "10", "dpi": "440dpi", "resolution": "1080x2160", "cpu": "sdm845", "app_version": "269.0.0.18.75", "version_code": "314665256"},
    {"manufacturer": "samsung", "device": "SM-G975F", "model": "beyond2", "android_version": 29, "android_release": "10", "dpi": "420dpi", "resolution": "1440x3040", "cpu": "exynos9820", "app_version": "269.0.0.18.75", "version_code": "314665256"},
]


def get_random_device() -> Dict:
    return random.choice(DEVICE_PROFILES).copy()
