"""
Geographic consistency (from simple login + dm). Optional pytz.
"""
import random
from typing import Dict
from datetime import datetime

try:
    import pytz
    PYTZ_AVAILABLE = True
except ImportError:
    PYTZ_AVAILABLE = False

GEOGRAPHIC_REGIONS = [
    {"country_code": "US", "timezone": "America/New_York", "locale": "en_US", "weight": 0.40},
    {"country_code": "GB", "timezone": "Europe/London", "locale": "en_GB", "weight": 0.15},
    {"country_code": "DE", "timezone": "Europe/Berlin", "locale": "de_DE", "weight": 0.15},
    {"country_code": "FR", "timezone": "Europe/Paris", "locale": "fr_FR", "weight": 0.10},
    {"country_code": "CA", "timezone": "America/Toronto", "locale": "en_CA", "weight": 0.10},
    {"country_code": "AU", "timezone": "Australia/Sydney", "locale": "en_AU", "weight": 0.10},
]


def get_random_region() -> Dict:
    weights = [r["weight"] for r in GEOGRAPHIC_REGIONS]
    return random.choices(GEOGRAPHIC_REGIONS, weights=weights)[0].copy()


def get_current_time_in_region(region: Dict) -> datetime:
    if PYTZ_AVAILABLE:
        try:
            return datetime.now(pytz.timezone(region["timezone"]))
        except Exception:
            pass
    return datetime.utcnow()
