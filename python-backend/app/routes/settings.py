import json
import os
from fastapi import APIRouter

router = APIRouter()

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "..", "settings.json")

@router.get("")
async def get_settings():
    """Get global settings"""
    try:
        with open(SETTINGS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

@router.post("")
async def save_settings(settings: dict):
    """Save global settings"""
    with open(SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(settings, f, indent=2)
    return {"message": "Settings saved"}