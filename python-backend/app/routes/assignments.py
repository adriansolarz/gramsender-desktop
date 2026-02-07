from fastapi import APIRouter, HTTPException
import json
import os

from ..config import ASSIGNMENTS_FILE, STORAGE_MODE

router = APIRouter()

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


def load_assignments():
    if not os.path.exists(ASSIGNMENTS_FILE):
        return {}
    try:
        with open(ASSIGNMENTS_FILE, "r", encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def save_assignments(data: dict):
    with open(ASSIGNMENTS_FILE, "w", encoding='utf-8') as f:
        json.dump(data, f, indent=2)


@router.get("")
async def get_assignments():
    if STORAGE_MODE == "supabase" and db_service:
        assignments = db_service.get_assignments()
        return {"assignments": assignments}
    else:
        return {"assignments": load_assignments()}


@router.post("")
async def assign(data: dict):
    username = data.get("username")
    campaign_id = data.get("campaign_id")
    if not username or not campaign_id:
        raise HTTPException(status_code=400, detail="username and campaign_id required")
    
    if STORAGE_MODE == "supabase" and db_service:
        db_service.create_assignment(username, campaign_id)
        assignments = db_service.get_assignments()
        return {"assignments": assignments}
    else:
        assignments = load_assignments()
        assignments[username] = campaign_id
        save_assignments(assignments)
        return {"assignments": assignments}


@router.delete("/{username}")
async def unassign(username: str):
    if STORAGE_MODE == "supabase" and db_service:
        deleted = db_service.delete_assignment(username)
        if not deleted:
            raise HTTPException(status_code=404, detail="Assignment not found")
        assignments = db_service.get_assignments()
        return {"assignments": assignments}
    else:
        assignments = load_assignments()
        if username not in assignments:
            raise HTTPException(status_code=404, detail="Assignment not found")
        del assignments[username]
        save_assignments(assignments)
        return {"assignments": assignments}
