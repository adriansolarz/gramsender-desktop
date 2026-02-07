import csv
import io
from fastapi import APIRouter, HTTPException, UploadFile, File
from typing import List, Dict, Optional
import json
import os
from datetime import datetime

from ..config import CAMPAIGNS_FILE, STORAGE_MODE, LEADS_DIR

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

def load_campaigns() -> Dict:
    """Load campaigns from JSON file"""
    if not os.path.exists(CAMPAIGNS_FILE):
        return {}
    try:
        with open(CAMPAIGNS_FILE, "r", encoding='utf-8') as f:
            return json.load(f)
    except:
        return {}

def save_campaigns(campaigns: Dict):
    """Save campaigns to JSON file"""
    with open(CAMPAIGNS_FILE, "w", encoding='utf-8') as f:
        json.dump(campaigns, f, indent=2)

@router.get("")
async def get_campaigns():
    """Get all campaigns"""
    if STORAGE_MODE == "supabase" and db_service:
        campaigns = db_service.get_campaigns()
        return {"campaigns": campaigns}
    else:
        campaigns = load_campaigns()
        return {"campaigns": campaigns}

@router.get("/{campaign_id}")
async def get_campaign(campaign_id: str):
    """Get a specific campaign"""
    if STORAGE_MODE == "supabase" and db_service:
        campaign = db_service.get_campaign(campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return campaign
    else:
        campaigns = load_campaigns()
        if campaign_id not in campaigns:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return campaigns[campaign_id]

@router.post("")
async def create_campaign(campaign_data: dict):
    """Create a new campaign"""
    # Generate campaign ID
    campaign_id = campaign_data.get("id") or f"campaign_{int(datetime.now().timestamp())}"
    target_mode = campaign_data.get("target_mode", 0)
    # target_input optional when target_mode is 3 (CSV leads)
    required_fields = ["name", "followers_threshold", "message_count", "message_templates"]
    if target_mode != 3:
        required_fields.append("target_input")
    for field in required_fields:
        if field not in campaign_data:
            raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
    
    # Validate follow_ups (optional, up to 10)
    follow_ups_raw = campaign_data.get("follow_ups") or []
    follow_ups = []
    for fu in (follow_ups_raw[:10] if isinstance(follow_ups_raw, list) else []):
        if not isinstance(fu, dict):
            continue
        msg = (fu.get("message") or "").strip()
        if not msg:
            continue
        try:
            delay_value = max(0, int(fu.get("delay_value", 0)))
        except (TypeError, ValueError):
            delay_value = 0
        unit = (fu.get("delay_unit") or "hours").lower()
        if unit not in ("minutes", "hours", "days"):
            unit = "hours"
        follow_ups.append({"message": msg, "delay_value": delay_value, "delay_unit": unit})

    campaign = {
        "name": campaign_data["name"],
        "target_mode": target_mode,
        "target_input": campaign_data.get("target_input", ""),
        "followers_threshold": campaign_data["followers_threshold"],
        "message_count": campaign_data["message_count"],
        "country_filter_enabled": campaign_data.get("country_filter_enabled", False),
        "bio_filter_enabled": campaign_data.get("bio_filter_enabled", False),
        "bio_keywords": campaign_data.get("bio_keywords", ""),
        "gender_filter": campaign_data.get("gender_filter", "all"),
        "message_templates": campaign_data["message_templates"],
        "follow_ups": follow_ups,
        "webhook_url": campaign_data.get("webhook_url", ""),
        "status": "draft",
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "lead_count": campaign_data.get("lead_count", 0),
    }
    mapping = campaign_data.get("csv_column_mapping")
    if isinstance(mapping, dict) and mapping.get("username"):
        campaign["csv_column_mapping"] = {
            "username": str(mapping["username"]).strip(),
            **({k: str(v).strip() for k, v in mapping.items() if k != "username" and v}),
        }
    
    if STORAGE_MODE == "supabase" and db_service:
        created = db_service.create_campaign(campaign_id, campaign)
        return {"id": campaign_id, **created}
    else:
        # Legacy JSON implementation
        campaigns = load_campaigns()
        campaigns[campaign_id] = campaign
        save_campaigns(campaigns)
        return {"id": campaign_id, **campaign}

@router.put("/{campaign_id}")
async def update_campaign(campaign_id: str, campaign_data: dict):
    """Update an existing campaign"""
    if STORAGE_MODE == "supabase" and db_service:
        updated = db_service.update_campaign(campaign_id, campaign_data)
        if not updated:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return updated
    else:
        # Legacy JSON implementation
        campaigns = load_campaigns()
        if campaign_id not in campaigns:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        # Update fields
        campaigns[campaign_id].update(campaign_data)
        campaigns[campaign_id]["updated_at"] = datetime.now().isoformat()
        # Ensure webhook_url is saved
        if "webhook_url" in campaign_data:
            campaigns[campaign_id]["webhook_url"] = campaign_data["webhook_url"]
        
        save_campaigns(campaigns)
        return campaigns[campaign_id]

def _parse_csv_leads(content: bytes) -> List[str]:
    """Parse CSV or plain text (one username per line). Returns deduplicated list of usernames."""
    text = content.decode("utf-8", errors="replace")
    usernames = set()
    # Try CSV: column named username/instagram/handle or first column
    try:
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return []
        header = [h.strip().lower() for h in rows[0]]
        username_col = None
        for name in ("username", "instagram", "handle", "user", "insta"):
            if name in header:
                username_col = header.index(name)
                break
        start = 1 if username_col is not None else 0
        for row in rows[start:]:
            if username_col is not None and username_col < len(row):
                u = row[username_col].strip()
            else:
                u = row[0].strip() if row else ""
            if u and not u.startswith("#") and u.lower() not in ("username", "instagram", "handle", "user", "insta"):
                usernames.add(u)
        if usernames:
            return sorted(usernames)
    except Exception:
        pass
    # Plain text: one username per line or comma-separated
    for line in text.splitlines():
        for part in line.replace(",", " ").split():
            u = part.strip()
            if u and not u.startswith("#"):
                usernames.add(u)
    return sorted(usernames)


def _parse_csv_leads_with_mapping(
    content: bytes, mapping: Dict[str, str]
) -> List[Dict[str, str]]:
    """Parse CSV with column mapping. Returns list of dicts with username, fullname?, firstname?."""
    text = content.decode("utf-8", errors="replace")
    username_col = (mapping.get("username") or "").strip()
    if not username_col:
        return []
    fullname_col = (mapping.get("fullname") or "").strip() or None
    firstname_col = (mapping.get("firstname") or "").strip() or None
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            return []
        # Match mapping column names to CSV header keys (case-insensitive)
        header_lower = {k.strip().lower(): k for k in rows[0].keys()}
        def key_for(col: Optional[str]):
            if not (col or "").strip():
                return None
            return header_lower.get(col.strip().lower()) or (col if col in rows[0] else None)
        username_key = key_for(username_col) or username_col
        fullname_key = key_for(fullname_col) if fullname_col else None
        firstname_key = key_for(firstname_col) if firstname_col else None
        seen = set()
        out = []
        for row in rows:
            u = (row.get(username_key) or "").strip()
            if not u or u.startswith("#") or u.lower() in ("username", "instagram", "handle"):
                continue
            if u in seen:
                continue
            seen.add(u)
            lead = {"username": u}
            if fullname_key:
                fn = (row.get(fullname_key) or "").strip()
                if fn:
                    lead["fullname"] = fn
            if firstname_key:
                fn = (row.get(firstname_key) or "").strip()
                if fn:
                    lead["firstname"] = fn
            out.append(lead)
        return out
    except Exception:
        return []


@router.post("/{campaign_id}/leads")
async def upload_leads(campaign_id: str, file: UploadFile = File(...)):
    """Upload CSV (or plain text) leads. With csv_column_mapping stores JSONL (username + fullname/firstname); else one username per line."""
    if STORAGE_MODE == "supabase" and db_service:
        campaign = db_service.get_campaign(campaign_id)
    else:
        campaigns = load_campaigns()
        campaign = campaigns.get(campaign_id) if campaigns else None
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    content = await file.read()
    mapping = campaign.get("csv_column_mapping") if isinstance(campaign.get("csv_column_mapping"), dict) else None
    os.makedirs(LEADS_DIR, exist_ok=True)
    txt_path = os.path.join(LEADS_DIR, f"{campaign_id}.txt")
    jsonl_path = os.path.join(LEADS_DIR, f"{campaign_id}.jsonl")
    if mapping and mapping.get("username"):
        leads = _parse_csv_leads_with_mapping(content, mapping)
        lead_count = len(leads)
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for lead in leads:
                f.write(json.dumps(lead, ensure_ascii=False) + "\n")
        with open(txt_path, "w", encoding="utf-8") as f:
            for lead in leads:
                f.write(lead["username"] + "\n")
    else:
        usernames = _parse_csv_leads(content)
        lead_count = len(usernames)
        if os.path.exists(jsonl_path):
            try:
                os.remove(jsonl_path)
            except OSError:
                pass
        with open(txt_path, "w", encoding="utf-8") as f:
            for u in usernames:
                f.write(u + "\n")
    update_data = {"lead_count": lead_count, "target_mode": 3}
    if STORAGE_MODE == "supabase" and db_service:
        db_service.update_campaign(campaign_id, update_data)
    else:
        campaigns = load_campaigns()
        if campaign_id in campaigns:
            campaigns[campaign_id].update(update_data)
            campaigns[campaign_id]["updated_at"] = datetime.now().isoformat()
            save_campaigns(campaigns)
    return {"campaign_id": campaign_id, "lead_count": lead_count}


@router.delete("/{campaign_id}")
async def delete_campaign(campaign_id: str):
    """Delete a campaign"""
    if STORAGE_MODE == "supabase" and db_service:
        deleted = db_service.delete_campaign(campaign_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Campaign not found")
        return {"message": "Campaign deleted successfully"}
    else:
        # Legacy JSON implementation
        campaigns = load_campaigns()
        if campaign_id not in campaigns:
            raise HTTPException(status_code=404, detail="Campaign not found")
        
        del campaigns[campaign_id]
        save_campaigns(campaigns)
        for ext in (".txt", ".jsonl"):
            leads_path = os.path.join(LEADS_DIR, f"{campaign_id}{ext}")
            if os.path.exists(leads_path):
                try:
                    os.remove(leads_path)
                except OSError:
                    pass
        return {"message": "Campaign deleted successfully"}
