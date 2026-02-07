"""Reply tracking API: list detected DM replies from replies.csv or Supabase."""
import csv
import os
from datetime import datetime
from typing import Optional, Tuple

from fastapi import APIRouter, Query

from ..config import REPLIES_CSV, STORAGE_MODE
from ..reply_monitor import REPLIES_CSV_HEADER

router = APIRouter()

# Conditional import for DatabaseService
DatabaseService = None
if STORAGE_MODE == "supabase":
    try:
        from ..services.database import DatabaseService
    except Exception as e:
        print(f"Warning: Could not import DatabaseService: {e}")


def _parse_optional_datetime(s: Optional[str]):
    """Parse ISO date or datetime; if date-only, return start of day. Returns None if s is falsy or invalid."""
    if not s or not (s := s.strip()):
        return None
    s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        return dt
    except ValueError:
        return None


def count_replies_and_inbounds_in_range(
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Tuple[int, int]:
    """Return (replies_count, inbounds_count) for rows in replies.csv in [start, end] (inclusive).
    If start/end are None, use today. message_type 'reply' vs 'inbound'."""
    if not os.path.isfile(REPLIES_CSV):
        return 0, 0
    use_range = start is not None and end is not None
    start_dt = _parse_optional_datetime(start) if use_range else None
    end_dt = _parse_optional_datetime(end) if use_range else None
    if use_range and (start_dt is None or end_dt is None):
        use_range = False
    if not use_range:
        today = datetime.now().date()
    replies_count = 0
    inbounds_count = 0
    try:
        with open(REPLIES_CSV, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, fieldnames=REPLIES_CSV_HEADER)
            next(reader, None)  # skip header if present
            for row in reader:
                if row.get("timestamp") == "timestamp":
                    continue
                ts = row.get("timestamp") or ""
                if not ts:
                    continue
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if use_range:
                        if dt < start_dt or dt > end_dt:
                            continue
                    else:
                        if dt.date() != today:
                            continue
                except ValueError:
                    continue
                msg_type = (row.get("message_type") or "").strip().lower()
                if msg_type == "inbound":
                    inbounds_count += 1
                else:
                    replies_count += 1
    except Exception:
        return 0, 0
    return replies_count, inbounds_count


def count_replies_and_inbounds_for_today() -> Tuple[int, int]:
    """Return (replies_count, inbounds_count) for today (no date range)."""
    return count_replies_and_inbounds_in_range(None, None)


def count_replies_for_today() -> int:
    """Return number of rows in replies.csv with timestamp today and message_type 'reply'."""
    replies, _ = count_replies_and_inbounds_for_today()
    return replies


def _parse_replies_csv() -> list:
    """Read replies.csv and return list of dicts (newest first)."""
    if not os.path.isfile(REPLIES_CSV):
        return []
    rows = []
    try:
        with open(REPLIES_CSV, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, fieldnames=REPLIES_CSV_HEADER)
            next(reader, None)  # skip header if present
            for row in reader:
                if row.get("timestamp") == "timestamp":
                    continue
                if len(row) >= len(REPLIES_CSV_HEADER) or row.get("timestamp"):
                    rows.append(dict(row))
    except Exception:
        return []
    # Newest first
    rows.reverse()
    return rows


@router.get("")
async def get_replies(
    account: Optional[str] = Query(None, description="Filter by account_username"),
    campaign_id: Optional[str] = Query(None, description="Filter by campaign_id"),
    since: Optional[str] = Query(None, description="Only replies after this ISO date/time"),
    limit: int = Query(100, ge=1, le=500, description="Max replies to return"),
):
    """List detected DM replies. Optional filters: account, campaign_id, since."""
    
    # Try Supabase first if enabled
    if STORAGE_MODE == "supabase" and DatabaseService:
        try:
            db = DatabaseService.get_instance()
            rows = db.get_replies(limit=limit)
            
            # Apply filters
            if account:
                account_lower = account.strip().lower()
                rows = [r for r in rows if (r.get("account_username") or "").strip().lower() == account_lower]
            if since:
                try:
                    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
                    rows = [r for r in rows if r.get("timestamp") and datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) >= since_dt]
                except ValueError:
                    pass
            
            return {"replies": rows, "total": len(rows)}
        except Exception as e:
            print(f"Error fetching replies from Supabase: {e}")
            # Fall through to CSV
    
    # Fallback to CSV
    rows = _parse_replies_csv()
    if account:
        account_lower = account.strip().lower()
        rows = [r for r in rows if (r.get("account_username") or "").strip().lower() == account_lower]
    if campaign_id:
        cid = (campaign_id or "").strip()
        if cid:
            rows = [r for r in rows if (r.get("campaign_id") or "").strip() == cid]
    if since:
        try:
            since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
            rows = [r for r in rows if r.get("timestamp") and datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")) >= since_dt]
        except ValueError:
            pass
    rows = rows[:limit]
    return {"replies": rows, "total": len(rows)}
