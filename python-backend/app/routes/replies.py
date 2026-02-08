"""Reply tracking API: list detected DM replies from local SQLite."""
import os
from datetime import datetime
from typing import Optional, Tuple

from fastapi import APIRouter, Query

router = APIRouter()


def count_replies_and_inbounds_in_range(
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> Tuple[int, int]:
    """Return (replies_count, inbounds_count) from local SQLite.
    If start/end are None, use today."""
    try:
        from ..services.local_storage import count_replies_and_inbounds_in_range as sqlite_count
        return sqlite_count(start, end)
    except Exception as e:
        print(f"[Replies] Error counting from SQLite: {e}")
        return 0, 0


def count_replies_and_inbounds_for_today() -> Tuple[int, int]:
    """Return (replies_count, inbounds_count) for today (no date range)."""
    return count_replies_and_inbounds_in_range(None, None)


def count_replies_for_today() -> int:
    """Return number of replies today."""
    replies, _ = count_replies_and_inbounds_for_today()
    return replies


@router.get("")
async def get_replies(
    account: Optional[str] = Query(None, description="Filter by account_username"),
    campaign_id: Optional[str] = Query(None, description="Filter by campaign_id"),
    since: Optional[str] = Query(None, description="Only replies after this ISO date/time"),
    limit: int = Query(100, ge=1, le=500, description="Max replies to return"),
):
    """List detected DM replies from local SQLite."""
    try:
        from ..services.local_storage import get_replies as sqlite_get_replies
        rows = sqlite_get_replies(
            limit=limit,
            account=account,
            campaign_id=campaign_id,
            since=since,
        )
        return {"replies": rows, "total": len(rows)}
    except Exception as e:
        print(f"[Replies] Error fetching from SQLite: {e}")
        return {"replies": [], "total": 0}
