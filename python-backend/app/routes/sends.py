"""Sends tracking API: list sent DMs from local SQLite."""
from typing import Optional

from fastapi import APIRouter, Query

router = APIRouter()


@router.get("")
async def get_sends(
    account: Optional[str] = Query(None, description="Filter by account_username"),
    campaign_id: Optional[str] = Query(None, description="Filter by campaign_id"),
    since: Optional[str] = Query(None, description="Only sends after this ISO date/time"),
    limit: int = Query(100, ge=1, le=500, description="Max sends to return"),
):
    """List sent DMs from local SQLite."""
    try:
        from ..services.local_storage import get_sends as sqlite_get_sends
        rows = sqlite_get_sends(
            limit=limit,
            account=account,
            campaign_id=campaign_id,
            since=since,
        )
        return {"sends": rows, "total": len(rows)}
    except Exception as e:
        print(f"[Sends] Error fetching from SQLite: {e}")
        return {"sends": [], "total": 0}
