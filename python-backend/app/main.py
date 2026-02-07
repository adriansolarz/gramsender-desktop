from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from typing import List, Dict, Optional
import json
import asyncio
import threading
from datetime import datetime
import uuid
import os

# Load environment variables from .env file if it exists
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)
except ImportError:
    pass  # python-dotenv not installed, use system env vars

# Apply instagrapi patches before any Client use (fixes extract_user_gql, pinned_channels, bio_links)
try:
    from .patch_instagrapi import patch_instagrapi
    patch_instagrapi()
except Exception:
    pass

from .routes import campaigns, accounts, workers, assignments, replies, settings
from .connection_manager import ConnectionManager

app = FastAPI(title="Instagram Outreach API", version="1.0.0")


class RequestLogMiddleware(BaseHTTPMiddleware):
    """Log every HTTP request to terminal and broadcast api_log for Logs page."""

    async def dispatch(self, request, call_next):
        method = request.method
        path = request.url.path
        print(f"[API] {method} {path}", flush=True)
        response = await call_next(request)
        status = response.status_code
        print(f"[API] {method} {path} -> {status}", flush=True)
        # Broadcast to WebSocket clients for Logs page (skip /ws to avoid noise)
        if not path.startswith("/ws"):
            mgr = getattr(request.app.state, "connection_manager", None)
            if mgr:
                try:
                    await mgr.broadcast({
                        "type": "api_log",
                        "method": method,
                        "path": path,
                        "status_code": status,
                        "timestamp": datetime.now().isoformat(),
                    })
                except Exception:
                    pass
        return response


app.add_middleware(RequestLogMiddleware)

# CORS middleware for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize connection manager for WebSocket
connection_manager = ConnectionManager()

# Include routers
app.include_router(campaigns.router, prefix="/api/campaigns", tags=["campaigns"])
app.include_router(accounts.router, prefix="/api/accounts", tags=["accounts"])
app.include_router(workers.router, prefix="/api/workers", tags=["workers"])
app.include_router(assignments.router, prefix="/api/assignments", tags=["assignments"])
app.include_router(replies.router, prefix="/api/replies", tags=["replies"])
app.include_router(settings.router, prefix="/api/settings", tags=["settings"])

@app.get("/")
async def root():
    return {"message": "Instagram Outreach API", "status": "running"}

@app.get("/api/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

def _parse_optional_datetime(s):
    """Parse ISO date or datetime. Returns None if s is falsy or invalid."""
    if not s or not (s := str(s).strip()):
        return None
    s = s.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _count_sends_in_range(start_dt, end_dt):
    """Count rows in sends.csv where timestamp is in [start_dt, end_dt] (inclusive)."""
    from .config import SENDS_CSV
    if not os.path.isfile(SENDS_CSV):
        return 0
    count = 0
    try:
        import csv
        from .routes.workers import SENDS_CSV_HEADER
        with open(SENDS_CSV, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f, fieldnames=SENDS_CSV_HEADER)
            next(reader, None)  # skip header
            for row in reader:
                ts = row.get("timestamp") or ""
                if not ts or ts == "timestamp":
                    continue
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if start_dt <= dt <= end_dt:
                        count += 1
                except ValueError:
                    continue
    except Exception:
        return 0
    return count


@app.get("/api/stats")
async def get_stats(start: Optional[str] = None, end: Optional[str] = None):
    """Get dashboard statistics. Optional start/end (ISO date or datetime) filter time-based stats to that range."""
    try:
        from .config import CAMPAIGNS_FILE, ACCOUNTS_FILE, STORAGE_MODE, SENDS_CSV, REPLIES_CSV
        
        if STORAGE_MODE == "supabase":
            try:
                from .services.database import DatabaseService
                db_service = DatabaseService.get_instance()
                campaigns_data = db_service.get_campaigns()
                accounts_data = db_service.get_accounts()
            except Exception as e:
                print(f"Error loading from Supabase: {e}")
                campaigns_data = {}
                accounts_data = {}
        else:
            # Load campaigns
            try:
                with open(CAMPAIGNS_FILE, "r", encoding='utf-8') as f:
                    campaigns_data = json.load(f)
            except:
                campaigns_data = {}
            
            # Load accounts
            try:
                with open(ACCOUNTS_FILE, "r", encoding='utf-8') as f:
                    accounts_data = json.load(f)
            except:
                accounts_data = {}
        
        # Calculate stats
        active_campaigns = len([c for c in campaigns_data.values() if c.get("status") == "running"])
        total_campaigns = len(campaigns_data)
        total_accounts = len(accounts_data)
        
        # Get worker stats
        from .worker_manager import WorkerManager
        worker_manager = WorkerManager.get_instance()
        active_workers = len(worker_manager.active_workers)
        # Time range filter: when both start and end provided, filter messages/replies/inbounds to [start, end]
        use_range = start is not None and end is not None
        start_dt = _parse_optional_datetime(start) if use_range else None
        end_dt = _parse_optional_datetime(end) if use_range else None
        if use_range and (start_dt is None or end_dt is None):
            use_range = False

        # Total messages = rows in sends.csv (all-time, or in range when filter active)
        total_messages = 0
        if use_range:
            total_messages = _count_sends_in_range(start_dt, end_dt)
        elif os.path.isfile(SENDS_CSV):
            try:
                with open(SENDS_CSV, "r", encoding="utf-8") as f:
                    total_messages = sum(1 for _ in f) - 1  # subtract header row
                if total_messages < 0:
                    total_messages = 0
            except Exception:
                total_messages = 0

        # Replies vs inbounds: today (default) or in [start, end] when filter active
        total_replies, total_inbounds = replies.count_replies_and_inbounds_in_range(start, end)
        
        return {
            "totalMessages": total_messages,
            "totalReplies": total_replies,
            "totalInbounds": total_inbounds,
            "activeCampaigns": active_campaigns,
            "totalCampaigns": total_campaigns,
            "successRate": 95,  # Placeholder
            "accounts": total_accounts,
            "activeWorkers": active_workers
        }
    except Exception as e:
        return {
            "totalMessages": 0,
            "totalReplies": 0,
            "totalInbounds": 0,
            "activeCampaigns": 0,
            "totalCampaigns": 0,
            "successRate": 0,
            "accounts": 0,
            "activeWorkers": 0
        }

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time updates"""
    await connection_manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle incoming messages
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                # Handle different message types if needed
                if message.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except:
                pass
    except WebSocketDisconnect:
        connection_manager.disconnect(websocket)

# Make connection_manager available to other modules
app.state.connection_manager = connection_manager


@app.on_event("startup")
async def startup_event():
    """Start background threads on startup."""
    from .config import REPLY_MONITOR_ENABLED, STORAGE_MODE
    
    loop = asyncio.get_running_loop()
    cm = app.state.connection_manager
    
    def broadcast_sync(msg: dict):
        """Thread-safe broadcast function for background threads."""
        try:
            asyncio.run_coroutine_threadsafe(cm.broadcast(msg), loop)
        except Exception:
            pass
    
    # Start reply monitor if enabled
    if REPLY_MONITOR_ENABLED:
        from .reply_monitor import run_reply_monitor_loop
        thread = threading.Thread(target=run_reply_monitor_loop, args=(broadcast_sync,), daemon=True)
        thread.start()
        print("[ReplyMonitor] Background reply monitor started (REPLY_MONITOR_ENABLED=true).")
    
    # Start campaign poller if using Supabase
    if STORAGE_MODE == "supabase":
        from .campaign_poller import run_campaign_poller
        poller_thread = threading.Thread(
            target=run_campaign_poller, 
            args=(broadcast_sync,), 
            kwargs={"poll_interval": 10},
            daemon=True
        )
        poller_thread.start()
        print("[CampaignPoller] Background campaign poller started (polls every 10s for running campaigns).")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8012)
