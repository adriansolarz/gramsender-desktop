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
import sys

# Force unbuffered output so Electron can see logs immediately
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(line_buffering=True)
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(line_buffering=True)
os.environ['PYTHONUNBUFFERED'] = '1'

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

from .routes import campaigns, accounts, workers, assignments, replies, sends, settings
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
    allow_origins=["http://localhost:3000", "http://localhost:3001", "http://localhost:5173", "https://gramsender.com", "https://app.gramsender.com", "https://www.gramsender.com", "https://gramsender.vercel.app"],
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
app.include_router(sends.router, prefix="/api/sends", tags=["sends"])
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


@app.get("/api/stats")
async def get_stats(start: Optional[str] = None, end: Optional[str] = None):
    """Get dashboard statistics. Uses local SQLite for sends/replies, Supabase for campaigns/accounts."""
    try:
        from .config import CAMPAIGNS_FILE, ACCOUNTS_FILE, STORAGE_MODE
        
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
            try:
                with open(CAMPAIGNS_FILE, "r", encoding='utf-8') as f:
                    campaigns_data = json.load(f)
            except:
                campaigns_data = {}
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
        
        # Total messages from local SQLite
        from .services.local_storage import count_sends_in_range, count_total_sends
        use_range = start is not None and end is not None
        start_dt = _parse_optional_datetime(start) if use_range else None
        end_dt = _parse_optional_datetime(end) if use_range else None
        if use_range and (start_dt is None or end_dt is None):
            use_range = False
        
        if use_range:
            total_messages = count_sends_in_range(start_dt, end_dt)
        else:
            total_messages = count_total_sends()

        # Replies vs inbounds from local SQLite
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
    from .config import REPLY_MONITOR_ENABLED, STORAGE_MODE, SQLITE_DB_PATH
    
    # Initialize local SQLite database (sends, replies, conversations stored locally)
    from .services.local_storage import init as init_local_storage
    init_local_storage(SQLITE_DB_PATH)
    
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
