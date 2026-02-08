from fastapi import APIRouter, HTTPException, BackgroundTasks
from typing import Dict, Optional
import csv
import json
import sys
import uuid
import threading
import asyncio
import os
from datetime import datetime

from ..config import CAMPAIGNS_FILE, ACCOUNTS_FILE, KEY_FILE, STORAGE_MODE, SENDS_CSV

# #region agent log
def _agent_log(location: str, message: str, data: dict, hypothesis_id: str = ""):
    try:
        p = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".cursor", "debug.log"))
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({"location": location, "message": message, "data": data, "hypothesisId": hypothesis_id, "timestamp": datetime.now().isoformat(), "sessionId": "debug-session"}) + "\n")
    except Exception:
        pass
# #endregion
from ..worker_manager import WorkerManager

# Conditional import for DatabaseService
DatabaseService = None
if STORAGE_MODE == "supabase":
    try:
        from ..services.database import DatabaseService
    except Exception as e:
        print(f"Warning: Could not import DatabaseService: {e}")
from ..instagram_worker import InstagramWorkerThread

router = APIRouter()

# Lock for appending to sends.csv from multiple workers
_sends_csv_lock = threading.Lock()
# Lock for updating campaign messages_sent (multiple workers may send for same campaign)
_campaign_messages_sent_lock = threading.Lock()

SENDS_CSV_HEADER = (
    "timestamp",
    "account_username",
    "account_name",
    "campaign_id",
    "campaign_name",
    "lead_source",
    "lead_target",
    "recipient_username",
    "recipient_user_id",
    "message_preview",
)


@router.post("/start")
async def start_worker(worker_data: dict, background_tasks: BackgroundTasks):
    """Start a new Instagram worker"""
    try:
        # Validate required fields
        required_fields = ["username", "campaign_id"]
        for field in required_fields:
            if field not in worker_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")
        
        username = worker_data["username"]
        campaign_id = worker_data["campaign_id"]
        
        # Load account and campaign based on storage mode
        if STORAGE_MODE == "supabase":
            try:
                db_service = DatabaseService.get_instance()
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Database service unavailable: {e}")
            
            # Get account
            account = db_service.get_account(username)
            if not account:
                raise HTTPException(status_code=404, detail="Account not found")
            
            password = account.get("password")
            proxy = account.get("proxy")
            session_cookies = account.get("session_cookies")
            account_name = account.get("account_name", username)
            
            # Get campaign
            campaign = db_service.get_campaign(campaign_id)
            if not campaign:
                raise HTTPException(status_code=404, detail="Campaign not found")
            
            # Update campaign status to "running"
            db_service.update_campaign(campaign_id, {"status": "running"})
        else:
            # Legacy JSON implementation
            import base64
            from cryptography.fernet import Fernet
            import os
            
            try:
                with open(ACCOUNTS_FILE, "r", encoding='utf-8') as f:
                    accounts = json.load(f)
            except:
                raise HTTPException(status_code=404, detail="Accounts file not found")
            
            if username not in accounts:
                raise HTTPException(status_code=404, detail="Account not found")
            
            # Decrypt password and other fields
            if not os.path.exists(KEY_FILE):
                raise HTTPException(status_code=500, detail="Encryption key not found")
            
            with open(KEY_FILE, "rb") as f:
                key = f.read()
            
            fernet = Fernet(key)
            
            password = None
            if 'password' in accounts[username] and accounts[username]['password']:
                encrypted_password = base64.b64decode(accounts[username]['password'])
                password = fernet.decrypt(encrypted_password).decode()
            
            proxy = None
            if 'proxy' in accounts[username] and accounts[username]['proxy']:
                encrypted_proxy = base64.b64decode(accounts[username]['proxy'])
                proxy = fernet.decrypt(encrypted_proxy).decode()
            
            session_cookies = None
            if 'session_cookies' in accounts[username] and accounts[username]['session_cookies']:
                encrypted_cookies = base64.b64decode(accounts[username]['session_cookies'])
                session_cookies = fernet.decrypt(encrypted_cookies).decode()
            
            account_name = accounts[username].get("account_name", username)
            
            # Load campaign
            try:
                with open(CAMPAIGNS_FILE, "r", encoding='utf-8') as f:
                    campaigns = json.load(f)
            except:
                raise HTTPException(status_code=404, detail="Campaigns file not found")
            
            if campaign_id not in campaigns:
                raise HTTPException(status_code=404, detail="Campaign not found")
            
            campaign = campaigns[campaign_id]
            
            # Update campaign status to "running"
            campaigns[campaign_id]["status"] = "running"
            campaigns[campaign_id]["updated_at"] = datetime.now().isoformat()
            with open(CAMPAIGNS_FILE, "w", encoding='utf-8') as f:
                json.dump(campaigns, f, indent=2)
        
        # Generate worker ID
        worker_id = str(uuid.uuid4())
        
        # Parse bio keywords
        bio_keywords = []
        if campaign.get("bio_filter_enabled") and campaign.get("bio_keywords"):
            bio_keywords = [k.strip() for k in campaign["bio_keywords"].split(",") if k.strip()]
        
        # Create worker data (include lead source for sends.csv)
        target_mode = campaign.get("target_mode", 0)
        target_input = campaign.get("target_input", "")
        lead_source_labels = {0: "hashtag", 1: "followers", 2: "following", 3: "csv_leads"}
        worker_info = {
            "id": worker_id,
            "username": worker_data["username"],
            "account_name": worker_data.get("account_name", worker_data["username"]),
            "campaign_id": campaign_id,
            "campaign_name": campaign["name"],
            "target_mode": target_mode,
            "target_input": target_input,
            "lead_source": lead_source_labels.get(target_mode, "unknown"),
            "message_count": campaign["message_count"],
            "status": "starting",
            "messages_sent": 0,
            "errors": 0,
            "started_at": datetime.now().isoformat(),
            "progress": 0
        }
        
        # Create callback functions for WebSocket updates
        from ..main import app
        connection_manager = app.state.connection_manager
        # Capture the event loop so worker thread can schedule broadcasts thread-safely
        main_loop = asyncio.get_running_loop()
        
        def create_broadcast_task(message_dict):
            """Schedule broadcast on the main event loop (safe to call from worker thread)."""
            try:
                asyncio.run_coroutine_threadsafe(
                    connection_manager.broadcast(message_dict),
                    main_loop,
                )
            except Exception:
                pass  # Don't fail worker if broadcast drops
        
        def log_terminal(msg: str, is_error: bool = False):
            """Print worker update to terminal; safe for Windows console (cp1252)."""
            stream = sys.stderr if is_error else sys.stdout
            prefix = f"[{worker_info.get('account_name', 'worker')}]"
            out = f"{prefix} {msg}"
            enc = getattr(stream, "encoding", None) or "utf-8"
            try:
                stream.buffer.write((out + "\n").encode(enc, errors="replace"))
                stream.buffer.flush()
            except (AttributeError, OSError):
                stream.write(out.encode(enc, errors="replace").decode(enc) + "\n")
                stream.flush()
        
        def on_update(message: str):
            worker_info["last_update"] = message
            log_terminal(message)
            create_broadcast_task({
                "type": "log",
                "worker_id": worker_id,
                "message": message,
                "timestamp": datetime.now().isoformat(),
                "source": "instagrapi",
            })
        
        def on_progress(progress: int):
            worker_info["progress"] = progress
            log_terminal(f"Progress: {progress}%")
            create_broadcast_task({
                "type": "progress",
                "worker_id": worker_id,
                "progress": progress,
                "timestamp": datetime.now().isoformat(),
                "source": "instagrapi",
            })
        
        def on_error(error: str):
            worker_info["errors"] += 1
            worker_info["status"] = "error"
            log_terminal(f"ERROR: {error}", is_error=True)
            create_broadcast_task({
                "type": "error",
                "worker_id": worker_id,
                "error": error,
                "timestamp": datetime.now().isoformat(),
                "source": "instagrapi",
            })
        
        def on_complete(success: bool = True):
            # #region agent log
            _agent_log("workers.py:on_complete", "entry", {"success": success}, "H4")
            # #endregion
            sent = worker_info.get("messages_sent", 0)
            if success:
                worker_info["status"] = "completed"
                log_terminal(f"Completed. Sent {sent} messages.")
                campaign_status = "draft"
                complete_message = f"Completed. Sent {sent} messages."
            else:
                worker_info["status"] = "error"
                log_terminal("Finished with error (e.g. login required).", is_error=True)
                campaign_status = "failed"
                complete_message = "Finished with error (e.g. session expired / login required)."
            try:
                if STORAGE_MODE == "supabase":
                    db_service = DatabaseService.get_instance()
                    db_service.update_campaign(campaign_id, {"status": campaign_status})
                else:
                    with open(CAMPAIGNS_FILE, "r", encoding='utf-8') as f:
                        campaigns = json.load(f)
                    if campaign_id in campaigns:
                        campaigns[campaign_id]["status"] = campaign_status
                        campaigns[campaign_id]["updated_at"] = datetime.now().isoformat()
                        with open(CAMPAIGNS_FILE, "w", encoding='utf-8') as f:
                            json.dump(campaigns, f, indent=2)
            except Exception as e:
                print(f"Error updating campaign status: {e}")
            create_broadcast_task({
                "type": "complete",
                "worker_id": worker_id,
                "message": complete_message,
                "success": success,
                "timestamp": datetime.now().isoformat(),
                "source": "instagrapi",
            })
            WorkerManager.get_instance().remove_worker(worker_id)
        
        def on_message_sent(recipient_username: str, recipient_user_id: int, message_text: str):
            worker_info["messages_sent"] += 1
            n = worker_info["messages_sent"]
            log_terminal(f"Message #{n} sent")
            create_broadcast_task({
                "type": "message_sent",
                "worker_id": worker_id,
                "messages_sent": worker_info["messages_sent"],
                "timestamp": datetime.now().isoformat(),
                "source": "instagrapi",
            })
            # Record send to local SQLite (data stays on device)
            message_preview = (message_text or "")[:500]
            try:
                from ..services.local_storage import record_send as local_record_send, record_conversation as local_record_convo
                local_record_send(
                    account_username=worker_info.get("username", ""),
                    recipient_username=recipient_username or "",
                    account_name=worker_info.get("account_name", ""),
                    campaign_id=worker_info.get("campaign_id", ""),
                    campaign_name=worker_info.get("campaign_name", ""),
                    lead_source=worker_info.get("lead_source", ""),
                    lead_target=worker_info.get("target_input", ""),
                    recipient_user_id=str(recipient_user_id) if recipient_user_id else "",
                    message_preview=message_preview,
                )
                local_record_convo(
                    account_username=worker_info.get("username", ""),
                    recipient_username=recipient_username or "",
                    direction="outbound",
                    message_text=message_text or "",
                    campaign_id=worker_info.get("campaign_id", ""),
                )
            except Exception as e:
                log_terminal(f"Failed to write to local SQLite: {e}", is_error=True)
            # Update campaign messages_sent so dashboard shows live progress
            with _campaign_messages_sent_lock:
                try:
                    if STORAGE_MODE == "supabase":
                        db = DatabaseService.get_instance()
                        campaign_data = db.get_campaign(campaign_id)
                        if campaign_data is not None:
                            new_sent = (campaign_data.get("messages_sent") or 0) + 1
                            db.update_campaign(campaign_id, {"messages_sent": new_sent})
                    else:
                        if os.path.exists(CAMPAIGNS_FILE):
                            with open(CAMPAIGNS_FILE, "r", encoding="utf-8") as f:
                                campaigns = json.load(f)
                            if campaign_id in campaigns:
                                campaigns[campaign_id]["messages_sent"] = campaigns[campaign_id].get("messages_sent", 0) + 1
                                campaigns[campaign_id]["updated_at"] = datetime.now().isoformat()
                                with open(CAMPAIGNS_FILE, "w", encoding="utf-8") as f:
                                    json.dump(campaigns, f, indent=2)
                except Exception as e:
                    log_terminal(f"Failed to update campaign messages_sent: {e}", is_error=True)
        
        def on_request_challenge_code(username_arg, choice):
            """Called by worker thread when Instagram requires 2FA/challenge code. Blocks until code is submitted."""
            choice_str = getattr(choice, "name", str(choice))
            log_terminal(f"2FA/challenge required for @{username_arg} ({choice_str}). Waiting for code...")
            pending = WorkerManager.get_instance().get_or_create_pending_challenge(worker_id)
            create_broadcast_task({
                "type": "need_2fa",
                "worker_id": worker_id,
                "username": username_arg,
                "choice": choice_str,
                "timestamp": datetime.now().isoformat(),
                "source": "instagrapi",
            })
            pending["event"].wait(timeout=300)
            code = pending.get("code")
            WorkerManager.get_instance().clear_pending_challenge(worker_id)
            return code or False
        
        # Create and start worker thread
        worker_thread = InstagramWorkerThread(
            worker_id=worker_id,
            username=username,
            password=password,
            account_name=worker_data.get("account_name", account_name),
            target_mode=campaign["target_mode"],
            target_input=campaign.get("target_input", ""),
            followers_threshold=campaign["followers_threshold"],
            campaign_id=campaign_id,
            lead_count=campaign.get("lead_count", 0),
            message_templates=campaign["message_templates"],
            message_count=campaign["message_count"],
            follow_ups=campaign.get("follow_ups") or [],
            country_filter_enabled=campaign.get("country_filter_enabled", False),
            bio_filter_enabled=campaign.get("bio_filter_enabled", False),
            bio_keywords=bio_keywords,
            gender_filter=campaign.get("gender_filter", "all"),
            proxy=proxy,
            session_cookies=session_cookies,
            debug_mode=worker_data.get("debug_mode", False),
            min_delay=worker_data.get("min_delay", 3),
            max_delay=worker_data.get("max_delay", 8),
            min_message_delay=worker_data.get("min_message_delay", 2),
            max_message_delay=worker_data.get("max_message_delay", 5),
            daily_limit=worker_data.get("daily_limit", 50),
            enable_rotation=worker_data.get("enable_rotation", True),
            enable_sessions=worker_data.get("enable_sessions", True),
            human_behavior=worker_data.get("human_behavior", True),
            on_update=on_update,
            on_progress=on_progress,
            on_error=on_error,
            on_complete=on_complete,
            on_message_sent=on_message_sent,
            on_request_challenge_code=on_request_challenge_code,
        )
        
        # Add to worker manager
        WorkerManager.get_instance().add_worker(worker_id, worker_info, worker_thread)
        
        # Start worker in background
        worker_thread.start()
        worker_info["status"] = "running"
        print(f"[{worker_info.get('account_name', 'worker')}] Started worker {worker_id} for campaign {campaign.get('name', campaign_id)}")
        
        return {
            "worker_id": worker_id,
            "status": "started",
            "worker_info": worker_info
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("")
async def get_workers():
    """Get all active workers"""
    worker_manager = WorkerManager.get_instance()
    workers = worker_manager.get_all_workers()
    return {"workers": workers}

@router.get("/{worker_id}")
async def get_worker(worker_id: str):
    """Get a specific worker"""
    worker_manager = WorkerManager.get_instance()
    worker = worker_manager.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    return worker

@router.post("/{worker_id}/challenge-code")
async def submit_challenge_code(worker_id: str, body: dict):
    """Submit 2FA/challenge verification code for a worker waiting for it."""
    code = body.get("code")
    if not code or not str(code).strip():
        raise HTTPException(status_code=400, detail="code is required")
    if not WorkerManager.get_instance().set_challenge_code(worker_id, str(code).strip()):
        raise HTTPException(status_code=404, detail="No pending challenge for this worker")
    return {"status": "ok"}

@router.post("/{worker_id}/stop")
async def stop_worker(worker_id: str):
    """Stop a running worker"""
    worker_manager = WorkerManager.get_instance()
    worker = worker_manager.get_worker(worker_id)
    if not worker:
        raise HTTPException(status_code=404, detail="Worker not found")
    
    campaign_id = worker.get("campaign_id")
    
    success = worker_manager.stop_worker(worker_id)
    if not success:
        raise HTTPException(status_code=404, detail="Worker not found or already stopped")
    
    print(f"[{worker.get('account_name', 'worker')}] Worker {worker_id} stopped")
    
    # Update status
    if worker:
        worker["status"] = "stopped"
        # Update campaign status back to "draft" when worker is stopped
        if campaign_id:
            try:
                if STORAGE_MODE == "supabase":
                    db_service = DatabaseService.get_instance()
                    db_service.update_campaign(campaign_id, {"status": "draft"})
                else:
                    with open(CAMPAIGNS_FILE, "r", encoding='utf-8') as f:
                        campaigns = json.load(f)
                    if campaign_id in campaigns:
                        campaigns[campaign_id]["status"] = "draft"
                        campaigns[campaign_id]["updated_at"] = datetime.now().isoformat()
                        with open(CAMPAIGNS_FILE, "w", encoding='utf-8') as f:
                            json.dump(campaigns, f, indent=2)
            except Exception as e:
                print(f"Error updating campaign status: {e}")
        
        from ..main import app
        connection_manager = app.state.connection_manager
        import asyncio
        asyncio.run(connection_manager.broadcast({
            "type": "stopped",
            "worker_id": worker_id,
            "timestamp": datetime.now().isoformat()
        }))
    
    return {"message": "Worker stopped successfully"}

@router.delete("/{worker_id}")
async def delete_worker(worker_id: str):
    """Delete a worker"""
    worker_manager = WorkerManager.get_instance()
    worker_manager.remove_worker(worker_id)
    return {"message": "Worker deleted successfully"}
