"""
Campaign Poller - Monitors Supabase for campaigns with status 'running'
and automatically starts workers for assigned accounts.
"""
import time
import threading
from datetime import datetime
from typing import Callable, Dict, Set

from .config import STORAGE_MODE


def run_campaign_poller(broadcast_fn: Callable[[dict], None], poll_interval: int = 10):
    """
    Background thread that polls for running campaigns and auto-starts workers.
    
    Args:
        broadcast_fn: Function to broadcast messages (for logging)
        poll_interval: How often to poll in seconds (default 10)
    """
    if STORAGE_MODE != "supabase":
        print("[CampaignPoller] Not running - STORAGE_MODE is not 'supabase'")
        return
    
    print(f"[CampaignPoller] Starting campaign poller (interval: {poll_interval}s)")
    print(f"[CampaignPoller] STORAGE_MODE = {STORAGE_MODE}")
    
    # Track which campaign+account combos have active workers to avoid duplicates
    active_campaign_accounts: Set[str] = set()
    
    # Wait a bit for the database service to initialize
    time.sleep(5)
    
    poll_count = 0
    while True:
        poll_count += 1
        try:
            from .services.database import DatabaseService
            from .worker_manager import WorkerManager
            
            db = DatabaseService.get_instance()
            worker_manager = WorkerManager.get_instance()
            
            # Get all campaigns with status 'running'
            campaigns = db.get_campaigns()
            running_campaigns = {
                cid: c for cid, c in campaigns.items() 
                if c.get("status") == "running"
            }
            
            # Log every 6th poll (every minute) or when there are running campaigns
            if poll_count % 6 == 0 or running_campaigns:
                print(f"[CampaignPoller] Poll #{poll_count}: {len(campaigns)} campaigns, {len(running_campaigns)} running")
            
            if not running_campaigns:
                # Clean up tracking set when no campaigns running
                active_campaign_accounts.clear()
                time.sleep(poll_interval)
                continue
            
            # Get assignments to know which accounts are assigned to which campaigns
            assignments = db.get_assignments()
            
            # Get all accounts with credentials
            accounts = db.get_accounts()
            
            # Get currently active workers to avoid duplicates
            current_workers = worker_manager.get_all_workers()
            active_combos = {
                f"{w.get('username')}:{w.get('campaign_id')}" 
                for w in current_workers.values()
                if w.get("status") in ("starting", "running")
            }
            
            for campaign_id, campaign in running_campaigns.items():
                # Find accounts assigned to this campaign
                assigned_accounts = [
                    username for username, cid in assignments.items()
                    if cid == campaign_id
                ]
                
                if not assigned_accounts:
                    print(f"[CampaignPoller] Campaign '{campaign.get('name')}' is running but has no assigned accounts")
                    continue
                
                for username in assigned_accounts:
                    combo_key = f"{username}:{campaign_id}"
                    
                    # Skip if already has an active worker
                    if combo_key in active_combos:
                        continue
                    
                    # Skip if we recently started this (avoid race conditions)
                    if combo_key in active_campaign_accounts:
                        continue
                    
                    # Get account details with credentials
                    account = db.get_account(username)
                    if not account:
                        print(f"[CampaignPoller] Account @{username} not found")
                        continue
                    
                    password = account.get("password")
                    session_cookies = account.get("session_cookies")
                    
                    if not password and not session_cookies:
                        print(f"[CampaignPoller] Account @{username} has no credentials - skipping")
                        continue
                    
                    # Start worker for this account+campaign
                    print(f"[CampaignPoller] Auto-starting worker for @{username} on campaign '{campaign.get('name')}'")
                    
                    try:
                        _start_worker_sync(
                            username=username,
                            account=account,
                            campaign_id=campaign_id,
                            campaign=campaign,
                            broadcast_fn=broadcast_fn,
                        )
                        active_campaign_accounts.add(combo_key)
                        
                        broadcast_fn({
                            "type": "log",
                            "message": f"Auto-started worker for @{username} on campaign '{campaign.get('name')}'",
                            "timestamp": datetime.now().isoformat(),
                            "source": "campaign_poller",
                        })
                    except Exception as e:
                        print(f"[CampaignPoller] Failed to start worker for @{username}: {e}")
                        broadcast_fn({
                            "type": "error",
                            "error": f"Failed to auto-start worker for @{username}: {e}",
                            "timestamp": datetime.now().isoformat(),
                            "source": "campaign_poller",
                        })
            
            # Clean up tracking for campaigns that are no longer running
            active_campaign_accounts = {
                k for k in active_campaign_accounts
                if k.split(":")[1] in running_campaigns
            }
            
        except Exception as e:
            print(f"[CampaignPoller] Error: {e}")
        
        time.sleep(poll_interval)


def _start_worker_sync(
    username: str,
    account: Dict,
    campaign_id: str,
    campaign: Dict,
    broadcast_fn: Callable[[dict], None],
):
    """Start a worker synchronously (called from poller thread)"""
    import uuid
    from .worker_manager import WorkerManager
    from .instagram_worker import InstagramWorkerThread
    from .config import STORAGE_MODE
    
    # Conditional import for DatabaseService
    DatabaseService = None
    if STORAGE_MODE == "supabase":
        try:
            from .services.database import DatabaseService
        except Exception:
            pass
    
    worker_id = str(uuid.uuid4())
    
    # Parse bio keywords
    bio_keywords = []
    if campaign.get("bio_filter_enabled") and campaign.get("bio_keywords"):
        bio_keywords = [k.strip() for k in campaign["bio_keywords"].split(",") if k.strip()]
    
    # Lead source labels
    target_mode = campaign.get("target_mode", 0)
    target_input = campaign.get("target_input", "")
    lead_source_labels = {0: "hashtag", 1: "followers", 2: "following", 3: "csv_leads"}
    
    worker_info = {
        "id": worker_id,
        "username": username,
        "account_name": account.get("account_name", username),
        "campaign_id": campaign_id,
        "campaign_name": campaign.get("name", ""),
        "target_mode": target_mode,
        "target_input": target_input,
        "lead_source": lead_source_labels.get(target_mode, "unknown"),
        "message_count": campaign.get("message_count", 50),
        "status": "starting",
        "messages_sent": 0,
        "errors": 0,
        "started_at": datetime.now().isoformat(),
        "progress": 0,
    }
    
    # Callbacks
    def on_update(message: str):
        worker_info["last_update"] = message
        print(f"[{username}] {message}")
        broadcast_fn({
            "type": "log",
            "worker_id": worker_id,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "source": "instagrapi",
        })
    
    def on_progress(progress: int):
        worker_info["progress"] = progress
        broadcast_fn({
            "type": "progress",
            "worker_id": worker_id,
            "progress": progress,
            "timestamp": datetime.now().isoformat(),
            "source": "instagrapi",
        })
    
    def on_error(error: str):
        worker_info["errors"] += 1
        worker_info["status"] = "error"
        print(f"[{username}] ERROR: {error}")
        broadcast_fn({
            "type": "error",
            "worker_id": worker_id,
            "error": error,
            "timestamp": datetime.now().isoformat(),
            "source": "instagrapi",
        })
    
    def on_complete(success: bool = True):
        sent = worker_info.get("messages_sent", 0)
        if success:
            worker_info["status"] = "completed"
            print(f"[{username}] Completed. Sent {sent} messages.")
            campaign_status = "draft"
        else:
            worker_info["status"] = "error"
            print(f"[{username}] Finished with error.")
            campaign_status = "failed"
        
        try:
            if STORAGE_MODE == "supabase" and DatabaseService:
                db = DatabaseService.get_instance()
                db.update_campaign(campaign_id, {"status": campaign_status})
        except Exception as e:
            print(f"[{username}] Error updating campaign status: {e}")
        
        broadcast_fn({
            "type": "complete",
            "worker_id": worker_id,
            "success": success,
            "timestamp": datetime.now().isoformat(),
            "source": "instagrapi",
        })
        WorkerManager.get_instance().remove_worker(worker_id)
    
    def on_message_sent(recipient_username: str, recipient_user_id: int, message_text: str):
        worker_info["messages_sent"] += 1
        n = worker_info["messages_sent"]
        print(f"[{username}] Message #{n} sent to @{recipient_username}")
        
        broadcast_fn({
            "type": "message_sent",
            "worker_id": worker_id,
            "messages_sent": n,
            "timestamp": datetime.now().isoformat(),
            "source": "instagrapi",
        })
        
        # Record to Supabase
        if STORAGE_MODE == "supabase" and DatabaseService:
            try:
                db = DatabaseService.get_instance()
                db.record_send(
                    account_username=username,
                    recipient_username=recipient_username or "",
                    account_name=worker_info.get("account_name", ""),
                    campaign_id=campaign_id,
                    campaign_name=worker_info.get("campaign_name", ""),
                    lead_source=worker_info.get("lead_source", ""),
                    lead_target=target_input,
                    recipient_user_id=str(recipient_user_id) if recipient_user_id else None,
                    message_preview=(message_text or "")[:500],
                )
                
                # Update campaign messages_sent
                campaign_data = db.get_campaign(campaign_id)
                if campaign_data:
                    new_sent = (campaign_data.get("messages_sent") or 0) + 1
                    db.update_campaign(campaign_id, {"messages_sent": new_sent})
            except Exception as e:
                print(f"[{username}] Failed to record send: {e}")
    
    def on_request_challenge_code(username_arg, choice):
        """2FA handler - for now just log and return False (can't handle interactively in poller)"""
        choice_str = getattr(choice, "name", str(choice))
        print(f"[{username}] 2FA required ({choice_str}) - cannot handle in auto-poller mode")
        broadcast_fn({
            "type": "need_2fa",
            "worker_id": worker_id,
            "username": username_arg,
            "choice": choice_str,
            "timestamp": datetime.now().isoformat(),
            "source": "instagrapi",
        })
        # Wait a bit for possible manual code submission
        pending = WorkerManager.get_instance().get_or_create_pending_challenge(worker_id)
        pending["event"].wait(timeout=300)
        code = pending.get("code")
        WorkerManager.get_instance().clear_pending_challenge(worker_id)
        return code or False
    
    # Create worker thread
    worker_thread = InstagramWorkerThread(
        worker_id=worker_id,
        username=username,
        password=account.get("password"),
        account_name=account.get("account_name", username),
        target_mode=target_mode,
        target_input=target_input,
        followers_threshold=campaign.get("followers_threshold", 0),
        campaign_id=campaign_id,
        lead_count=campaign.get("lead_count", 0),
        message_templates=campaign.get("message_templates", []),
        message_count=campaign.get("message_count", 50),
        follow_ups=campaign.get("follow_ups") or [],
        country_filter_enabled=campaign.get("country_filter_enabled", False),
        bio_filter_enabled=campaign.get("bio_filter_enabled", False),
        bio_keywords=bio_keywords,
        gender_filter=campaign.get("gender_filter", "all"),
        proxy=account.get("proxy"),
        session_cookies=account.get("session_cookies"),
        debug_mode=False,
        min_delay=3,
        max_delay=8,
        min_message_delay=2,
        max_message_delay=5,
        daily_limit=40,
        enable_rotation=True,
        enable_sessions=True,
        human_behavior=True,
        on_update=on_update,
        on_progress=on_progress,
        on_error=on_error,
        on_complete=on_complete,
        on_message_sent=on_message_sent,
        on_request_challenge_code=on_request_challenge_code,
    )
    
    # Add to manager and start
    WorkerManager.get_instance().add_worker(worker_id, worker_info, worker_thread)
    worker_thread.start()
    worker_info["status"] = "running"
    
    print(f"[CampaignPoller] Started worker {worker_id} for @{username} on '{campaign.get('name')}'")
