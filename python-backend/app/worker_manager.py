"""Manages active Instagram workers and pending 2FA/challenge codes"""
from typing import Dict, Optional, Any
import threading

class WorkerManager:
    _instance = None
    _lock = threading.Lock()
    
    def __init__(self):
        self.active_workers: Dict[str, dict] = {}
        self.worker_threads: Dict[str, any] = {}
        self.pending_challenges: Dict[str, dict] = {}  # worker_id -> {"event": Event(), "code": None}
        self.lock = threading.Lock()
    
    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance
    
    def add_worker(self, worker_id: str, worker_data: dict, thread):
        with self.lock:
            self.active_workers[worker_id] = worker_data
            self.worker_threads[worker_id] = thread
    
    def remove_worker(self, worker_id: str):
        with self.lock:
            if worker_id in self.active_workers:
                del self.active_workers[worker_id]
            if worker_id in self.worker_threads:
                del self.worker_threads[worker_id]
            if worker_id in self.pending_challenges:
                del self.pending_challenges[worker_id]
    
    def get_or_create_pending_challenge(self, worker_id: str) -> dict:
        """Get or create pending challenge slot for a worker (thread-safe). Returns {"event": Event(), "code": None}."""
        with self.lock:
            if worker_id not in self.pending_challenges:
                self.pending_challenges[worker_id] = {"event": threading.Event(), "code": None}
            return self.pending_challenges[worker_id]
    
    def set_challenge_code(self, worker_id: str, code: str) -> bool:
        """Set the verification code for a worker and wake the waiting thread. Returns True if worker was waiting."""
        with self.lock:
            if worker_id not in self.pending_challenges:
                return False
            self.pending_challenges[worker_id]["code"] = code
            self.pending_challenges[worker_id]["event"].set()
        return True
    
    def clear_pending_challenge(self, worker_id: str):
        with self.lock:
            if worker_id in self.pending_challenges:
                del self.pending_challenges[worker_id]
    
    def get_worker(self, worker_id: str) -> Optional[dict]:
        with self.lock:
            return self.active_workers.get(worker_id)
    
    def get_all_workers(self) -> Dict[str, dict]:
        with self.lock:
            return self.active_workers.copy()
    
    def stop_worker(self, worker_id: str) -> bool:
        with self.lock:
            if worker_id in self.worker_threads:
                thread = self.worker_threads[worker_id]
                if hasattr(thread, 'stop'):
                    thread.stop()
                    return True
        return False
