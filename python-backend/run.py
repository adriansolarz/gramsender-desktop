#!/usr/bin/env python3
"""
GramSender Desktop - Python Backend Runner
This script starts the FastAPI backend for the desktop app.
All configuration comes from environment variables set by Electron.
"""
import os
import sys

# Handle PyInstaller frozen state
if getattr(sys, 'frozen', False):
    # Running as PyInstaller bundle
    base_dir = os.path.dirname(sys.executable)
    # Also set the internal temp dir for module imports
    if hasattr(sys, '_MEIPASS'):
        base_dir = sys._MEIPASS
    sys.path.insert(0, base_dir)
else:
    # Running as normal Python script
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    import uvicorn
    from app.main import app
    
    print("[GramSender] Starting backend server...", flush=True)
    print(f"[GramSender] Storage mode: {os.getenv('STORAGE_MODE', 'json')}", flush=True)
    print(f"[GramSender] User ID: {os.getenv('SUPABASE_USER_ID', 'not set')[:8]}...", flush=True)
    print(f"[GramSender] Frozen: {getattr(sys, 'frozen', False)}", flush=True)
    
    # Run the FastAPI server
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8012,
        log_level="info"
    )

if __name__ == "__main__":
    main()
