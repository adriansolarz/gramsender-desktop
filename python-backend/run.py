#!/usr/bin/env python3
"""
GramSender Desktop - Python Backend Runner
This script starts the FastAPI backend for the desktop app.
All configuration comes from environment variables set by Electron.
"""
import os
import sys

# Add the app directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def main():
    import uvicorn
    from app.main import app
    
    print("[GramSender] Starting backend server...")
    print(f"[GramSender] Storage mode: {os.getenv('STORAGE_MODE', 'json')}")
    print(f"[GramSender] User ID: {os.getenv('SUPABASE_USER_ID', 'not set')[:8]}...")
    
    # Run the FastAPI server
    uvicorn.run(
        app,
        host="127.0.0.1",
        port=8012,
        log_level="info"
    )

if __name__ == "__main__":
    main()
