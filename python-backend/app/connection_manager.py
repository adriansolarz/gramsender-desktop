from fastapi import WebSocket
from typing import List
import json
import os
from datetime import datetime

# #region agent log
def _agent_log(location: str, message: str, data: dict, hypothesis_id: str = ""):
    try:
        p = os.path.normpath(os.path.join(os.path.dirname(__file__), "..", "..", ".cursor", "debug.log"))
        with open(p, "a", encoding="utf-8") as f:
            f.write(json.dumps({"location": location, "message": message, "data": data, "hypothesisId": hypothesis_id, "timestamp": datetime.now().isoformat(), "sessionId": "debug-session"}) + "\n")
    except Exception:
        pass
# #endregion

class ConnectionManager:
    """Manages WebSocket connections for real-time updates"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        """Broadcast message to all connected clients"""
        # #region agent log
        _agent_log("connection_manager.py:broadcast", "entry", {"n_connections": len(self.active_connections), "msg_type": message.get("type")}, "H2")
        # #endregion
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                disconnected.append(connection)
        
        # Remove disconnected connections
        for conn in disconnected:
            self.disconnect(conn)

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        """Send message to a specific client"""
        try:
            await websocket.send_json(message)
        except:
            self.disconnect(websocket)
