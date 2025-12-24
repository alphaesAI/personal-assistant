"""
Minimal Memory Service for chat context
"""

import os
import yaml
from typing import List, Dict

class MemoryService:
    def __init__(self):
        self.config = self._load_config()
        self.max_items = self.config.get("chat", {}).get("max_memory_items", 5)
        self.sessions: Dict[str, List[Dict]] = {}
    
    def _load_config(self):
        with open(os.environ.get("CONFIG", "app/config/app.yml")) as f:
            return yaml.safe_load(f)
    
    def add_message(self, session_id: str, role: str, content: str):
        if session_id not in self.sessions:
            self.sessions[session_id] = []
        
        self.sessions[session_id].append({
            "role": role,
            "content": content
        })
        
        # Keep only recent messages
        if len(self.sessions[session_id]) > self.max_items:
            self.sessions[session_id] = self.sessions[session_id][-self.max_items:]
    
    def get_history(self, session_id: str) -> List[Dict]:
        return self.sessions.get(session_id, [])

# Global instance
_memory_service = None

def get_memory_service() -> MemoryService:
    global _memory_service
    if _memory_service is None:
        _memory_service = MemoryService()
    return _memory_service
