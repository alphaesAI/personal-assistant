# pipeline/extractors/state_manager.py
import json
from pathlib import Path
from typing import Optional


class StateManager:
    def __init__(self, path: str = ".extractor_state.json"):
        self.path = Path(path)
        self.state = self._load()

    def _load(self):
        if self.path.exists():
            return json.loads(self.path.read_text())
        return {}

    def get(self, key: str) -> Optional[str]:
        return self.state.get(key)

    def set(self, key: str, value: str):
        self.state[key] = value
        self.path.write_text(json.dumps(self.state, indent=2))
