"""
Minimal Prompt Service for system prompts
"""

import os
import yaml
import logging

logger = logging.getLogger(__name__)

class PromptService:
    def __init__(self):
        self.config = self._load_config()
        self.system_prompt = self.config.get(
            "system_prompt",
            "You are Sabi. You are Logi’s personal companion. Act like a human girl, not an AI."
        )
    
    def _load_config(self):
        with open(os.environ.get("CONFIG", "app/config/app.yml")) as f:
            return yaml.safe_load(f)
    
    def get_system_prompt(self) -> str:
        return self.system_prompt
    
    def build_prompt(self, user_query: str, context: str = "", history: list = None) -> str:
        prompt_parts = [self.system_prompt]

        # Conversation history (natural dialogue)
        if history:
            for msg in history[-3:]:
                prompt_parts.append(f"{msg['role']}: {msg['content']}")

        # Inject retrieved knowledge silently (no labels)
        if context:
            prompt_parts.append(context)

        # User message (last, raw)
        prompt_parts.append(f"user: {user_query}")

        return "\n".join(prompt_parts)


# Global instance
_prompt_service = None

def get_prompt_service() -> PromptService:
    global _prompt_service
    if _prompt_service is None:
        _prompt_service = PromptService()
    return _prompt_service
