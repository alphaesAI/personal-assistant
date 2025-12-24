"""
LLM Service using txtai LLM pipeline
"""

import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)

class LLMService:
    """Service for LLM operations using txtai pipeline"""
    
    def __init__(self):
        self.llm_instance = None
        self._initialize_llm()
    
    def _initialize_llm(self):
        """Initialize LLM from existing txtai pipeline"""
        try:
            # Import txtai LLM pipeline
            from txtai.pipeline.llm import LLM
            
            # Load config from environment
            config_path = os.environ.get("CONFIG")
            if not config_path:
                logger.warning("CONFIG environment variable not set")
                return
            
            # Read config using existing Application
            import sys
            # Add txtai submodule path to Python path
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            txtai_path = os.path.join(project_root, 'txtai', 'src', 'python')
            if txtai_path not in sys.path:
                sys.path.insert(0, txtai_path)
            from txtai.api.application import Application
            
            config = Application.read(config_path)
            llm_config = config.get("llm", {})
            
            if not llm_config:
                logger.warning("No LLM configuration found")
                return
            
            # Create LLM instance
            path = llm_config.get("path", "gpt-3.5-turbo")
            method = llm_config.get("method", "litellm")
            temperature = llm_config.get("temperature", 0.7)
            
            self.llm_instance = LLM(path=path, method=method, temperature=temperature)
            logger.info(f"LLM initialized: {path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize LLM: {e}")
    
    def generate(self, query: str, **kwargs) -> str:
        """Generate response using LLM"""
        if not self.llm_instance:
            return "LLM service not available"
        
        try:
            return self.llm_instance(query, defaultrole="user", **kwargs)
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return f"Error: {str(e)}"
    
    def is_available(self) -> bool:
        """Check if LLM is available"""
        return self.llm_instance is not None

# Global instance
_llm_service = None

def get_llm_service() -> LLMService:
    """Get global LLM service instance"""
    global _llm_service
    if _llm_service is None:
        _llm_service = LLMService()
    return _llm_service
