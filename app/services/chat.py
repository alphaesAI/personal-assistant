"""
Chat Service - Minimal WebSocket session handler
"""

import logging
from fastapi import WebSocket

from .llm_service import get_llm_service
from .retriever_service import get_retriever_service
from .memory_service import get_memory_service
from .prompt_service import get_prompt_service

logger = logging.getLogger(__name__)

class Chat:
    def __init__(self):
        self.llm_service = get_llm_service()
        self.retriever_service = get_retriever_service()
        self.memory_service = get_memory_service()
        self.prompt_service = get_prompt_service()

    async def session(self, ws: WebSocket):
        """WebSocket session handler"""
        await ws.accept()
        session_id = str(id(ws))
        logger.info(f"Session started: {session_id}")

        try:
            while True:
                message = await ws.receive_text()
                response = await self.handle_message(session_id, message)
                await ws.send_text(response)
        except Exception as e:
            logger.error(f"Session error: {e}")
            # Don't try to close if already closed
            pass

    async def handle_message(self, session_id: str, message: str) -> str:
        """Process message using LLM, retriever, memory and prompt"""
        try:
            # Store user message
            self.memory_service.add_message(session_id, "user", message)
            
            # Get context from retriever
            context = ""
            if self.retriever_service.is_available():
                logger.info(f"Searching for: {message}")
                results = self.retriever_service.search(message)
                logger.info(f"Retriever results: {len(results)} items found")
                if results:
                    context = "\n".join([r["text"] for r in results[:20]])
                    logger.info(f"Context retrieved: {context[:100]}...")
            else:
                logger.warning("Retriever service not available")
            
            # Get conversation history
            history = self.memory_service.get_history(session_id)
            
            # Build prompt
            prompt = self.prompt_service.build_prompt(message, context, history)
            logger.info(f"Final prompt length: {len(prompt)} chars")
            
            # Generate response
            response = self.llm_service.generate(prompt)
            
            # Store assistant response
            self.memory_service.add_message(session_id, "assistant", response)
            
            return response

        except Exception as e:
            logger.error(f"Error in handle_message: {e}")
            return f"Error: {str(e)}"
