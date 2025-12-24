"""
WebSocket Router - Only redirects to services
"""

import logging
from fastapi import APIRouter, WebSocket

logger = logging.getLogger(__name__)

router = APIRouter()

@router.websocket("/chat")
async def chat_ws(ws: WebSocket):
    """
    Router does NOTHING except forward.
    Exactly like txtai LLM router.
    """
    try:
        logger.info("WebSocket connection attempt received")
        from services.chat import Chat
        logger.info("Importing Chat service successful")
        chat = Chat()
        logger.info("Chat service initialized successfully")
        return await chat.session(ws)
    except Exception as e:
        logger.error(f"Failed to initialize chat service: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        await ws.close(code=1011, reason="Service unavailable")
