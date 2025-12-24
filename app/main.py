"""
FastAPI server for Sabi Conversational Assistant
"""

import logging
import os
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create static directory if it doesn't exist
static_dir = Path(__file__).parent / "static"
static_dir.mkdir(exist_ok=True)

try:
    from router.websocket_router import router
    logger.info("WebSocket router imported successfully")
except Exception as e:
    logger.error(f"Failed to import WebSocket router: {e}")
    import traceback
    logger.error(f"Traceback: {traceback.format_exc()}")
    router = None

app = FastAPI(title="Sabi Conversational Assistant")

# Mount static files
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Templates
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))

@app.get("/", response_class=HTMLResponse)
async def chat_ui(request: Request):
    """Serve the chat interface"""
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.get("/ws")
async def ws_info():
    return {"websocket_endpoint": "/ws/chat"}

# Include WebSocket router
if router:
    app.include_router(router, prefix="/ws")
    logger.info("WebSocket router included successfully")
else:
    logger.error("WebSocket router not available")

if __name__ == "__main__":
    logger.info("Starting server...")
    uvicorn.run("main:app", host="localhost", port=8001, reload=True)
