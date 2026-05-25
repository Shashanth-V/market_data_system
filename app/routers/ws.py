import asyncio
from typing import List, Optional
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query
from app.core.logger import logger
from app.processing.pipeline import broadcaster

router = APIRouter(prefix="/api/v1", tags=["websockets"])

class ConnectionManager:
    """
    Registry of active WebSocket connections.
    Handles thread-safe registration, disconnection, and cleanups.
    """
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New client connected. Total active connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"Client disconnected. Total active connections: {len(self.active_connections)}")

# Singleton connection manager
manager = ConnectionManager()

@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    symbol: Optional[str] = Query(None, description="Optional symbol filter, e.g. BTC-USD")
):
    """
    WebSocket endpoint streaming real-time market ticks and computed analytics.
    Support client-side symbol filtration using query parameter: `/api/v1/ws?symbol=BTC-USD`.
    """
    await manager.connect(websocket)

    # Define the queue/callback that will receive broadcaster events
    event_queue = asyncio.Queue()

    async def callback(data: dict):
        # Apply symbol filter if requested by the client
        if symbol:
            tick_symbol = data.get("tick", {}).get("symbol")
            if tick_symbol != symbol:
                return
        await event_queue.put(data)

    # Subscribe callback to the global broadcaster
    broadcaster.subscribe(callback)
    
    # Task to consume queue events and send them to the client
    async def send_loop():
        try:
            while True:
                data = await event_queue.get()
                await websocket.send_json(data)
                event_queue.task_done()
        except WebSocketDisconnect:
            pass
        except Exception as e:
            logger.error(f"Error in WebSocket send loop: {e}")

    send_task = asyncio.create_task(send_loop())

    try:
        # Keep connection open and listen for close signals or messages
        while True:
            # We discard client messages for now (uni-directional streaming backend),
            # but reading keeps the connection socket buffer drained and alive.
            _ = await websocket.receive_text()
    except WebSocketDisconnect:
        logger.info("Client triggered normal close sequence.")
    except Exception as e:
        logger.warning(f"WebSocket client connection broken: {e}")
    finally:
        # Unsubscribe broadcaster callback to prevent memory leak
        broadcaster.unsubscribe(callback)
        manager.disconnect(websocket)
        
        # Terminate async send task
        send_task.cancel()
        try:
            await send_task
        except asyncio.CancelledError:
            pass
