import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.core.database import init_db
from app.core.logger import logger
from app.core.queue import get_market_queue
from app.ingestion.collector import MarketDataCollector
from app.processing.pipeline import MarketDataPipeline
from app.routers import market, ws

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup and shutdown lifecycles.
    Spawns background tasks for ingestion and processing.
    """
    logger.info("Initializing system startup sequences...")
    
    # 1. Initialize Postgres Tables
    await init_db()
    
    # 2. Initialize Queue Backplane
    queue = await get_market_queue()
    
    # 3. Create Ingestion Collector and start execution loop
    collector = MarketDataCollector(queue)
    collector_task = asyncio.create_task(collector.run())
    
    # 4. Create Processing Pipeline and start ingestion consumer
    pipeline = MarketDataPipeline(queue)
    await pipeline.start()
    
    # Cache active lifecycles on app state
    app.state.queue = queue
    app.state.collector = collector
    app.state.collector_task = collector_task
    app.state.pipeline = pipeline

    logger.info("Startup sequences successfully completed. System is active.")
    
    yield
    
    logger.info("Initiating system shutdown sequences...")
    
    # 1. Stop background processing loops
    await pipeline.stop()
    collector.stop()
    
    # 2. Cancel and await collector task
    collector_task.cancel()
    try:
        await collector_task
    except asyncio.CancelledError:
        pass
        
    # 3. Close open HTTP sessions
    await collector.client.aclose()
    
    logger.info("Shutdown sequences completed. System offline.")

from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(
    title="Real-Time Market Data Processing System",
    description="Production-grade, decoupled real-time streaming, aggregation, and analytics engine.",
    version="1.0.0",
    lifespan=lifespan
)

# Enable CORS for Streamlit and cross-origin clients
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount APIRouters
app.include_router(market.router)
app.include_router(ws.router)

# Mount static folder
app.mount("/static", StaticFiles(directory="app/static"), name="static")

@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serves the premium custom HTML5/CSS3/JS real-time dashboard directly."""
    import os
    file_path = os.path.join("app", "static", "index.html")
    if not os.path.exists(file_path):
        file_path = os.path.join(os.path.dirname(__file__), "static", "index.html")
    with open(file_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content)
