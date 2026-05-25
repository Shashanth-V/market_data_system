from typing import AsyncGenerator, Generator
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlmodel import SQLModel, Session
from app.config import settings
from app.core.logger import logger

# Asynchronous setup
async_engine = create_async_engine(
    settings.async_database_url,
    echo=False,
    pool_pre_ping=True,
    pool_size=20,
    max_overflow=10
)

async_session_maker = sessionmaker(
    async_engine,
    class_=AsyncSession,
    expire_on_commit=False
)

# Synchronous setup (primarily for table creation & test sync sessions)
sync_engine = create_engine(
    settings.sync_database_url,
    echo=False,
    pool_pre_ping=True
)

sync_session_maker = sessionmaker(
    sync_engine,
    class_=Session,
    expire_on_commit=False
)

async def init_db() -> None:
    """
    Initializes the database schema and creates all tables if they do not exist.
    """
    logger.info("Initializing database schemas...")
    try:
        # Run table creation synchronously
        SQLModel.metadata.create_all(sync_engine)
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        # We don't crash here so that in-memory fallbacks or tests can proceed,
        # but in production, this is a critical log.

async def get_async_session() -> AsyncGenerator[AsyncSession, None]:
    """
    Asynchronous database session generator dependency.
    """
    async with async_session_maker() as session:
        try:
            yield session
        finally:
            await session.close()

def get_sync_session() -> Generator[Session, None, None]:
    """
    Synchronous database session generator dependency.
    """
    with sync_session_maker() as session:
        try:
            yield session
        finally:
            session.close()
