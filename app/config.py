import os
from typing import List
from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )

    # General configuration
    ENVIRONMENT: str = Field(default="development", env="ENVIRONMENT")
    LOG_LEVEL: str = Field(default="INFO", env="LOG_LEVEL")

    # Database configuration override (e.g. for SQLite local fallback)
    DATABASE_URL: str = Field(default="", env="DATABASE_URL")
    DATABASE_URL_SYNC: str = Field(default="", env="DATABASE_URL_SYNC")

    # Database configuration
    POSTGRES_USER: str = Field(default="postgres", env="POSTGRES_USER")
    POSTGRES_PASSWORD: str = Field(default="postgres", env="POSTGRES_PASSWORD")
    POSTGRES_DB: str = Field(default="market_data", env="POSTGRES_DB")
    POSTGRES_HOST: str = Field(default="localhost", env="POSTGRES_HOST")
    POSTGRES_PORT: int = Field(default=5432, env="POSTGRES_PORT")

    # Redis configuration
    REDIS_HOST: str = Field(default="localhost", env="REDIS_HOST")
    REDIS_PORT: int = Field(default=6379, env="REDIS_PORT")
    REDIS_USE_FALLBACK: bool = Field(default=True, env="REDIS_USE_FALLBACK")

    # Market Data ingestion settings
    API_POLL_INTERVAL_SECONDS: float = Field(default=2.0, env="API_POLL_INTERVAL_SECONDS")
    DEFAULT_SYMBOLS: str = Field(
        default="BTC-USD,ETH-USD,SOL-USD,ADA-USD,DOGE-USD",
        env="DEFAULT_SYMBOLS"
    )

    @computed_field
    @property
    def symbols_list(self) -> List[str]:
        """Parses the comma-separated list of default trading symbols."""
        return [sym.strip() for sym in self.DEFAULT_SYMBOLS.split(",") if sym.strip()]

    @computed_field
    @property
    def async_database_url(self) -> str:
        """Asynchronous database connection URL using asyncpg or override."""
        if self.DATABASE_URL:
            return self.DATABASE_URL
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @computed_field
    @property
    def sync_database_url(self) -> str:
        """Synchronous database connection URL using psycopg2 or override."""
        if self.DATABASE_URL_SYNC:
            return self.DATABASE_URL_SYNC
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

# Singleton settings instance
settings = Settings()
