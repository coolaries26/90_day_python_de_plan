#!/usr/bin/env python3
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from datetime import datetime

class ETLConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Pipeline settings
    PIPELINE_NAME: str = "customer_lifetime"
    TARGET_TABLE: str = "analytics_customer_lifetime_v2"
    OUTPUT_CSV: str = "customer_lifetime_v2.csv"

    # Data quality
    MIN_ROWS_EXPECTED: int = 500

    # Runtime
    LOAD_TIMESTAMP: str = Field(default_factory=lambda: datetime.now().isoformat())

settings = ETLConfig()