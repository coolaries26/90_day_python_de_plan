#!/usr/bin/env python3
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, SecretStr
from pathlib import Path

class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    JIRA_URL: str
    JIRA_USERNAME: str
    JIRA_API_TOKEN: SecretStr
    JIRA_PROJECT_KEY: str = "EP01"

    APP_ENV: str = "development"
    LOG_LEVEL: str = "INFO"

    @property
    def jira_auth(self):
        return (self.JIRA_USERNAME, self.JIRA_API_TOKEN.get_secret_value())

settings = AppSettings()