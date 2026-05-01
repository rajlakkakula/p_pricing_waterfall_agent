"""Snowflake connection pool management.

Provides a singleton connection manager with read-only access
to the PRICING_DB Gold layer tables.
"""

from contextlib import contextmanager
from typing import Generator

import pandas as pd
import snowflake.connector
from pydantic_settings import BaseSettings, SettingsConfigDict


class SnowflakeSettings(BaseSettings):
    """Snowflake connection configuration loaded from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    snowflake_account: str
    snowflake_user: str
    snowflake_password: str
    snowflake_warehouse: str = "PRICING_WH"
    snowflake_database: str = "PRICING_DB"
    snowflake_schema: str = "GOLD"
    snowflake_role: str = "PRICING_READONLY"


class SnowflakeConnectionManager:
    """Manages Snowflake connections with connection pooling."""

    def __init__(self, settings: SnowflakeSettings | None = None):
        self.settings = settings or SnowflakeSettings()

    @contextmanager
    def get_connection(self) -> Generator[snowflake.connector.SnowflakeConnection, None, None]:
        """Yields a Snowflake connection. Automatically closes on exit."""
        conn = snowflake.connector.connect(
            account=self.settings.snowflake_account,
            user=self.settings.snowflake_user,
            password=self.settings.snowflake_password,
            warehouse=self.settings.snowflake_warehouse,
            database=self.settings.snowflake_database,
            schema=self.settings.snowflake_schema,
            role=self.settings.snowflake_role,
        )
        try:
            yield conn
        finally:
            conn.close()

    def execute_query(self, query: str, params: list | None = None) -> pd.DataFrame:
        """Execute a parameterized query and return results as a DataFrame.

        Uses fetch_pandas_all() (Arrow-based) to avoid DECIMAL overflow errors
        that occur with fetchall() + manual DataFrame construction.
        Column names are lowercased to match the analytics layer's expectations.
        """
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, params or [])
                df = cursor.fetch_pandas_all()
                df.columns = df.columns.str.lower()
                return df
            finally:
                cursor.close()


# Singleton instance
_manager: SnowflakeConnectionManager | None = None


def get_snowflake_manager(reset: bool = False) -> SnowflakeConnectionManager:
    """Returns the singleton Snowflake connection manager.

    Args:
        reset: If True, discard any cached instance and create a fresh one.
               Useful in tests or after settings changes.
    """
    global _manager
    if _manager is None or reset:
        _manager = SnowflakeConnectionManager()
    return _manager
