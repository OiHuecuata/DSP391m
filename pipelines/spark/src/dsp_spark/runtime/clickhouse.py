"""HTTP serving connection; independent of Spark and remote lakehouse settings."""

import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ClickHouseSettings:
    host: str = "localhost"
    port: int = 8123
    database: str = "dsp391m"
    user: str = "dsp391m"
    password: str = ""
    timeout_seconds: float = 30

    def __post_init__(self):
        if not self.host or not 1 <= self.port <= 65535:
            raise ValueError("Invalid CLICKHOUSE_HOST or CLICKHOUSE_PORT")
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", self.database):
            raise ValueError("CLICKHOUSE_DATABASE must be a simple SQL identifier")
        if not self.user or self.timeout_seconds <= 0:
            raise ValueError("Invalid ClickHouse user or timeout")

    @classmethod
    def from_env(cls):
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
            database=os.getenv("CLICKHOUSE_DATABASE", "dsp391m"),
            user=os.getenv("CLICKHOUSE_USER", "dsp391m"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        )
