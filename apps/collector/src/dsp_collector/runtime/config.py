"""Minimal local collector artifact configuration."""

import os
import re
from dataclasses import dataclass
from pathlib import Path

KAFKA_SOCIAL_POST_TOPIC = "dsp391.social-posts.v1"


def kafka_enabled_from_env() -> bool:
    value = os.getenv("KAFKA_ENABLED", "false").strip().lower()
    if value not in ("true", "false", "1", "0"):
        raise ValueError("KAFKA_ENABLED must be true, false, 1 or 0")
    return value in ("true", "1")


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    enabled: bool = False
    bootstrap_servers: str = "localhost:9092"
    social_post_topic: str = KAFKA_SOCIAL_POST_TOPIC
    client_id: str = "dsp391-collector"
    delivery_timeout_ms: int = 30_000
    request_timeout_ms: int = 5_000
    retries: int = 10

    def __post_init__(self) -> None:
        if not self.bootstrap_servers.strip() or not self.client_id.strip():
            raise ValueError("Kafka bootstrap servers and client ID must be nonempty")
        if re.fullmatch(
            r"[A-Za-z0-9._-]{1,249}", self.social_post_topic
        ) is None or self.social_post_topic in (".", ".."):
            raise ValueError("invalid Kafka social post topic")
        if not 0 < self.request_timeout_ms <= self.delivery_timeout_ms:
            raise ValueError(
                "Kafka delivery timeout must be >= positive request timeout"
            )
        if self.retries < 1:
            raise ValueError("Kafka retries must be positive for idempotent production")

    @classmethod
    def from_env(cls) -> "KafkaSettings":
        return cls(
            enabled=kafka_enabled_from_env(),
            bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            social_post_topic=os.getenv(
                "KAFKA_SOCIAL_POST_TOPIC", KAFKA_SOCIAL_POST_TOPIC
            ),
            client_id=os.getenv("KAFKA_CLIENT_ID", "dsp391-collector"),
            delivery_timeout_ms=int(os.getenv("KAFKA_DELIVERY_TIMEOUT_MS", "30000")),
            request_timeout_ms=int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "5000")),
            retries=int(os.getenv("KAFKA_RETRIES", "10")),
        )


def default_artifact_root() -> Path:
    """Return apps/collector/.local, independently of the working directory."""
    return Path(__file__).resolve().parents[3] / ".local"
