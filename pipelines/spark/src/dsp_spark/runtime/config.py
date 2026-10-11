"""One portable runtime and one centrally derived lakehouse path convention."""

import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from ..io.s3 import storage_uri

SPARK_VERSION = "4.0.2"
DELTA_VERSION = "4.0.1"
SCALA_BINARY_VERSION = "2.13"
HADOOP_VERSION = "3.4.1"


def default_base_uri() -> str:
    return (Path(__file__).resolve().parents[3] / ".local/lakehouse").as_uri()


def default_social_topic() -> str:
    from dsp_collector.runtime.config import KAFKA_SOCIAL_POST_TOPIC

    return KAFKA_SOCIAL_POST_TOPIC


@dataclass(frozen=True)
class LakehousePaths:
    base: str
    youtube_bronze: str = ""
    social_source_activity_daily: str = ""
    threads_bronze: str = ""
    pbplus_bronze: str = ""
    social_posts: str = ""
    courts: str = ""
    social_checkpoint: str = ""
    rejected_social: str = ""
    social_activity_daily: str = ""
    court_supply_by_location: str = ""
    unresolved_court_locations: str = ""
    location_market_signal_snapshot: str = ""
    bronze_court_observations: str = ""
    bronze_social_posts: str = ""
    bronze_social_comments: str = ""
    bronze_engagement_snapshots: str = ""
    bronze_collection_runs: str = ""
    bronze_source_changes: str = ""
    rejected_bronze: str = ""
    social_comments: str = ""

    def __post_init__(self):
        base = storage_uri(self.base)
        object.__setattr__(self, "base", base)
        for field, suffix in {
            "bronze_court_observations": "bronze/court_observations",
            "bronze_social_posts": "bronze/social_posts",
            "bronze_social_comments": "bronze/social_comments",
            "bronze_engagement_snapshots": "bronze/engagement_snapshots",
            "bronze_collection_runs": "bronze/collection_runs",
            "bronze_source_changes": "bronze/source_changes",
            "social_comments": "silver/social_comments",
            "rejected_bronze": "rejected/bronze",
            "youtube_bronze": "bronze/youtube",
            "social_source_activity_daily": "gold/social_source_activity_daily",
            "threads_bronze": "bronze/threads",
            "pbplus_bronze": "bronze/pbplus",
            "social_posts": "silver/social_posts",
            "courts": "silver/courts",
            "social_checkpoint": "checkpoints/social_posts_kafka_to_delta",
            "rejected_social": "rejected/social_posts",
            "social_activity_daily": "gold/social_activity_daily",
            "court_supply_by_location": "gold/court_supply_by_location",
            "unresolved_court_locations": "quality/unresolved_court_locations",
            "location_market_signal_snapshot": "gold/location_market_signal_snapshot",
        }.items():
            object.__setattr__(
                self, field, storage_uri(getattr(self, field) or f"{base}/{suffix}")
            )
        locations = [
            getattr(self, name) for name in self.__dataclass_fields__ if name != "base"
        ]
        if len(set(locations)) != len(locations):
            raise ValueError("table, rejection and checkpoint paths must be distinct")

    @classmethod
    def from_env(cls):
        return cls(
            base=os.getenv("DSP_LAKEHOUSE_BASE_URI", default_base_uri()),
            bronze_court_observations=os.getenv(
                "DSP_DELTA_BRONZE_COURT_OBSERVATIONS_PATH", ""
            ),
            bronze_social_posts=os.getenv("DSP_DELTA_BRONZE_SOCIAL_POSTS_PATH", ""),
            bronze_social_comments=os.getenv(
                "DSP_DELTA_BRONZE_SOCIAL_COMMENTS_PATH", ""
            ),
            bronze_engagement_snapshots=os.getenv(
                "DSP_DELTA_BRONZE_ENGAGEMENT_SNAPSHOTS_PATH", ""
            ),
            bronze_collection_runs=os.getenv(
                "DSP_DELTA_BRONZE_COLLECTION_RUNS_PATH", ""
            ),
            bronze_source_changes=os.getenv("DSP_DELTA_BRONZE_SOURCE_CHANGES_PATH", ""),
            rejected_bronze=os.getenv("DSP_DELTA_REJECTED_BRONZE_PATH", ""),
            social_comments=os.getenv("DSP_DELTA_SOCIAL_COMMENTS_PATH", ""),
            youtube_bronze=os.getenv("DSP_DELTA_YOUTUBE_BRONZE_PATH", ""),
            social_source_activity_daily=os.getenv(
                "DSP_DELTA_SOCIAL_SOURCE_ACTIVITY_DAILY_PATH", ""
            ),
            threads_bronze=os.getenv("DSP_DELTA_THREADS_BRONZE_PATH", ""),
            pbplus_bronze=os.getenv("DSP_DELTA_PBPLUS_BRONZE_PATH", ""),
            social_posts=os.getenv("DSP_DELTA_SOCIAL_POSTS_PATH", ""),
            courts=os.getenv("DSP_DELTA_COURTS_PATH", ""),
            social_checkpoint=os.getenv("DSP_SPARK_SOCIAL_CHECKPOINT_PATH", ""),
            rejected_social=os.getenv("DSP_DELTA_REJECTED_SOCIAL_PATH", ""),
            social_activity_daily=os.getenv("DSP_DELTA_SOCIAL_ACTIVITY_DAILY_PATH", ""),
            court_supply_by_location=os.getenv(
                "DSP_DELTA_COURT_SUPPLY_BY_LOCATION_PATH", ""
            ),
            unresolved_court_locations=os.getenv(
                "DSP_DELTA_UNRESOLVED_COURT_LOCATIONS_PATH", ""
            ),
            location_market_signal_snapshot=os.getenv(
                "DSP_DELTA_LOCATION_MARKET_SIGNAL_SNAPSHOT_PATH", ""
            ),
        )


@dataclass(frozen=True)
class MarketSignalSettings:
    window_days: int = 30
    end_date: str | None = None

    def __post_init__(self):
        if type(self.window_days) is not int or self.window_days <= 0:
            raise ValueError("DSP_MARKET_SIGNAL_WINDOW_DAYS must be a positive integer")
        if self.end_date is not None:
            try:
                if not isinstance(self.end_date, str) or not re.fullmatch(
                    r"[0-9]{4}-[0-9]{2}-[0-9]{2}", self.end_date
                ):
                    raise ValueError()
                date.fromisoformat(self.end_date)
            except ValueError:
                raise ValueError(
                    "DSP_MARKET_SIGNAL_END_DATE must be a valid YYYY-MM-DD date"
                ) from None

    @classmethod
    def from_env(cls):
        raw = os.getenv("DSP_MARKET_SIGNAL_WINDOW_DAYS", "30")
        if not re.fullmatch(r"[0-9]+", raw):
            raise ValueError("DSP_MARKET_SIGNAL_WINDOW_DAYS must be a positive integer")
        return cls(int(raw), os.getenv("DSP_MARKET_SIGNAL_END_DATE"))


@dataclass(frozen=True)
class SparkSettings:
    paths: LakehousePaths
    master: str = "local[2]"
    app_name: str = "dsp391-lakehouse"
    kafka_bootstrap_servers: str = "localhost:9092"
    social_post_topic: str = ""
    starting_offsets: str = "earliest"
    trigger_interval: str = "10 seconds"
    s3_endpoint: str | None = None
    s3_region: str | None = None
    s3_path_style: bool = False
    max_offsets_per_trigger: int = 1000

    def __post_init__(self):
        if (
            type(self.max_offsets_per_trigger) is not int
            or self.max_offsets_per_trigger <= 0
        ):
            raise ValueError("Kafka max offsets per trigger must be positive")
        if not self.social_post_topic:
            object.__setattr__(self, "social_post_topic", default_social_topic())
        if (
            not self.master
            or not self.app_name
            or not self.kafka_bootstrap_servers
            or not self.trigger_interval
        ):
            raise ValueError("Spark/Kafka settings must be nonempty")

    @classmethod
    def from_env(cls):
        return cls(
            paths=LakehousePaths.from_env(),
            master=os.getenv("DSP_SPARK_MASTER", "local[2]"),
            app_name=os.getenv("DSP_SPARK_APP_NAME", "dsp391-lakehouse"),
            kafka_bootstrap_servers=os.getenv(
                "DSP_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
            ),
            social_post_topic=os.getenv("DSP_KAFKA_SOCIAL_POST_TOPIC", ""),
            max_offsets_per_trigger=int(
                os.getenv("DSP_KAFKA_MAX_OFFSETS_PER_TRIGGER", "1000")
            ),
            starting_offsets=os.getenv("DSP_KAFKA_STARTING_OFFSETS", "earliest"),
            trigger_interval=os.getenv("DSP_SPARK_TRIGGER_INTERVAL", "10 seconds"),
            s3_endpoint=os.getenv("DSP_S3_ENDPOINT"),
            s3_region=os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"),
            s3_path_style=os.getenv("DSP_S3_PATH_STYLE", "false").lower() == "true",
        )
