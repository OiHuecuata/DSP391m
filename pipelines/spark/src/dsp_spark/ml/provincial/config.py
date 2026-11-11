"""Benchmark protocol. All time boundaries are explicit and reproducible."""

import math
from dataclasses import asdict, dataclass
from datetime import date, timedelta

CHANNELS = (
    "target",
    "geo_linked_posts",
    "seek_court",
    "seek_players",
    "price_inquiry",
    "commercial_geo_posts",
)
DIRECT_INTENTS = ("SEEK_COURT", "SEEK_PLAYERS", "PRICE_INQUIRY")
TABLES = (
    "silver_social_posts",
    "silver_social_geo_mentions",
    "silver_social_intent_evidence",
    "dim_admin_unit",
)
ARTIFACT_VERSION = "provincial-ridge-v1"


class InsufficientHistory(ValueError):
    """Data does not meet the declared benchmark, never a successful model run."""


@dataclass(frozen=True)
class ForecastConfig:
    as_of_date: str
    observed_through: str
    panel_start: str | None = None
    admin_version: str = "VN_ADMIN_POST_2025"
    min_panel_weeks: int = 48
    min_geo_posts_history: int = 5
    min_direct_posts_history: int = 2
    min_provinces: int = 3
    lookback: int = 8
    horizon: int = 1
    n_folds: int = 3
    val_weeks: int = 12
    alpha: float = 10.0
    seed: int = 42
    max_iter: int = 1000
    tol: float = 1e-6
    max_provinces: int = 256
    max_panel_weeks: int = 10400

    def __post_init__(self):
        for name in ("as_of_date", "observed_through", "panel_start"):
            value = getattr(self, name)
            if value is not None:
                parsed = date.fromisoformat(value)
                if parsed.isoformat() != value:
                    raise ValueError(f"{name} must be YYYY-MM-DD")
        if self.panel_start and date.fromisoformat(self.panel_start).weekday() != 0:
            raise ValueError("panel_start must be a Monday")
        if self.lookback != 8 or self.horizon != 1:
            raise ValueError("this benchmark requires LOOKBACK=8 and HORIZON=1")
        for name in (
            "min_panel_weeks",
            "min_provinces",
            "n_folds",
            "val_weeks",
            "max_iter",
            "max_provinces",
            "max_panel_weeks",
        ):
            if type(getattr(self, name)) is not int or getattr(self, name) < 1:
                raise ValueError(f"{name} must be a positive integer")
        if self.min_panel_weeks < 48 or self.min_provinces < 3:
            raise ValueError("benchmark requires >=48 panel weeks and >=3 provinces")
        if (
            min(self.min_geo_posts_history, self.min_direct_posts_history) < 0
            or not self.admin_version
        ):
            raise ValueError("invalid eligibility/geography configuration")
        if (
            not math.isfinite(self.alpha)
            or self.alpha <= 0
            or not math.isfinite(self.tol)
            or self.tol <= 0
        ):
            raise ValueError("alpha and tol must be positive and finite")
        if 48 + self.max_provinces > 4096:
            raise ValueError(
                "feature bound must keep Spark auto on its normal-equation path"
            )

    @property
    def last_complete_week(self):
        # as_of_date is exclusive at VN midnight; observed_through is inclusive.
        end = min(
            date.fromisoformat(self.observed_through),
            date.fromisoformat(self.as_of_date) - timedelta(days=1),
        )
        sunday = end - timedelta(days=(end.weekday() + 1) % 7)
        return sunday - timedelta(days=6)

    def to_dict(self):
        return asdict(self)


def fold_plan(total_weeks, config):
    if total_weeks < config.min_panel_weeks:
        raise InsufficientHistory(
            f"panel has {total_weeks} completed weeks; requires {config.min_panel_weeks}"
        )
    first_test = max(config.lookback + config.val_weeks + 20, int(total_weeks * 0.75))
    remaining = total_weeks - first_test
    if remaining < config.n_folds:
        raise InsufficientHistory(
            "not enough test weeks for consecutive nonempty folds"
        )
    q, r = divmod(remaining, config.n_folds)
    folds, start = [], first_test
    for i in range(config.n_folds):
        end = start + q + int(i < r)
        folds.append(
            {
                "fold": i + 1,
                "train_start_index": config.lookback,
                "train_end_exclusive": start - config.val_weeks,
                "validation_start_index": start - config.val_weeks,
                "validation_end_exclusive": start,
                "test_start_index": start,
                "test_end_exclusive": end,
            }
        )
        start = end
    return folds
