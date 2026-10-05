"""Minimal local collector artifact configuration."""

from pathlib import Path


def default_artifact_root() -> Path:
    """Return apps/collector/.local, independently of the working directory."""
    return Path(__file__).resolve().parents[3] / ".local"
