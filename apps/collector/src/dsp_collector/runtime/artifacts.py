"""Atomic local JSON artifact writes shared by collector jobs."""

from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any


def write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    """Commit a UTF-8 JSON file with no partially written destination.

    Existing artifacts are rejected. Run IDs must be unique between concurrent
    processes; this local single-run boundary does not implement writer locking.
    """
    if "bronze" in path.parts:
        from .bronze import apply_source_policy, check_secrets, load_policies

        check_secrets(payload)
        policy = load_policies(os.getenv("DSP_SOURCE_POLICY_REGISTRY")).get(
            payload.get("source"), {}
        )
        if policy:
            apply_source_policy(
                {
                    "source_platform": payload.get("source"),
                    "collected_at": payload.get("collected_at"),
                },
                {payload.get("source"): policy},
            )
    serialized = (
        json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        raise FileExistsError(f"artifact already exists: {path}")
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as file:
            file.write(serialized)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
