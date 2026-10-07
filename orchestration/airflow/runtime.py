"""Run-scoped orchestration configuration and immutable artifact handoff."""

import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class RunConfig:
    root: Path
    run_id: str
    mode: str
    replay: Path | None
    geography: Path | None

    @property
    def directory(self):
        # Airflow IDs contain punctuation; this is only a filesystem namespace,
        # never a new source observation identity.
        key = hashlib.sha256(self.run_id.encode()).hexdigest()
        return self.root / ".local/airflow/runs" / key

    @classmethod
    def from_env(cls, root=ROOT):
        conf = json.loads(os.getenv("DSP_AIRFLOW_RUN_CONF", "{}"))
        allowed = {"pbplus_mode", "replay_path", "geography_path"}
        if not isinstance(conf, dict) or set(conf) - allowed:
            raise ValueError(f"DAG run configuration permits only {sorted(allowed)}")
        if any(not isinstance(v, str) for v in conf.values()):
            raise ValueError("DAG run configuration values must be strings")
        run_id = os.getenv("DSP_AIRFLOW_RUN_ID", "")
        if not run_id:
            raise ValueError("DSP_AIRFLOW_RUN_ID is required")
        mode = conf.get("pbplus_mode", os.getenv("DSP_AIRFLOW_PBPLUS_MODE", "replay"))
        if mode not in ("replay", "live"):
            raise ValueError("PBPlus mode must be replay or live")

        def path(key, variable):
            value = conf.get(key, os.getenv(variable, ""))
            if not value:
                return None
            supplied = Path(value).expanduser()
            return (
                (root / supplied).resolve()
                if not supplied.is_absolute()
                else supplied.resolve()
            )

        replay = path("replay_path", "DSP_AIRFLOW_PBPLUS_REPLAY")
        geography = path("geography_path", "DSP_AIRFLOW_PBPLUS_GEOGRAPHY")
        if mode == "replay" and replay is None:
            raise ValueError("Replay requires replay_path or DSP_AIRFLOW_PBPLUS_REPLAY")
        if mode == "live" and (replay is not None or geography is not None):
            raise ValueError("Live collection cannot use replay/geography artifacts")
        return cls(root, run_id, mode, replay, geography)

    def input_identity(self):
        return {
            "mode": self.mode,
            "inputs": {
                str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (self.replay, self.geography)
                if path is not None
            },
        }


def write_report(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".report-", dir=path.parent)
    temporary = Path(name)
    try:
        with os.fdopen(fd, "w") as output:
            json.dump(payload, output, ensure_ascii=False, allow_nan=False, indent=2)
            output.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_collection(config):
    path = config.directory / "collection.json"
    manifest = json.loads(path.read_text())
    if manifest["inputs"] != config.input_identity():
        raise ValueError("Collection inputs changed within this DAG run; use a new run")
    for name in ("bronze_path", "silver_path"):
        artifact = Path(manifest["result"][name]).resolve()
        if not artifact.is_relative_to(config.directory):
            raise ValueError("Collection artifact escaped its run directory")
        if (
            hashlib.sha256(artifact.read_bytes()).hexdigest()
            != manifest["hashes"][name]
        ):
            raise ValueError(f"Collection artifact changed: {artifact}")
    return manifest["result"]
