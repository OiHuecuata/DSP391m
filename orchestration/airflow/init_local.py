"""Prepare only local orchestration directories and generated development keys."""

import base64
import json
import os
import secrets
from pathlib import Path

from control import compose

ROOT = Path(__file__).resolve().parents[2]


def initialize():
    settings = json.loads(compose("config", "--format", "json"))["services"][
        "airflow-init"
    ]["environment"]
    username = settings["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS"].split(":")[0]
    local = ROOT / ".local/airflow"
    for name in ("config", "logs", "runs"):
        (local / name).mkdir(parents=True, exist_ok=True)
    (ROOT / "pipelines/spark/.local").mkdir(parents=True, exist_ok=True)
    values = {
        "jwt_secret": secrets.token_urlsafe(48),
        "fernet_key": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode(),
        "passwords.json": json.dumps(
            {
                username: os.getenv("AIRFLOW_LOCAL_PASSWORD")
                or settings.get("AIRFLOW_LOCAL_PASSWORD")
                or secrets.token_urlsafe(24)
            }
        ),
    }
    for name, value in values.items():
        path = local / "config" / name
        try:
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            continue  # Restarting never rotates authentication/signing keys.
        with os.fdopen(fd, "w") as output:
            output.write(value + "\n")
    print(
        f"Local Airflow configuration prepared in {local / 'config'}; existing keys preserved"
    )


if __name__ == "__main__":
    initialize()
