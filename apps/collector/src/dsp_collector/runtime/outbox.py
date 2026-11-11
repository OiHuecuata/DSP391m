"""Durable, opt-in collection outbox. No cloud credentials or Spark required."""

import fcntl
import hashlib
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

from .bronze import check_secrets, decode, encode


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".pending-")
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(encode(value) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)


@contextmanager
def queue_lock(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    with (root / "outbox.lock").open("a") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        yield


def enqueue(artifact, root):
    """Copy the immutable envelope before acknowledging durable pending state."""
    payload = decode(Path(artifact).read_text())
    check_secrets(payload)
    if payload.get("source") not in ("pbplus", "threads", "youtube"):
        raise ValueError("unsupported outbox source")
    if not payload.get("run_id") or not payload.get("collected_at"):
        raise ValueError("outbox requires stable run and collection identity")
    identity = hashlib.sha256(
        encode([payload["source"], payload["run_id"]]).encode()
    ).hexdigest()
    checksum = hashlib.sha256(encode(payload).encode()).hexdigest()
    root = Path(root)
    with queue_lock(root):
        state_path = root / "jobs" / f"{identity}.json"
        envelope_path = root / "envelopes" / f"{identity}.json"
        if state_path.exists():
            state = decode(state_path.read_text())
            if state["payload_sha256"] != checksum:
                raise ValueError("conflicting outbox payload for existing source/run")
            return state
        if envelope_path.exists() and decode(envelope_path.read_text()) != payload:
            raise ValueError("conflicting saved outbox envelope")
        atomic_json(envelope_path, payload)
        state = {
            "id": identity,
            "source": payload["source"],
            "run_id": payload["run_id"],
            "collected_at": payload["collected_at"],
            "payload_sha256": checksum,
            "artifact": str(envelope_path.resolve()),
            "status": "pending",
            "attempts": 0,
            "retry_at": 0,
            "notification_status": "pending",
        }
        atomic_json(state_path, state)
        return state


def jobs(root):
    for path in sorted((Path(root) / "jobs").glob("*.json")):
        yield path, decode(path.read_text())


def drain(root, commit, *, notify=None, now=None):
    """One local drain owner. Commit retry and notification retry are independent.

    A crash after remote commit but before state fsync replays the same envelope.
    The remote writer must verify its idempotent merge before returning a receipt.
    Failed entries remain durable and retry with bounded exponential backoff.
    """
    results = []
    now = time.time() if now is None else now
    with queue_lock(root):
        for path, state in jobs(root):
            if state["status"] != "committed" and state["retry_at"] <= now:
                state["attempts"] += 1
                atomic_json(path, state)
                try:
                    payload = decode(Path(state["artifact"]).read_text())
                    if (
                        hashlib.sha256(encode(payload).encode()).hexdigest()
                        != state["payload_sha256"]
                    ):
                        raise ValueError("outbox envelope changed")
                    receipt = commit(state)
                    if receipt.get("status") != "committed":
                        raise ValueError("writer did not confirm Bronze commit")
                    state.update(
                        status="committed", receipt=receipt, error_type=None, retry_at=0
                    )
                except Exception as exc:  # noqa: BLE001
                    state.update(
                        status="failed",
                        error_type=type(exc).__name__,
                        retry_at=now + min(3600, 2 ** min(state["attempts"], 12)),
                    )
                atomic_json(path, state)
            if (
                state["status"] == "committed"
                and notify
                and state["notification_status"] != "sent"
            ):
                try:
                    notify(state)
                    state["notification_status"] = "sent"
                except Exception as exc:  # noqa: BLE001
                    state["notification_error_type"] = type(exc).__name__
                atomic_json(path, state)
            results.append(
                {
                    "id": state["id"],
                    "status": state["status"],
                    "notification_status": state["notification_status"],
                }
            )
    return results
