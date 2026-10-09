"""Local full-refresh ownership using existing flock semantics.

The base group remains serialized. Shared Delta paths and serving databases
also have guards because path overrides can overlap across different bases.
A target-set hash is diagnostic identity, never the exclusion mechanism.
This is cooperative, single-host, same-user ownership, not distributed fencing.
"""

import fcntl
import hashlib
import json
import os
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlsplit


def _local_path(value):
    if not isinstance(value, (str, Path)):
        raise TypeError("Invalid local analytical target path")
    uri = urlsplit(str(value))
    if uri.scheme:
        if (
            uri.scheme != "file"
            or uri.netloc not in ("", "localhost")
            or uri.query
            or uri.fragment
            or not uri.path.startswith("/")
        ):
            raise ValueError("Local pipeline writer lock requires a local file URI")
        value = unquote(uri.path)
    if not str(value) or "\x00" in str(value):
        raise ValueError("Invalid local analytical target path")
    return Path(value).expanduser().resolve()


@dataclass(frozen=True)
class AnalyticalTargets:
    base: Path
    resources: tuple[str, ...]

    @property
    def identity(self):
        raw = json.dumps(self.resources, ensure_ascii=False, separators=(",", ":"))
        return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def canonical_targets(config):
    """Snapshot physical targets, excluding credentials and execution metadata."""
    paths = config.spark.paths
    base = _local_path(paths.base)
    resources = {"group:" + base.as_uri()}
    for name, value in vars(paths).items():
        if name != "base":
            resources.add("delta:" + _local_path(value).as_uri())
    warehouse = getattr(config, "warehouse", None)
    if warehouse is not None:
        if (
            not isinstance(warehouse.host, str)
            or not warehouse.host
            or type(warehouse.port) is not int
            or not 1 <= warehouse.port <= 65535
            or not isinstance(warehouse.database, str)
            or not warehouse.database
        ):
            raise ValueError("Invalid local analytical serving target")
        host = warehouse.host.casefold().rstrip(".")
        if host in ("localhost", "127.0.0.1"):
            host = "127.0.0.1"
        resources.add(
            "clickhouse:"
            + json.dumps(
                [host, warehouse.port, warehouse.database], separators=(",", ":")
            )
        )
    return AnalyticalTargets(base, tuple(sorted(resources)))


@dataclass(frozen=True, eq=False)
class AnalyticalOwnership:
    """Opaque identity token; a copied/constructed token grants no ownership."""

    target_identity: str


@dataclass
class _Lease:
    targets: AnalyticalTargets
    pid: int
    thread: int
    handles: list


_active = {}
_registry_lock = threading.RLock()


def _before_fork():
    _registry_lock.acquire()


def _after_parent_fork():
    _registry_lock.release()


def _after_fork():
    # Close inherited descriptors without LOCK_UN, which would unlock the
    # parent's shared open-file descriptions. Children cannot reuse its token.
    global _registry_lock
    for lease in _active.values():
        for _, handle in lease.handles:
            handle.close()
    _active.clear()
    _registry_lock = threading.RLock()


os.register_at_fork(
    before=_before_fork,
    after_in_parent=_after_parent_fork,
    after_in_child=_after_fork,
)


def _validate(ownership, targets):
    with _registry_lock:
        lease = (
            _active.get(ownership) if type(ownership) is AnalyticalOwnership else None
        )
        if (
            lease is None
            or lease.pid != os.getpid()
            or lease.thread != threading.get_ident()
        ):
            raise ValueError("Invalid or inactive analytical ownership context")
        if not set(targets.resources).issubset(lease.targets.resources):
            raise ValueError(
                "Ownership context does not cover requested analytical targets"
            )
        for path, handle in lease.handles:
            if handle.closed:
                raise ValueError("Invalid or inactive analytical ownership context")
            try:
                held, current = os.fstat(handle.fileno()), path.stat()
            except OSError:
                raise ValueError(
                    "Invalid or inactive analytical ownership context"
                ) from None
            if (held.st_dev, held.st_ino) != (current.st_dev, current.st_ino):
                raise ValueError("Analytical ownership lock file was replaced")


def _lock_paths(targets):
    # A fixed host-local registry avoids TMPDIR/cwd-dependent exclusion. Never
    # unlink lock files: replacing their inodes could create two lock owners.
    registry = Path("/tmp") / f"dsp391m-analytical-ownership-{os.getuid()}"
    registry.mkdir(mode=0o700, exist_ok=True)
    info = registry.lstat()
    if (
        not stat.S_ISDIR(info.st_mode)
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
    ):
        raise ValueError("Invalid local analytical lock registry")
    targets.base.mkdir(parents=True, exist_ok=True)
    group = "group:" + targets.base.as_uri()
    return [
        (
            targets.base / ".pipeline-writer.lock"
            if resource == group
            else registry
            / (hashlib.sha256(resource.encode("utf-8")).hexdigest() + ".lock")
        )
        for resource in targets.resources
    ]


@contextmanager
def analytical_ownership(config, *, ownership=None):
    """External calls fail fast on contention; internal calls reuse a live token.

    Reuse is explicit, thread/process-bound, and limited to already-held targets.
    The outer operation alone releases ownership, including after exceptions.
    """
    targets = canonical_targets(config)
    if ownership is not None:
        _validate(ownership, targets)
        yield ownership
        return
    handles = []
    token = AnalyticalOwnership(targets.identity)
    try:
        with _registry_lock:
            # Track partial acquisition too, so a concurrent fork cannot keep
            # failed-attempt descriptors alive between exception and cleanup.
            _active[token] = _Lease(
                targets, os.getpid(), threading.get_ident(), handles
            )
            for path in _lock_paths(targets):
                descriptor = os.open(
                    path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
                )
                handle = os.fdopen(descriptor, "a")
                handles.append((path, handle))
                try:
                    fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise RuntimeError(
                        "Another local pipeline writer is active"
                    ) from None
        yield token
    finally:
        with _registry_lock:
            _active.pop(token, None)
            for _, handle in reversed(handles):
                if not handle.closed:
                    handle.close()
