"""Project actual CLI write targets into the existing local ownership mechanism.

Remote S3 outputs are outside local ownership. They never justify bypassing a
guard on local outputs or accepting an unverifiable inherited ownership token.
"""

import sys
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit


def job_ownership(
    settings=None, *, outputs=(), warehouse=None, ownership=None, refresh_group=True
):
    local = {}
    if settings is not None:
        for name in outputs:
            value = getattr(settings.paths, name)
            if urlsplit(value).scheme == "s3a":
                continue
            local[name] = value  # Existing validator rejects nonlocal/invalid URIs.
    if not local and warehouse is None:
        if ownership is not None:
            raise ValueError(
                "A local ownership context cannot cover remote-only outputs"
            )
        return nullcontext()

    # Fully remote managed jobs need neither a checkout nor local OS locks.
    # Local wheels carry this exact module from the repository, not a second lock.
    root = next(
        (
            p
            for p in Path(__file__).resolve().parents
            if (p / "orchestration/local/analytical_refresh.py").is_file()
        ),
        None,
    )
    if root is not None and str(root) not in sys.path:
        sys.path.insert(0, str(root))
    from orchestration.local.analytical_refresh import analytical_ownership

    local_base = settings is not None and urlsplit(settings.paths.base).scheme == "file"
    base = settings.paths.base if local_base else Path("/tmp").as_uri()
    config = SimpleNamespace(
        spark=SimpleNamespace(paths=SimpleNamespace(base=base, **local)),
        warehouse=warehouse,
    )
    return analytical_ownership(
        config,
        ownership=ownership,
        include_group=refresh_group and bool(local) and local_base,
    )
