"""Source acquisition outcomes, separate from ingestion and normalization."""


def threads_outcome(envelope):
    discovery = envelope.get("discovery", {})
    failures = (
        len(envelope.get("errors", []))
        + sum(
            q.get("status") == "error" and q.get("failure_stage") != "detail"
            for q in envelope.get("query_runs", [])
        )
        + sum(p.get("detail_status") == "error" for p in envelope.get("posts", []))
    )
    unresolved = bool(discovery.get("terminal_failed_queries"))
    incomplete = (
        bool(
            discovery.get("has_more")
            or discovery.get("unsupported_continuation_queries")
        )
        or unresolved
    )
    if failures or unresolved:
        successful = any(
            q.get("status") == "ok" or q.get("pages")
            for q in envelope.get("query_runs", [])
        )
        status = (
            "partial"
            if envelope.get("posts") or successful or discovery.get("exhausted_queries")
            else "failed"
        )
    else:
        status = "partial" if incomplete else "success"
    return {
        "status": status,
        "technical_failures": failures,
        "survey_complete": not incomplete,
        "resumable": bool(discovery.get("has_more")),
        "unresolved_terminal_failures": discovery.get("terminal_failed_queries", 0),
        "stopping_reason": envelope.get("collection_stopping_reason"),
    }
