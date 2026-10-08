"""Bounded live acceptance through real frontend buttons and backend scheduler.

Start backend with an isolated COLLECTION_CONFIG first. This never calls the
collector/processing worker directly. It closes Chromium after Start and opens
a new browser for Stop; all collection responsibility remains server-side.
"""

import json
import os
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from playwright.sync_api import sync_playwright


def main():
    frontend = os.getenv("DATA_OPS_FRONTEND", "http://127.0.0.1:3100")
    backend = os.getenv("DATA_OPS_BACKEND", "http://127.0.0.1:8080")
    output = Path(os.getenv("DATA_OPS_ACCEPTANCE_DIR", ".local/data-ops-acceptance"))
    output.mkdir(parents=True, exist_ok=True)
    evidence = {
        "started_at": datetime.now(UTC).isoformat(),
        "events": [],
        "status": "FAILED",
    }
    with (
        httpx.Client(base_url=backend, timeout=20) as client,
        sync_playwright() as playwright,
    ):

        def state():
            response = client.get("/api/v1/collection/status")
            response.raise_for_status()
            return response.json()

        initial = state()
        if initial["continuous"] or initial.get("active_run_id"):
            raise RuntimeError("Stop existing collection before bounded acceptance")
        previous = {r["run_id"] for r in initial["runs"]}
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(frontend + "/data-operations")
        page.get_by_role("heading", name="Data Lake", exact=True).wait_for()
        page.get_by_label("Enable Threads", exact=True).check()
        page.get_by_label("Enable YouTube", exact=True).check()
        page.get_by_label("Enable PBPlus", exact=True).uncheck()
        page.get_by_label("Threads interval seconds", exact=True).fill("10")
        page.get_by_label("YouTube interval seconds", exact=True).fill("10")
        t = time.monotonic()
        with page.expect_response(
            lambda r: r.url.endswith("/api/v1/collection/start")
            and r.request.method == "POST"
        ) as response:
            page.get_by_role(
                "button", name="Start continuous collection", exact=True
            ).click()
        if response.value.status != 202:
            raise RuntimeError("Frontend Start rejected")
        evidence["start_response_seconds"] = time.monotonic() - t
        evidence["events"].append({"event": "Start", "state": state()})
        browser.close()
        evidence["browser_closed_after_start"] = True
        deadline = time.monotonic() + 1800
        last_active = None
        stopped = False
        try:
            while time.monotonic() < deadline:
                current = state()
                runs = [r for r in current["runs"] if r["run_id"] not in previous]
                counts = {
                    source: sum(r["source"] == source for r in runs)
                    for source in ("threads", "youtube")
                }
                if current.get("active_run_id") != last_active:
                    last_active = current.get("active_run_id")
                    evidence["events"].append(
                        {
                            "event": "cycle_change",
                            "at": datetime.now(UTC).isoformat(),
                            "state": current,
                        }
                    )
                    print(
                        json.dumps(
                            {
                                "cycles": counts,
                                "active": last_active,
                                "status": current["status"],
                            }
                        ),
                        flush=True,
                    )
                if all(n >= 2 for n in counts.values()) and not stopped:
                    browser = playwright.chromium.launch(headless=True)
                    page = browser.new_page()
                    page.goto(frontend + "/data-operations")
                    with page.expect_response(
                        lambda r: r.url.endswith("/api/v1/collection/stop")
                    ) as response:
                        page.get_by_role(
                            "button", name="Stop after current run", exact=True
                        ).click()
                    if response.value.status != 202:
                        raise RuntimeError("Frontend Stop rejected")
                    evidence["events"].append({"event": "Stop", "state": state()})
                    browser.close()
                    stopped = True
                if stopped and not current.get("active_run_id"):
                    break
                if runs and any(r["status"] == "FAILED" for r in runs):
                    raise RuntimeError(
                        "Live source cycle failed; inspect structured summaries"
                    )
                time.sleep(1)
            else:
                raise TimeoutError("Bounded acceptance timed out")
            final = state()
            runs = [r for r in final["runs"] if r["run_id"] not in previous]
            if len(runs) != 4 or any(
                r["status"] not in ("SUCCESS", "PARTIAL") for r in runs
            ):
                raise RuntimeError("Expected exactly two completed cycles per source")
            time.sleep(12)
            if len([r for r in state()["runs"] if r["run_id"] not in previous]) != 4:
                raise RuntimeError("Cycle 3 was scheduled after Stop")
            evidence.update(status="VERIFIED", runs=runs, cycle_3_prevented=True)
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(frontend + "/data-operations")
            page.get_by_role("heading", name="Data Lake", exact=True).wait_for()
            if page.get_by_role("main").get_by_role("alert").count():
                raise RuntimeError("Dashboard displayed a backend error")
            page.screenshot(path=str(output / "dashboard-stopped.png"), full_page=True)
            evidence["frontend_stopped_rendered"] = True
            browser.close()
        finally:
            # Always stop future scheduling, including when verification fails.
            client.post("/api/v1/collection/stop", json={}).raise_for_status()
            client.patch(
                "/api/v1/collection/config",
                json={
                    "sources": {
                        "threads": {"enabled": True, "interval_seconds": 43200},
                        "youtube": {"enabled": True, "interval_seconds": 21600},
                        "pbplus": {"enabled": False, "interval_seconds": 86400},
                    }
                },
            ).raise_for_status()
            evidence["final_collection"] = state()
            evidence["final_operations"] = client.get("/api/v1/data-ops/status").json()
            evidence["completed_at"] = datetime.now(UTC).isoformat()
            (output / "acceptance.json").write_text(
                json.dumps(evidence, indent=2) + "\n"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
