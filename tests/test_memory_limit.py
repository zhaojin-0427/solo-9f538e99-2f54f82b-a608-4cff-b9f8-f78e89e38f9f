"""Hard memory-limit regression: the ticket's exact 256 MiB reproduction.

Runs in a fresh subprocess with ``RLIMIT_AS=256 MiB`` so an interpreter-wide
allocation blow-up (not just tracemalloc's view) fails the test the same way
the original incident did.  Also exercises the HTTP endpoint end to end,
including the summary-only ``include_items=false`` mode.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from fastapi.testclient import TestClient

from app.main import create_app

CHILD = r"""
import json, os, resource, sys
sys.path.insert(0, %(workspace)r)
resource.setrlimit(resource.RLIMIT_AS, (256 * 1024**2, 256 * 1024**2))
from app.db import Database
from app.engine import reconcile_report
from app.schemas import ReconcileRequest
from app.storage import get_report_items, import_readings, import_receipts

db = Database(%(db_path)r)
db.initialize()
T, M, S = "WT-1", "2026-01-01T10:00:00Z", "2026-01-01T11:00:00Z"
import_readings(db, [
    {"turbine_id": T, "sample_minute": M, "revision": rev,
     "ingested_at": f"2026-01-01T10:{rev %% 60:02d}:00Z",
     "wind_speed": 5.0 + rev}
    for rev in range(1, 401)
])
import_receipts(db, [
    {"receipt_id": f"R{i:05d}", "turbine_id": T, "score_minute": S,
     "used_sample_minute": M, "used_revision": 1}
    for i in range(4000)
])
summary = reconcile_report(db, ReconcileRequest(turbine_id=T), "rpt_big")
assert summary["receipt_count"] == 4000
page, total = get_report_items(db, "rpt_big", offset=3000, limit=50)
assert total == 4000 and len(page) == 50
assert len(page[0]["evidence"]["revisions"]) == 400
print("CHILD-OK")
"""


def test_reconcile_survives_256mib_address_space(tmp_path):
    db_path = str(tmp_path / "windscore.db")
    script = CHILD % {
        "workspace": str(Path(__file__).resolve().parents[1]),
        "db_path": db_path,
    }
    env = {
        "PATH": os.environ.get("PATH", ""),
        "PYTHONPATH": str(Path(__file__).resolve().parents[1]),
    }
    result = subprocess.run(
        [sys.executable, "-I", "-c", script],
        capture_output=True,
        text=True,
        env=env,
        timeout=300,
    )
    assert "CHILD-OK" in result.stdout, (
        f"stdout={result.stdout}\nstderr={result.stderr}"
    )
    assert result.returncode == 0, result.stderr


def _seed(client) -> None:
    readings = [
        {
            "turbine_id": "WT-1",
            "sample_minute": "2026-01-01T10:00:00Z",
            "revision": rev,
            "ingested_at": f"2026-01-01T10:{rev % 60:02d}:00Z",
            "wind_speed": 5.0 + rev,
        }
        for rev in range(1, 401)
    ]
    receipts = [
        {
            "receipt_id": f"R{i:05d}",
            "turbine_id": "WT-1",
            "score_minute": "2026-01-01T11:00:00Z",
            "used_sample_minute": "2026-01-01T10:00:00Z",
            "used_revision": 1,
        }
        for i in range(4_000)
    ]
    # Chunk the imports so the request bodies stay modest.
    for start in range(0, len(readings), 200):
        assert client.post("/api/readings", json=readings[start : start + 200]).status_code == 200
    for start in range(0, len(receipts), 500):
        assert client.post("/api/receipts", json=receipts[start : start + 500]).status_code == 200


def test_http_summary_only_and_paginated_detail(tmp_path):
    app = create_app(str(tmp_path / "windscore.db"))
    with TestClient(app) as client:
        _seed(client)

        summary_only = client.post(
            "/api/reconcile",
            json={"turbine_id": "WT-1"},
            params={"include_items": "false"},
        )
        assert summary_only.status_code == 200, summary_only.text
        body = summary_only.json()
        assert body["receipt_count"] == 4_000
        assert body["items"] == []
        assert body["item_total"] == 4_000
        report_id = body["report_id"]

        # Default inline page is capped, not the whole batch.
        inline = client.post(
            "/api/reconcile", json={"turbine_id": "WT-1"}
        ).json()
        assert len(inline["items"]) == 200
        assert len(inline["items"][0]["evidence"]["revisions"]) == 400

        # Paginated detail expands one page at a time.
        page = client.get(
            f"/api/reports/{report_id}",
            params={"offset": 3_990, "limit": 20},
        ).json()
        assert page["item_total"] == 4_000
        assert len(page["items"]) == 10
        assert page["items"][0]["receipt_id"] == "R03990"
        trail = page["items"][0]["evidence"]["revisions"]
        assert len(trail) == 400
        assert {"revision", "ingested_at", "wind_speed",
                "ingested_before_score", "eligible"} <= set(trail[0])

        # Receipt-filtered read still resolves its shared trail.
        one = client.get(
            f"/api/reports/{report_id}", params={"receipt_id": "R01234"}
        ).json()
        assert one["item_total"] == 1
        assert len(one["items"][0]["evidence"]["revisions"]) == 400

        # Listing stays summary-only.
        listing = client.get("/api/reports").json()
        assert listing["total"] == 2
        assert "items" not in listing["reports"][0]
