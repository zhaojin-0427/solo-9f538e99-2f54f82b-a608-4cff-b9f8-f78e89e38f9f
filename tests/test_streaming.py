"""Regression tests for the shared-trail, streaming reconciliation design.

Covers the ticket requirements:

* summary-only reconciliation of 4,000 receipts sharing one 400-revision
  minute stays within a small memory budget (the old design needed ~300 MiB
  of trail dicts alone and died under a 256 MiB cap);
* storage holds the revision trail once per distinct pair, not per receipt;
* paginated report reads expand only the requested page and derive the
  per-receipt ingestion/eligibility flags from each receipt's own cutoff;
* back-filling or overwriting the same revision number never changes an
  already stored report;
* a database created by the pre-change schema opens unchanged and its old
  reports stay fully readable.
"""

from __future__ import annotations

import json
import sqlite3
import tracemalloc

import pytest

from app.db import Database
from app.engine import CHUNK_SIZE, reconcile_report, reconcile_snapshot
from app.schemas import ReconcileRequest
from app.storage import (
    get_report_items,
    get_report_row,
    import_readings,
    import_receipts,
    persist_report,
)

TURBINE = "WT-1"
SAMPLE_MINUTE = "2026-01-01T10:00:00Z"
SCORE_MINUTE = "2026-01-01T11:00:00Z"


def _reading(rev: int, *, wind: float | None = None) -> dict:
    return {
        "turbine_id": TURBINE,
        "sample_minute": SAMPLE_MINUTE,
        "revision": rev,
        "ingested_at": f"2026-01-01T10:{rev % 60:02d}:00Z",
        "wind_speed": 5.0 + rev if wind is None else wind,
    }


def _receipt(rid: str, *, used_revision: int = 1) -> dict:
    return {
        "receipt_id": rid,
        "turbine_id": TURBINE,
        "score_minute": SCORE_MINUTE,
        "used_sample_minute": SAMPLE_MINUTE,
        "used_revision": used_revision,
    }


def _seed(db, n_receipts: int, n_revisions: int) -> None:
    import_readings(db, [_reading(rev) for rev in range(1, n_revisions + 1)])
    import_receipts(
        db, [_receipt(f"R{i:05d}") for i in range(n_receipts)]
    )


def test_streaming_summary_memory_grows_with_receipts_plus_trails(db):
    _seed(db, n_receipts=4_000, n_revisions=400)

    tracemalloc.start()
    try:
        summary = reconcile_report(
            db, ReconcileRequest(turbine_id=TURBINE), "rpt_mem"
        )
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert summary["receipt_count"] == 4_000
    # The old per-receipt trail design needed ~300 MiB for this shape; the
    # streaming/shared design stays an order of magnitude below the 256 MiB
    # process cap even with interpreter overhead.
    assert peak < 100 * 1024**2, peak

    conn = db.connect()
    try:
        # One distinct (turbine, sample minute) -> exactly one shared trail.
        assert conn.execute(
            "SELECT COUNT(*) FROM report_pairs WHERE report_id='rpt_mem'"
        ).fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM report_items WHERE report_id='rpt_mem'"
        ).fetchone()[0] == 4_000
        # Slim evidence rows never embed the 400-row trail themselves.
        big = conn.execute(
            "SELECT MAX(LENGTH(evidence_json)) FROM report_items "
            "WHERE report_id='rpt_mem'"
        ).fetchone()[0]
        assert big < 4_000
    finally:
        conn.close()


def test_doubling_receipts_with_same_trail_does_not_double_trail_storage(db):
    _seed(db, n_receipts=1_000, n_revisions=200)
    reconcile_report(db, ReconcileRequest(turbine_id=TURBINE), "rpt_a")

    conn = db.connect()
    try:
        trail_bytes_a = conn.execute(
            "SELECT SUM(LENGTH(trail_json)) FROM report_pairs WHERE report_id='rpt_a'"
        ).fetchone()[0]
        item_bytes_a = conn.execute(
            "SELECT SUM(LENGTH(evidence_json)) FROM report_items "
            "WHERE report_id='rpt_a'"
        ).fetchone()[0]
    finally:
        conn.close()

    # Another 1,000 receipts on the same minute in a second report: the shared
    # trail stays the same size; only the (small) per-item evidence grows.
    import_receipts(
        db, [_receipt(f"S{i:05d}") for i in range(1_000)]
    )
    reconcile_report(
        db,
        ReconcileRequest(receipt_ids=[f"S{i:05d}" for i in range(1_000)]),
        "rpt_b",
    )
    conn = db.connect()
    try:
        trail_bytes_b = conn.execute(
            "SELECT SUM(LENGTH(trail_json)) FROM report_pairs WHERE report_id='rpt_b'"
        ).fetchone()[0]
        item_bytes_b = conn.execute(
            "SELECT SUM(LENGTH(evidence_json)) FROM report_items "
            "WHERE report_id='rpt_b'"
        ).fetchone()[0]
    finally:
        conn.close()

    assert trail_bytes_a == trail_bytes_b
    assert item_bytes_b == item_bytes_a


def test_paginated_read_expands_only_this_page(db):
    n_revisions = 400
    _seed(db, n_receipts=1_200, n_revisions=n_revisions)
    reconcile_report(db, ReconcileRequest(turbine_id=TURBINE), "rpt_page")

    page1, total = get_report_items(db, "rpt_page", offset=0, limit=100)
    page2, _ = get_report_items(db, "rpt_page", offset=1_100, limit=100)
    assert total == 1_200
    assert len(page1) == 100 and len(page2) == 100

    for item in page1 + page2:
        trail = item["evidence"]["revisions"]
        assert len(trail) == n_revisions
        # Flags exist on every trail entry and are booleans.
        assert all(
            isinstance(row["ingested_before_score"], bool)
            and isinstance(row["eligible"], bool)
            for row in trail
        )
        # All seed revisions landed before the 11:00 score minute.
        assert all(row["ingested_before_score"] for row in trail)
        assert all(row["eligible"] for row in trail)


def test_trail_flags_follow_each_receipts_own_score_minute(db):
    # rev 2 enters the warehouse at 11:30, i.e. after an 11:00 score but
    # before a 12:00 score; the shared trail must be annotated differently
    # for the two receipts even though they reference the same pair.  The
    # second receipt uses a sample minute inside its own 12:00 window so the
    # eligibility flag is meaningful there.
    import_readings(
        db,
        [
            _reading(1),
            {
                "turbine_id": TURBINE,
                "sample_minute": SAMPLE_MINUTE,
                "revision": 2,
                "ingested_at": "2026-01-01T11:30:00Z",
                "wind_speed": 9.9,
            },
            {
                "turbine_id": TURBINE,
                "sample_minute": "2026-01-01T11:05:00Z",
                "revision": 1,
                "ingested_at": "2026-01-01T11:06:00Z",
                "wind_speed": 3.3,
            },
            {
                "turbine_id": TURBINE,
                "sample_minute": "2026-01-01T11:05:00Z",
                "revision": 2,
                "ingested_at": "2026-01-01T11:30:00Z",
                "wind_speed": 3.4,
            },
        ],
    )
    import_receipts(
        db,
        [
            _receipt("EARLY"),
            {
                "receipt_id": "LATE",
                "turbine_id": TURBINE,
                "score_minute": "2026-01-01T12:00:00Z",
                "used_sample_minute": "2026-01-01T11:05:00Z",
                "used_revision": 2,
            },
        ],
    )
    reconcile_report(
        db, ReconcileRequest(receipt_ids=["EARLY", "LATE"]), "rpt_flags"
    )
    items, _ = get_report_items(
        db, "rpt_flags", offset=0, limit=None
    )
    by_id = {item["receipt_id"]: item for item in items}
    early = {
        row["revision"]: row
        for row in by_id["EARLY"]["evidence"]["revisions"]
    }
    late_trail = by_id["LATE"]["evidence"]["revisions"]
    late = {row["revision"]: row for row in late_trail}

    # EARLY scores at 11:00: the 11:30 ingestion is neither before score nor
    # eligible (and 10:00 is inside its [10:00, 11:00) window).
    assert early[2]["ingested_before_score"] is False
    assert early[2]["eligible"] is False
    # LATE scores at 12:00 with an in-window sample minute: 11:30 now counts.
    assert late[2]["ingested_before_score"] is True
    assert late[2]["eligible"] is True
    # The stored shared trails themselves carry no per-receipt flags.
    conn = db.connect()
    try:
        stored = [
            json.loads(row["trail_json"])
            for row in conn.execute(
                "SELECT trail_json FROM report_pairs WHERE report_id='rpt_flags'"
            ).fetchall()
        ]
    finally:
        conn.close()
    for trail in stored:
        assert all("ingested_before_score" not in row for row in trail)
        assert all("eligible" not in row for row in trail)


def test_backfill_and_revision_overwrite_leave_old_report_untouched(db):
    import_readings(db, [_reading(1)])
    import_receipts(db, [_receipt("R1")])
    reconcile_report(db, ReconcileRequest(receipt_ids=["R1"]), "rpt_old")

    before_row = get_report_row(db, "rpt_old")
    before_items, _ = get_report_items(db, "rpt_old", offset=0, limit=None)
    assert before_items[0]["evidence"]["current_highest"]["revision"] == 1
    assert before_items[0]["drift"] == []

    # Back-fill a new revision ...
    import_readings(
        db,
        [
            {
                "turbine_id": TURBINE,
                "sample_minute": SAMPLE_MINUTE,
                "revision": 2,
                "ingested_at": "2026-01-01T11:30:00Z",
                "wind_speed": 7.7,
            }
        ],
    )
    # ... and overwrite the same revision number with corrected values.
    import_readings(db, [{**_reading(1), "wind_speed": 42.0}])

    after_row = get_report_row(db, "rpt_old")
    after_items, _ = get_report_items(db, "rpt_old", offset=0, limit=None)
    assert after_row["snapshot_json"] == before_row["snapshot_json"]
    assert after_items == before_items
    assert after_items[0]["evidence"]["actual"]["wind_speed"] == 6.0

    # A fresh report sees the new warehouse state.
    reconcile_report(db, ReconcileRequest(receipt_ids=["R1"]), "rpt_new")
    new_items, _ = get_report_items(db, "rpt_new", offset=0, limit=None)
    assert new_items[0]["evidence"]["current_highest"]["revision"] == 2
    assert [f["kind"] for f in new_items[0]["drift"]] == ["revision"]


def test_failed_publish_leaves_no_report(db, monkeypatch):
    _seed(db, n_receipts=10, n_revisions=5)
    from app import storage

    def boom(*args, **kwargs):
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(storage, "copy_staged_report", boom)
    with pytest.raises(sqlite3.OperationalError, match="disk full"):
        reconcile_report(db, ReconcileRequest(turbine_id=TURBINE), "rpt_fail")

    conn = db.connect()
    try:
        assert conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM report_items").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM report_pairs").fetchone()[0] == 0
    finally:
        conn.close()


def test_legacy_database_opens_and_old_reports_remain_readable(db_path):
    # Build a database with the ORIGINAL schema (no report_pairs table, no
    # pair_seq column), then write a legacy-shaped report with an embedded
    # full trail.
    legacy = Database(db_path)
    legacy_conn = legacy.connect()
    legacy_conn.executescript(
        """
        CREATE TABLE wind_readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            turbine_id TEXT NOT NULL, sample_minute TEXT NOT NULL,
            revision INTEGER NOT NULL, ingested_at TEXT NOT NULL,
            wind_speed REAL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            UNIQUE (turbine_id, sample_minute, revision));
        CREATE TABLE receipts (
            receipt_id TEXT PRIMARY KEY, turbine_id TEXT NOT NULL,
            score_minute TEXT NOT NULL, used_sample_minute TEXT NOT NULL,
            used_revision INTEGER NOT NULL,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL);
        CREATE TABLE reports (
            report_id TEXT PRIMARY KEY, status TEXT NOT NULL,
            created_at TEXT NOT NULL, receipt_count INTEGER NOT NULL,
            usage_finding_count INTEGER NOT NULL,
            drift_finding_count INTEGER NOT NULL,
            items_with_findings INTEGER NOT NULL,
            snapshot_json TEXT NOT NULL, request_json TEXT NOT NULL);
        CREATE TABLE report_items (
            report_id TEXT NOT NULL, seq INTEGER NOT NULL,
            receipt_id TEXT NOT NULL, turbine_id TEXT NOT NULL,
            score_minute TEXT NOT NULL, used_sample_minute TEXT NOT NULL,
            used_revision INTEGER NOT NULL,
            usage_json TEXT NOT NULL, drift_json TEXT NOT NULL,
            evidence_json TEXT NOT NULL,
            usage_finding_count INTEGER NOT NULL,
            drift_finding_count INTEGER NOT NULL,
            PRIMARY KEY (report_id, seq));
        """
    )
    legacy_conn.close()
    legacy.close()

    # Reopen through the current code: migration must be additive.
    db = Database(db_path)
    db.initialize()
    import_readings(db, [_reading(1)])
    import_receipts(db, [_receipt("R1")])
    items, snapshot = reconcile_snapshot(db, ReconcileRequest(receipt_ids=["R1"]))
    # Simulate a legacy stored row: evidence with an embedded full trail and
    # no pair_seq linkage.
    assert "revisions" in items[0]["evidence"]
    persist_report(db, items, snapshot, {}, "rpt_legacy")

    # Strip pair linkage the way the old schema would have stored it, to make
    # sure reads fall back to the embedded evidence rather than crash.
    conn = db.connect()
    try:
        cols = {row["name"] for row in conn.execute("PRAGMA table_info(report_items)")}
        assert "pair_seq" in cols  # migration added it
        conn.execute(
            "UPDATE report_items SET pair_seq=NULL WHERE report_id='rpt_legacy'"
        )
        conn.commit()
    finally:
        conn.close()

    row = get_report_row(db, "rpt_legacy")
    assert row is not None
    legacy_items, total = get_report_items(
        db, "rpt_legacy", offset=0, limit=None
    )
    assert total == 1
    assert len(legacy_items[0]["evidence"]["revisions"]) == 1

    # New reports on the migrated volume use the shared-trail layout.
    reconcile_report(db, ReconcileRequest(receipt_ids=["R1"]), "rpt_new")
    conn = db.connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) FROM report_pairs WHERE report_id='rpt_new'"
        ).fetchone()[0] == 1
    finally:
        conn.close()
    new_items, _ = get_report_items(db, "rpt_new", offset=0, limit=None)
    assert len(new_items[0]["evidence"]["revisions"]) == 1


def test_streaming_path_matches_in_memory_engine_output(db):
    _seed(db, n_receipts=2_500, n_revisions=50)
    # Force several chunks.
    assert 2_500 > CHUNK_SIZE

    mem_items, mem_snapshot = reconcile_snapshot(
        db, ReconcileRequest(turbine_id=TURBINE)
    )
    summary = reconcile_report(
        db, ReconcileRequest(turbine_id=TURBINE), "rpt_eq"
    )
    stored_items, total = get_report_items(
        db, "rpt_eq", offset=0, limit=None
    )
    assert total == len(mem_items) == summary["receipt_count"]

    mem_by_id = {item["receipt_id"]: item for item in mem_items}
    for stored in stored_items:
        expected = mem_by_id[stored["receipt_id"]]
        assert stored["usage"] == expected["usage"]
        assert stored["drift"] == expected["drift"]
        assert stored["evidence"] == expected["evidence"]
