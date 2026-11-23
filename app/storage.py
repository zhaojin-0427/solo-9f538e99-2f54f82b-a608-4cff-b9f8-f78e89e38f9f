"""Import, persistence and retrieval of warehouse data and reports.

The HTTP reconciliation path (``engine.reconcile_report``) evaluates receipts
in chunks into connection-private TEMP staging tables while its snapshot read
transaction is open, then publishes everything through
:func:`copy_staged_report` in one short ``BEGIN IMMEDIATE`` transaction.  A
report is therefore either stored completely (header + shared trails + every
item) or not at all, so a failed batch never leaves a half-written report
behind - and continuous back-fill writes are never blocked while the batch
runs, because TEMP tables take no lock on the main database.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Sequence

from pydantic import ValidationError as PydanticValidationError

from .db import Database
from .domain import (
    ValidationError,
    format_instant,
    parse_instant,
    parse_minute,
    parse_revision,
    parse_text,
    parse_wind,
)
from .schemas import ReadingIn, ReceiptIn


def _now() -> str:
    return format_instant(datetime.now(timezone.utc))


# --------------------------------------------------------------------------
# streaming report writers (driven by engine.reconcile_report)
# --------------------------------------------------------------------------
def insert_report_header(
    conn: sqlite3.Connection,
    *,
    report_id: str,
    created_at: str,
    receipt_count: int,
    snapshot: dict[str, Any],
    request_doc: dict[str, Any],
) -> None:
    """Insert the report row with placeholder counts (finalised at commit)."""
    conn.execute(
        """
        INSERT INTO reports
            (report_id, status, created_at, receipt_count,
             usage_finding_count, drift_finding_count,
             items_with_findings, snapshot_json, request_json)
        VALUES (?, 'complete', ?, ?, 0, 0, 0, ?, ?)
        """,
        (
            report_id,
            created_at,
            receipt_count,
            json.dumps(snapshot, ensure_ascii=False),
            json.dumps(request_doc, ensure_ascii=False),
        ),
    )


def insert_report_pairs(
    conn: sqlite3.Connection,
    report_id: str,
    pair_records: Sequence[tuple[int, str, str, list[dict[str, Any]]]],
) -> None:
    """Store each distinct (turbine, sample minute) trail exactly once."""
    conn.executemany(
        """
        INSERT INTO report_pairs
            (report_id, pair_seq, turbine_id, sample_minute, trail_json)
        VALUES (?, ?, ?, ?, ?)
        """,
        [
            (report_id, seq, turbine_id, sample_minute, json.dumps(trail, ensure_ascii=False))
            for seq, turbine_id, sample_minute, trail in pair_records
        ],
    )


def insert_report_items(
    conn: sqlite3.Connection,
    report_id: str,
    first_seq: int,
    items: Sequence[dict[str, Any]],
) -> None:
    """Insert one evaluated chunk.  Evidence stays slim (no revision trail)."""
    conn.executemany(
        """
        INSERT INTO report_items
            (report_id, seq, receipt_id, turbine_id, score_minute,
             used_sample_minute, used_revision, usage_json, drift_json,
             evidence_json, usage_finding_count, drift_finding_count, pair_seq)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                report_id,
                first_seq + offset,
                item["receipt_id"],
                item["turbine_id"],
                item["score_minute"],
                item["used_sample_minute"],
                item["used_revision"],
                json.dumps(item["usage"], ensure_ascii=False),
                json.dumps(item["drift"], ensure_ascii=False),
                json.dumps(item["evidence"], ensure_ascii=False),
                len(item["usage"]),
                len(item["drift"]),
                item.get("pair_seq"),
            )
            for offset, item in enumerate(items)
        ],
    )


def finish_report(
    conn: sqlite3.Connection,
    report_id: str,
    *,
    usage_count: int,
    drift_count: int,
    items_with_findings: int,
) -> None:
    conn.execute(
        """
        UPDATE reports
           SET usage_finding_count = ?,
               drift_finding_count = ?,
               items_with_findings = ?
         WHERE report_id = ?
        """,
        (usage_count, drift_count, items_with_findings, report_id),
    )


def copy_staged_report(
    conn: sqlite3.Connection,
    report_id: str,
    *,
    usage_count: int,
    drift_count: int,
    items_with_findings: int,
) -> None:
    """Publish a report staged in TEMP tables, atomically.

    One short ``BEGIN IMMEDIATE`` transaction copies the staged header,
    shared trails and items into the real tables.  Any failure rolls the whole
    publish back, so a failed reconciliation never leaves a half report;
    because evaluation only touched TEMP tables, nothing else is left behind.
    """
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            INSERT INTO reports
                (report_id, status, created_at, receipt_count,
                 usage_finding_count, drift_finding_count,
                 items_with_findings, snapshot_json, request_json)
            SELECT ?, 'complete', created_at, receipt_count, ?, ?, ?,
                   snapshot_json, request_json
              FROM _stage_report
            """,
            (report_id, usage_count, drift_count, items_with_findings),
        )
        conn.execute(
            """
            INSERT INTO report_pairs
                (report_id, pair_seq, turbine_id, sample_minute, trail_json)
            SELECT ?, pair_seq, turbine_id, sample_minute, trail_json
              FROM _stage_pairs
            """,
            (report_id,),
        )
        conn.execute(
            """
            INSERT INTO report_items
                (report_id, seq, receipt_id, turbine_id, score_minute,
                 used_sample_minute, used_revision, usage_json, drift_json,
                 evidence_json, usage_finding_count, drift_finding_count, pair_seq)
            SELECT ?, seq, receipt_id, turbine_id, score_minute,
                   used_sample_minute, used_revision, usage_json, drift_json,
                   evidence_json, usage_finding_count, drift_finding_count, pair_seq
              FROM _stage_items
            """,
            (report_id,),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.execute("DELETE FROM _stage_report")
        conn.execute("DELETE FROM _stage_pairs")
        conn.execute("DELETE FROM _stage_items")


def _row_error(index: int, exc: Exception) -> dict[str, Any]:
    if isinstance(exc, PydanticValidationError):
        errors = []
        for item in exc.errors():
            field = ".".join(str(part) for part in item.get("loc", ())) or "<row>"
            errors.append({"field": field, "message": item.get("msg", "invalid value")})
        return {"row": index, "errors": errors}
    return {"row": index, "errors": [{"field": "<row>", "message": str(exc)}]}


def to_reading(row: ReadingIn) -> tuple[str, str, int, str, float | None]:
    return (
        parse_text(row.turbine_id, field_name="turbine_id"),
        parse_minute(row.sample_minute, field_name="sample_minute"),
        parse_revision(row.revision),
        parse_instant(row.ingested_at, field_name="ingested_at"),
        parse_wind(row.wind_speed),
    )


def to_receipt(row: ReceiptIn) -> tuple[str, str, str, str, int]:
    return (
        parse_text(row.receipt_id, field_name="receipt_id"),
        parse_text(row.turbine_id, field_name="turbine_id"),
        parse_minute(row.score_minute, field_name="score_minute"),
        parse_minute(row.used_sample_minute, field_name="used_sample_minute"),
        parse_revision(row.used_revision, field_name="used_revision"),
    )


def validate_reading_rows(rows: Sequence[Any]) -> list[tuple[str, str, int, str, float | None]]:
    parsed = []
    errors = []
    for index, raw in enumerate(rows):
        try:
            parsed.append(to_reading(ReadingIn.model_validate(raw)))
        except (PydanticValidationError, ValidationError) as exc:
            errors.append(_row_error(index, exc))
    if errors:
        raise ImportValidationError(errors)
    return parsed


def validate_receipt_rows(rows: Sequence[Any]) -> list[tuple[str, str, str, str, int]]:
    parsed = []
    errors = []
    for index, raw in enumerate(rows):
        try:
            parsed.append(to_receipt(ReceiptIn.model_validate(raw)))
        except (PydanticValidationError, ValidationError) as exc:
            errors.append(_row_error(index, exc))
    if errors:
        raise ImportValidationError(errors)
    return parsed


class ImportValidationError(ValueError):
    """Row-level import validation failures, reported with 1-based row numbers."""

    def __init__(self, errors: list[dict[str, Any]]) -> None:
        self.errors = errors
        super().__init__(f"{len(errors)} row(s) failed validation")

    def payload(self) -> dict[str, Any]:
        return {
            "message": self.args[0] if self.args else "validation failed",
            "errors": self.errors,
        }


def _dedupe_payload(payload: list[Any]) -> tuple[list[Any], int]:
    """Collapse byte-identical duplicate dict rows inside one request."""
    seen: set[str] = set()
    unique: list[Any] = []
    identical = 0
    for raw in payload:
        try:
            key = json.dumps(raw, sort_keys=True, ensure_ascii=False, default=str)
        except TypeError:
            unique.append(raw)
            continue
        if key in seen:
            identical += 1
            continue
        seen.add(key)
        unique.append(raw)
    return unique, identical


def import_readings(db: Database, payload: list[Any]) -> dict[str, Any]:
    unique, identical = _dedupe_payload(payload)
    parsed = validate_reading_rows(unique)
    # Last occurrence of a duplicated key wins inside the request.
    latest: dict[tuple[str, str, int], tuple[str, str, int, str, float | None]] = {}
    for row in parsed:
        latest[(row[0], row[1], row[2])] = row
    now = _now()
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                """
                INSERT INTO wind_readings
                    (turbine_id, sample_minute, revision, ingested_at,
                     wind_speed, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (turbine_id, sample_minute, revision) DO UPDATE SET
                    ingested_at = excluded.ingested_at,
                    wind_speed  = excluded.wind_speed,
                    updated_at  = excluded.updated_at
                """,
                [
                    (t, m, rev, ing, wind, now, now)
                    for (t, m, rev, ing, wind) in latest.values()
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        total = conn.execute("SELECT COUNT(*) AS n FROM wind_readings").fetchone()["n"]
    finally:
        conn.close()
    return {
        "accepted": len(latest),
        "total": int(total),
        "identical_duplicates": identical,
    }


def import_receipts(db: Database, payload: list[Any]) -> dict[str, Any]:
    unique, identical = _dedupe_payload(payload)
    parsed = validate_receipt_rows(unique)
    latest: dict[str, tuple[str, str, str, str, int]] = {}
    for row in parsed:
        latest[row[0]] = row
    now = _now()
    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.executemany(
                """
                INSERT INTO receipts
                    (receipt_id, turbine_id, score_minute, used_sample_minute,
                     used_revision, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (receipt_id) DO UPDATE SET
                    turbine_id         = excluded.turbine_id,
                    score_minute       = excluded.score_minute,
                    used_sample_minute = excluded.used_sample_minute,
                    used_revision      = excluded.used_revision,
                    updated_at         = excluded.updated_at
                """,
                [
                    (rid, t, sm, usm, urev, now, now)
                    for (rid, t, sm, usm, urev) in latest.values()
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        total = conn.execute("SELECT COUNT(*) AS n FROM receipts").fetchone()["n"]
    finally:
        conn.close()
    return {
        "accepted": len(latest),
        "total": int(total),
        "identical_duplicates": identical,
    }


def persist_report(
    db: Database,
    items: Sequence[dict[str, Any]],
    snapshot: dict[str, Any],
    request: dict[str, Any],
    report_id: str,
) -> dict[str, Any]:
    """Store a fully-materialised report atomically.

    Compatibility wrapper used by tests and callers that already hold every
    item in memory; the HTTP reconciliation path streams through
    :func:`app.engine.reconcile_report` instead.  A failure still leaves no
    report row behind (single ``BEGIN IMMEDIATE`` transaction).

    Items produced by the current engine are slim (no ``revisions`` inside
    ``evidence``) and carry ``pair_seq``; their shared trails are written to
    ``report_pairs``.  Legacy-shaped items (evidence already containing a
    full ``revisions`` list, no ``pair_seq``) are stored verbatim and remain
    readable on older database files.
    """
    created_at = format_instant(datetime.now(timezone.utc))
    usage_count = sum(len(item["usage"]) for item in items)
    drift_count = sum(len(item["drift"]) for item in items)
    items_with_findings = sum(
        1 for item in items if item["usage"] or item["drift"]
    )

    pair_records: list[tuple[int, str, str, list[dict[str, Any]]]] = []
    slim_items: list[dict[str, Any]] = []
    seen_pairs: set[int] = set()
    for item in items:
        pair_seq = item.get("pair_seq")
        if pair_seq is not None and pair_seq not in seen_pairs:
            seen_pairs.add(pair_seq)
            trail = item["evidence"].get("revisions", [])
            pair_records.append(
                (
                    pair_seq,
                    item["turbine_id"],
                    item["used_sample_minute"],
                    trail,
                )
            )
        slim_items.append(item)

    conn = db.connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            insert_report_header(
                conn,
                report_id=report_id,
                created_at=created_at,
                receipt_count=len(items),
                snapshot=snapshot,
                request_doc=request,
            )
            if pair_records:
                insert_report_pairs(conn, report_id, pair_records)
            for start in range(0, len(slim_items), 500):
                insert_report_items(
                    conn, report_id, start, slim_items[start : start + 500]
                )
            finish_report(
                conn,
                report_id,
                usage_count=usage_count,
                drift_count=drift_count,
                items_with_findings=items_with_findings,
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
    return {
        "report_id": report_id,
        "status": "complete",
        "created_at": created_at,
        "receipt_count": len(items),
        "usage_finding_count": usage_count,
        "drift_finding_count": drift_count,
        "items_with_findings": items_with_findings,
    }


def _summary(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "report_id": row["report_id"],
        "status": row["status"],
        "created_at": row["created_at"],
        "receipt_count": row["receipt_count"],
        "usage_finding_count": row["usage_finding_count"],
        "drift_finding_count": row["drift_finding_count"],
        "items_with_findings": row["items_with_findings"],
    }


SUMMARY_COLUMNS = """
    report_id, status, created_at, receipt_count, usage_finding_count,
    drift_finding_count, items_with_findings
"""


def list_reports(db: Database, offset: int, limit: int) -> tuple[list[dict], int]:
    conn = db.connect()
    try:
        total = conn.execute("SELECT COUNT(*) AS n FROM reports").fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT {SUMMARY_COLUMNS}
              FROM reports
             ORDER BY created_at DESC, report_id DESC
             LIMIT ? OFFSET ?
            """,
            (limit, offset),
        ).fetchall()
    finally:
        conn.close()
    return [_summary(row) for row in rows], int(total)


def get_report_row(db: Database, report_id: str) -> sqlite3.Row | None:
    conn = db.connect()
    try:
        return conn.execute(
            f"SELECT {SUMMARY_COLUMNS}, snapshot_json, request_json "
            "FROM reports WHERE report_id = ?",
            (report_id,),
        ).fetchone()
    finally:
        conn.close()


def get_report_items(
    db: Database,
    report_id: str,
    *,
    offset: int,
    limit: int | None,
    receipt_id: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return one page of report items with fully-expanded evidence.

    Only the items on this page are deserialised, and the shared revision
    trails are fetched solely for the pair seqs that this page references.
    Per-receipt flags (``ingested_before_score`` / ``eligible``) are derived
    from each receipt's own score minute rather than shared, as required by
    the business rules.  Legacy rows without ``pair_seq`` keep the trail that
    was originally embedded in their evidence.
    """
    from .engine import annotate_evidence

    conn = db.connect()
    try:
        where = "report_id = ?"
        params: list[Any] = [report_id]
        if receipt_id is not None:
            where += " AND receipt_id = ?"
            params.append(receipt_id)
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM report_items WHERE {where}", params
        ).fetchone()["n"]
        clause = ""
        page_params = list(params)
        if limit is not None:
            clause = " LIMIT ? OFFSET ?"
            page_params.extend([limit, offset])
        elif offset:
            clause = " LIMIT -1 OFFSET ?"
            page_params.append(offset)
        rows = conn.execute(
            f"""
            SELECT receipt_id, turbine_id, score_minute, used_sample_minute,
                   used_revision, usage_json, drift_json, evidence_json,
                   pair_seq
              FROM report_items
             WHERE {where}
             ORDER BY seq
            {clause}
            """,
            page_params,
        ).fetchall()
        pair_seqs = {
            row["pair_seq"] for row in rows if row["pair_seq"] is not None
        }
        trails: dict[int, list[dict[str, Any]]] = {}
        if pair_seqs:
            placeholders = ",".join("?" for _ in pair_seqs)
            trail_rows = conn.execute(
                f"""
                SELECT pair_seq, trail_json
                  FROM report_pairs
                 WHERE report_id = ?
                   AND pair_seq IN ({placeholders})
                """,
                [report_id, *sorted(pair_seqs)],
            ).fetchall()
            trails = {
                row["pair_seq"]: json.loads(row["trail_json"]) for row in trail_rows
            }
    finally:
        conn.close()
    items: list[dict[str, Any]] = []
    for row in rows:
        evidence = json.loads(row["evidence_json"])
        pair_seq = row["pair_seq"]
        if pair_seq is not None and pair_seq in trails:
            cutoff = evidence["selection"]["cutoff_instant"]
            evidence = annotate_evidence(evidence, trails[pair_seq], cutoff)
        items.append(
            {
                "receipt_id": row["receipt_id"],
                "turbine_id": row["turbine_id"],
                "score_minute": row["score_minute"],
                "used_sample_minute": row["used_sample_minute"],
                "used_revision": row["used_revision"],
                "usage": json.loads(row["usage_json"]),
                "drift": json.loads(row["drift_json"]),
                "evidence": evidence,
            }
        )
    return items, int(total)


def reports_for_receipt(db: Database, receipt_id: str) -> list[dict[str, Any]]:
    conn = db.connect()
    try:
        rows = conn.execute(
            f"""
            SELECT DISTINCT {', '.join('r.' + c.strip() for c in SUMMARY_COLUMNS.split(','))}
              FROM reports r
              JOIN report_items i ON i.report_id = r.report_id
             WHERE i.receipt_id = ?
             ORDER BY r.created_at DESC, r.report_id DESC
            """,
            (receipt_id,),
        ).fetchall()
    finally:
        conn.close()
    return [_summary(row) for row in rows]


def list_readings(
    db: Database,
    *,
    turbine_id: str | None,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    where: list[str] = []
    params: list[Any] = []
    if turbine_id is not None:
        where.append("turbine_id = ?")
        params.append(turbine_id)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = db.connect()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM wind_readings{clause}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT turbine_id, sample_minute, revision, ingested_at, wind_speed
              FROM wind_readings{clause}
             ORDER BY turbine_id, sample_minute, revision
             LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows], int(total)


def list_receipts(
    db: Database,
    *,
    turbine_id: str | None,
    offset: int,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    where: list[str] = []
    params: list[Any] = []
    if turbine_id is not None:
        where.append("turbine_id = ?")
        params.append(turbine_id)
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    conn = db.connect()
    try:
        total = conn.execute(
            f"SELECT COUNT(*) AS n FROM receipts{clause}", params
        ).fetchone()["n"]
        rows = conn.execute(
            f"""
            SELECT receipt_id, turbine_id, score_minute, used_sample_minute,
                   used_revision
              FROM receipts{clause}
             ORDER BY score_minute, receipt_id
             LIMIT ? OFFSET ?
            """,
            [*params, limit, offset],
        ).fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows], int(total)
