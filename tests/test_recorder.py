"""Unit tests for moktan.recorder.RunRecorder.

Golden values are copied verbatim from designdoc/flume_logging_spec.md §12.1
(initial full run) and §12.2 (resume), per §9-6.
"""

import logging

import polars as pl
import pytest

from conftest import four_node_dag as _four_node_dag
from moktan.events import RunContext, _emit
from moktan.node import Node
from moktan.recorder import RunRecorder

RUN_ID = "7f3a1c9e2b04"


def _emit_planned(ctx, node, decision, reason, deps):
    _emit(
        ctx,
        "node_planned",
        logging.DEBUG,
        node=node,
        decision=decision,
        reason=reason,
        deps=[str(d.path) for d in deps],
    )


def test_to_mermaid_matches_golden_initial_full_run(tmp_path):
    """§12.1: 4-node DAG, everything computed."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    ctx = RunContext(run_id=RUN_ID)
    recorder = RunRecorder()

    with recorder.attach():
        _emit(ctx, "run_started", logging.INFO, root=str(joined.path), force=False, max_workers=1)
        _emit_planned(ctx, users_raw, "compute", "missing", [])
        _emit_planned(ctx, orders_raw, "compute", "missing", [])
        _emit_planned(ctx, orders_clean, "compute", "missing", [orders_raw])
        _emit_planned(ctx, joined, "compute", "missing", [users_raw, orders_clean])
        _emit(ctx, "node_computed", logging.INFO, node=users_raw, duration_s=0.05, rows=500, columns=3, bytes=8192)
        _emit(
            ctx, "node_computed", logging.INFO, node=orders_raw,
            duration_s=0.08, rows=1200, columns=4, bytes=24576,
        )
        _emit(
            ctx, "node_computed", logging.INFO, node=orders_clean,
            duration_s=0.12, rows=1180, columns=4, bytes=22528,
        )
        _emit(
            ctx, "node_computed", logging.INFO, node=joined,
            duration_s=0.19, rows=1180, columns=6, bytes=32768,
        )
        _emit(ctx, "run_finished", logging.INFO, status="ok", duration_s=0.44, n_computed=4, n_loaded=0, n_skipped=0)

    expected = (
        "flowchart LR\n"
        f'    n0["users_raw.parquet<br/>computed 0.05s, 500 rows"]:::computed --> n3\n'
        f'    n1["orders_raw.parquet<br/>computed 0.08s, 1200 rows"]:::computed --> n2\n'
        f'    n2["orders_clean.parquet<br/>computed 0.12s, 1180 rows"]:::computed --> n3\n'
        f'    n3["joined.parquet<br/>computed 0.19s, 1180 rows"]:::computed\n'
        "    classDef computed fill:#dcfce7,stroke:#16a34a\n"
        "    classDef loaded   fill:#dbeafe,stroke:#2563eb\n"
        "    classDef skipped  fill:#f3f4f6,stroke:#9ca3af,color:#6b7280\n"
        "    classDef failed   fill:#fee2e2,stroke:#dc2626\n"
        "    classDef cancelled fill:#fef9c3,stroke:#ca8a04"
    )
    assert recorder.to_mermaid() == expected


def test_to_mermaid_matches_golden_resume_run(tmp_path):
    """§12.2: same DAG, users_raw loaded, rest recomputed."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    ctx = RunContext(run_id="b21e9f7a5c31")
    recorder = RunRecorder()

    with recorder.attach():
        _emit_planned(ctx, users_raw, "load", "fresh", [])
        _emit_planned(ctx, orders_raw, "compute", "missing", [])
        _emit_planned(ctx, orders_clean, "compute", "dep_stale", [orders_raw])
        _emit_planned(ctx, joined, "compute", "dep_stale", [users_raw, orders_clean])
        _emit(ctx, "node_loaded", logging.INFO, node=users_raw, duration_s=0.01, rows=500)
        _emit(
            ctx, "node_computed", logging.INFO, node=orders_raw,
            duration_s=0.07, rows=1200, columns=4, bytes=24576,
        )
        _emit(
            ctx, "node_computed", logging.INFO, node=orders_clean,
            duration_s=0.11, rows=1180, columns=4, bytes=22528,
        )
        _emit(
            ctx, "node_computed", logging.INFO, node=joined,
            duration_s=0.18, rows=1180, columns=6, bytes=32768,
        )

    expected = (
        "flowchart LR\n"
        '    n0["users_raw.parquet<br/>loaded 0.01s, 500 rows"]:::loaded --> n3\n'
        '    n1["orders_raw.parquet<br/>computed 0.07s, 1200 rows"]:::computed --> n2\n'
        '    n2["orders_clean.parquet<br/>computed 0.11s, 1180 rows"]:::computed --> n3\n'
        '    n3["joined.parquet<br/>computed 0.18s, 1180 rows"]:::computed\n'
        "    classDef computed fill:#dcfce7,stroke:#16a34a\n"
        "    classDef loaded   fill:#dbeafe,stroke:#2563eb\n"
        "    classDef skipped  fill:#f3f4f6,stroke:#9ca3af,color:#6b7280\n"
        "    classDef failed   fill:#fee2e2,stroke:#dc2626\n"
        "    classDef cancelled fill:#fef9c3,stroke:#ca8a04"
    )
    assert recorder.to_mermaid() == expected


def test_to_markdown_contains_all_nodes_and_summary(tmp_path):
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    ctx = RunContext(run_id="b21e9f7a5c31")
    recorder = RunRecorder()

    with recorder.attach():
        _emit_planned(ctx, users_raw, "load", "fresh", [])
        _emit_planned(ctx, orders_raw, "compute", "missing", [])
        _emit_planned(ctx, orders_clean, "compute", "dep_stale", [orders_raw])
        _emit_planned(ctx, joined, "compute", "dep_stale", [users_raw, orders_clean])
        _emit(ctx, "node_loaded", logging.INFO, node=users_raw, duration_s=0.01, rows=500)
        _emit(
            ctx, "node_computed", logging.INFO, node=orders_raw,
            duration_s=0.07, rows=1200, columns=4, bytes=24576,
        )
        _emit(
            ctx, "node_computed", logging.INFO, node=orders_clean,
            duration_s=0.11, rows=1180, columns=4, bytes=22528,
        )
        _emit(
            ctx, "node_computed", logging.INFO, node=joined,
            duration_s=0.18, rows=1180, columns=6, bytes=32768,
        )
        _emit(ctx, "run_finished", logging.INFO, status="ok", duration_s=0.37, n_computed=3, n_loaded=1, n_skipped=0)

    report = recorder.to_markdown()
    assert "b21e9f7a5c31" in report
    assert "status: ok" in report
    for node in (users_raw, orders_raw, orders_clean, joined):
        assert str(node.path) in report
    assert "```mermaid" in report
    assert recorder.to_mermaid() in report


def test_to_markdown_failure_section_lists_primary_notes_and_cancelled(tmp_path):
    """§12.8 shape: 2 failures, 1 success, 2 cancelled, 1 untouched."""
    leaves = [Node(tmp_path / f"leaf{i}.parquet", lambda **_: pl.DataFrame()) for i in range(1, 6)]
    combined = Node(
        tmp_path / "combined.parquet",
        lambda **_: pl.DataFrame(),
        deps={f"n{i}": leaf for i, leaf in enumerate(leaves)},
    )
    ctx = RunContext(run_id="f18d3c6b0a72")
    recorder = RunRecorder()

    with recorder.attach():
        _emit_planned(ctx, leaves[0], "compute", "forced", [])
        _emit_planned(ctx, leaves[1], "compute", "forced", [])
        _emit_planned(ctx, leaves[2], "compute", "forced", [])
        _emit_planned(ctx, leaves[3], "compute", "forced", [])
        _emit_planned(ctx, leaves[4], "compute", "forced", [])
        _emit_planned(ctx, combined, "compute", "forced", leaves)
        _emit(ctx, "node_failed", logging.ERROR, node=leaves[0], error="RuntimeError", message="fetch failed: leaf1")
        _emit(ctx, "node_failed", logging.ERROR, node=leaves[1], error="ValueError", message="schema mismatch")
        _emit(
            ctx, "node_computed", logging.INFO, node=leaves[2],
            duration_s=0.15, rows=100, columns=1, bytes=2048,
        )
        _emit(ctx, "node_cancelled", logging.DEBUG, node=leaves[3])
        _emit(ctx, "node_cancelled", logging.DEBUG, node=leaves[4])
        _emit(
            ctx, "run_failed", logging.ERROR, status="failed", duration_s=0.21,
            failed=[str(leaves[0].path), str(leaves[1].path)],
        )

    report = recorder.to_markdown()
    assert "## Failure" in report
    assert f"node: {leaves[0].path}" in report
    assert "error: RuntimeError" in report
    assert "Also failed" in report
    assert str(leaves[1].path) in report
    assert "Cancelled (not started)" in report
    assert str(leaves[3].path) in report
    assert str(leaves[4].path) in report
    assert str(combined.path) in report  # untouched node still listed


def test_attach_detaches_on_exit_and_stops_recording(tmp_path):
    node = Node(tmp_path / "x.parquet", lambda **_: pl.DataFrame())
    ctx = RunContext(run_id="aaa")
    recorder = RunRecorder()
    with recorder.attach():
        _emit(ctx, "run_started", logging.INFO, root=str(node.path), force=False, max_workers=1)
    n_events_after_detach = len(recorder.events)
    _emit(ctx, "run_started", logging.INFO, root=str(node.path), force=False, max_workers=1)
    assert len(recorder.events) == n_events_after_detach


def test_attach_can_be_nested_and_used_by_multiple_recorders(tmp_path):
    node = Node(tmp_path / "x.parquet", lambda **_: pl.DataFrame())
    ctx = RunContext(run_id="bbb")
    outer = RunRecorder()
    inner = RunRecorder()
    with outer.attach():
        _emit(ctx, "run_started", logging.INFO, root=str(node.path), force=False, max_workers=1)
        with inner.attach():
            _emit(ctx, "plan_computed", logging.INFO, n_nodes=1, n_compute=1, n_load=0, n_skip=0, duration_s=0.0)
        _emit(ctx, "run_finished", logging.INFO, status="ok", duration_s=0.1, n_computed=1, n_loaded=0, n_skipped=0)

    assert len(outer.events) == 3
    assert len(inner.events) == 1


def test_to_mermaid_raises_without_events():
    recorder = RunRecorder()
    with pytest.raises(ValueError):
        recorder.to_mermaid()


def test_label_collision_appends_parent_directory(tmp_path):
    a = Node(tmp_path / "a" / "x.parquet", lambda **_: pl.DataFrame())
    b = Node(tmp_path / "b" / "x.parquet", lambda **_: pl.DataFrame(), deps={"a": a})
    ctx = RunContext(run_id="ccc")
    recorder = RunRecorder()
    with recorder.attach():
        _emit_planned(ctx, a, "compute", "missing", [])
        _emit_planned(ctx, b, "compute", "missing", [a])
        _emit(ctx, "node_computed", logging.INFO, node=a, duration_s=0.01, rows=1, columns=1, bytes=1)
        _emit(ctx, "node_computed", logging.INFO, node=b, duration_s=0.01, rows=1, columns=1, bytes=1)

    mermaid = recorder.to_mermaid()
    assert "a/x.parquet" in mermaid
    assert "b/x.parquet" in mermaid
