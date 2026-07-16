"""Integration tests for designdoc/flume_logging_spec.md §12: each test drives
a real `run()` end-to-end and checks the resulting event stream against the
case it corresponds to. Where §12 marks a case as non-golden (12.3, 12.8 --
timing-dependent), the assertions check structural/count invariants instead
of exact sequences.
"""

import logging
import os
import re
import time

import polars as pl
import pytest

from conftest import moktan_event
from moktan import Node, PipelineError, RunRecorder, run


def _four_node_dag(tmp_path):
    """§12.1-12.2/12.5-12.7's DAG: users_raw, orders_raw -> orders_clean -> joined."""

    def make_users_raw() -> pl.DataFrame:
        return pl.DataFrame({"id": range(500), "name": ["u"] * 500, "email": ["e"] * 500})

    def make_orders_raw() -> pl.DataFrame:
        return pl.DataFrame({"id": range(1200), "user_id": [1] * 1200, "amount": [1] * 1200, "d": [1] * 1200})

    def make_orders_clean(orders: pl.DataFrame) -> pl.DataFrame:
        return orders.head(1180)

    def make_joined(users: pl.DataFrame, orders: pl.DataFrame) -> pl.DataFrame:
        return orders.with_columns(
            pl.lit(1).alias("a"), pl.lit(1).alias("b"), pl.lit(1).alias("c")
        )

    users_raw = Node(tmp_path / "users_raw.parquet", make_users_raw)
    orders_raw = Node(tmp_path / "orders_raw.parquet", make_orders_raw)
    orders_clean = Node(tmp_path / "orders_clean.parquet", make_orders_clean, deps={"orders": orders_raw})
    joined = Node(
        tmp_path / "joined.parquet", make_joined, deps={"users": users_raw, "orders": orders_clean}
    )
    return users_raw, orders_raw, orders_clean, joined


def _capture(caplog: pytest.LogCaptureFixture, level: int = logging.INFO):
    caplog.set_level(level, logger="moktan")
    return caplog


def _events(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [moktan_event(r) for r in caplog.records]


def _by_event(events: list[dict], name: str) -> list[dict]:
    return [e for e in events if e["event"] == name]


CONSOLE_LINE_RE = {
    "computed": re.compile(r"^computed (?P<path>\S+) \((?P<dur>\d+\.\d{2})s\) (?P<kv>.+)$"),
    "loaded": re.compile(r"^loaded (?P<path>\S+) \((?P<dur>\d+\.\d{2})s\) (?P<kv>.+)$"),
    "skipped": re.compile(r"^skipped (?P<path>\S+) (?P<kv>.+)$"),
}


def test_case_12_1_initial_full_run_sequential(tmp_path, caplog):
    """§12.1: 4 fresh files, sequential run -> everything computed."""
    _capture(caplog)
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)

    run(joined, max_workers=1)

    events = _events(caplog)
    assert [e["event"] for e in events] == [
        "run_started",
        "plan_computed",
        "node_computed",
        "node_computed",
        "node_computed",
        "node_computed",
        "run_finished",
    ]
    plan = events[1]
    assert (plan["n_nodes"], plan["n_compute"], plan["n_load"], plan["n_skip"]) == (4, 4, 0, 0)

    computed_order = [e["node"] for e in _by_event(events, "node_computed")]
    assert computed_order == [
        str(users_raw.path),
        str(orders_raw.path),
        str(orders_clean.path),
        str(joined.path),
    ]

    run_id = events[0]["run_id"]
    assert re.match(r"^[0-9a-f]{12}$", run_id)
    assert all(e["run_id"] == run_id for e in events)

    joined_computed = _by_event(events, "node_computed")[-1]
    assert joined_computed["rows"] == 1180
    assert joined_computed["columns"] == 7
    assert joined_computed["bytes"] > 0

    # console line shape matches §6.1/§12.1 exactly (verb, path, duration, kv tail)
    joined_record = next(r for r in caplog.records if moktan_event(r) is joined_computed)
    m = CONSOLE_LINE_RE["computed"].match(joined_record.getMessage())
    assert m is not None
    assert m.group("path") == str(joined.path)
    assert f"run_id={run_id}" in m.group("kv")
    assert "thread=MainThread" in m.group("kv")


def test_case_12_2_partial_resume_reasons_and_counts(tmp_path, caplog):
    """§12.2: delete orders_raw.parquet -> missing/dep_stale propagation,
    users_raw becomes a load target, n_skip stays 0."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    run(joined, max_workers=1)

    orders_raw.path.unlink()
    _capture(caplog, level=logging.DEBUG)
    run(joined, max_workers=1)

    events = _events(caplog)
    planned = {e["node"]: e for e in _by_event(events, "node_planned")}
    assert planned[str(users_raw.path)]["decision"] == "load"
    assert planned[str(users_raw.path)]["reason"] == "fresh"
    assert planned[str(orders_raw.path)]["decision"] == "compute"
    assert planned[str(orders_raw.path)]["reason"] == "missing"
    assert planned[str(orders_clean.path)]["decision"] == "compute"
    assert planned[str(orders_clean.path)]["reason"] == "dep_stale"
    assert planned[str(orders_clean.path)]["deps"] == [str(orders_raw.path)]
    assert planned[str(joined.path)]["decision"] == "compute"
    assert planned[str(joined.path)]["reason"] == "dep_stale"
    assert planned[str(joined.path)]["deps"] == [str(users_raw.path), str(orders_clean.path)]

    plan = _by_event(events, "plan_computed")[0]
    assert (plan["n_compute"], plan["n_load"], plan["n_skip"]) == (3, 1, 0)

    assert [e["event"] for e in _by_event(events, "node_loaded") + _by_event(events, "node_skipped")] == [
        "node_loaded"
    ]
    loaded = _by_event(events, "node_loaded")[0]
    assert loaded["node"] == str(users_raw.path)


def test_case_12_3_parallel_run_id_and_thread_correlation(tmp_path, caplog):
    """§12.3: 3 independent leaves + combine, max_workers=3. Non-golden
    (completion order varies) -- check structural invariants only."""
    _capture(caplog)

    def make_leaf(i: int):
        def f() -> pl.DataFrame:
            return pl.DataFrame({"v": [i]})

        return f

    leaves = {f"n{i}": Node(tmp_path / f"leaf{i}.parquet", make_leaf(i)) for i in range(3)}
    combined = Node(
        tmp_path / "combined.parquet",
        lambda **dep_dfs: pl.DataFrame({"v": [sum(df["v"][0] for df in dep_dfs.values())]}),
        deps=leaves,
    )

    run(combined, max_workers=3)

    events = _events(caplog)
    computed = _by_event(events, "node_computed")
    assert len(computed) == 4
    run_id = events[0]["run_id"]
    assert all(e["run_id"] == run_id for e in events)  # §5: consistent across worker threads

    leaf_paths = {str(n.path) for n in leaves.values()}
    leaf_events = [e for e in computed if e["node"] in leaf_paths]
    assert len(leaf_events) == 3
    # _compute_or_load (and thus node_computed) runs on whichever worker
    # thread the executor assigns, for every pass2 node including the root --
    # only bookkeeping (_finish_node) and run-level events are MainThread-only.
    finished = _by_event(events, "run_finished")[0]
    assert finished["thread"] == "MainThread"
    assert computed[-1]["node"] == str(combined.path)  # combined always finishes last


def test_case_12_4_failure_run_untouched_node_gets_no_info_event(tmp_path, caplog):
    """§12.4: force=True, orders_clean's f raises -> node_failed, run_failed,
    joined gets a node_planned (DEBUG) but zero INFO events."""
    _capture(caplog, level=logging.DEBUG)

    def make_users_raw() -> pl.DataFrame:
        return pl.DataFrame({"id": [1]})

    def make_orders_raw() -> pl.DataFrame:
        return pl.DataFrame({"id": [1]})

    def make_orders_clean(orders: pl.DataFrame) -> pl.DataFrame:
        raise RuntimeError("unexpected null in order_id")

    def make_joined(users: pl.DataFrame, orders: pl.DataFrame) -> pl.DataFrame:
        return orders

    users_raw = Node(tmp_path / "users_raw.parquet", make_users_raw)
    orders_raw = Node(tmp_path / "orders_raw.parquet", make_orders_raw)
    orders_clean = Node(tmp_path / "orders_clean.parquet", make_orders_clean, deps={"orders": orders_raw})
    joined = Node(
        tmp_path / "joined.parquet", make_joined, deps={"users": users_raw, "orders": orders_clean}
    )

    with pytest.raises(PipelineError) as exc_info:
        run(joined, force=True, max_workers=1)
    assert exc_info.value.node is orders_clean
    assert exc_info.value.failed == [orders_clean]

    events = _events(caplog)
    failed = _by_event(events, "node_failed")
    assert len(failed) == 1
    assert failed[0]["node"] == str(orders_clean.path)
    assert failed[0]["error"] == "RuntimeError"
    assert failed[0]["message"] == "unexpected null in order_id"

    run_failed = _by_event(events, "run_failed")
    assert len(run_failed) == 1
    assert run_failed[0]["failed"] == [str(orders_clean.path)]
    assert _by_event(events, "run_finished") == []

    joined_events = [e for e in events if e.get("node") == str(joined.path)]
    assert [e["event"] for e in joined_events] == ["node_planned"]  # planned but never submitted


def test_case_12_5_force_true_reason_is_forced_even_when_fresh(tmp_path, caplog):
    """§12.5: force=True overrides freshness -- reason=forced, not fresh."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    run(joined, max_workers=1)  # everything now fresh on disk

    _capture(caplog, level=logging.DEBUG)
    run(joined, force=True, max_workers=1)

    events = _events(caplog)
    planned = _by_event(events, "node_planned")
    assert len(planned) == 4
    assert all(e["reason"] == "forced" for e in planned)
    assert all(e["decision"] == "compute" for e in planned)
    plan = _by_event(events, "plan_computed")[0]
    assert (plan["n_compute"], plan["n_load"], plan["n_skip"]) == (4, 0, 0)


def test_case_12_6_all_fresh_rerun_skipped_and_loaded_root(tmp_path, caplog):
    """§12.6: fresh-root shortcut. skipped x3 always precede loaded (root)."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    run(joined, max_workers=1)

    _capture(caplog)
    run(joined, max_workers=1)

    events = _events(caplog)
    plan = _by_event(events, "plan_computed")[0]
    assert (plan["n_compute"], plan["n_load"], plan["n_skip"]) == (0, 1, 3)

    non_run_events = [e for e in events if e["event"] not in ("run_started", "plan_computed", "run_finished")]
    assert [e["event"] for e in non_run_events] == [
        "node_skipped",
        "node_skipped",
        "node_skipped",
        "node_loaded",
    ]
    assert non_run_events[-1]["node"] == str(joined.path)
    skipped_paths = {e["node"] for e in non_run_events[:3]}
    assert skipped_paths == {str(users_raw.path), str(orders_raw.path), str(orders_clean.path)}


def test_case_12_7_external_upstream_rewrite_dep_newer(tmp_path, caplog):
    """§12.7: orders_raw's mtime bumped externally -> orders_raw itself stays
    `fresh`, its consumer becomes `dep_newer`, further downstream `dep_stale`."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    run(joined, max_workers=1)

    future = time.time() + 10
    os.utime(orders_raw.path, (future, future))

    _capture(caplog, level=logging.DEBUG)
    run(joined, max_workers=1)

    events = _events(caplog)
    planned = {e["node"]: e for e in _by_event(events, "node_planned")}
    assert planned[str(users_raw.path)]["reason"] == "fresh"
    assert planned[str(orders_raw.path)]["reason"] == "fresh"  # the rewritten node itself
    assert planned[str(orders_clean.path)]["reason"] == "dep_newer"  # its direct consumer
    assert planned[str(joined.path)]["reason"] == "dep_stale"  # propagated further downstream

    plan = _by_event(events, "plan_computed")[0]
    assert (plan["n_compute"], plan["n_load"], plan["n_skip"]) == (2, 2, 0)


def test_case_12_8_parallel_multiple_failures_and_cancellation(tmp_path, caplog):
    """§12.8: 5 leaves, 2 fail, max_workers=3. Non-golden (cancel is best-
    effort) -- assert the timing-independent invariants only."""
    _capture(caplog, level=logging.DEBUG)

    def make_bad(i: int):
        def f() -> pl.DataFrame:
            raise RuntimeError(f"fetch failed: leaf{i}")

        return f

    def make_ok(i: int):
        def f() -> pl.DataFrame:
            return pl.DataFrame({"v": [i]})

        return f

    leaves = [
        Node(tmp_path / "leaf1.parquet", make_bad(1)),
        Node(tmp_path / "leaf2.parquet", make_bad(2)),
        Node(tmp_path / "leaf3.parquet", make_ok(3)),
        Node(tmp_path / "leaf4.parquet", make_ok(4)),
        Node(tmp_path / "leaf5.parquet", make_ok(5)),
    ]
    combined = Node(
        tmp_path / "combined.parquet",
        lambda **dep_dfs: next(iter(dep_dfs.values())),
        deps={f"n{i}": leaf for i, leaf in enumerate(leaves)},
    )

    with pytest.raises(PipelineError) as exc_info:
        run(combined, force=True, max_workers=3)

    events = _events(caplog)
    failed = _by_event(events, "node_failed")
    assert {e["node"] for e in failed} == {str(leaves[0].path), str(leaves[1].path)}

    run_failed = _by_event(events, "run_failed")[0]
    assert set(run_failed["failed"]) == {str(leaves[0].path), str(leaves[1].path)}
    assert exc_info.value.node in (leaves[0], leaves[1])
    assert {n.path for n in exc_info.value.failed} == {leaves[0].path, leaves[1].path}

    # leaf3/leaf4/leaf5 each land in exactly one of {computed, cancelled} --
    # never both, never neither, regardless of scheduling timing.
    for leaf in leaves[2:]:
        outcomes = [e["event"] for e in events if e.get("node") == str(leaf.path)]
        terminal = [e for e in outcomes if e in ("node_computed", "node_cancelled")]
        assert len(terminal) == 1, f"{leaf.path}: {outcomes}"

    submitted = _by_event(events, "node_submitted")
    assert {e["node"] for e in submitted} == {str(n.path) for n in leaves}  # combined never submitted
    assert _by_event(events, "run_finished") == []


def test_run_recorder_end_to_end_matches_manual_events(tmp_path):
    """RunRecorder attached around a real run() produces a to_mermaid() that
    round-trips through to_markdown() and lists every node."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    recorder = RunRecorder()
    with recorder.attach():
        run(joined, max_workers=1)

    mermaid = recorder.to_mermaid()
    assert mermaid.startswith("flowchart LR\n")
    for node in (users_raw, orders_raw, orders_clean, joined):
        assert node.path.name in mermaid  # labels are path.name, not full path (§7)
    assert ":::computed" in mermaid

    report = recorder.to_markdown()
    assert "status: ok" in report
    assert mermaid in report
