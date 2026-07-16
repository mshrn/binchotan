"""Integration tests for designdoc/flume_logging_spec.md §12: each test drives
a real `run()` end-to-end and checks the resulting event stream against the
case it corresponds to. Where §12 marks a case as non-golden (12.3, 12.8 --
timing-dependent), the assertions check structural/count invariants instead
of exact sequences.
"""

import json
import logging
import os
import re
import time

import polars as pl
import pytest

from conftest import four_node_dag as _four_node_dag
from conftest import moktan_event
from moktan import Node, PipelineError, RunRecorder, run


def _capture(caplog: pytest.LogCaptureFixture, level: int = logging.INFO):
    caplog.set_level(level, logger="moktan")
    return caplog


def _events(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [moktan_event(r) for r in caplog.records]


def _by_event(events: list[dict], name: str) -> list[dict]:
    return [e for e in events if e["event"] == name]


COMPUTED_LINE_RE = re.compile(r"^computed (?P<path>\S+) \((?P<dur>\d+\.\d{2})s\) (?P<kv>.+)$")


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
    m = COMPUTED_LINE_RE.match(joined_record.getMessage())
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


def test_fresh_root_load_failure_emits_node_failed(tmp_path, caplog):
    """§1.2 (rev3): a corrupted-but-fresh root must go through the normal
    node_failed path, not just run_failed -- otherwise RunRecorder's Failure
    section has nowhere to pull error/message from."""
    _capture(caplog, level=logging.DEBUG)

    node = Node(tmp_path / "root.parquet", lambda: pl.DataFrame({"x": [1]}))
    run(node, max_workers=1)  # write a real, valid parquet once
    node.path.write_bytes(b"not a parquet file")  # corrupt it in place, mtime unchanged

    caplog.clear()
    recorder = RunRecorder()
    with recorder.attach(), pytest.raises(PipelineError) as exc_info:
        run(node, max_workers=1)
    assert exc_info.value.node is node
    assert exc_info.value.failed == [node]

    events = _events(caplog)
    planned = _by_event(events, "node_planned")[0]
    assert planned["decision"] == "load"
    assert planned["reason"] == "fresh"

    failed = _by_event(events, "node_failed")
    assert len(failed) == 1
    assert failed[0]["node"] == str(node.path)

    run_failed = _by_event(events, "run_failed")[0]
    assert run_failed["failed"] == [str(node.path)]

    report = recorder.to_markdown()
    assert "## Failure" in report
    assert str(node.path) in report


def test_run_started_pairs_with_run_failed_even_for_non_pipeline_error(tmp_path, caplog, monkeypatch):
    """§1.3 (rev3): an exception that isn't PipelineError (simulated here by
    monkeypatching _plan to raise) still closes the run with run_failed, so
    run_started never dangles unpaired."""
    import moktan.runner as runner_module

    _capture(caplog)
    node = Node(tmp_path / "x.parquet", lambda: pl.DataFrame({"x": [1]}))

    def broken_plan(graph: object, root: object, *, force: bool) -> object:
        raise OSError("simulated permission error")

    monkeypatch.setattr(runner_module, "_plan", broken_plan)
    with pytest.raises(OSError):
        run(node, max_workers=1)

    events = _events(caplog)
    assert [e["event"] for e in events] == ["run_started", "run_failed"]
    assert events[1]["error"] == "OSError"
    assert events[1]["message"] == "simulated permission error"
    assert events[1]["failed"] == []


def test_graph_validation_error_emits_no_events_at_all(tmp_path, caplog):
    """§1.3 (rev3): a cyclic DAG fails before run_started is ever emitted --
    graph validation isn't part of "the run" (no dangling run_started)."""
    from moktan import CycleError

    _capture(caplog, level=logging.DEBUG)

    def fa() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def fb(x: pl.DataFrame) -> pl.DataFrame:
        return x

    a = Node(tmp_path / "a.parquet", fa)
    b = Node(tmp_path / "b.parquet", fb, deps={"x": a})
    object.__setattr__(a, "deps", {"x": b})

    with pytest.raises(CycleError):
        run(b)

    assert _events(caplog) == []


def test_failing_run_is_silent_without_configure_logging(tmp_path):
    """§1.4/§9-10: a run() that FAILS (node_failed/run_failed are ERROR) must
    stay silent absent configure_logging()/an app handler. Run in a
    subprocess -- pytest's own logging plugin keeps a handler attached to the
    root logger for the whole session, so logging.lastResort never fires
    inside the test process itself; capsys can't observe this regression
    class (see tests/test_events.py::test_library_is_silent_for_error_level_events_too)."""
    import subprocess
    import sys

    script = (
        "from moktan import Node, PipelineError, run\n"
        "def make_bad():\n"
        "    raise RuntimeError('boom')\n"
        f"node = Node(__import__('pathlib').Path({str(tmp_path / 'bad.parquet')!r}), make_bad)\n"
        "try:\n"
        "    run(node, max_workers=1)\n"
        "except PipelineError:\n"
        "    pass\n"
    )
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.stdout == ""
    assert result.stderr == ""


def test_run_id_consistent_across_max_workers_4_and_differs_between_runs(tmp_path, caplog):
    """§9-2 (verbatim): with max_workers=4, every event (including ones
    emitted from worker threads) shares one run_id; two consecutive run()
    calls get different run_ids."""
    _capture(caplog)
    leaves = {f"n{i}": Node(tmp_path / f"leaf{i}.parquet", (lambda i=i: pl.DataFrame({"v": [i]}))) for i in range(4)}
    combined = Node(
        tmp_path / "combined.parquet",
        lambda **dep_dfs: pl.DataFrame({"v": [sum(df["v"][0] for df in dep_dfs.values())]}),
        deps=leaves,
    )

    run(combined, max_workers=4)
    first_run_ids = {e["run_id"] for e in _events(caplog)}
    assert len(first_run_ids) == 1  # every event this run agrees, incl. worker-thread ones

    caplog.clear()
    run(combined, max_workers=4)
    second_run_ids = {e["run_id"] for e in _events(caplog)}
    assert len(second_run_ids) == 1
    assert first_run_ids != second_run_ids  # consecutive runs never reuse a run_id


def test_jsonl_event_count_matches_console_event_count(tmp_path, caplog):
    """§9-7 (latter half, previously untested): the JSON Lines sink and the
    console sink must see the same number of events for the same run."""
    from moktan.events import configure_logging

    _capture(caplog)
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)

    json_path = tmp_path / "moktan.jsonl"
    configure_logging(json_path=json_path, console=False)
    try:
        run(joined, max_workers=1)
        for handler in logging.getLogger("moktan").handlers:
            handler.flush()
    finally:
        logger = logging.getLogger("moktan")
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logger.setLevel(logging.NOTSET)

    console_events = _events(caplog)
    jsonl_lines = json_path.read_text().strip().splitlines()
    assert len(jsonl_lines) == len(console_events)
    for line in jsonl_lines:
        json.loads(line)  # each line independently parses


def test_write_report_round_trips_to_markdown(tmp_path):
    """§7/§11 step 2 (previously untested): write_report(path) writes exactly
    what to_markdown() returns."""
    users_raw, orders_raw, orders_clean, joined = _four_node_dag(tmp_path)
    recorder = RunRecorder()
    with recorder.attach():
        run(joined, max_workers=1)

    report_path = tmp_path / "report.md"
    recorder.write_report(report_path)
    assert report_path.read_text() == recorder.to_markdown()
