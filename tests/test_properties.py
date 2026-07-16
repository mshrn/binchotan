"""Property-based tests over random DAGs.

Each property drives `run()` end-to-end with real file I/O and checks an
invariant the unit tests only cover for fixed topologies:

- resume: deleting an arbitrary subset of parquets recomputes exactly the
  stale closure (deleted nodes + transitive consumers), nothing else, with
  exactly one correct log line per node.
- failure resume: a failing node aborts the run, and the retry recomputes
  only what the first run didn't checkpoint (every node runs once overall,
  the failed node twice).
- memory release: after Pass 2, the cache holds only the root.
- cycles: a back edge anywhere on a path to the root raises CycleError
  before any node function runs.
"""

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import polars as pl
import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from conftest import moktan_event
from moktan import CycleError, Node, PipelineError, run
from moktan.events import RunContext
from moktan.graph import build_graph
from moktan.runner import _execute_pass2, _plan

DagSpec = list[list[int]]


@st.composite
def dag_spec(draw: st.DrawFn) -> DagSpec:
    """Random DAG as dep-index lists (deps only point to earlier indices).

    The last node is the root; every sink is wired into the root's deps so
    that all nodes are reachable from it.
    """
    n = draw(st.integers(min_value=2, max_value=15))
    dep_indices: DagSpec = [[]]
    for i in range(1, n):
        deps = draw(st.lists(st.sampled_from(range(i)), max_size=min(3, i), unique=True))
        dep_indices.append(deps)
    consumers = {j for deps in dep_indices for j in deps}
    sinks = [i for i in range(n - 1) if i not in consumers]
    dep_indices[-1] = sorted(set(dep_indices[-1]) | set(sinks))
    return dep_indices


def _build_dag(base: Path, dep_indices: DagSpec, calls: dict[int, int]) -> list[Node]:
    def make_fn(idx: int):
        def f(**dep_dfs: pl.DataFrame) -> pl.DataFrame:
            calls[idx] += 1
            total = idx + sum(df["v"][0] for df in dep_dfs.values())
            return pl.DataFrame({"v": [total]})

        return f

    nodes: list[Node] = []
    for i, deps in enumerate(dep_indices):
        dep_map = {f"d{j}": nodes[j] for j in deps}
        nodes.append(Node(base / f"n{i}.parquet", make_fn(i), deps=dep_map))
    return nodes


def _expected_values(dep_indices: DagSpec) -> list[int]:
    values: list[int] = []
    for i, deps in enumerate(dep_indices):
        values.append(i + sum(values[j] for j in deps))
    return values


def _stale_closure(dep_indices: DagSpec, deleted: set[int]) -> set[int]:
    stale: list[bool] = []
    for i, deps in enumerate(dep_indices):
        stale.append(i in deleted or any(stale[j] for j in deps))
    return {i for i, s in enumerate(stale) if s}


def _transitive_deps(dep_indices: DagSpec, v: int) -> set[int]:
    seen: set[int] = set()
    stack = list(dep_indices[v])
    while stack:
        j = stack.pop()
        if j not in seen:
            seen.add(j)
            stack.extend(dep_indices[j])
    return seen


class _ListHandler(logging.Handler):
    """Deliberately goes through the real stdlib logging path (unlike
    RunRecorder, which receives events directly via moktan.events._dispatch,
    bypassing logging entirely) -- this is what actually exercises
    structlog's wrap_logger + _render_console_message + the level gate, which
    is the thing PBT here wants to hammer with random DAGs. RunRecorder would
    only re-test the dispatch path already covered by test_recorder.py."""

    def __init__(self) -> None:
        super().__init__()
        self.events: list[dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.events.append(moktan_event(record))


@contextmanager
def _capture_moktan_log(level: int = logging.INFO) -> Iterator[list[dict]]:
    logger = logging.getLogger("moktan")
    handler = _ListHandler()
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(level)
    try:
        yield handler.events
    finally:
        logger.removeHandler(handler)
        logger.setLevel(old_level)


@settings(max_examples=30, deadline=None)
@given(
    spec=dag_spec(),
    delete_seed=st.lists(st.booleans(), min_size=15, max_size=15),
    max_workers=st.sampled_from([1, 4]),
)
def test_resume_recomputes_exactly_the_stale_closure(
    tmp_path_factory, spec, delete_seed, max_workers
):
    base = tmp_path_factory.mktemp("resume")
    calls = dict.fromkeys(range(len(spec)), 0)
    nodes = _build_dag(base, spec, calls)
    root = nodes[-1]
    root_idx = len(spec) - 1

    primed = run(root, max_workers=1)
    assert all(count == 1 for count in calls.values())

    deleted = {i for i in range(len(spec)) if delete_seed[i]}
    for i in deleted:
        nodes[i].path.unlink()

    stale = _stale_closure(spec, deleted)
    load_targets = {j for i in stale for j in spec[i] if j not in stale}

    with _capture_moktan_log(level=logging.DEBUG) as events:
        resumed = run(root, max_workers=max_workers)

    # value and file invariants
    assert resumed.equals(primed)
    values = _expected_values(spec)
    for i, node in enumerate(nodes):
        assert node.path.exists()
        assert pl.read_parquet(node.path)["v"].to_list() == [values[i]]
        assert calls[i] == 1 + (1 if i in stale else 0), f"node {i}"

    # event invariant: exactly one terminal INFO event per node, with the
    # right event name, and node_planned's reason/decision agree with it.
    terminal_events: dict[str, list[str]] = {}
    planned: dict[str, dict] = {}
    for event in events:
        if "node" not in event:
            continue
        if event["event"] == "node_planned":
            planned[event["node"]] = event
        elif event["event"] in ("node_computed", "node_loaded", "node_skipped"):
            terminal_events.setdefault(event["node"], []).append(event["event"])

    assert set(terminal_events) == {str(node.path) for node in nodes}
    assert set(planned) == {str(node.path) for node in nodes}
    for i, node in enumerate(nodes):
        path = str(node.path)
        if i in stale:
            expected_event = "node_computed"
            expected_decision = "compute"
            assert planned[path]["reason"] in ("missing", "forced", "dep_stale")
        elif i in load_targets or (i == root_idx and not stale):
            expected_event = "node_loaded"
            expected_decision = "load"
            assert planned[path]["reason"] == "fresh"
        else:
            expected_event = "node_skipped"
            expected_decision = "skip"
            assert planned[path]["reason"] == "fresh"
        assert terminal_events[path] == [expected_event], f"node {i}"
        assert planned[path]["decision"] == expected_decision, f"node {i}"


@settings(max_examples=20, deadline=None)
@given(spec=dag_spec(), data=st.data(), max_workers=st.sampled_from([1, 4]))
def test_failed_run_checkpoints_survive_and_retry_does_no_rework(
    tmp_path_factory, spec, data, max_workers
):
    base = tmp_path_factory.mktemp("failure")
    calls = dict.fromkeys(range(len(spec)), 0)
    nodes = _build_dag(base, spec, calls)
    root = nodes[-1]

    bad = data.draw(st.sampled_from(range(len(spec))), label="failing node")
    failing = {"flag": True}
    original_f = nodes[bad].f

    def flaky(**dep_dfs: pl.DataFrame) -> pl.DataFrame:
        if failing["flag"]:
            calls[bad] += 1
            raise RuntimeError("boom")
        return original_f(**dep_dfs)

    object.__setattr__(nodes[bad], "f", flaky)

    with pytest.raises(PipelineError) as exc_info:
        run(root, max_workers=max_workers)
    assert exc_info.value.node is nodes[bad]
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    assert not nodes[bad].path.exists()
    tmp_file = nodes[bad].path.with_name(nodes[bad].path.name + ".tmp")
    assert not tmp_file.exists()

    failing["flag"] = False
    resumed = run(root, max_workers=max_workers)

    values = _expected_values(spec)
    assert resumed["v"].to_list() == [values[len(spec) - 1]]
    for i in range(len(spec)):
        expected = 2 if i == bad else 1
        assert calls[i] == expected, f"node {i}"


@settings(max_examples=20, deadline=None)
@given(
    spec=dag_spec(),
    delete_seed=st.lists(st.booleans(), min_size=15, max_size=15),
    max_workers=st.sampled_from([1, 4]),
)
def test_cache_holds_only_root_after_pass2(tmp_path_factory, spec, delete_seed, max_workers):
    base = tmp_path_factory.mktemp("mem")
    calls = dict.fromkeys(range(len(spec)), 0)
    nodes = _build_dag(base, spec, calls)
    root = nodes[-1]

    run(root, max_workers=1)
    deleted = {i for i in range(len(spec)) if delete_seed[i]}
    for i in deleted:
        nodes[i].path.unlink()

    graph = build_graph(root)
    plan = _plan(graph, root, force=False)
    assume(plan.needs_compute[root])

    ctx = RunContext(run_id="test")
    cache = _execute_pass2(ctx, graph, plan, root, max_workers=max_workers)
    assert set(cache) == {root}


@settings(max_examples=20, deadline=None)
@given(spec=dag_spec(), data=st.data())
def test_back_edge_raises_cycle_error_before_any_execution(tmp_path_factory, spec, data):
    base = tmp_path_factory.mktemp("cycle")
    calls = dict.fromkeys(range(len(spec)), 0)
    nodes = _build_dag(base, spec, calls)
    root_idx = len(spec) - 1

    candidates = [v for v in range(len(spec)) if spec[v]]
    v = data.draw(st.sampled_from(candidates), label="cycle head")
    u = data.draw(st.sampled_from(sorted(_transitive_deps(spec, v))), label="cycle tail")
    object.__setattr__(nodes[u], "deps", {**nodes[u].deps, "back": nodes[v]})

    with pytest.raises(CycleError):
        run(nodes[root_idx])
    assert all(count == 0 for count in calls.values())
