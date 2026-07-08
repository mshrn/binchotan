import os
import threading
import time
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from moktan import Node, PipelineError, run


def _linear_three(tmp_path: Path) -> tuple[Node, Node, Node, dict[str, int]]:
    calls = {"a": 0, "b": 0, "c": 0}

    def make_a() -> pl.DataFrame:
        calls["a"] += 1
        return pl.DataFrame({"x": [1]})

    def make_b(a: pl.DataFrame) -> pl.DataFrame:
        calls["b"] += 1
        return a.with_columns((pl.col("x") + 1).alias("x"))

    def make_c(b: pl.DataFrame) -> pl.DataFrame:
        calls["c"] += 1
        return b.with_columns((pl.col("x") + 1).alias("x"))

    node_a = Node(tmp_path / "a.parquet", make_a)
    node_b = Node(tmp_path / "b.parquet", make_b, deps={"a": node_a})
    node_c = Node(tmp_path / "c.parquet", make_c, deps={"b": node_b})
    return node_a, node_b, node_c, calls


def test_linear_three_nodes_recompute_then_skip(tmp_path):
    node_a, node_b, node_c, calls = _linear_three(tmp_path)

    df1 = run(node_c)
    assert df1["x"].to_list() == [3]
    assert calls == {"a": 1, "b": 1, "c": 1}

    df2 = run(node_c)
    assert df2["x"].to_list() == [3]
    assert calls == {"a": 1, "b": 1, "c": 1}


def test_diamond_shared_dep_called_once(tmp_path):
    calls = {"base": 0}

    def make_base() -> pl.DataFrame:
        calls["base"] += 1
        return pl.DataFrame({"x": [1]})

    def left(base: pl.DataFrame) -> pl.DataFrame:
        return base.with_columns((pl.col("x") * 2).alias("x"))

    def right(base: pl.DataFrame) -> pl.DataFrame:
        return base.with_columns((pl.col("x") * 3).alias("x"))

    def combine(left: pl.DataFrame, right: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame({"sum": [left["x"][0] + right["x"][0]]})

    base = Node(tmp_path / "base.parquet", make_base)
    left_n = Node(tmp_path / "left.parquet", left, deps={"base": base})
    right_n = Node(tmp_path / "right.parquet", right, deps={"base": base})
    combined = Node(
        tmp_path / "combined.parquet", combine, deps={"left": left_n, "right": right_n}
    )

    df = run(combined)
    assert df["sum"].to_list() == [5]
    assert calls["base"] == 1


def test_stale_propagates_downstream_only(tmp_path):
    node_a, node_b, node_c, calls = _linear_three(tmp_path)

    run(node_c)
    assert calls == {"a": 1, "b": 1, "c": 1}

    node_b.path.unlink()
    run(node_c)
    assert calls == {"a": 1, "b": 2, "c": 2}


def test_mtime_triggers_recompute(tmp_path):
    node_a, node_b, node_c, calls = _linear_three(tmp_path)

    run(node_c)
    assert calls == {"a": 1, "b": 1, "c": 1}

    future = time.time() + 10
    os.utime(node_a.path, (future, future))
    run(node_c)
    assert calls == {"a": 1, "b": 2, "c": 2}


def test_root_fresh_skips_recompute_and_loads_root_only(tmp_path, monkeypatch):
    node_a, node_b, node_c, calls = _linear_three(tmp_path)
    run(node_c)
    assert calls == {"a": 1, "b": 1, "c": 1}

    read_calls: list[Path] = []
    original_read_parquet = pl.read_parquet

    def counting_read_parquet(path: Path, *args: Any, **kwargs: Any) -> pl.DataFrame:
        read_calls.append(path)
        return original_read_parquet(path, *args, **kwargs)

    monkeypatch.setattr(pl, "read_parquet", counting_read_parquet)

    df = run(node_c)
    assert df["x"].to_list() == [3]
    assert calls == {"a": 1, "b": 1, "c": 1}
    assert read_calls == [node_c.path]


def test_failing_node_preserves_existing_artifact(tmp_path):
    def make_a() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    should_fail = {"flag": False}

    def make_b(a: pl.DataFrame) -> pl.DataFrame:
        if should_fail["flag"]:
            raise RuntimeError("boom")
        return a.with_columns((pl.col("x") + 1).alias("x"))

    node_a = Node(tmp_path / "a.parquet", make_a)
    node_b = Node(tmp_path / "b.parquet", make_b, deps={"a": node_a})

    run(node_b)
    original = pl.read_parquet(node_b.path)

    should_fail["flag"] = True
    with pytest.raises(PipelineError) as exc_info:
        run(node_b, force=True)

    assert exc_info.value.node is node_b
    assert isinstance(exc_info.value.__cause__, RuntimeError)
    tmp_file = node_b.path.with_name(node_b.path.name + ".tmp")
    assert not tmp_file.exists()
    assert pl.read_parquet(node_b.path).equals(original)


def test_resume_after_failure_skips_upstream_successes(tmp_path):
    calls = {"a": 0, "b": 0}
    should_fail = {"flag": True}

    def make_a() -> pl.DataFrame:
        calls["a"] += 1
        return pl.DataFrame({"x": [1]})

    def make_b(a: pl.DataFrame) -> pl.DataFrame:
        calls["b"] += 1
        if should_fail["flag"]:
            raise RuntimeError("boom")
        return a

    node_a = Node(tmp_path / "a.parquet", make_a)
    node_b = Node(tmp_path / "b.parquet", make_b, deps={"a": node_a})

    with pytest.raises(PipelineError):
        run(node_b)
    assert calls == {"a": 1, "b": 1}

    should_fail["flag"] = False
    run(node_b)
    assert calls == {"a": 1, "b": 2}


def _verbs_by_path(caplog: pytest.LogCaptureFixture) -> dict[str, list[str]]:
    verbs: dict[str, list[str]] = {}
    for record in caplog.records:
        verb, path = record.getMessage().split()[:2]
        verbs.setdefault(path, []).append(verb)
    return verbs


@pytest.mark.parametrize("max_workers", [1, 2])
def test_each_node_logs_exactly_one_line(tmp_path, caplog, max_workers):
    """spec §8: every node logs exactly one computed/loaded/skipped line, on
    both the fresh-root early-return path and the partial-resume path."""
    node_a, node_b, node_c, calls = _linear_three(tmp_path)
    run(node_c)
    assert calls == {"a": 1, "b": 1, "c": 1}

    with caplog.at_level("INFO", logger="moktan"):
        run(node_c, max_workers=max_workers)
    assert _verbs_by_path(caplog) == {
        str(node_a.path): ["skipped"],
        str(node_b.path): ["skipped"],
        str(node_c.path): ["loaded"],
    }

    caplog.clear()
    node_c.path.unlink()
    with caplog.at_level("INFO", logger="moktan"):
        run(node_c, max_workers=max_workers)
    assert _verbs_by_path(caplog) == {
        str(node_a.path): ["skipped"],
        str(node_b.path): ["loaded"],
        str(node_c.path): ["computed"],
    }


@pytest.mark.parametrize("max_workers", [0, -1])
def test_max_workers_below_one_raises_before_any_work(tmp_path, max_workers):
    calls = {"a": 0}

    def make_a() -> pl.DataFrame:
        calls["a"] += 1
        return pl.DataFrame({"x": [1]})

    node_a = Node(tmp_path / "a.parquet", make_a)

    # stale root: must raise before Pass 1 runs anything
    with pytest.raises(ValueError):
        run(node_a, max_workers=max_workers)
    assert calls == {"a": 0}

    # fresh root: must raise too, not silently succeed via the early return
    run(node_a)
    assert calls == {"a": 1}
    with pytest.raises(ValueError):
        run(node_a, max_workers=max_workers)
    assert calls == {"a": 1}


def test_escaping_exception_cancels_not_yet_started_futures(tmp_path, monkeypatch):
    """Regression test: if an exception escapes the parallel completion loop
    (e.g. KeyboardInterrupt, or a bug elsewhere), queued-but-not-yet-started
    futures must be cancelled rather than left for the executor's shutdown to
    run to completion. Previously, `with ThreadPoolExecutor(...)` would call
    shutdown(wait=True) with nothing cancelled, so every queued leaf ran to
    completion regardless of how early the exception fired.
    """
    n = 10
    started: list[int] = []
    started_lock = threading.Lock()

    def make_leaf(i: int):
        def f() -> pl.DataFrame:
            with started_lock:
                started.append(i)
            time.sleep(0.15)
            return pl.DataFrame({"v": [i]})

        return f

    def combine(**dep_dfs: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame({"v": [sum(df["v"][0] for df in dep_dfs.values())]})

    leaves = {f"n{i}": Node(tmp_path / f"leaf{i}.parquet", make_leaf(i)) for i in range(n)}
    root = Node(tmp_path / "root.parquet", combine, deps=leaves)

    import moktan.runner as runner_module

    original_finish_node = runner_module._finish_node
    raised = {"flag": False}

    def flaky_finish_node(*args: Any, **kwargs: Any) -> None:
        if not raised["flag"]:
            raised["flag"] = True
            raise KeyboardInterrupt("simulated escape")
        return original_finish_node(*args, **kwargs)

    monkeypatch.setattr(runner_module, "_finish_node", flaky_finish_node)

    with pytest.raises(KeyboardInterrupt):
        run(root, max_workers=2)

    assert len(started) < n


def test_multiple_parallel_failures_report_all_nodes(tmp_path):
    # Barrier forces all 4 leaf tasks to actually start (and be past the point
    # where ThreadPoolExecutor could still cancel them) before any of them
    # raises, so the test doesn't depend on OS thread-scheduling timing.
    barrier = threading.Barrier(4)

    def make_bad(i: int):
        def f() -> pl.DataFrame:
            barrier.wait()
            raise RuntimeError(f"boom-{i}")

        return f

    def combine(**dep_dfs: pl.DataFrame) -> pl.DataFrame:
        return next(iter(dep_dfs.values()))

    bad_nodes = [Node(tmp_path / f"bad{i}.parquet", make_bad(i)) for i in range(4)]
    root = Node(
        tmp_path / "root.parquet",
        combine,
        deps={f"n{i}": node for i, node in enumerate(bad_nodes)},
    )

    with pytest.raises(PipelineError) as exc_info:
        run(root, max_workers=4)

    err = exc_info.value
    assert err.node in bad_nodes
    assert isinstance(err.__cause__, RuntimeError)
    assert set(err.__notes__) == {
        f"also failed: {node.path}" for node in bad_nodes if node is not err.node
    }
