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


@pytest.mark.parametrize("max_workers", [1, 2])
def test_skipped_node_is_logged_exactly_once(tmp_path, caplog, max_workers):
    node_a, node_b, node_c, calls = _linear_three(tmp_path)
    run(node_c)
    assert calls == {"a": 1, "b": 1, "c": 1}

    node_c.path.unlink()
    with caplog.at_level("INFO", logger="moktan"):
        run(node_c, max_workers=max_workers)

    skip_lines = [
        record.getMessage() for record in caplog.records if "skipped" in record.getMessage()
    ]
    assert skip_lines.count(f"skipped {node_a.path}") == 1


@pytest.mark.parametrize("max_workers", [0, -1])
def test_max_workers_below_one_raises_before_any_work(tmp_path, max_workers):
    calls = {"a": 0}

    def make_a() -> pl.DataFrame:
        calls["a"] += 1
        return pl.DataFrame({"x": [1]})

    node_a = Node(tmp_path / "a.parquet", make_a)

    with pytest.raises(ValueError):
        run(node_a, max_workers=max_workers)
    assert calls == {"a": 0}


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
    failed_paths = {str(node.path) for node in bad_nodes}
    reported_paths = {str(err.node.path)} | {
        note.removeprefix("also failed: ") for note in err.__notes__
    }
    assert reported_paths == failed_paths
