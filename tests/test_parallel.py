from pathlib import Path

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from moktan import Node, run


def test_parallel_matches_sequential_wide_independent_nodes(tmp_path):
    def make_leaf(i: int):
        def f() -> pl.DataFrame:
            return pl.DataFrame({"v": [i]})

        return f

    def combine(**dep_dfs: pl.DataFrame) -> pl.DataFrame:
        total = sum(df["v"][0] for df in dep_dfs.values())
        return pl.DataFrame({"v": [total]})

    def build_root(base: Path) -> Node:
        leaves = {f"n{i}": Node(base / f"leaf{i}.parquet", make_leaf(i)) for i in range(4)}
        return Node(base / "root.parquet", combine, deps=leaves)

    seq_dir = tmp_path / "seq"
    par_dir = tmp_path / "par"
    seq_dir.mkdir()
    par_dir.mkdir()

    seq_df = run(build_root(seq_dir), max_workers=1)
    par_df = run(build_root(par_dir), max_workers=4)

    assert seq_df.equals(par_df)


@pytest.mark.parametrize("max_workers", [1, 2])
def test_resume_with_stale_root_after_fresh_chain(tmp_path, max_workers):
    """Regression test: resuming a linear a -> b -> c chain where only the root
    is missing must not stall the scheduler. Previously, in the parallel path,
    submit_ready only iterated a single get_ready() snapshot, so marking the
    non-pass2 node `a` done() unblocked `b` without ever fetching it, leaving
    `futures` empty and run() crashing with KeyError instead of computing `c`.
    The sequential leg guards the same topology through the subgraph sorter.
    """

    def make_a() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def make_b(a: pl.DataFrame) -> pl.DataFrame:
        return a.with_columns((pl.col("x") + 1).alias("x"))

    def make_c(b: pl.DataFrame) -> pl.DataFrame:
        return b.with_columns((pl.col("x") + 1).alias("x"))

    node_a = Node(tmp_path / "a.parquet", make_a)
    node_b = Node(tmp_path / "b.parquet", make_b, deps={"a": node_a})
    node_c = Node(tmp_path / "c.parquet", make_c, deps={"b": node_b})

    run(node_c, max_workers=max_workers)
    node_c.path.unlink()

    df = run(node_c, max_workers=max_workers)
    assert df["x"].to_list() == [3]


def _make_node_fn(idx: int):
    def f(**dep_dfs: pl.DataFrame) -> pl.DataFrame:
        total = idx
        for df in dep_dfs.values():
            total += df["v"][0]
        return pl.DataFrame({"v": [total]})

    return f


def _build_dag(base: Path, dep_indices: list[list[int]]) -> Node:
    nodes: list[Node] = []
    for i, deps in enumerate(dep_indices):
        dep_map = {f"d{j}": nodes[j] for j in deps}
        nodes.append(Node(base / f"n{i}.parquet", _make_node_fn(i), deps=dep_map))
    return nodes[-1]


@st.composite
def _random_dag_spec(draw: st.DrawFn) -> list[list[int]]:
    n = draw(st.integers(min_value=2, max_value=20))
    dep_indices: list[list[int]] = [[]]
    for i in range(1, n):
        candidates = list(range(i))
        max_deps = min(3, i)
        deps = draw(st.lists(st.sampled_from(candidates), max_size=max_deps, unique=True))
        dep_indices.append(deps)
    return dep_indices


@settings(max_examples=25, deadline=None)
@given(_random_dag_spec())
def test_parallel_matches_sequential_random_dag(tmp_path_factory, dep_indices):
    base = tmp_path_factory.mktemp("dag")
    seq_dir = base / "seq"
    par_dir = base / "par"
    seq_dir.mkdir()
    par_dir.mkdir()

    run(_build_dag(seq_dir, dep_indices), max_workers=1)
    run(_build_dag(par_dir, dep_indices), max_workers=4)

    for i in range(len(dep_indices)):
        seq_path = seq_dir / f"n{i}.parquet"
        par_path = par_dir / f"n{i}.parquet"
        assert seq_path.exists() == par_path.exists()
        if seq_path.exists():
            assert pl.read_parquet(seq_path).equals(pl.read_parquet(par_path))
