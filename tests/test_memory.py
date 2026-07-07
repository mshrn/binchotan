import polars as pl

from moktan import Node
from moktan.graph import build_graph
from moktan.runner import _determine_stale, _execute_pass2


def test_memory_released_after_execution(tmp_path):
    def make_a() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def make_b(a: pl.DataFrame) -> pl.DataFrame:
        return a.with_columns((pl.col("x") + 1).alias("x"))

    def make_c(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame({"x": [a["x"][0] + b["x"][0]]})

    node_a = Node(tmp_path / "a.parquet", make_a)
    node_b = Node(tmp_path / "b.parquet", make_b, deps={"a": node_a})
    node_c = Node(tmp_path / "c.parquet", make_c, deps={"a": node_a, "b": node_b})

    graph = build_graph(node_c)
    needs_compute = _determine_stale(graph, force=True)
    recompute_nodes = {n for n in graph.order if needs_compute[n]}
    load_targets = {
        dep
        for node in recompute_nodes
        for dep in node.deps.values()
        if not needs_compute[dep]
    }
    pass2_nodes = recompute_nodes | load_targets

    cache = _execute_pass2(graph, pass2_nodes, needs_compute, node_c, max_workers=1)
    assert set(cache) == {node_c}


def test_memory_released_after_parallel_execution(tmp_path):
    def make_a() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def make_b(a: pl.DataFrame) -> pl.DataFrame:
        return a.with_columns((pl.col("x") + 1).alias("x"))

    def make_c(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame({"x": [a["x"][0] + b["x"][0]]})

    node_a = Node(tmp_path / "a.parquet", make_a)
    node_b = Node(tmp_path / "b.parquet", make_b, deps={"a": node_a})
    node_c = Node(tmp_path / "c.parquet", make_c, deps={"a": node_a, "b": node_b})

    graph = build_graph(node_c)
    needs_compute = _determine_stale(graph, force=True)
    recompute_nodes = {n for n in graph.order if needs_compute[n]}
    load_targets = {
        dep
        for node in recompute_nodes
        for dep in node.deps.values()
        if not needs_compute[dep]
    }
    pass2_nodes = recompute_nodes | load_targets

    cache = _execute_pass2(graph, pass2_nodes, needs_compute, node_c, max_workers=4)
    assert set(cache) == {node_c}
