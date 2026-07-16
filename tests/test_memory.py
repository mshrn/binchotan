import polars as pl
import pytest

from moktan import Node
from moktan.events import RunContext
from moktan.graph import build_graph
from moktan.runner import _execute_pass2, _plan


@pytest.mark.parametrize("max_workers", [1, 4])
def test_memory_released_after_execution(tmp_path, max_workers):
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
    plan = _plan(graph, node_c, force=True)

    ctx = RunContext(run_id="test")
    cache = _execute_pass2(ctx, graph, plan, node_c, max_workers=max_workers)
    assert set(cache) == {node_c}
