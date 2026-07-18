"""Shared test helpers."""

from __future__ import annotations

import logging
import subprocess
import sys
from pathlib import Path
from typing import Any

import polars as pl

from moktan.events import moktan_event as _moktan_event
from moktan.node import Node


def assert_subprocess_silent(script: str) -> None:
    """Run ``script`` in a fresh child process and assert it produced no
    stdout/stderr output. Used by the "library is silent without
    configure_logging()" regression tests: pytest's own logging plugin keeps
    a handler attached to the root logger for the whole session, so
    ``logging.lastResort`` never fires inside the test process itself --
    capsys can't observe that regression class, but a subprocess inherits the
    real OS-level stderr, unaffected by pytest's capture."""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)
    assert result.stdout == ""
    assert result.stderr == ""


def moktan_event(record: logging.LogRecord) -> dict[str, Any]:
    """Non-optional wrapper around ``moktan.events.moktan_event`` for tests:
    every record captured in these test files was emitted by moktan, so a
    ``None`` here means the test itself is broken -- fail loudly rather than
    propagating ``Optional`` through every caller."""
    event = _moktan_event(record)
    assert event is not None, "record was not emitted by moktan"
    return event


def linear_three(tmp_path: Path) -> tuple[Node, Node, Node, dict[str, int]]:
    """a -> b -> c, each ``+1``-ing an ``x`` column. ``calls`` counts each
    node function's invocations, for tests asserting skip/recompute behavior."""
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


def four_node_dag(tmp_path: Path) -> tuple[Node, Node, Node, Node]:
    """designdoc/flume_logging_spec.md §12.1's DAG: users_raw, orders_raw ->
    orders_clean -> joined. Real (non-trivial) row/column counts so §12's
    node_computed rows/columns/bytes fields are exercised meaningfully."""

    def make_users_raw() -> pl.DataFrame:
        return pl.DataFrame({"id": range(500), "name": ["u"] * 500, "email": ["e"] * 500})

    def make_orders_raw() -> pl.DataFrame:
        return pl.DataFrame(
            {"id": range(1200), "user_id": [1] * 1200, "amount": [1] * 1200, "d": [1] * 1200}
        )

    def make_orders_clean(orders: pl.DataFrame) -> pl.DataFrame:
        return orders.head(1180)

    def make_joined(users: pl.DataFrame, orders: pl.DataFrame) -> pl.DataFrame:
        return orders.with_columns(
            pl.lit(1).alias("a"), pl.lit(1).alias("b"), pl.lit(1).alias("c")
        )

    users_raw = Node(tmp_path / "users_raw.parquet", make_users_raw)
    orders_raw = Node(tmp_path / "orders_raw.parquet", make_orders_raw)
    orders_clean = Node(
        tmp_path / "orders_clean.parquet", make_orders_clean, deps={"orders": orders_raw}
    )
    joined = Node(
        tmp_path / "joined.parquet", make_joined, deps={"users": users_raw, "orders": orders_clean}
    )
    return users_raw, orders_raw, orders_clean, joined
