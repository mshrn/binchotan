"""Shared test helpers."""

from __future__ import annotations

import logging
import re
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from moktan.events import moktan_event as _moktan_event
from moktan.node import Node

MOKTAN_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


@pytest.fixture
def moktan_logger_state() -> Iterator[logging.Logger]:
    """For a test that calls ``configure_logging()``: yields the ``"moktan"``
    logger, then restores exactly what the test itself added -- removing and
    closing only handlers not present in the before-snapshot, and resetting
    the level. events.py installs a permanent ``NullHandler`` at import time;
    a teardown that strips *all* handlers (rather than diffing against a
    snapshot) sweeps that up too and makes other tests order-dependent on
    whether this one ran first (rev4 §2.1, recurred at a second call site in
    rev5 §2.1 before being centralized here)."""
    logger = logging.getLogger("moktan")
    handlers_before = set(logger.handlers)
    yield logger
    for handler in set(logger.handlers) - handlers_before:
        logger.removeHandler(handler)
        handler.close()
    logger.setLevel(logging.NOTSET)


class AppendFailsForEvent(list):
    """A list whose .append() raises for one specific event name, so a sink
    built on it fails at exactly one emission point instead of every one.
    Used as ``RunRecorder(events=AppendFailsForEvent(...))`` by the broken-sink
    isolation tests (rev5 §1.1 / rev6 acceptance)."""

    def __init__(self, target_event: str) -> None:
        super().__init__()
        self._target_event = target_event

    def append(self, item: dict) -> None:  # type: ignore[override]
        if item.get("event") == self._target_event:
            raise RuntimeError("sink is broken")
        super().append(item)


def moktan_warnings(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    """WARNING-level records emitted by the "moktan" logger specifically.
    caplog's handler sits on the root logger and captures every propagating
    record, so filtering by level alone would count warnings from polars/other
    libraries too -- tests asserting "exactly one moktan warning" must filter
    by logger name as well (rev6 §1.2). Exactly-WARNING (not >=) because
    moktan's own node_failed/run_failed event records are ERROR and must not
    be counted as warnings."""
    return [
        r for r in caplog.records if r.name == "moktan" and r.levelno == logging.WARNING
    ]


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
    intended for records known to be moktan *events* (a ``None`` here means
    the test itself is broken -- fail loudly rather than propagating
    ``Optional`` through every caller). Not every record on the "moktan"
    logger is an event, though: ``events._dispatch``'s best-effort
    broken-sink warning is a plain record with no event payload. A test that
    maps this helper over a captured-records list containing such a record
    (e.g. a broken-sink scenario) must filter it out first."""
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
