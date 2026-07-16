"""Unit tests for moktan.events: _emit, console rendering, RunContext, registry.

Console line expectations are taken verbatim from designdoc/flume_logging_spec.md
§12 (the "console 出力" blocks), field ordering from §6.1/§12.0, and event field
sets from §3.
"""

import json
import logging
import re

import polars as pl
import pytest

from moktan.events import RunContext, _emit, _register, _unregister
from moktan.node import Node


@pytest.fixture
def ctx() -> RunContext:
    return RunContext(run_id="7f3a1c9e2b04")


@pytest.fixture
def caplog_moktan(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    caplog.set_level(logging.DEBUG, logger="moktan")
    return caplog


def _sole_message(caplog: pytest.LogCaptureFixture) -> str:
    assert len(caplog.records) == 1
    return caplog.records[0].getMessage()


def test_computed_console_line_matches_spec_example(caplog_moktan, ctx, tmp_path):
    node = Node(tmp_path / "joined.parquet", lambda: pl.DataFrame())
    _emit(
        ctx,
        "node_computed",
        logging.INFO,
        node=node,
        duration_s=3.42,
        rows=9812,
        columns=14,
        bytes=1048576,
    )
    assert _sole_message(caplog_moktan) == (
        f"computed {node.path} (3.42s) rows=9812 columns=14 bytes=1048576"
        f" thread=MainThread run_id={ctx.run_id}"
    )


def test_loaded_console_line_matches_spec_example(caplog_moktan, ctx, tmp_path):
    node = Node(tmp_path / "users.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_loaded", logging.INFO, node=node, duration_s=0.01, rows=1200)
    assert _sole_message(caplog_moktan) == (
        f"loaded {node.path} (0.01s) rows=1200 thread=MainThread run_id={ctx.run_id}"
    )


def test_skipped_console_line_matches_spec_example(caplog_moktan, ctx, tmp_path):
    node = Node(tmp_path / "raw.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_skipped", logging.INFO, node=node)
    assert _sole_message(caplog_moktan) == f"skipped {node.path} thread=MainThread run_id={ctx.run_id}"


def test_split_first_two_tokens_are_verb_and_path_for_legacy_events(caplog_moktan, ctx, tmp_path):
    """§6.1/§9-9 back-compat contract: split()[:2] == [verb, path]."""
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
    verb, path = caplog_moktan.records[0].getMessage().split()[:2]
    assert verb == "computed"
    assert path == str(node.path)


def test_run_started_uses_root_key_value_not_bare_token(caplog_moktan, ctx, tmp_path):
    root = Node(tmp_path / "joined.parquet", lambda: pl.DataFrame())
    _emit(ctx, "run_started", logging.INFO, root=str(root.path), force=False, max_workers=1)
    assert _sole_message(caplog_moktan) == (
        f"run_started root={root.path} force=False max_workers=1 "
        f"thread=MainThread run_id={ctx.run_id}"
    )


def test_plan_computed_console_line(caplog_moktan, ctx):
    _emit(
        ctx,
        "plan_computed",
        logging.INFO,
        n_nodes=4,
        n_compute=4,
        n_load=0,
        n_skip=0,
        duration_s=0.0,
    )
    assert _sole_message(caplog_moktan) == (
        "plan_computed n_nodes=4 n_compute=4 n_load=0 n_skip=0 duration_s=0.00 "
        f"thread=MainThread run_id={ctx.run_id}"
    )


def test_node_planned_console_line_with_deps(caplog_moktan, ctx, tmp_path):
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(
        ctx,
        "node_planned",
        logging.DEBUG,
        node=node,
        decision="compute",
        reason="dep_stale",
        deps=["out/orders_raw.parquet"],
    )
    assert _sole_message(caplog_moktan) == (
        f"node_planned node={node.path} decision=compute reason=dep_stale "
        "deps=['out/orders_raw.parquet'] thread=MainThread run_id=" + ctx.run_id
    )


def test_node_failed_console_line_quotes_message_with_spaces(caplog_moktan, ctx, tmp_path):
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(
        ctx,
        "node_failed",
        logging.ERROR,
        node=node,
        error="RuntimeError",
        message="unexpected null in order_id",
    )
    assert _sole_message(caplog_moktan) == (
        f'node_failed node={node.path} error=RuntimeError message="unexpected null in order_id" '
        f"thread=MainThread run_id={ctx.run_id}"
    )


def test_run_failed_console_line_renders_failed_list_as_repr(caplog_moktan, ctx):
    _emit(
        ctx,
        "run_failed",
        logging.ERROR,
        status="failed",
        duration_s=0.15,
        failed=["out/orders_clean.parquet"],
    )
    assert _sole_message(caplog_moktan) == (
        "run_failed status=failed duration_s=0.15 failed=['out/orders_clean.parquet'] "
        f"thread=MainThread run_id={ctx.run_id}"
    )


def test_run_finished_console_line(caplog_moktan, ctx):
    _emit(
        ctx,
        "run_finished",
        logging.INFO,
        status="ok",
        duration_s=0.44,
        n_computed=4,
        n_loaded=0,
        n_skipped=0,
    )
    assert _sole_message(caplog_moktan) == (
        "run_finished status=ok duration_s=0.44 n_computed=4 n_loaded=0 n_skipped=0 "
        f"thread=MainThread run_id={ctx.run_id}"
    )


def test_node_submitted_and_node_cancelled_console_lines(caplog_moktan, ctx, tmp_path):
    node = Node(tmp_path / "leaf4.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_submitted", logging.DEBUG, node=node)
    _emit(ctx, "node_cancelled", logging.DEBUG, node=node)
    messages = [r.getMessage() for r in caplog_moktan.records]
    assert messages == [
        f"node_submitted node={node.path} thread=MainThread run_id={ctx.run_id}",
        f"node_cancelled node={node.path} thread=MainThread run_id={ctx.run_id}",
    ]


def test_debug_events_hidden_at_info_level(caplog, ctx, tmp_path):
    caplog.set_level(logging.INFO, logger="moktan")
    node = Node(tmp_path / "x.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_planned", logging.DEBUG, node=node, decision="compute", reason="missing", deps=[])
    assert caplog.records == []


def test_emit_dispatches_full_event_dict_to_registered_sinks(ctx, tmp_path):
    sink_events: list[dict] = []

    class _Sink:
        events = sink_events

    sink = _Sink()
    _register(sink)
    try:
        node = Node(tmp_path / "x.parquet", lambda: pl.DataFrame())
        _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
    finally:
        _unregister(sink)

    assert len(sink_events) == 1
    event = sink_events[0]
    assert event["event"] == "node_computed"
    assert event["node"] == str(node.path)
    assert event["run_id"] == ctx.run_id
    assert event["duration_s"] == 0.1
    assert "timestamp" in event
    assert re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$", event["timestamp"])


def test_recorder_dispatch_is_independent_of_logging_level(ctx, tmp_path):
    """RunRecorder-style sinks must receive DEBUG events even when the stdlib
    logger is configured above DEBUG (spec §2)."""
    logging.getLogger("moktan").setLevel(logging.ERROR)
    try:
        sink_events: list[dict] = []

        class _Sink:
            events = sink_events

        sink = _Sink()
        _register(sink)
        try:
            node = Node(tmp_path / "x.parquet", lambda: pl.DataFrame())
            _emit(ctx, "node_planned", logging.DEBUG, node=node, decision="load", reason="fresh", deps=[])
        finally:
            _unregister(sink)
        assert len(sink_events) == 1
    finally:
        logging.getLogger("moktan").setLevel(logging.NOTSET)


def test_run_id_is_12_hex_chars_and_unique_per_run():
    from moktan.events import new_run_id

    a = new_run_id()
    b = new_run_id()
    assert re.match(r"^[0-9a-f]{12}$", a)
    assert re.match(r"^[0-9a-f]{12}$", b)
    assert a != b


def test_configure_logging_json_lines_output(tmp_path, ctx):
    from moktan.events import configure_logging

    json_path = tmp_path / "moktan.jsonl"
    configure_logging(json_path=json_path, console=False)
    try:
        node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
        _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
        for handler in logging.getLogger("moktan").handlers:
            handler.flush()
    finally:
        logger = logging.getLogger("moktan")
        for handler in list(logger.handlers):
            logger.removeHandler(handler)
            handler.close()
        logger.setLevel(logging.NOTSET)  # configure_logging() sets it; don't leak to other tests

    lines = json_path.read_text().strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "node_computed"
    assert payload["node"] == str(node.path)
    assert payload["run_id"] == ctx.run_id


def test_library_is_silent_without_configure_logging(capsys, ctx, tmp_path):
    """No configure_logging() call, no app handlers -> no stdout/stderr output."""
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""
