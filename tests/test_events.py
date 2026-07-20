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

from conftest import MOKTAN_TIMESTAMP_RE, AppendFailsForEvent, assert_subprocess_silent
from moktan import PipelineError, RunRecorder, run
from moktan.events import _LINE_BREAK_ESCAPES, RunContext, _emit, _register, _unregister
from moktan.events import moktan_event as _raw_moktan_event
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


def test_node_failed_console_line_escapes_quotes_and_newlines_in_message(caplog_moktan, ctx, tmp_path):
    """§1.6 (rev3): embedded `"` or newlines in a free-form field (e.g. an
    exception message) must not break the one-event-one-line console
    contract or produce unbalanced quoting."""
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(
        ctx,
        "node_failed",
        logging.ERROR,
        node=node,
        error="RuntimeError",
        message='unexpected "null"\nin order_id',
    )
    message = _sole_message(caplog_moktan)
    assert "\n" not in message
    assert message == (
        f"node_failed node={node.path} error=RuntimeError "
        f'message="unexpected \\"null\\"\\nin order_id" '
        f"thread=MainThread run_id={ctx.run_id}"
    )


def test_console_value_quoted_when_it_contains_bare_equals(caplog_moktan, ctx, tmp_path):
    """A value containing `=` unquoted would look like an extra key=value
    pair to a naive parser."""
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_failed", logging.ERROR, node=node, error="ValueError", message="a=b")
    assert _sole_message(caplog_moktan) == (
        f'node_failed node={node.path} error=ValueError message="a=b" '
        f"thread=MainThread run_id={ctx.run_id}"
    )


@pytest.mark.parametrize("raw,escaped", list(_LINE_BREAK_ESCAPES.items()))
def test_legacy_verb_console_line_escapes_line_breaks_in_node_path(
    caplog_moktan, ctx, tmp_path, raw, escaped
):
    """§1.2 (rev4) / rev5 §1.3 / rev6 §1.4: a node path is spliced bare into
    the legacy-verb head (unlike other fields, it can't be wrapped in quotes
    without breaking the split()[:2] back-compat contract), so any character
    str.splitlines() treats as a line break must still be neutralized to
    preserve the one-event-one-line console contract. Parametrized directly
    over _LINE_BREAK_ESCAPES so a future addition to that dict is
    automatically exercised here instead of needing a new clone test."""
    node = Node(tmp_path / f"weird{raw}name.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_skipped", logging.INFO, node=node)
    message = _sole_message(caplog_moktan)
    assert raw not in message
    escaped_path = str(node.path).replace(raw, escaped)
    assert message == f"skipped {escaped_path} thread=MainThread run_id={ctx.run_id}"


@pytest.mark.parametrize("raw", list(_LINE_BREAK_ESCAPES))
def test_scalar_field_line_breaks_render_as_json_quoted(caplog_moktan, ctx, tmp_path, raw):
    """rev5 §1.3 / rev6 §1.4 / rev7 §2.1: any splitlines boundary char in a
    scalar field triggers json.dumps quoting (independent oracle: json.dumps
    itself), keeping the one-event-one-line contract. Parametrized over
    _LINE_BREAK_ESCAPES so dict additions are exercised automatically."""
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_failed", logging.ERROR, node=node, error="RuntimeError", message=f"a{raw}b")
    message = _sole_message(caplog_moktan)
    assert raw not in message
    assert len(message.splitlines()) == 1
    assert message == (
        f"node_failed node={node.path} error=RuntimeError message={json.dumps(f'a{raw}b')} "
        f"thread=MainThread run_id={ctx.run_id}"
    )


@pytest.mark.parametrize("raw", list(_LINE_BREAK_ESCAPES))
def test_deps_element_line_breaks_render_as_json_quoted(caplog_moktan, ctx, tmp_path, raw):
    """rev7 §2.1: same contract for list elements (previously only \\n/\\r
    were covered anywhere for the deps position)."""
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_planned", logging.DEBUG, node=node, decision="compute",
          reason="dep_stale", deps=[f"out/a{raw}b.parquet"])
    message = _sole_message(caplog_moktan)
    assert raw not in message
    assert len(message.splitlines()) == 1
    assert message == (
        f"node_planned node={node.path} decision=compute reason=dep_stale "
        f"deps=[{json.dumps(f'out/a{raw}b.parquet')}] thread=MainThread run_id={ctx.run_id}"
    )


def test_node_planned_console_line_escapes_quotes_and_newlines_in_deps(caplog_moktan, ctx, tmp_path):
    """§1.3 (rev4): unlike a bare scalar, a list element (deps path) is
    always quoted, but the quote style must switch to an escaping form when
    the element itself contains a `"` or newline, or the one-line console
    contract / quote balance would break."""
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(
        ctx,
        "node_planned",
        logging.DEBUG,
        node=node,
        decision="compute",
        reason="dep_stale",
        deps=['out/weird"raw\n.parquet'],
    )
    message = _sole_message(caplog_moktan)
    assert "\n" not in message
    assert message == (
        f"node_planned node={node.path} decision=compute reason=dep_stale "
        'deps=["out/weird\\"raw\\n.parquet"] thread=MainThread run_id=' + ctx.run_id
    )


def test_node_planned_console_line_deps_without_escaping_still_matches_repr(
    caplog_moktan, ctx, tmp_path
):
    """No-regression check: a deps element that needs no escaping keeps the
    plain single-quoted repr() form (matching the existing §12 golden
    examples) rather than switching to json.dumps()'s double quotes."""
    node = Node(tmp_path / "orders_clean.parquet", lambda: pl.DataFrame())
    _emit(
        ctx,
        "node_planned",
        logging.DEBUG,
        node=node,
        decision="compute",
        reason="dep_stale",
        deps=["out/orders_raw.parquet", "out/other.parquet"],
    )
    assert _sole_message(caplog_moktan) == (
        f"node_planned node={node.path} decision=compute reason=dep_stale "
        "deps=['out/orders_raw.parquet', 'out/other.parquet'] thread=MainThread run_id="
        + ctx.run_id
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
    assert MOKTAN_TIMESTAMP_RE.match(event["timestamp"])


def test_recorder_dispatch_is_independent_of_logging_level(ctx, tmp_path, moktan_logger_state):
    """RunRecorder-style sinks must receive DEBUG events even when the stdlib
    logger is configured above DEBUG (spec §2)."""
    moktan_logger_state.setLevel(logging.ERROR)
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


def test_run_id_is_12_hex_chars_and_unique_per_run():
    from moktan.events import new_run_id

    a = new_run_id()
    b = new_run_id()
    assert re.match(r"^[0-9a-f]{12}$", a)
    assert re.match(r"^[0-9a-f]{12}$", b)
    assert a != b


def test_configure_logging_json_lines_output(tmp_path, ctx, moktan_logger_state):
    """§2.1 (rev4/rev5): uses the shared moktan_logger_state fixture (rather
    than assuming the logger starts empty) so teardown never sweeps up the
    permanent NullHandler events.py installs at import time -- that would
    make other tests order-dependent on this one having run first."""
    from moktan.events import configure_logging

    logger = moktan_logger_state
    json_path = tmp_path / "moktan.jsonl"
    configure_logging(json_path=json_path, console=False)
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
    for handler in logger.handlers:
        handler.flush()

    lines = json_path.read_text().strip().splitlines()
    assert len(lines) == 1
    payload = json.loads(lines[0])
    assert payload["event"] == "node_computed"
    assert payload["node"] == str(node.path)
    assert payload["run_id"] == ctx.run_id


def test_configure_logging_is_idempotent(tmp_path, ctx, moktan_logger_state):
    """§1.5 (rev3): calling configure_logging() twice must replace, not
    stack, handlers -- otherwise every event prints/writes N times.

    §2.1 (rev4/rev5): counts handlers relative to a before-snapshot (the
    shared moktan_logger_state fixture), not an absolute "== 1", so this
    doesn't depend on whether some earlier test left the permanent
    NullHandler (installed at import, events.py) in place or already
    stripped it -- this test previously only passed by accident of
    file-level test ordering (verified: failed when run alone).
    """
    from moktan.events import configure_logging

    logger = moktan_logger_state
    json_path = tmp_path / "moktan.jsonl"
    handlers_before = set(logger.handlers)
    configure_logging(json_path=json_path, console=False)
    configure_logging(json_path=json_path, console=False)  # re-configure, same target
    assert len(set(logger.handlers) - handlers_before) == 1

    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
    for handler in logger.handlers:
        handler.flush()

    lines = json_path.read_text().strip().splitlines()
    assert len(lines) == 1  # not 2


def test_library_is_silent_without_configure_logging(capsys, ctx, tmp_path):
    """No configure_logging() call, no app handlers -> no stdout/stderr output."""
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_computed", logging.INFO, node=node, duration_s=0.1, rows=1, columns=1, bytes=1)
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_library_is_silent_for_error_level_events_too(tmp_path):
    """Without a handler, WARNING+ records (node_failed/run_failed are ERROR)
    must not fall through to logging.lastResort and print to stderr -- that's
    what makes "no handler configured" actually mean "silent" (§1, §9-10).

    Run in a subprocess: pytest's own logging plugin keeps at least one
    handler attached to the root logger for the whole session, which means
    `logging.lastResort` never fires inside the test process regardless of
    whether moktan's NullHandler fix is present -- capsys alone cannot catch
    this class of regression. A subprocess inherits the real OS-level stderr,
    unaffected by pytest's capture.
    """
    script = (
        "from moktan.events import RunContext, _emit\n"
        "import logging\n"
        "ctx = RunContext(run_id='deadbeefcafe')\n"
        "_emit(ctx, 'node_failed', logging.ERROR, error='RuntimeError', message='boom')\n"
        "_emit(ctx, 'run_failed', logging.ERROR, status='failed', duration_s=0.1, failed=[])\n"
    )
    assert_subprocess_silent(script)


def test_structlog_global_config_does_not_affect_moktan(caplog_moktan, ctx, tmp_path):
    """§1: moktan's wrapped logger must be independent of an application's own
    structlog.configure() -- both the "DEBUG events silently vanish" and the
    "every _emit crashes" failure modes are real without wrapper_class/
    context_class pinned explicitly (reproduced against structlog 26.1)."""
    import structlog

    node = Node(tmp_path / "x.parquet", lambda: pl.DataFrame())
    for wrapper_class in (
        structlog.make_filtering_bound_logger(logging.INFO),
        structlog.BoundLogger,
    ):
        structlog.configure(wrapper_class=wrapper_class)
        try:
            caplog_moktan.clear()
            _emit(ctx, "node_planned", logging.DEBUG, node=node, decision="compute", reason="missing", deps=[])
            assert len(caplog_moktan.records) == 1, wrapper_class
        finally:
            structlog.reset_defaults()


class _AlwaysRaisingFilter(logging.Filter):
    """アプリが "moktan" ロガーに取り付けた壊れた logging.Filter のモデル。
    CPython の Logger.filter()/Handler.handle() は Filter の例外を一切吸収しない。"""

    def filter(self, record: logging.LogRecord) -> bool:
        raise RuntimeError("broken app filter")


class _RaisesOnPlainRecords(logging.Filter):
    """moktan 自身のイベントレコード(moktan_event 属性あり)は通すが、素のレコード
    (_dispatch のシンク失敗 warning がまさにこれ)で raise する Filter。
    「moktan のレコードだけ想定してアプリがフィルタを書いた」現実的なケースのモデル。"""

    def filter(self, record: logging.LogRecord) -> bool:
        if _raw_moktan_event(record) is None:
            raise RuntimeError("app filter chokes on non-event records")
        return True


def test_broken_app_filter_does_not_fail_a_successful_run(tmp_path, moktan_logger_state):
    """§1.1-a (rev6): 壊れた Filter を "moktan" ロガーに付けても、成功する run() は
    df を返し、例外は漏れず、シンク(Filter と無関係な経路)は全イベントを受け取る。"""
    moktan_logger_state.setLevel(logging.DEBUG)  # stdlib 経路を確実に通す
    broken_filter = _AlwaysRaisingFilter()
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame({"x": [1]}))
    recorder = RunRecorder()

    moktan_logger_state.addFilter(broken_filter)
    try:
        with recorder.attach():
            df = run(node, max_workers=1)
    finally:
        moktan_logger_state.removeFilter(broken_filter)

    assert df["x"].to_list() == [1]
    assert [e["event"] for e in recorder.events] == [
        "run_started",
        "plan_computed",
        "node_planned",
        "node_computed",
        "run_finished",
    ]


def test_broken_app_filter_does_not_replace_pipeline_error(tmp_path, moktan_logger_state):
    """§1.1-b (rev6): 壊れた Filter があっても、失敗する run() の呼び出し元に見える例外は
    PipelineError のまま(Filter の例外に置換されない)。"""
    moktan_logger_state.setLevel(logging.DEBUG)
    broken_filter = _AlwaysRaisingFilter()

    def make_bad() -> pl.DataFrame:
        raise RuntimeError("boom")

    node = Node(tmp_path / "a.parquet", make_bad)

    moktan_logger_state.addFilter(broken_filter)
    try:
        with pytest.raises(PipelineError) as exc_info:
            run(node, force=True, max_workers=1)
    finally:
        moktan_logger_state.removeFilter(broken_filter)
    assert exc_info.value.node is node


def test_broken_sink_plus_broken_filter_does_not_fail_a_successful_run(
    tmp_path, moktan_logger_state
):
    """§1.1-c (rev6): 壊れたシンク + 「素のレコードでだけ壊れる Filter」の複合。
    シンク失敗を通知する warning(素のレコード)が Filter で raise しても、
    run() の成否に影響しない。rev5 の分離機構自体の例外安全性を固定する。"""
    moktan_logger_state.setLevel(logging.DEBUG)
    plain_record_filter = _RaisesOnPlainRecords()
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame({"x": [1]}))
    broken_sink = RunRecorder(events=AppendFailsForEvent("run_finished"))

    moktan_logger_state.addFilter(plain_record_filter)
    try:
        with broken_sink.attach():
            df = run(node, max_workers=1)
    finally:
        moktan_logger_state.removeFilter(plain_record_filter)

    assert df["x"].to_list() == [1]
    # シンクは run_finished 以外を通常どおり受け取っている
    assert [e["event"] for e in broken_sink.events] == [
        "run_started",
        "plan_computed",
        "node_planned",
        "node_computed",
    ]


# §2.1 (rev6) の「シンク失敗 warning はシンク型名を含む」契約は、
# test_logging_examples.py の _assert_broken_sink_isolated が全パラメトライズ
# ケースでピンする(rev7 レビューで単発テストから統合)。
