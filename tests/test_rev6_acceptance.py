"""rev6 受入テスト -- designdoc/flume_review_fixes_rev6.md の実行可能な受入基準。

このファイルは指示書の一部であり、修正内容を prose ではなくテストで拘束する。

実装者への制約:
- 各テストの本体(セットアップ・アサート)を**変更してはならない**。テストが誤っていると
  思った場合は実装を進めず、指示書の該当セクションと突き合わせて相談すること。
- 対応する修正を実装したら、そのテストの ``@pytest.mark.xfail`` デコレータ**だけ**を外す。
  ``strict=True`` なので、修正が入るとマーカーを外すまでスイートが XPASS で fail する --
  マーカーを外す行為が「この契約を満たした」という明示的な宣言になる。
- 全マーカーが外れてこのファイルが素の状態で green になったら rev6 完了。
- 修正前の red 確認は ``uv run pytest tests/test_rev6_acceptance.py --runxfail``
  (全テストが FAILED になるのが正: 2026-07-20 時点の HEAD = cefd0a5 で確認済み)。
"""

import json
import logging
import re

import polars as pl
import pytest

from conftest import AppendFailsForEvent, moktan_warnings
from moktan import Node, PipelineError, RunRecorder, run
from moktan.events import RunContext, _emit
from moktan.events import moktan_event as _raw_moktan_event

_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

# str.splitlines() が行区切りとして扱う、\n・\r 以外の全文字(rev6 §3.1)。
_EXOTIC_LINE_BREAKERS = ["\x0b", "\x0c", "\x1c", "\x1d", "\x1e", "\x85", "\u2028", "\u2029"]


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


@pytest.mark.xfail(strict=True, reason="rev6 §1.1: _emit の stdlib 経路が無保護")
def test_broken_app_filter_does_not_fail_a_successful_run(tmp_path, moktan_logger_state):
    """§1.1-a: 壊れた Filter を "moktan" ロガーに付けても、成功する run() は
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


@pytest.mark.xfail(strict=True, reason="rev6 §1.1: _emit の stdlib 経路が無保護")
def test_broken_app_filter_does_not_replace_pipeline_error(tmp_path, moktan_logger_state):
    """§1.1-b: 壊れた Filter があっても、失敗する run() の呼び出し元に見える例外は
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


@pytest.mark.xfail(strict=True, reason="rev6 §1.1: _dispatch のシンク失敗 warning 自体が無保護")
def test_broken_sink_plus_broken_filter_does_not_fail_a_successful_run(
    tmp_path, moktan_logger_state
):
    """§1.1-c: 壊れたシンク + 「素のレコードでだけ壊れる Filter」の複合。
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


@pytest.mark.xfail(strict=True, reason="rev6 §2.1: warning にシンクの型名が入っていない")
def test_sink_failure_warning_names_the_sink_type(tmp_path, caplog):
    """§2.1: シンク失敗 warning は「どのシンクが」を型名で含む
    (rev5 doc が明記した深さ)。イベント名も引き続き含む。"""
    caplog.set_level(logging.WARNING, logger="moktan")
    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame({"x": [1]}))
    broken = RunRecorder(events=AppendFailsForEvent("run_finished"))

    with broken.attach():
        run(node, max_workers=1)

    warnings = moktan_warnings(caplog)
    assert len(warnings) == 1
    message = warnings[0].getMessage()
    assert "RunRecorder" in message  # type(sink).__name__
    assert "run_finished" in message


@pytest.mark.xfail(strict=True, reason="rev6 §2.2: フォールバック行に timestamp/run_id がない")
def test_jsonl_fallback_line_has_timestamp_and_run_id(tmp_path, moktan_logger_state):
    """§2.2: _JSONFormatter のフォールバック行(event=log_message)にも、通常行と
    同形式の timestamp と、発行元の run_id が入る。"""
    from moktan.events import configure_logging

    json_path = tmp_path / "moktan.jsonl"
    configure_logging(json_path=json_path, console=False, level=logging.WARNING)

    node = Node(tmp_path / "a.parquet", lambda: pl.DataFrame({"x": [1]}))
    broken = RunRecorder(events=AppendFailsForEvent("run_finished"))
    with broken.attach():
        run(node, max_workers=1)
    for handler in moktan_logger_state.handlers:
        handler.flush()

    lines = json_path.read_text().strip().splitlines()
    assert len(lines) == 1  # WARNING レベルではフォールバック warning 行のみ
    payload = json.loads(lines[0])
    assert payload["event"] == "log_message"
    assert _TIMESTAMP_RE.match(payload["timestamp"])
    assert payload["run_id"] == broken.events[0]["run_id"]


@pytest.mark.xfail(strict=True, reason="rev6 §3.1: splitlines 文字集合の残り8文字が未エスケープ")
@pytest.mark.parametrize("ch", _EXOTIC_LINE_BREAKERS)
def test_scalar_field_with_exotic_line_breaker_stays_one_line(caplog, ch):
    """§3.1(スカラ位置): \\n・\\r 以外の splitlines 行区切り文字を含む message も
    1 イベント 1 行契約を守る(クォート/エスケープの方式は実装の自由)。"""
    caplog.set_level(logging.DEBUG, logger="moktan")
    ctx = RunContext(run_id="7f3a1c9e2b04")
    _emit(ctx, "run_failed", logging.ERROR, status="failed", duration_s=0.1,
          failed=[], error="RuntimeError", message=f"a{ch}b")
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert ch not in message
    assert len(message.splitlines()) == 1


@pytest.mark.xfail(strict=True, reason="rev6 §3.1: splitlines 文字集合の残り8文字が未エスケープ")
@pytest.mark.parametrize("ch", _EXOTIC_LINE_BREAKERS)
def test_bare_token_path_with_exotic_line_breaker_stays_one_line(caplog, tmp_path, ch):
    """§3.1(裸トークン位置): legacy verb の path に同文字が入っても 1 行を保つ。"""
    caplog.set_level(logging.DEBUG, logger="moktan")
    ctx = RunContext(run_id="7f3a1c9e2b04")
    node = Node(tmp_path / f"weird{ch}name.parquet", lambda: pl.DataFrame())
    _emit(ctx, "node_skipped", logging.INFO, node=node)
    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert ch not in message
    assert len(message.splitlines()) == 1
