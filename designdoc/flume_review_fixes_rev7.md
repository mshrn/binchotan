# flume レビュー指摘事項と改修指示(rev7)

rev6 実装(working tree、ベース `c46df8e`)に対する deep review の結果。
8 アングルで候補を収集、最重要指摘は 2 エージェント + レビュー統括の三重に実行再現済み。
本指示書は rev6 で確立したプロトコル(全意思決定済み・シンボルアンカー・貼り付け可能
コード・受入テスト同梱)に従う。

## 受入テストのプロトコル

`tests/test_rev7_acceptance.py`(2 ケース、xfail(strict=True) 付き)が §1.1 の受入基準。
- red 確認: `uv run pytest tests/test_rev7_acceptance.py --runxfail`(2 failed が正、
  rev6 実装済み working tree で確認済み)。
- テスト本体は変更禁止。実装後に xfail デコレータだけを外す。
- **新設ライフサイクル規約(§4.1)**: rev 完了時に受入テストはテーマ別ファイルへ移設し
  rev ファイルは削除する。rev6 の受入ファイルにも本 rev で遡及適用する(§2.3)。

## 0. 総括: 「経路の数え漏れ」クラス、6 世代目

rev6 は `_emit` **内部**の消費経路(シンク / stdlib)を両方ガードした。しかし
**引数の具現化は呼び出し箇所で、ガードに入る前に起きる**。`message=str(exc)` は
`_emit` に到達する前(かつ `_listening` の早期リターンよりも前)に評価されるため、
ユーザー例外の `__str__` が壊れていると:

- 逐次: `_run_sequential` の node_failed 発行で `str(exc)` が raise →
  `raise PipelineError` に到達せず、呼び出し元の `except PipelineError:` が効かない
  (ValueError 等に置換)。node_failed も消える(§8 の「ノード毎に必ず終端イベント」違反)。
- 並列: `_run_parallel` の同型箇所で同じことが起き、`except BaseException` 経由で
  run_failed は `failed=[]`・`error=ValueError` になる(真の失敗ノードが特定不能)。
- **ロギング完全未設定でも発生する**(引数評価はリスナー判定の前)。

実行再現済み(逐次・並列とも、シンク有無both)。review_notes の教訓
「_emit 内部の消費経路も数える」は正しかったが不完全で、**呼び出し箇所での
フィールド具現化も発行経路の一部**として数える必要がある。

## 1. 重大(修正必須)

### 1.1 例外の文字列化を単一の安全ヘルパーに集約する

**受入テスト**: `test_broken_str_exception_keeps_pipeline_error_sequential` /
`test_broken_str_exception_keeps_pipeline_error_parallel`

**意思決定済み**: `str(exc)` を直接書くことを禁止し、events.py の `_safe_str` に
一本化する。フォールバック書式は `<unprintable {型名}>` に固定(受入テストが
この文字列を厳密にピンする)。

**修正 1**: `events.py` の `_iso_timestamp` の直後に追加:

```python
def _safe_str(obj: object) -> str:
    """str(obj) -- unless obj's __str__ itself raises, in which case a
    placeholder is returned instead of letting the exception escape.
    Every exception-to-message conversion feeding an event field MUST go
    through this: plain str(exc) at an _emit call site is evaluated before
    any of _emit's guards run, so a user exception with a broken __str__
    would otherwise replace PipelineError with the __str__ error (rev7 §1.1).
    KeyboardInterrupt/SystemExit from __str__ still propagate.
    """
    try:
        return str(obj)
    except Exception:  # noqa: BLE001
        return f"<unprintable {type(obj).__name__}>"
```

**修正 2**: `runner.py` — import に `_safe_str` を追加:

```python
from moktan.events import Decision, Reason, RunContext, _emit, _listening, _safe_str, new_run_id
```

**修正 3**: `runner.py` の `str(exc)` 3 箇所をすべて `_safe_str(exc)` に置換する。
対象は次の 3 シンボル内(行番号ではなくシンボルで探すこと):
- `_emit_run_failed` の `message=str(exc)` → `message=_safe_str(exc)`
- `_run_sequential` の node_failed 発行の `message=str(exc)` → `message=_safe_str(exc)`
- `_run_parallel` の node_failed 発行の `message=str(exc)` → `message=_safe_str(exc)`

置換後、`grep -n "str(exc)" src/moktan/` が **0 件**になることを確認する
(`_safe_str(exc)` は grep にかからない表記なので、`bstr(exc)` 的な取りこぼしがないか
`grep -nE "[^_a-z]str\(exc\)" src/moktan/` でも確認)。

**修正 4**: `run()` docstring の「Event emission never raises (KeyboardInterrupt/
SystemExit excepted):」の直後に次の 1 文を挿入する:

```
even the stringification of user exceptions into event fields goes through
``events._safe_str`` so a broken ``__str__`` cannot replace ``PipelineError``;
```

**確認**: 受入テスト 2 本のマーカーを外し PASSED、`uv run pytest` 全体 green。

### 1.2 `_LINE_BREAK_ESCAPES` の U+2028/U+2029 キーを明示エスケープ表記へ

**場所**: `events.py` `_LINE_BREAK_ESCAPES` の末尾 2 エントリ

**問題**(4 エージェントが独立指摘): 現在この 2 キーはソース上**生の不可視文字**で、
指示書 rev6 §1.4 の規定スニペット(バックスラッシュ u 2 0 2 8 のASCIIエスケープ表記)からの無断逸脱。エディタ・
フォーマッタ・コピペが不可視文字を正規化/除去すると静かに壊れ、テストは同じ辞書から
導出されるため道連れで縮んで検出できない。

**修正**: 該当 2 行を次に置き換える(エディタで直接編集せず、確実を期すなら
`python - <<'EOF'` でバイト置換してもよい):

```python
    "\u2028": "\\u2028",   # ← ソース上は必ずこのASCIIエスケープ表記で書く
    "\u2029": "\\u2029",   # ← 同上(生の不可視文字を埋め込まない)
```

**確認**: `uv run python -c "src=open('src/moktan/events.py',encoding='utf-8').read(); assert chr(0x2028) not in src and chr(0x2029) not in src"` が通り、
`uv run python -c "from moktan.events import _LINE_BREAK_ESCAPES; assert chr(0x2028) in _LINE_BREAK_ESCAPES and len(_LINE_BREAK_ESCAPES)==10"` も通ること(辞書の中身は不変)。

## 2. テスト負債(rev6 §3-3 の未完部分と受入ファイルの整理)

### 2.1 scalar / deps 位置を `_LINE_BREAK_ESCAPES` でパラメトライズ【§3-3 の完遂】

**問題**: rev6 §3-3(3 組のクローンをパラメトライズ)は legacy head の 1 組しか実施
されていない(チェックリストは [x] のままだった — §5 プロセス教訓参照)。現状、
**deps 要素に exotic 8 文字を通すテストはリポジトリのどこにも存在しない**。

**修正**: `test_events.py` の `test_node_failed_console_line_escapes_carriage_return_in_message`
と `test_node_planned_console_line_escapes_carriage_return_in_deps` を削除し、代わりに
次の 2 本を追加する(オラクルは stdlib の `json.dumps` — 実装定数の値ミラーではない
独立オラクル):

```python
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
```

(既存の `"` 込みゴールデン `test_node_failed_console_line_escapes_quotes_and_newlines_in_message`
と `test_node_planned_console_line_escapes_quotes_and_newlines_in_deps` は `"` の
エスケープを固定する別契約なので**残す**。)

### 2.2 rev6 受入ファイルの exotic 2 テストは冗長化したため削除

`tests/test_rev6_acceptance.py` の `test_scalar_field_with_exotic_line_breaker_stays_one_line`
と `test_bare_token_path_with_exotic_line_breaker_stays_one_line`(計 16 ケース)を削除する。
前者は §2.1 の新テスト(より強いゴールデン)に、後者は test_events.py の
`test_legacy_verb_console_line_escapes_line_breaks_in_node_path`(全 10 文字ゴールデン)に
完全に包含される。`_EXOTIC_LINE_BREAKERS` 定数と不要になった import も削除。

### 2.3 rev6 受入ファイルの残り 5 テストをテーマ別ファイルへ移設し、ファイルを削除

**新設ライフサイクル規約(§4.1)の遡及適用**。移設先(§参照 docstring は保持):
- `test_broken_app_filter_does_not_fail_a_successful_run` /
  `test_broken_app_filter_does_not_replace_pipeline_error` /
  `test_broken_sink_plus_broken_filter_does_not_fail_a_successful_run` /
  `test_sink_failure_warning_names_the_sink_type` → `tests/test_events.py`
  (`_AlwaysRaisingFilter` / `_RaisesOnPlainRecords` も一緒に移す)
- `test_jsonl_fallback_line_has_timestamp_and_run_id` → `tests/test_logging_examples.py`
- `_TIMESTAMP_RE` は `tests/conftest.py` に `MOKTAN_TIMESTAMP_RE` として移し、
  `test_events.py` の同一インライン正規表現(timestamp 検証箇所)もそれを使う。
- 移設後 `tests/test_rev6_acceptance.py` を削除。review_notes.md 内の同ファイルへの
  参照(プロトコル参照 1 箇所)は「指示書 rev6/rev7 の受入テスト節」への参照に改める。

## 3. 文書化(コード変更なし or コメントのみ、正確な文面を指定)

### 3.1 silent-drop 挙動をユーザー向け契約(spec)に明記

**場所**: `flume_logging_spec.md` §6.2 の末尾(rev6 で追記した診断行の段落の直後)に追記:

```
なお、アプリが "moktan" ロガーに取り付けた Filter やハンドラ自体が例外を投げる場合、
moktan はその例外を握りつぶし、該当イベントのコンソール/JSON Lines 出力を静かに
スキップする(通知チャネル自体が壊れているため通知しない設計。シンクへの配送と
パイプラインの実行結果には影響しない)。「ハンドラを設定したのに moktan の行だけ
消える」場合は、まず自分の Filter/Handler が moktan のレコードで例外を投げていないか
を疑うこと。
```

### 3.2 `_emit` の stdlib ガードのコメント修正(原因帰属の是正)

現在のコメントは失敗を「アプリの logging 設定」のみに帰属させているが、実行再現の
とおり **moktan 自身の `_render_console_message` のバグも同じ try 内で握りつぶされる**
(structlog の processor はこの呼び出しの内側で走る)。`events.py` `_emit` の
except 節のコメント全体を次に置き換える:

```python
        except Exception:  # noqa: BLE001
            # Either the app's logging setup on the "moktan" logger raised
            # (stdlib absorbs only conforming Handler.emit errors -- see
            # review_notes.md rev6 §1.1), or moktan's own render processor
            # did (it runs inside this call). Both are notification-channel
            # failures with nothing sane to notify through: drop it. Sinks
            # already got the event via _dispatch. KeyboardInterrupt/
            # SystemExit still propagate.
            pass
```

同時に review_notes.md の既知バグクラス節に 1 行追記:

```
- **「発行経路」には呼び出し箇所での引数具現化も含まれる**(rev7 §1.1): rev6 は _emit 内部の消費経路(シンク/stdlib)を両方ガードしたが、`message=str(exc)` は _emit に入る前・_listening の早期リターンより前に評価される。ユーザー例外の壊れた __str__ が PipelineError を置換し node_failed を消した(実行再現済み)。例外→文字列変換は必ず events._safe_str を通すこと(`grep -E "[^_a-z]str\(exc\)" src/` を 0 件に保つ)。また、_emit の stdlib ガードは moktan 自身のレンダラのバグも握りつぶす(構造上不可分)— レンダラ変更時はコンソール行のゴールデンテストが検出網であることを意識する。
```

### 3.3 軽微なコメント/表記の整理(各、指定文面へ置換)

1. `_dispatch` の except 節コメントを次に置き換え(runner.py 対比の考古学を除去):

```python
        except Exception as exc:  # noqa: BLE001 - sink isolation, see comment
            # Sink isolation (rev5 §1.1): a broken sink must never affect what
            # run() returns/raises, nor starve sinks later in the loop.
            # KeyboardInterrupt/SystemExit deliberately propagate -- Ctrl-C
            # must still abort even mid-append.
```

2. `_LINE_BREAK_ESCAPES` のコメントを次に置き換え(不変条件を先頭へ):

```python
# Single source of truth for characters that break the one-event-one-line
# console contract: exactly the set str.splitlines() treats as line
# boundaries (rev6 §1.4 -- the rationale is splitlines-safety, so the set
# must match it). _needs_quoting and _escape_bare_token both derive from
# this dict. Quoted positions are safe via json.dumps escaping; the
# bare-token position uses the escape text on the right.
```

3. `events.py` モジュール docstring 冒頭を次に置き換え(主張を先に、例外は後に):

```
"""Structured event emission: the single source of truth for run observability.

``_emit`` is the only place moktan emits events (flume_logging_spec.md §2).
It fans out to two independent consumers:
```

(bullet リストの後、`moktan never calls ...` の段落の前に次の 1 行を挿入)

```
The sole non-event write to the "moktan" logger is ``_dispatch``'s broken-sink
warning -- a plain record, covered by ``_JSONFormatter``'s fallback.
```

4. `.gitignore` のコメントを次に置き換え:

```
# Claude Code local state (intentionally not shared; whitelist specific
# files here if the team later wants to commit shared .claude/ config)
.claude/
```

5. `tests/test_rev6_acceptance.py` の docstring 陳腐化は §2.3 のファイル削除で解消
   (個別の書き換えは不要)。

## 4. プロセス規約の更新

### 4.1 受入テストファイルのライフサイクル(新設)

review_notes.md の「指示書の粒度」項に次を追記:

```
受入テストファイル(tests/test_revN_acceptance.py)の寿命: rev 完了(全マーカー解除+
最終検証 green)をもって、各テストを §参照 docstring ごとテーマ別ファイルへ移設し、
rev ファイル自体は削除する。恒久リグレッションはテーマで引けるべきで、rev 番号の
ファイルを蓄積しない。移設は次 rev の指示書に必ず含める(rev7 §2.3 が初適用)。
```

### 4.2 exact-paste プロトコルの検証手順(強化)

同項にさらに追記:

```
実装完了時、指示書の規定コード/文面と working tree を逐語 diff する検証ステップを
必ず行う(rev6 では U+2028 キーの表記逸脱・§3-3 の 1/3 実装・未規定の spec 書き換えが
チェックリスト [x] のまま通過した)。チェックリストの [x] は項目ごとの検証後にのみ
付ける。規定外の変更が必要になった場合は黙って実装せず、指示書側に追記してから行う。
```

## 5. 修正不要と判断した項目(記録)

- **`_escape_bare_token` の 10 連 .replace**: 実測 0.26µs/call。`str.translate` 代替は
  2.6 倍**遅い**ことを実測確認。現行形が正解(再提案不要)。
- **`_QUOTE_TRIGGERS` の定数畳み込み**: dis で修正済みを確認。
- **`run()` docstring の残余ニット(`str(exc)` 以外)**: §1.1 修正 4 で文言も是正される。
- **JSONL 診断行を通常イベント(event='sink_failed')に昇格させる案**: 将来 rev の
  再構築候補として記録(診断行スキーマが再び肥大化したら着手)。今回は見送り。
- **broken-sink テストの「他イベント受信」アサート**: rev6 改訂時に実装済み(§2.1 の
  `_assert_broken_sink_isolated` が両面をピン)。
- **flush ループのヘルパー化**: 引き続き見送り(settled)。

## 6. 作業順序(チェックリスト)

各項目は**検証後にのみ** [x] を付けること(§4.2)。

1. [ ] §1.2(U+2028 表記修正)+ 確認コマンド 2 種 green。
2. [ ] §1.1 修正 1〜4 → `grep -E "[^_a-z]str\(exc\)" src/moktan/` 0 件 →
       受入テスト 2 本のマーカー解除 → PASSED 確認。
3. [ ] §2.1(scalar/deps パラメトライズ、20 ケース)→ 旧 \r クローン 2 本削除 →
       test_events.py 単独 green。
4. [ ] §2.2(rev6 受入の exotic 16 ケース削除)→ §2.3(残り 5 テスト移設 +
       ファイル削除 + `_TIMESTAMP_RE` の conftest 集約 + review_notes 参照更新)。
5. [ ] §3.1〜§3.3(spec 追記・コメント差し替え)。
6. [ ] §4.1/§4.2(review_notes.md 追記)。
7. [ ] rev7 受入テストの移設(§4.1 規約に従い test_runner.py へ)+
       `tests/test_rev7_acceptance.py` 削除。
8. [ ] 最終検証: 指示書との逐語 diff(§4.2)→ `uv run pytest`(全ファイル単独も)+
       `uv run ty check` + PBT random seed 3 回 + 全体連続 5 回。
