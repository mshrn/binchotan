# flume レビュー指摘事項と改修指示(rev6)【改訂第2版】

rev5 実装(`cefd0a5`、差分 `ba845ee..HEAD`)に対する deep review の結果。
8 アングルで候補を収集し、上位クラスタは 3 エージェントが独立にコードを実行して再現済み。

**改訂第2版について**: 初版は「(a)/(b) を意思決定する」「〜省略可」のような選択肢・裁量を
実装者に残しており、過去 5 世代のバグ再発の一因(指示の解釈余地)を自ら再生産していた。
本版では (1) すべての意思決定を済ませ、(2) 修正箇所をシンボル名でアンカーし(行番号は
実装中にずれる)、(3) 貼り付け可能なコード断片を示し、(4) **受入テストを
`tests/test_rev6_acceptance.py` として実装済み同梱**する。

## 受入テストのプロトコル(最初に読むこと)

`tests/test_rev6_acceptance.py` が本指示書の実行可能な受入基準である(全 21 ケース)。

- 現在は全テストに `@pytest.mark.xfail(strict=True)` が付いており、スイートは green。
  red の確認: `uv run pytest tests/test_rev6_acceptance.py --runxfail`(21 failed が正、
  `cefd0a5` で確認済み)。
- **テスト本体(セットアップ・アサート)は変更禁止**。テストが誤っていると思ったら
  実装せず相談すること。
- 各修正を実装したら、対応するテストの xfail デコレータ**だけ**を外す。strict なので
  修正後にマーカーを残すと XPASS でスイートが fail する — マーカー外しが契約充足の宣言。
- 全マーカーが外れ、このファイルが素で green になったら rev6 のコード修正は完了。

既存テストの検出力修正(初版 §1.2/§1.3: warning のロガー名フィルタ、壊れたシンクの
「他イベントは受信」アサート、`AppendFailsForEvent`/`moktan_warnings` の conftest 移動)は
**本改訂と同時に実施済み**。実装者の作業対象ではない。

## 0. 総括: 発行例外安全性のバグクラス、5 世代目の再発

rev5 §1.1 は「発行失敗は実行結果に影響させない」契約を `_dispatch` のシンク毎分離で
一元化した — が、**`_emit` には消費経路が 2 本ある**。シンク経路は塞がったが、
**stdlib 経路(`_struct_logger.log` → `Logger.handle`)は無保護のまま**。
アプリが `"moktan"` ロガーに取り付けた壊れた `logging.Filter` や、stdlib の try/except
規約に従わないカスタム `Handler.emit` は、今も任意の `_emit` 箇所で成功 run を失敗させる。

rev5 doc の「stdlib logging は handleError で吸収する」という論拠は部分的に誤り:
- `Logger.filter()` / `Filterer.filter()` は何も吸収しない — Filter の例外は伝播する。
- `Handler.handle()` も `emit()` 呼び出しを包まない — `handleError` に到達するのは
  emit 本体が規約通り try/except を書いている場合だけ。
- 規約準拠ハンドラ(StreamHandler + 壊れた Formatter 等)だけは吸収される。

さらに `_dispatch` のシンク失敗 warning は同じ "moktan" ロガーへの素の `logger.warning`
なので、moktan_event 属性のないレコードで choke する Filter に当たると、
**分離機構自体が新たな例外源になる**(実行再現済み)。

教訓(review_notes.md に追記済みであること): 経路を数えるときは「_emit の呼び出し箇所」
だけでなく「**_emit 内部の消費経路**」も数える。

## 1. 重大(修正必須)

### 1.1 stdlib 消費経路にも例外分離を入れる【根本修正】

**受入テスト**: `test_broken_app_filter_does_not_fail_a_successful_run` /
`test_broken_app_filter_does_not_replace_pipeline_error` /
`test_broken_sink_plus_broken_filter_does_not_fail_a_successful_run`

**意思決定済み**: stdlib 経路の失敗は**黙って落とす**(warning を出さない)。
理由: 失敗したのは通知チャネルそのもの(アプリの logging 設定)であり、そこへ再通知
すると再帰する。シンク(moktan の管轄)は warning で通知し、アプリの logging 設定
(アプリの管轄)は通知しない、という非対称は正当。ヘルパーは新設せず、下記 2 箇所に
直接 try/except を書く(2 箇所で形が違うためヘルパー化すると却って条件が複雑になる)。

**修正 1**: `events.py` の `_emit` 末尾。現在:

```python
    if logger.isEnabledFor(level):
        # A sink-only listener ... (既存コメント)
        _struct_logger.log(level, event, **{k: v for k, v in ordered.items() if k != "event"})
```

を、次に置き換える(既存コメントは保持):

```python
    if logger.isEnabledFor(level):
        # A sink-only listener ... (既存コメントをそのまま残す)
        try:
            _struct_logger.log(level, event, **{k: v for k, v in ordered.items() if k != "event"})
        except Exception:  # noqa: BLE001
            # A failure here means the application's logging setup on the
            # "moktan" logger is broken (a raising Filter, a non-conforming
            # Handler.emit -- stdlib's handleError only covers emit bodies
            # that follow the try/except convention). The notification
            # channel itself is what failed, so there is nothing sane to
            # notify through: drop it. Sinks already received the event via
            # _dispatch above. KeyboardInterrupt/SystemExit still propagate.
            pass
```

**修正 2**: `events.py` の `_dispatch` の except 節にある `logger.warning(...)` を、
§2.1・§2.2 の要件(シンク型名・run_id extra)も同時に満たす次の形に置き換える:

```python
        except Exception as exc:  # noqa: BLE001 - sink isolation, see comment
            try:
                logger.warning(
                    "moktan: sink %s failed to record event %r (run_id=%s): %r",
                    type(sink).__name__,
                    event.get("event"),
                    event.get("run_id"),
                    exc,
                    extra={"moktan_run_id": event.get("run_id")},
                )
            except Exception:  # noqa: BLE001
                # The warning channel itself (app logging config) is broken
                # too -- same reasoning as in _emit: drop it.
                pass
```

**修正 3**: `run()`(runner.py)の docstring 中の発行例外安全性の段落を次の 1 文に
差し替える(現在の「no `_emit` call anywhere...」の文は反証済みのため):

```
Event emission never raises (KeyboardInterrupt/SystemExit excepted): broken
sinks are isolated per-event in ``events._dispatch`` (with a warning), and a
broken application-side logging setup on the "moktan" logger is silently
dropped inside ``events._emit`` -- so only genuine pipeline failure
determines which closing event (run_finished / run_failed) fires.
```

**修正 4**: `events.py` モジュール docstring の
「``_emit`` is the only place moktan writes log output」の文を次に差し替える:

```
``_emit`` is the only place moktan emits *events*; the sole other write to
the "moktan" logger is ``_dispatch``'s best-effort broken-sink warning (a
plain record, handled by ``_JSONFormatter``'s fallback).
```

### 1.2 シンク失敗 warning にシンク型名を含める

**受入テスト**: `test_sink_failure_warning_names_the_sink_type`

§1.1 修正 2 のフォーマット文字列(`type(sink).__name__` が第 1 引数)で同時に満たされる。
個別作業は不要 — マーカーを外して green を確認するのみ。

### 1.3 _JSONFormatter フォールバック行に timestamp / run_id を入れる

**受入テスト**: `test_jsonl_fallback_line_has_timestamp_and_run_id`

**修正 1**: `events.py` に timestamp 整形ヘルパーを切り出し、`_emit` と共有する:

```python
def _iso_timestamp(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")
```

`_emit` 内の `ordered["timestamp"] = datetime.now(UTC).isoformat(...)...` は
`ordered["timestamp"] = _iso_timestamp(datetime.now(UTC))` に置き換える。

**修正 2**: `_JSONFormatter.format` のフォールバック分岐(`event is None` 側)を
次に置き換える:

```python
        payload: dict[str, Any] = {
            "event": "log_message",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "timestamp": _iso_timestamp(datetime.fromtimestamp(record.created, UTC)),
        }
        run_id = getattr(record, "moktan_run_id", None)
        if run_id is not None:
            payload["run_id"] = run_id
        return json.dumps(payload)
```

(`moktan_run_id` は §1.1 修正 2 の `extra=` が付与する。付与元のない素のレコードでは
run_id フィールドは付かない — スキーマ上「あれば付く」フィールドとして spec に注記する。)

**修正 3**: spec(flume_logging_spec.md)§6.2 に次を追記:

```
JSON Lines には通常イベント行のほか、`"event": "log_message"` の内部診断行
(シンク故障時の warning 等)が混ざりうる。診断行にも `timestamp`(同形式)は必ず
含まれ、`run_id` は発行元が判明している場合のみ含まれる。各行が単独で json.loads
可能である契約は全行種で維持される。
```

### 1.4 行破壊文字の集合を str.splitlines() の全文字に拡張する

**受入テスト**: `test_scalar_field_with_exotic_line_breaker_stays_one_line[8ケース]` /
`test_bare_token_path_with_exotic_line_breaker_stays_one_line[8ケース]`

**意思決定済み**: 拡張する(spec の根拠が「splitlines ベースのコンシューマ保護」である
以上、\n/\r だけでは根拠と実装が食い違う)。`events.py` の `_LINE_BREAK_ESCAPES` を
次の内容に置き換える:

```python
# Every character str.splitlines() treats as a line boundary. Quoted
# positions are safe once _needs_quoting triggers (json.dumps escapes all
# control chars and, with ensure_ascii=True, all non-ASCII); the bare-token
# position uses the explicit escape text on the right.
_LINE_BREAK_ESCAPES: dict[str, str] = {
    "\n": "\\n",
    "\r": "\\r",
    "\v": "\\x0b",
    "\f": "\\x0c",
    "\x1c": "\\x1c",
    "\x1d": "\\x1d",
    "\x1e": "\\x1e",
    "\x85": "\\x85",
    "\u2028": "\\u2028",
    "\u2029": "\\u2029",
}
```

`_needs_quoting` と `_escape_bare_token` は既にこの辞書から導出されるので他の変更は
不要。ゴールデン例(§12)への影響なし(これらの文字を含む例は存在しない)。
同時に、毎呼び出しのタプル再構築を避けるため `_needs_quoting` の判定集合を
モジュール定数に持ち上げる:

```python
_QUOTE_TRIGGERS: tuple[str, ...] = (" ", '"', "=", *_LINE_BREAK_ESCAPES)

def _needs_quoting(value: str) -> bool:
    # (既存コメント維持)
    return any(c in value for c in _QUOTE_TRIGGERS)
```

## 2. 文書化のみ(コード変更なし、正確な文面を指定)

### 2.1 裸トークン位置のバックスラッシュ非単射性(受容)

spec §6.1 の既知の制約段落に次の 1 文を追記する:

```
また、裸トークン位置のエスケープはバックスラッシュ自体を二重化しないため、リテラルな
2 文字「\n」を含む path と実改行を含む path はエスケープ後に同一表記となる(受容済み。
機械的な復元が必要な場合は JSON Lines 側の node フィールドを使うこと)。
```

### 2.2 途中 attach シンクの node_planned 欠落(受容)

spec §7(RunRecorder)に次の 1 文を追記し、`RunRecorder.attach` の docstring にも
同旨を 1 文追加する:

```
attach() は run() の開始前に行うこと。実行中の run に途中から attach した場合、
それ以前のイベント(一括発行される node_planned を含む)は記録されず、
to_mermaid()/to_markdown() は不完全な図・表を返す。
```

### 2.3 review_notes.md への追記(教訓 3 件)【実施済み】

本改訂と同時に追記済み(review_notes.md の既知バグクラス節、末尾 3 項目 +
rev5 §1.3 項目への splitlines 全集合の補足)。実装者の作業対象ではない。

## 3. リファクタリング(推奨・軽微、受入テスト対象外)

1. **`.claude/scheduled_tasks.lock` を `git rm --cached` し、`.gitignore` に
   `.claude/` を追加**(セッション固有ロックが rev5 コミットに混入)。
2. `test_recorder_dispatch_is_independent_of_logging_level`(test_events.py)の
   手書き setLevel/try/finally を `moktan_logger_state` fixture に載せ替える
   (fixture がレベル復元も行うため try/finally ごと削除できる)。
3. \n/\r エスケープテスト 3 組(legacy head / scalar / deps、test_events.py)を
   `_LINE_BREAK_ESCAPES.items()` で parametrize してソース定数と自動同期させる
   (§1.4 で集合が 10 文字になるため、手書きクローンでは追随不能)。
4. conftest の `moktan_event` docstring の前提(「全レコードが moktan 発」)を
   「イベントレコードのみ対象。_dispatch の warning 等の素レコードを含む場合は
   呼び出し側で除外すること」に更新する。
5. コメント/docstring の圧縮(初版 §4-7 と同じ、各 1〜2 文の現在形制約に):
   `_dispatch` except 節の履歴語り、`_listening` の呼び出し元列挙、
   runner.py node_planned ゲートの 4 行コメント。
6. flush ループ 4 箇所は現状維持(コスト対効果が薄いため見送り — 意思決定済み)。

## 4. 修正不要と判断した項目(記録、初版から変更なし)

- `isEnabledFor` の 2 回評価: stdlib キャッシュ済み ~30ns、除去は多所実装クラスを
  再導入するため現状維持。
- `_dispatch` per-sink try/except のオーバーヘッド: Python 3.12 zero-cost。非問題。
- `_escape_bare_token` の複数回 replace: ミス時同一オブジェクト返却。非問題。
- シンク失敗 warning の per-event 発火(洪水): イベント毎に別のデータ欠落なので
  per-event が正当。warn-once 化は将来うるさければ検討。
- `_listening` ゲートの既存テスト互換性: structlog → stdlib Logger.log が同一の
  isEnabledFor を内部再チェックするため挙動中立(トレース確認済み)。
- 壊れたシンクテストの 2 本構成: セットアップが本質的に異なるため分割妥当
  (アサート尾部は本改訂でヘルパー化済み)。

## 5. 作業順序(チェックリスト)【完了】

1. [x] §3-1: lock ファイル除去 + .gitignore(独立・即時)。
2. [x] §1.1 修正 1〜4 を実装 → 受入テスト §1.1 系 3 本のマーカーを外す →
       `uv run pytest tests/test_rev6_acceptance.py` で該当 3 本 PASSED を確認。
3. [x] §1.2 はマーカーを外すだけ(§1.1 修正 2 で充足済み)→ PASSED 確認。
4. [x] §1.3 修正 1〜3 → マーカー外し → PASSED 確認。
5. [x] §1.4 → 16 ケースのマーカー外し → PASSED 確認。
6. [x] §2.1〜§2.3 の文面追記(spec / review_notes.md / recorder.py docstring)。
7. [x] §3-2〜§3-5 のリファクタリング(§3-6 flush ループは見送りのまま維持)。
8. [x] 最終確認: `uv run pytest`(全ファイル単独実行含む、125 passed)+
       `uv run ty check`(green)+ PBT random seed 3 回(green)+
       全体スイート連続 5 回(green)。`tests/test_rev6_acceptance.py` の
       xfail マーカーは全 21 件解除済み。red→green は worktree(`c46df8e`)で
       21 件全 red を確認してから実装。
