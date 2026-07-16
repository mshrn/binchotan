# 改修指示書 rev3: 構造化ログ実装(d5627e0)のレビュー指摘対応

## 0. 背景

`flume_logging_spec.md` を実装したコミット d5627e0 に対して 7 角度のレビュー + 検証を実施した(主要指摘はファインダーが実環境で再現済み)。§12 のゴールデン一致・イベント網羅・decision↔イベント 1:1 不変条件・run_id 伝播など仕様の中核は正しく実装されていることを確認した上で、**失敗系とライブラリ境界(アプリの logging/structlog 設定との相互作用)に P1 が 4 件**見つかった。うち 2 件は「失敗イベントの発行箇所が分散して 1 箇所だけ漏れる」という rev2 §2.1 と同型の再発クラス。

## 1. バグ修正(必須)

### 1.1 structlog グローバル設定の継承でクラッシュ/イベント消失 【最重要・再現済み】

**場所**: `events.py` の `_struct_logger = structlog.wrap_logger(logger, processors=[...])`

**問題**: `wrap_logger` に `wrapper_class` を渡していないため、bind 時にアプリの `structlog.configure()` の `wrapper_class` を継承する(structlog `_config.py`: `self._wrapper_class or _CONFIG.default_wrapper_class`)。モジュール docstring と spec §1 の「グローバル設定から独立」が成立していない。再現済みの 2 モード:

- (a) アプリが structlog 公式レシピの `make_filtering_bound_logger(logging.INFO)` を設定 → moktan の DEBUG イベント(node_planned/node_submitted/node_cancelled)が `configure_logging(DEBUG)` してもコンソール/JSONL から**静かに消える**。
- (b) アプリが `wrapper_class=structlog.BoundLogger`(汎用)を設定 → `_struct_logger.log(level, event, **fields)` の呼び出し規約が合わず **全 `_emit` が TypeError でクラッシュ = パイプライン実行自体が死ぬ**。

**修正方針**: `wrap_logger(logger, processors=[...], wrapper_class=structlog.stdlib.BoundLogger, context_class=dict)` のように wrapper_class と context_class を明示固定する(`.log(level, event, **kw)` を持つクラスであることを確認して選定)。

**回帰テスト**: テスト内で `structlog.configure(wrapper_class=...)` を(a)(b)両モードで設定 → moktan の全イベントが影響を受けないことを検証(teardown で `structlog.reset_defaults()`)。修正前 red を確認すること。

### 1.2 fresh-root ロード失敗で node_failed が発行されない 【5角度が独立検出】

**場所**: `runner.py` `run()` の fresh-root 早期リターンの `except Exception` 節

**問題**: 全ノード fresh で root parquet が破損している場合、`PipelineError(root)` は raise され `run_failed` は `failed=[root]` で出るが、**`node_failed` がゼロ**(spec §8「失敗ノードは node_failed をちょうど 1 つ発行」違反)。`RunRecorder.to_markdown()` は `status: failed` なのに Failure セクションなし、`to_mermaid()` は root を not started 表示。失敗イベントの発行が `_run_sequential` / `_run_parallel` / (漏れた)fresh-root の 3 箇所に分散した結果で、rev2 §2.1 と同じ「多所実装の 1 箇所だけ drift」クラス。

**修正方針(推奨・深い形)**: **fresh root を pass2 に含めて早期リターン経路自体を廃止する**。`_plan` で recompute が空のとき `pass2 = {root}` とすれば、root のロードは既存の `_run_sequential` を通り、node_failed 発行・PipelineError 包装・メモリ管理すべてが単一経路になる。`_decision` の root 特例(`or node is root`)とコメント、`run()` の try/except がまとめて消える。`flume_spec.md` §4 の「Pass 2 省略」文言は「pass2 が {root} のロード 1 件に縮退する」に改訂。`max_workers>1` で root ロードのみのために executor が立つのを避けたければ「`plan.recompute` が空なら常に逐次パス」を 1 行足す。

**回帰テスト**: 全 fresh + root parquet 破損 → `node_failed`(root)がちょうど 1 つ、`run_failed.failed=[root]`、`to_markdown()` に Failure セクション、`to_mermaid()` で root が `:::failed`。修正前 red を確認。

### 1.3 run_started の対(ペアリング)不変条件が構造的に保証されていない

**場所**: `runner.py` `run()` — `run_started` 発行(build_graph の前)と `except PipelineError` のみの捕捉

**問題**: spec §8「run_started と run_finished/run_failed は全 run で対になる」が、(a) `build_graph` の `CycleError`/`DuplicatePathError`、(b) `_determine_stale` の stat が出す `PermissionError` 等の OSError、(c) `KeyboardInterrupt` など非 PipelineError の脱出、のすべてで破れる(test_back_edge PBT が (a) の到達可能性を証明済み)。recorder は status: unknown の宙ぶらりん run を抱え、JSONL 消費者は閉じない run_id を見る。

**修正方針**(2 段構え):

1. `run_started` の発行を **build_graph 成功後・Pass 1 の前**に移す。グラフ検証エラーは「run が始まらなかった」と定義し、イベントを一切発行しない(spec §3 の発行タイミング欄を「グラフ検証後、Pass 1 の前」に改訂)。
2. `run_started` 以降の全体を `try/except BaseException` で包み、PipelineError 以外の脱出でも `run_failed` を発行して re-raise する。§3 の `run_failed` に `error: str` / `message: str`(optional、非 PipelineError 用)を追記し、その場合 `failed` は空リストでよいと明記。

**回帰テスト**: (a) 閉路 DAG → イベントゼロ(run_started も出ない)。(b) Pass 1 中の PermissionError(monkeypatch)→ run_started と run_failed が対。(c) `_finish_node` への KeyboardInterrupt 注入(既存の cancel テストの手法を流用)→ run_failed 発行後に KeyboardInterrupt が伝播。

### 1.4 ハンドラ未設定でも失敗 run は沈黙しない(lastResort) 【再現済み】

**場所**: `events.py`(モジュール初期化)

**問題**: `"moktan"` ロガーにハンドラが 1 つもない場合、stdlib は WARNING 以上のレコードを `logging.lastResort` 経由で **stderr に出力**する。旧実装は INFO のみだったため顕在化しなかったが、新実装は node_failed/run_failed が ERROR なので、**ロギング未設定のアプリでも失敗 run は stderr に行を吐く**(spec §1「アプリがハンドラを設定しなければ沈黙」/§9-10 違反)。既存の沈黙テストは INFO イベント 1 件の手動 emit のみで、この経路を見ていない。

**修正方針**: ライブラリの標準作法どおり `logging.getLogger("moktan").addHandler(logging.NullHandler())` をモジュール import 時に 1 回実行する。§9-10 のテストを「**失敗する run() を実行しても** stdout/stderr が空」に強化する(capsys)。

### 1.5 configure_logging が非冪等(ハンドラ蓄積)

**場所**: `events.py` `configure_logging`

**問題**: 呼ぶたびに StreamHandler/FileHandler を無条件追加。notebook のセル再実行や複数モジュールからの呼び出しで全行が N 重に印字され、JSONL は重複行で §9-7 の件数一致を壊す。自前のテストすら手動 teardown を強いられている。

**修正方針**: moktan が取り付けたハンドラをモジュールレベルのリストで記録し、再呼び出し時は**先に自分のハンドラを remove/close してから**新設定を適用する(置換セマンティクス)。docstring に「再呼び出しは設定の置き換え」と明記。`propagate` はいじらないが、root ハンドラ併用時に二重出力になり得る旨を docstring に一言。テスト: 2 回呼んでから run → コンソール相当(caplog/handler)も JSONL も各イベントちょうど 1 行。

### 1.6 コンソール値のエスケープ欠如(引用符・改行・=)

**場所**: `events.py` `_format_value`

**問題**: 例外メッセージ等の任意文字列に対しエスケープが皆無。`"` を含むと引用が破綻、改行を含むと「1 イベント 1 行」のコンソール契約(§6.1)が壊れ、行指向の grep や JSONL との件数照合が狂う。

**修正方針**: 文字列値は「空白・`"`・改行・`=` のいずれかを含むとき `json.dumps` で引用する」に変更(JSON エスケープで引用符・改行が安全化される)。§12 のゴールデン(空白のみ含むケース)は `json.dumps` の出力と一致するため互換。テスト: `"` / 改行 / `=` 入りメッセージの node_failed が 1 行かつ引用整合。

## 2. リファクタリング(推奨)

### 2.1 RunRecorder のレンダリングを 1 パス化 + スナップショット共有

`_terminal_event` がノードごとに全イベントを線形スキャン(O(ノード×イベント)。5k ノード/15k イベントで write_report が約 1.5 億回の比較)、さらに `to_markdown` → `to_mermaid` で `_events_for_run`/`_graph_view` を二重実行。mid-run に呼ぶと 2 回のスナップショットが食い違い、サマリと埋め込み mermaid が矛盾するレポートになる整合性問題も同根。

**指示**: `_events_for_run` の結果(イベントリスト)から `terminal_by_node: dict[str, dict]`(最初の terminal イベント優先 = 現行意味論)と `_graph_view` を**一度だけ**構築し、mermaid/markdown 両レンダラに渡す私有関数に再構成する。`to_mermaid`/`to_markdown` は薄い公開ラッパーにする。O(E) 化と単一スナップショット化が同時に達成される。

### 2.2 `_emit` の早期リターンと dict コピー削減

消費者ゼロ(ハンドラなし・recorder なし)でも毎イベント約 5 個の dict 構築 + timestamp 整形 + コンソール文字列レンダリングが走る(structlog の processor は stdlib のレベル判定より前に実行されることを実測確認済み)。10k ノード DAG で 2〜3 万件/run の捨てられる仕事。

**指示**: `_emit` 冒頭に `if not _registry and not logger.isEnabledFor(level): return` を入れる(§1.4 の NullHandler は isEnabledFor に影響しない)。あわせてコピー戦略を 1 回に整理: `ordered` は emit 後不変なので `_dispatch` へそのまま渡し(sink は read-only 契約を docstring に明記)、`_render_console_message` の `raw = dict(event_dict)` は最終 processor である事実に基づき `event_dict` をそのまま使う。

### 2.3 イベント語彙の単一所有

イベント名→verb のマッピングが `events._LEGACY_VERBS` / `recorder._TERMINAL_VERBS` / `recorder._terminal_event` のハードコードタプルの 3 箇所に分散。`Decision` Literal も runner 内定義で recorder は生文字列。**events.py を語彙(イベント名・verb・terminal 集合・Decision/Reason 型)の単一所有者にし、recorder/runner は import する**。将来イベントを足すときの「3 箇所同期」を消す。

### 2.4 `moktan_event(record)` を公開 API に昇格

LogRecord から生イベント dict を取り出すアクセサが `tests/conftest.py`(getattr)と `events._JSONFormatter`(getattr)に重複。§6.2 が想定する「アプリが自前 JSON ハンドラを書く」ユースケースでも必要になる公開の継ぎ目なので、`moktan.events.moktan_event(record: LogRecord) -> dict[str, Any] | None` を公開し、_JSONFormatter・conftest・外部フォーマッタが共用する。

### 2.5 小規模整理(1 コミットでよい)

1. `_unregister` の手動 identity ループ → `_registry.remove(sink)`(eq=False により同義)。
2. `_format_value` のキー名 `"duration_s"` ディスパッチ → `isinstance(value, float)` の型ディスパッチに変更し、`(X.XXs)` ヘッドの `.2f` と共通ヘルパー化(将来の float フィールドが full-repr 精度で漏れるのを防ぐ)。
3. `node_computed` の `bytes` 用 stat が失敗した場合に成功済み compute が node_failed 化する件: stat を個別 try で包み、失敗時は `bytes` を省略(または -1)して computed を発行する。
4. `test_logging_examples.CONSOLE_LINE_RE` の未使用エントリ(loaded/skipped)を削除。

### 2.6 テストの重複整理(conftest 集約)

- `_four_node_dag` が test_recorder(no-op 版)と test_logging_examples(実関数版)に二重定義 → 実関数版を conftest に移して共用。
- `_linear_three` 相当のチェーンが test_parallel の回帰テストに逐語コピー → conftest へ。
- ランダム DAG 生成(test_parallel `_random_dag_spec` と test_properties `dag_spec`)がほぼ同一 → パラメタライズした単一 strategy に統合。
- test_properties の `_ListHandler`/`_capture_moktan_log` は「ロギング経路経由で検証する」意図なら docstring にその旨を明記して残す。意図がなければ `RunRecorder.attach()` で置換(22 行削減、ロガー状態の save/restore 不要)。

### 2.7 §9 テスト要件の未実施分

1. **§9-7 後半**: JSONL 行数がコンソール側イベント数と一致することを、実 run で検証。
2. **§9-2 後半**: 連続 2 回の `run()` で run_id が異なることを run() レベルで検証(new_run_id 単体テストでは `run()` が RunContext を使い回すバグを検出できない)。§9-2 指定の `max_workers=4` と test_case_12_3 の `max_workers=3` の不整合はどちらかに揃える(テストを 4 に)。
3. **write_report**: 未テスト。§9-6 に往復テスト(書いたファイルが to_markdown と一致)を追加。
4. **§9-10 強化**: §1.4 の失敗 run 沈黙テスト。

## 3. ドキュメント(spec 改訂)

- `flume_logging_spec.md` §3: `run_started` の発行タイミングを「グラフ検証後、Pass 1 の前」に改訂(§1.3)。`run_failed` に optional `error`/`message` を追記(非 PipelineError 用、`failed` は空可)。
- `flume_logging_spec.md` §4/§12.6 と `flume_spec.md` §4: §1.2 採用時、「fresh root は pass2={root} に縮退し通常経路でロードされる」旨に文言修正(観測可能な挙動・§12 のログ例は不変)。
- `review_notes.md`: 新規バグクラスを追記 — (a) `structlog.wrap_logger` は wrapper_class/context_class を明示しないとグローバル設定を継承する、(b) ハンドラ未設定ロガーの WARNING+ は `logging.lastResort` で stderr に漏れる(ライブラリは NullHandler 必須)、(c) configure 系ヘルパーは冪等にする、(d) 「同一契約の多所実装で 1 箇所だけ漏れる」klasse(rev2 §2.1 → 今回 §1.2 で再発)。

## 4. 本ラウンドで実施済み(参考記録)

なし(レビューのみ。テスト追加・修正はすべて本指示書の対応ラウンドで行う)。

## 5. 修正不要と判断した項目(記録)

- **`_dispatch` のワーカースレッド append と読み手の並行性**: CPython では list.append は原子的で、`_events_for_run` の内包表記イテレーションも安全(クラッシュしない)。mid-run の整合性は §2.1 のスナップショット共有で扱う。
- **structlog の KeyValueRenderer / TimeStamper を使わない判断**: KeyValueRenderer は値を repr し(`status='failed'`、duration 非パディング)§12 ゴールデンと非互換。TimeStamper はマイクロ秒精度でありログ経路しか刻印しない(sink 直行 dict にも timestamp が要る)。手書きレンダラは正当。
- **`from conftest import ...`**: pytest の既定 importmode(prepend、tests/ に `__init__.py` なし)では repo ルート/tests どちらからでも動作。`importmode=importlib` に変えると壊れる点だけ記録。
- **conftest の `moktan_event` と `_JSONFormatter` の getattr 重複**: §2.4(公開昇格)で解消するため個別修正はしない。

## 6. 作業順序

1. §1.1(wrapper_class 固定)+ §1.4(NullHandler)+ 各回帰テスト(修正前 red を確認)
2. §1.2 + §1.3(失敗イベント/ペアリングの構造化。§3 の spec 改訂込み)+ red→green
3. §1.5(冪等化)+ §1.6(エスケープ)
4. §2.1〜2.5(recorder 1 パス化、_emit 早期リターン、語彙一元化、moktan_event 公開、小規模整理)
5. §2.6〜2.7(テスト整理と §9 未実施分)
6. 全段で `uv run pytest` + `uv run ty check`。PBT は random seed 3 回、タイミング/並行系は連続 5 回パス。部分実施ゼロ(全項目「実施済み」or「意図的スキップ(理由付き)」)。
