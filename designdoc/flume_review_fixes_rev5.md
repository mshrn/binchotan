# flume レビュー指摘事項と改修指示(rev5)

rev4 実装(`ba845ee`、差分 `50116f0..HEAD`)に対する deep review の結果。
8 アングル(行単位/削除挙動/クロスファイル/再利用/簡素化/効率/抽象度/rev4 自己検証+テスト正しさ)で候補を収集し、
上位クラスタはレビュー中に実際にコードを実行して再現済み(CONFIRMED)。

## 0. 総括: rev4 §1.1 は「1 箇所パッチ」で止まっている

今回の最重要発見は単一の構造問題に収束する:
**「イベント発行の失敗はパイプラインの実行結果に影響させない」という契約(rev4 §1.1 の原則)が、
約 11 箇所ある `_emit` 呼び出しのうち run_finished の 1 箇所でしか守られていない。**

- `_dispatch`(events.py:203)はシンクの `events.append` が raise すると素通しする。
- したがって壊れたシンクは今も node_computed / plan_computed / node_skipped / run_started / run_failed のどこでも run を落とせる。
- run_failed 発行(`_emit_run_failed`)自体が except ハンドラ内で無保護に走るため、呼び出し元に見える例外型が
  PipelineError からシンクの例外に置き換わり、§8 ペアリング契約も破れる(実行再現済み)。
- これは review_notes.md の「同一契約の多所実装で 1 箇所だけ漏れる」クラスの **4 回目の再発**であり、
  同ノートが処方する根本対策「経路自体を 1 本に統合する」をまだ適用していない。

根本修正は runner.py 側にガードを増やすことではなく、**`_dispatch` にシンク毎の例外分離を入れて
`_emit` を(シンク起因では)非 raise 化すること**。これにより rev4 §1.1 で足した run() の
try/except スキャフォールドは撤去でき、§1.4(Ctrl-C 飲み込み)・§1.2(JSONL 破壊の主経路)も同時に消える。

## 1. 重大(修正必須)

### 1.1 `_dispatch` にシンク毎の例外分離を入れ、発行例外安全性を一元化する【根本修正】

**場所**: `events.py` `_dispatch`(203〜207 行)、`runner.py` `run()`(135〜151 行の run_finished ガード)

**問題**(すべて実行再現済み):
1. append が node_computed で raise する壊れたシンク → parquet の atomic write 成功後に例外が
   `_run_sequential` の `except Exception` に入り node_failed + PipelineError 化。データが全部ディスクに
   あるのに run_failed になる。
2. 壊れたシンクがレジストリ先頭にいると、`_dispatch` のループがそこで中断し、後続の健全な RunRecorder は
   以降のイベントを一切受け取れない(dangling run_started)。`_struct_logger.log` にも到達しないので
   コンソール/JSONL からもイベントが消える。
3. `_emit_run_failed` が except ハンドラ内で無保護 → run_failed で raise するシンクがいると、呼び出し元の
   `except PipelineError:` が捕捉できなくなる(見える例外型が RuntimeError に置き換わる)。
4. run_started の `_emit`(runner.py:75)は try ブロックの外 → 部分配達で未ペアの run_started が残る。
5. run_finished ガードの `except BaseException` は KeyboardInterrupt / SystemExit まで warning に変換して
   飲み込み、run() が正常リターンする(発行ウィンドウ中の Ctrl-C が失われる。rev2 の Ctrl-C ハングと同族)。

**修正方針**:
- `_dispatch` のループをシンク毎に `try: sink.events.append(event) except Exception: <warning>` にする。
  `Exception` に限定すること(KeyboardInterrupt / SystemExit は透過させる — 5 の再発防止)。
  warning は「どのシンクがどのイベントで壊れたか」を含め、events.py の module-level `logger` で出す
  (§1.2 の修正が前提。フォーマットは §1.2 の構造化フォールバックに乗せる)。
- 同一シンクからの warning 洪水を避けたい場合は「最初の失敗のみ warning、以降は黙って外す/スキップ」も
  選択肢だが、シンクを自動 unregister するのは attach() の対称性を壊すので**警告のみ・スキップ継続**を推奨。
- これにより `_emit` はシンク起因では raise しなくなる。残る raise 経路は自前の render コード
  (`_render_console_message` — 自制御下、テスト済み)と structlog 内部のみで、stdlib handler 内の例外は
  logging 自身が `handleError` で吸収する。よって **runner.py の run_finished 専用 try/except
  (135〜151 行)と `logging.getLogger("moktan").warning(...)` フォールバックは撤去する**。
  §8 ペアリングは「_emit が投げない」ことで全パス構造的に保証される。
- rev4 で追加した `run()` docstring の「壊れたシンクの例外は warning になる」という説明は
  「壊れたシンクはそのシンクだけがイベントを取りこぼし、warning が出る。実行には一切影響しない」に更新。

**回帰テスト(red → green を必ず確認)**:
- 既存の `_AppendFailsForEvent` をパラメタライズして流用する:
  `target_event ∈ {run_started, plan_computed, node_computed, run_failed, run_finished}` ×
  {成功する run, 失敗する run}。アサートする性質:
  (a) run の成否はシンクの状態と無関係(成功 run は df を返し、失敗 run は PipelineError のまま —
  **例外型が置き換わらない**こと)。
  (b) 健全なシンクを同時に attach し、そちらは**全イベント**を受け取る(dangling run_started なし、
  ペアリング完全)。
  (c) 壊れたシンクは target_event だけを取りこぼし、それ以外は受け取る。
- 現行コードで red になることを git worktree(`ba845ee`)で確認してから修正する。
  特に「run_failed で壊れるシンク + 実際に失敗するノード → except PipelineError が効かない」ケースは
  現行で確実に red(レビュー中に実行確認済み)。

### 1.2 `_JSONFormatter` のフォールバックが JSON Lines ファイルに非 JSON 行を書く

**場所**: `events.py` `_JSONFormatter.format`(250〜254 行)、`runner.py:147`(rev4 の warning)

**問題**(実行再現済み・3 エージェントが独立に再現): rev4 §1.1 のフォールバック warning は `_emit` を
経由しない素の LogRecord なので `moktan_event` 属性を持たず、`_JSONFormatter` が
`super().format()` にフォールバックして `.jsonl` に生テキスト行を書く。`json.loads` 毎行の
コンシューマ(§6.2 契約、jq、`pl.read_ndjson` 等)が、まさに観測が壊れた run でクラッシュする。
`events.py:252` の `# pragma: no cover - defensive, all our records set it` は rev4 時点で虚偽になっていた。

**修正方針**: §1.1 で runner.py:147 の warning 自体は消えるが、§1.1 の新 warning(シンク毎分離のもの)も
moktan ロガーを通る素のレコードなので、**フォーマッタ側を直すのが正しい深さ**:
`_JSONFormatter` のフォールバックを「プレーンレコードも JSON 化する」実装に変える。例:
```python
return json.dumps({"event": "log_message", "level": record.levelname,
                   "logger": record.name, "message": record.getMessage()})
```
`pragma: no cover` を外し、このフォールバック行を実際にカバーするテストを足す
(json_path 設定 + 壊れたシンク → .jsonl の全行が `json.loads` 可能、warning 行は
`event == "log_message"` で識別できる)。

### 1.3 `\r`(キャリッジリターン)が今もコンソールの 1 行契約を破る

**場所**: `events.py` `_needs_quoting`(95 行)と legacy verb ヘッドの置換(149 行)

**問題**(実行再現済み): rev4 §1.2/§1.3 は「改行は 1 イベント 1 行の不変条件そのものを壊すから
エスケープする」と規定したのに、実装は `\n` のみ。`\r` を含む path / message / deps 要素は
生の CR がコンソール行に混入し、`splitlines()` ベースのコンシューマは 2 行に分割、端末では行頭上書き。
さらに「行を破壊する文字の集合」が `_needs_quoting` のタプルと 149 行の置換の 2 箇所に重複しており、
既に両方で `\r` を欠くというドリフトが起きている(多所実装クラスがレンダラ内部で再発)。

**修正方針**:
- `_needs_quoting` の判定集合に `"\r"` を追加(json.dumps は `\r` を正しく `\\r` にエスケープする —
  確認済み)。
- legacy verb ヘッドの置換も `\r` → `\\r` を追加。
- 同時に「行破壊文字」を単一のモジュール定数(例 `_LINE_BREAKERS = ("\n", "\r")`)に集約し、
  `_needs_quoting` と ヘッド置換の両方がそこから導出される形にする(2 箇所のドリフトを構造的に防ぐ)。
- 回帰テスト: `\r` 入り path の legacy verb、`\r` 入り message のスカラ、`\r` 入り deps 要素。
  いずれも `"\r" not in message` と期待文字列一致。現行で red を確認。

## 2. テスト

### 2.1 rev4 §2.1 の teardown 修正が `test_logging_examples.py` 側の同型箇所に未適用

**場所**: `tests/test_logging_examples.py` `test_jsonl_event_count_matches_console_event_count`(474〜479 行)

**問題**: rev4 §2.1 は「teardown が import 時の NullHandler を剥がすことが他テストの順序依存を生む」を
直したが、修正されたのは `test_events.py` の 2 箇所だけ。この teardown は今も
`for handler in list(logger.handlers): logger.removeHandler(handler)` で**全**ハンドラを剥がす。
「同一契約多所実装で 1 箇所漏れ」クラスの 4 回目(テストコード側)。

**修正方針**: snapshot/diff パターン(`handlers_before = set(logger.handlers)` → 差分だけ除去)に
揃える。さらに同パターンが 3 箇所目になるので、**conftest.py の yield-fixture に切り出す**
(例 `moktan_logger_state`: setup で handlers と level をスナップショット、teardown で差分除去+復元)。
3 テストすべてをその fixture に載せ、コピペ 4 箇所目を構造的に不可能にする。

### 2.2 §1.1 回帰テストが成功パスしか固定していない

**場所**: `tests/test_logging_examples.py` `test_run_finished_emission_failure_does_not_fail_a_successful_run`

**問題**: rev4 doc 自身が「run_started は必ず run_finished / run_failed のどちらかと対になる」を
テスト仕様として明記したのに、実装されたテストは成功 run × run_finished 破壊の 1 ケースのみ。
失敗パス側(run_failed で壊れるシンク)は現行実装で red になる(実行確認済み)のに green スイートが
§1.1 完了を認定している。§1.1 の新パラメタライズテスト(上記)がこれを包含するので、
このテストはそこへ統合してよい。統合時に「健全シンクへの node イベント到達」のアサートも追加する。

### 2.3 (§1.1 実施時)壊れたシンクテストは `RunRecorder.attach()` 経由に寄せる

現行テストは `_register`/`_unregister` を直接 import して手動登録しているが、
`RunRecorder(events=_AppendFailsForEvent(...))` + `attach()` で同じシナリオを公開 API 経由で
表現できる(RunRecorder は non-frozen dataclass なので events はコンストラクタ注入可能 — 確認済み)。
attach() が将来挙動(再入ガード等)を持ったときにテストが実経路から乖離するのを防ぐ。

## 3. リファクタリング(推奨)

### 3.1 `_emit_run_failed` を `exc: BaseException | None` ベースの署名にする

現行の error/message 2 連オプショナル + `or` ガード + ほぼ同一の 2 つの `_emit` 呼び出しは、
`_emit_run_failed(ctx, start, *, failed, exc: BaseException | None = None)` として内部で
`error=type(exc).__name__, message=str(exc)` を導出すれば消える。**ty で通ることを実験確認済み**
(rev4 で ty に弾かれたのは `**dict` unpack であり、明示キーワードの if/else は問題ない)。
片方だけ設定される不正状態(`message=None` がリテラル出力される)も構造的に排除される。

### 3.2 ロガー参照の一元化

`runner.py:147` の `logging.getLogger("moktan")` は "moktan" というロガー名リテラルの 2 つ目の出現。
§1.1 でこの行自体が消える見込みだが、runner.py に warning 経路を残す場合は
`from moktan.events import logger` に統一する(ロガー名変更時に lastResort へ漏れる rev3 §1.4 クラスの
再発防止)。

### 3.3 効率: リスナー構成に応じた無駄レンダの排除

- `_emit` は「シンクだけがリスナーで stdlib レベルは無効」(RunRecorder 可視化の標準ケース)のとき、
  `_struct_logger.log` がコンソール行を全レンダリングした後 stdlib のレベルチェックで捨てている
  (processor chain はレベルチェックより先に走る — セッション初期に実験確認済み)。
  `_struct_logger.log` 呼び出しを `if logger.isEnabledFor(level):` で包む(既存の早期リターンとは
  別ケースで、両立する)。
- `runner.py` の node_planned ループは `_emit` が即リターンする構成でも
  `deps=[str(dep.path) ...]` を毎ノード構築する。同じガード条件
  (`_registry or logger.isEnabledFor(logging.DEBUG)`)をループ前に 1 回評価し、丸ごとスキップする。
  ※ このガード式が _emit 内の条件と 2 箇所目になるので、events.py に
  `def _listening(level: int) -> bool` を切り出して両方から使うこと(多所実装クラスの予防)。

### 3.4 コメント・テスト表記の整理(§1.1〜§1.3 の実装ついでに)

- `_format_list_element` の 11 行コメントと run() の履歴語りコメントを、spec §6.1 への参照 +
  現在形の制約 1〜2 文に圧縮(根拠の全文は spec が単一所有)。
- `test_legacy_verb_console_line_escapes_newline_in_node_path` の `chr(10)/chr(92)` 表記は
  Python 3.12(PEP 701)では不要。`escaped = str(node.path).replace("\n", "\\n")` を 1 行で前計算して
  平文の f-string でアサートする。
- `_format_list_element` と `_format_value` の str エスケープ分岐の重複は §1.3 の
  `_LINE_BREAKERS` 集約と同時に整理する。

## 4. 修正不要と判断した項目(記録)

- **`_configure_lock` を第 2 のロックにした判断**: 正しい。ロック順序は常に
  `_configure_lock` → `logging._lock` の一方向で逆順取得経路が存在しないことを確認済み(デッドロックなし)。
  `_registry_lock` に畳むとホットパスの dispatch と cold path のハンドラ設定を偽結合させるので分離が正解。
- **`pass2=frozenset(pass2)` → `pass2=pass2`**: 全経路(force 含む)で frozenset 不変が成立することを
  確認。真の no-op 除去だった。
- **`_emit_run_failed` の忠実性**: 旧インライン 2 箇所とフィールド単位でバイト同一のイベントを出すことを
  確認。§3.2(rev4)の移植ミスなし。
- **`assert_subprocess_silent` / `in _LEGACY_VERBS` / snapshot-diff テスト(rev4 §2.1 の test_events.py 側)**:
  いずれもアサーション弱化・ケース喪失なし。
- **3 つのエスケープ体制(スカラ / リスト要素 / 裸トークン)の統合**: 3 つの位置契約は本質的に異なる
  (未クォート既定 / 常時クォート / クォート不能)ため統合しない。ただし「行破壊文字」の定義だけは
  §1.3 で単一定数に共有する。
- **スペース入り path の裸トークン位置**: rev4 で spec §6.1 に明記済みの受容済み構造制約。再指摘しない。
- **`_format_value` リスト分岐の非文字列要素**: 旧 `repr()` と全型(int/None/bool/float/入れ子/Path/bytes)で
  バイト同一を実行確認済み。リグレッションなし。

## 5. 作業順序

1. §1.1(_dispatch シンク毎分離 + run() スキャフォールド撤去)。パラメタライズ回帰テストの red を
   worktree(`ba845ee`)で確認してから実装。§2.2・§2.3 はこのテスト実装に統合。
2. §1.2(_JSONFormatter フォールバックの JSON 化)。§1.1 の warning 経路が JSONL を壊さないことを
   テストで固定。
3. §1.3(`\r` + `_LINE_BREAKERS` 集約)。red → green。
4. §2.1(teardown の snapshot/diff 化 + fixture 切り出し、3 テスト載せ替え)。
   `uv run pytest tests/test_logging_examples.py` 単独 green も確認。
5. §3.1〜§3.4(リファクタリング)。§3.3 の `_listening` 切り出しは §1.1 実装後に行うと差分が最小。
6. 全段で `uv run pytest`(ファイル単独実行の green も確認)+ `uv run ty check`。
   PBT は random seed 3 回、タイミング依存テストは連続 5 回パス。
7. review_notes.md に追記: 「発行例外安全性はガードの多重化でなく dispatch 層で一元化する」
   「フォーマッタのフォールバック経路は『到達しない』ではなく『到達しても契約を守る』設計にする」
   「teardown 修正は同型 teardown を repo 全体で grep してから閉じる」。
