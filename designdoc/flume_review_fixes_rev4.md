# 改修指示書 rev4: 構造化ログ実装(50116f0, rev3 修正コミット)のレビュー指摘対応

## 0. 背景

`flume_review_fixes_rev3.md` を実装したコミット 50116f0 に対して、9 角度(標準 7 角度 + rev3 修正自己検証角度 + テスト正当性角度)のレビュー + 検証を実施した。対象は `git diff @{upstream}...HEAD`(d5627e0 + 50116f0、構造化ログ機能全体)。

rev3 で修正した §1.1〜§1.6・§2.1〜§2.7 の全項目は、専任の検証角度による再検証(実際にインストール済みの structlog 26.1.0 のソースまで確認、全 79 テスト実行)で**すべて正しく実装されていることを確認**した。今回新たに見つかったのは、**rev3 自身が導入したコードに残っていた「同一契約の一部だけ未適用」という、review_notes.md が繰り返し記録してきたバグクラスの 3 度目の再発**が中心。

## 1. バグ修正(必須)

### 1.1 `run_finished` の発行が try/except の外にあり、ペアリング保証に穴がある 【最重要】

**場所**: `runner.py` `run()` の末尾(139〜149 行付近)

**問題**: rev3 §1.3 で「`run_started` 以降は全て例外安全にした」はずが、成功パスの最後の `_emit(ctx, "run_finished", ...)` 呼び出しは `try/except PipelineError` / `except BaseException` ブロックの**外**にある(直接確認済み)。この `_emit` 呼び出し自体が例外を送出した場合(例: `attach()` されたカスタム `_EventSink` 実装の `events.append()` がバグで例外を投げる、あるいは将来 `_struct_logger.log()` 内部が例外を出すようになった場合)、その例外はそのまま `run()` の外へ伝播し、`run_failed` が発行されないまま `run_started` が宙ぶらりんになる。まさに rev3 §1.3 が解消したはずの症状の再発。

**修正方針**: `run_finished` の発行を try ブロックの内側、`df = cache[root]` の直後に移す。ただし単純に try 内へ移すと「`run_finished` の発行自体が失敗した」場合に `except BaseException` が捕捉して `run_failed` を追加発行することになり、同一 run に対して `run_finished` と `run_failed` が両方観測される可能性がある(`_dispatch` が `_struct_logger.log()` より先に走るため、sink には既に `run_finished` が届いた後で例外、というケースがあり得る)。これは「宙ぶらりんの run_started」よりは無害だが、望ましくは以下のいずれかで一意性を保つ:

- (a) シンプルな対応: `run_finished` の `_emit` を try 内に移すだけ(重複観測のリスクは残るが、無発行よりはるかにマシ)。
- (b) 厳密な対応: `run_finished` 発行だけを独立した `try/except BaseException: pass`(あるいは best-effort ログ)で包み、pipeline としては成功しているのでどんな理由であれ `run_failed` は発行しない(データ処理は成功しているため)。

(b) を推奨(データ処理の成否とイベント発行の成否を混同しない)。

**回帰テスト**: `attach()` した `RunRecorder` 相当のカスタム sink で `events.append` が例外を送出するようにモックし、正常終了する run に対して「`run_started` は必ず `run_finished` または `run_failed` のどちらかと対になる」ことを検証する。修正前 red を確認すること。

### 1.2 legacy verb(computed/loaded/skipped)のヘッドが `_format_value` のエスケープを経由しない

**場所**: `events.py` `_render_console_message`(125 行付近: `head = verb if node is None else f"{verb} {node}"`)

**問題**: rev3 §1.6 で追加した `_format_value`/`_needs_quoting` によるエスケープは、`rest` dict の tail 部分(`key=value ...`)にしか適用されない。`computed`/`loaded`/`skipped` の 3 legacy verb だけが持つ「先頭 2 トークン(`<verb> <path>`)」表現では、`node` の値を素通しで f-string に埋め込んでいるため、**path にスペース・`"`・改行・`=` を含むノードでは全くエスケープされない**。`test_split_first_two_tokens_are_verb_and_path_for_legacy_events` が保証しているはずの `split()[:2]` 契約が、スペースを含む現実のパス(例: `"My Documents/out.parquet"`)で破れる。

**修正方針**: legacy verb のヘッドでも `node` 値に対して同種のエスケープ判定を行う。ただし「先頭トークンとして裸の path を出す」という後方互換契約(§6.1)自体は変えられないため、**スペース・`=` を含む path は諦めて `"` と改行だけを最低限エスケープする**(スペースを含む path で `split()[:2]` が壊れるのは legacy フォーマットの構造的限界として受容し、その旨を spec に注記する)か、あるいは「legacy verb でも path にエスケープが必要な文字が含まれる場合は非 legacy 形式(`node=<quoted>`)にフォールバックする」の 2 択。後者は `split()[:2]` の意味が条件によって変わり呼び出し側が混乱するため、**前者(`"`/改行のみ最低限エスケープし、スペース混在 path のケースは既知の制約として spec に明記)を推奨**。

**回帰テスト**: path にスペースを含むノードで computed/loaded/skipped を発行し、少なくとも `"` と改行が壊れないことを検証。スペースを含む path のケースは「既知の制約」であることをテストのコメントで明示し、xfail にはしない(壊れる形を固定するのではなく、少なくとも `"`/改行が安全であることだけを固定する)。

### 1.3 `deps` フィールド(リスト値)のスペースがエスケープされない

**場所**: `events.py` `_format_value`(104 行付近: `if isinstance(value, list): return repr(value)`)

**問題**: `node_planned` の `deps` フィールドはリストの各要素(dep の path 文字列)をそのまま `repr()` している。Python の `repr(['raw data/in.parquet'])` は `"['raw data/in.parquet']"` となり、要素内のスペースはエスケープされない。`_needs_quoting`/`json.dumps` によるスカラー文字列のエスケープ処理が、リストの中身には適用されていない。

**修正方針**: リストの場合、各要素を `_format_value` (のスカラー分岐)で再帰的にフォーマットしてから repr 相当の `[...]` で包む。あるいはシンプルに、リスト全体を `json.dumps(value)` でレンダリングする(`json.dumps(['raw data/in.parquet'])` → `["raw data/in.parquet"]`、ダブルクォートになる点は §12 のゴールデン例(`deps=['out/orders_raw.parquet']`、シングルクォート)と非互換になるため、**ゴールデン例と §12 の記載を合わせて更新するか、要素ごとに `_needs_quoting` 判定して部分的にのみクォートする**かの判断が必要)。

**回帰テスト**: dep path にスペースを含む `node_planned` イベントの `deps` フィールドが、空白区切りパースで壊れないことを検証。

## 2. テストのバグ(必須)

### 2.1 `test_configure_logging_is_idempotent` が実行順序に依存し単独実行で失敗する 【実測確認済み】

**場所**: `tests/test_events.py:309`

**問題**: `uv run pytest tests/test_events.py::test_configure_logging_is_idempotent` を単独実行すると `assert 2 == 1` で失敗する(実測済み)。`events.py` はモジュール import 時に永続的な `NullHandler` を `"moktan"` ロガーに付与するが、このテストは `len(logger.handlers) == 1` を検証する際にその `NullHandler` の存在を勘定に入れていない。同ファイル内の別テスト(`test_configure_logging_json_lines_output`)の後始末が `logger.handlers` を丸ごと(`NullHandler` ごと)取り除いてしまうため、**同一ファイル内での実行順序が偶然一致したときだけ**このテストは green になる。`pytest -k`、テスト単体実行、`pytest-randomly`/`pytest-xdist` の導入、あるいは単純にテスト追加順が変わるだけで容易に赤くなる。

**修正方針**: 以下のいずれか。
- (a) アサーションを `len(logger.handlers) == 2`(NullHandler + 1 installed)に修正しつつ、NullHandler の存在をテスト内で明示的に確認する。
- (b) `_installed_handlers` が追跡するハンドラの集合だけを見て検証する(`configure_logging` 呼び出し前後の `set(logger.handlers)` の差分など)。
- (c) `test_configure_logging_json_lines_output` 側の後始末を「自分が追加したハンドラだけ」を取り除く形に直す(NullHandler を巻き込まない)。

(b) が最も堅牢(内部実装の変化に強い)。同時に (c) も行い、他のテストの後始末が NullHandler を誤って剥がさないようにする。

**確認**: 修正後、`uv run pytest tests/test_events.py::test_configure_logging_is_idempotent`(単独)と `uv run pytest tests/test_events.py`(ファイル全体)の両方で green になることを確認する。

## 3. リファクタリング(推奨)

### 3.1 `configure_logging` の `_installed_handlers` 操作に lock がない

**場所**: `events.py` `configure_logging`

**問題**: `_registry`/`_registry_lock` は明示的にロック保護されている(コメント付き)のに対し、`_installed_handlers` は無保護。2 スレッドがほぼ同時に `configure_logging()` を呼ぶと、両方が同じ古い `_installed_handlers` を読んでから clear するレースが起き、ハンドラが二重登録されうる(rev3 §1.5 が直した非冪等性のレースバージョンでの再発)。

**修正方針**: `_registry_lock` とは別の `_configure_lock = threading.Lock()` を設け、`configure_logging` 全体を包む。優先度は低(通常はアプリ起動時に 1 回だけ呼ばれる想定)だが、`_registry` との非対称性を解消する意味でも直しておく。

### 3.2 `run()` の 2 つの except 節が run_failed 発行のスキャフォールドを重複させている

**場所**: `runner.py` `run()` の `except PipelineError` / `except BaseException`

**問題**: 両方とも `status="failed"`, `duration_s=time.perf_counter() - start` を組み立てて `_emit(ctx, "run_failed", logging.ERROR, ...)` するコードがほぼ同じ形で 2 回書かれている。将来 run_failed に共通フィールドを足す際に片方だけ更新し忘れるリスク。

**修正方針**: `_emit_run_failed(ctx: RunContext, start: float, *, failed: list[str], error: str | None = None, message: str | None = None) -> None` のような小さなヘルパーに共通部分を切り出す。§1.1 の修正(run_finished も例外安全にする)と合わせて実施すると差分が最小になる。

### 3.3 `Reason`/`Decision` が events.py(ロギングモジュール)にホストされ、runner.py が逆方向 import している

**場所**: `events.py`(42〜48 行付近の型定義)

**問題**: `Reason`/`Decision` は stale 判定・Plan 構成という runner.py 本来のドメイン概念で、ロギング機能が存在しなくても意味を持つ型。rev3 §2.3 で「events.py がイベント語彙の単一所有者」という理由でここに集約したが、`TERMINAL_NODE_EVENTS`/`_LEGACY_VERBS`(真にロギング語彙)と `Reason`/`Decision`(実行エンジンのドメイン型)を同列に扱ったことで、実行エンジンが観測性モジュールに依存する逆転が生じている。

**修正方針**: 優先度は低い(実害はまだない)が、次にこのあたりを触る際に `Reason`/`Decision` は runner.py に戻す(node_planned イベントのフィールド型としては、runner.py からそのまま import すればよく、events.py 側が知る必要はない)ことを検討する。`TERMINAL_NODE_EVENTS`/`_LEGACY_VERBS` は現状どおり events.py に残す。

### 3.4 テストの小規模重複

1. `tests/test_events.py`(§1.4 の subprocess 沈黙テスト)と `tests/test_logging_examples.py`(§1.4 の同種テスト)が `subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)` + `stdout == "" and stderr == ""` のボイラープレートを重複させている。`conftest.py` に `assert_subprocess_silent(script: str) -> None` を切り出す。
2. `tests/test_properties.py` の `test_resume_recomputes_exactly_the_stale_closure` が「終端 INFO イベント」の判定に `("node_computed", "node_loaded", "node_skipped")` をハードコードしている。`events.py` の `_LEGACY_VERBS`(またはそのキー集合)を import して使う形に直し、将来のイベント追加で自動的に追随するようにする。
3. `runner.py` `_plan()` 末尾の `pass2=frozenset(pass2)` は `pass2` が既に frozenset である(`recompute: frozenset` と `frozenset | set` は常に frozenset を返すことを実験で確認済み)ため実質 no-op。`pass2=pass2` に簡略化する。

## 4. 修正不要と判断した項目(記録)

- **`_run_parallel`/`_run_sequential` の `node_failed` 発行が 2 箇所に重複している点**: rev2 §2.3/§2.6-5 で既に「スケジューラループの統合は低優先度」と明記済みの受容済み技術的負債。今回のレビューでも両者は完全に同一のフィールド構成であることを確認しており、drift のリスクは低い。
- **`recorder.py` の `_render_mermaid` が `labels` を別引数で受け取る点**(`_labels(order)` で内部導出可能): 軽微なスタイル上の指摘で、実害・drift リスクともに小さいため今回は見送り。
- **`configure_logging` が同一引数での再呼び出しでもハンドラを毎回作り直す点**: `FileHandler.close()` はフラッシュしてから閉じるためデータロストは起きない。アプリ起動時の 1 回限りの呼び出しが前提であり、ホットパスでもないため許容。

## 5. 作業順序

1. §1.1(run_finished を例外安全に)+ §2.1(test_configure_logging_is_idempotent の順序依存修正)。§1.1 は red→green を確認すること。
2. §1.2 + §1.3(エスケープの穴)。修正方針(b)の選択(deps のクォート方式)を決めてから実装し、§12 のゴールデン例との整合を取る。
3. §3.1(configure_logging の lock)+ §3.2(run_failed 発行の一元化)。§1.1 の修正と合わせて実施すると効率的。
4. §3.3(型の所在)+ §3.4(テスト重複整理)。
5. 全段で `uv run pytest`(単独実行でのファイルごとの green も確認 — 特に test_events.py)+ `uv run ty check`。PBT は random seed 3 回、タイミング依存テストは連続 5 回パス。
