# 改修指示書: flume 初期実装レビュー指摘対応

## 0. 背景

`flume_spec.md` に基づく初期実装(commit 581e6a9)に対して high effort のコードレビュー(7 角度のファインダー + 個別検証、主要指摘は実環境で再現確認済み)を実施した。本書はその指摘事項を修正指示としてまとめたものである。§1 は必須のバグ修正、§2 は推奨リファクタリング、§3 はレビューで検討したが「修正不要」と判断した項目の記録。

## 1. バグ修正(必須)

### 1.1 並列スケジューラのストール → KeyError クラッシュ 【最重要・再現済み】

**場所**: `runner.py` `_run_parallel` の `submit_ready`

**問題**: `submit_ready` は `sorter.get_ready()` の戻り値(スナップショットのタプル)を 1 回だけイテレートする。ループ内で非 pass2 ノードに `sorter.done()` を呼ぶと後続ノードが ready になるが、取得済みタプルには含まれないため回収されない。その時点で実行中の future が 0 件だと `while futures:` ループが未完了のままループを抜け、`run()` が `cache[root]` で `KeyError` を送出する。

**再現手順**: 線形 DAG `a → b → c` を一度実行して全 parquet を作成 → `c.parquet` のみ削除 → `run(c, max_workers=2)`。pass2 = {b(ロード対象), c(再計算)}、初回 ready は非 pass2 の `a` のみ。`done(a)` で `b` が ready になるが取りこぼされ、何も submit されずクラッシュ。**深さ 2 以上の fresh 上流を持つレジューム実行は並列モードで必ず失敗する**。checkpoint 再開が本ライブラリの存在意義なので致命的。

**修正方針(推奨)**: skip ノードを sorter に載せない。Pass 2 の `TopologicalSorter` を pass2_nodes のサブグラフ(predecessors を `deps ∩ pass2_nodes` に制限)で構築する。これにより:

- 実行ループから「非 pass2 ノードの skip 分岐」が消え、ストールの原因自体がなくなる。
- §1.2 の二重ログも同時に解消される(skip ログは Pass 1 直後の 1 箇所だけになる)。
- fresh ノードだらけの大規模グラフで sorter が全ノードを空回りするコストも消える。

代替案(スナップショットを `while` で回して `get_ready()` を再取得し続ける)はストールだけ直して二重ログと空回りが残るため採らない。

**回帰テスト(必須)**: 「一度実行 → root のみ削除 → `max_workers>=2` で再実行」のケースを追加する。現行スイートの並列テストは全ノード再計算(空ディレクトリ起点)しか試しておらず、このトポロジを検出できていない。逐次(`max_workers=1`)でも同一ケースを流すこと。

### 1.2 skipped ログの二重出力(spec §8 違反)【再現済み】

**場所**: `runner.py` `run()` の事前ループ(47 行付近)と `_run_sequential` / `_run_parallel` のスキップ分岐

**問題**: root が stale な run では、非 pass2 ノードが `run()` の事前ループと実行ループの両方で `skipped <path>` を出力し、同一ノードが 2 行ログされる。spec §8 は「ノードごとに 1 行」を要求している。

**修正方針**: §1.1 のサブグラフ化を採用すれば実行ループ側の skip 分岐ごと消えるので、skip ログは `run()` 側の 1 箇所に集約される。ログ出力が全経路で「ノードごとに厳密に 1 行(computed / loaded / skipped のいずれか)」であることを caplog で検証するテストを追加する。

### 1.3 `max_workers` のバリデーション欠如

**場所**: `runner.py` `run()`

**問題**: `max_workers == 1` のときだけ逐次パスに入るため、`0` や負値は Pass 1 実行後に `ThreadPoolExecutor` の stdlib `ValueError` として深部で死ぬ。しかも root が fresh なら早期リターンで検知されずに成功する(実行状態依存のエラー検出)。

**修正方針**: `run()` の冒頭で `max_workers < 1` なら即 `ValueError` を送出する。テスト: `run(root, max_workers=0)` が Pass 1 前に `ValueError` になること(root の fresh/stale 両方で)。

### 1.4 「最初の失敗」の非決定性(spec §2.3 逸脱)

**場所**: `runner.py` `_run_parallel` の完了処理ループ(204 行付近)

**問題**: `concurrent.futures.wait` の返す `done` は順序不定の set。複数の失敗が同一 wakeup で届くと、どの失敗が `PipelineError` の `node` / `__cause__` になるかが実行ごとに変わる。spec §2.3 は「最初の 1 件を raise」と規定している。

**修正方針**: 完了順を保存する。`wait` ベースのループを `add_done_callback` + `queue.SimpleQueue` の完了キュー方式に置き換えるのが素直(完了した future がコールバック発火順にキューへ積まれるため「最初に完了した失敗」が決定的に取れる)。これは §2.4 の `wait(list(futures))` 再構築コストの解消も兼ねる。キュー化が過剰と判断する場合は、spec 側の文言を「最初に検知した 1 件(同時完了時の順序は不定)」に緩和してもよいが、その場合は spec を先に改訂すること。

## 2. リファクタリング(推奨)

### 2.1 Pass 1 の結果を一級構造 `Plan` にする

`needs_compute` / `recompute_nodes` / `load_targets` / `pass2_nodes` がバラの dict/set として 5〜7 引数の私有関数群に引き回されており、`tests/test_memory.py` は同じ 8 行の pass2 導出を 2 回コピーペーストして run() の内部を再現している。導出ルールが変わるとテストが古いルールで検証し続け、メモリ解放保証の検証が空洞化する。

**指示**: `@dataclass(frozen=True) class Plan`(`needs_compute`, `recompute`, `load_targets`, `pass2` を保持)と、それを返す `_plan(graph: Graph, *, force: bool) -> Plan` を導入する。`run()` とテストの両方がこれを呼ぶ。`_execute_pass2` 以下の引数は `(graph, plan, root, max_workers)` 程度に集約する。

### 2.2 `build_graph` の二重 toposort 削除

`_collect_nodes` の DFS post-order は既に有効なトポロジカル順である(dep がすべて done になってから親が order に積まれる。ランダム DAG 2000 件のプロパティテストでも確認済み)。`TopologicalSorter.static_order()` による再ソートは純粋な重複 O(V+E) であり、付随する `except _GraphlibCycleError` 分岐は到達不能なデッドコード。

**指示**: `build_graph` は DFS の戻り値をそのまま `Graph.order` に使い、`static_order()` 呼び出しとそのデッドの except 分岐を削除する。graphlib.CycleError → CycleError 変換(spec §3.2)は `Graph.sorter()` 側の 1 箇所に残す。

### 2.3 逐次/並列ループの骨格共通化

`_run_sequential` と `_run_parallel` はノードごとの処理骨格(kwargs 構築 → 計算/ロード → cache 格納 → done → 参照カウント解放)をコピーペーストで重複しており、実際に §1.2 の二重ログバグは両方のコピーに存在した。逐次/並列の挙動一致は `test_parallel` の前提そのものなので、共有ロジックの drift はフレーキーな並列 vs 逐次不一致として顕在化する。

**指示**: スケジューラループを 1 本にし、「タスクをインラインで呼ぶ」か「executor に submit する」かの実行戦略だけを差し替える構造にする。spec §6 の「`max_workers=1` は Executor を使わない(スタックトレースを素直に保つ)」はディスパッチ部分のみの制約であり、ループ共通化とは両立する。§1.1 のサブグラフ化と同時に行うと差分が最小になる。

### 2.4 `_determine_stale` の stat メモ化

現状はノードごとに `exists()` + `stat()`、さらに消費エッジごとに `dep.path.stat()` を発行するため、N 消費者を持つ dep は N+1 回 stat される(O(E) syscalls)。NFS/SMB 上の大規模グラフで Pass 1 レイテンシがファンアウト倍になる。

**指示**: ノード単位の `try: mtime = path.stat().st_mtime / except FileNotFoundError: None` を dict にメモ化し、`exists()` を stat に統合して O(V)(1 ノード 1 stat)にする。

### 2.5 `test_memory.py` の parametrize 化

2 本のテストが `max_workers=1` / `4` 以外バイト単位で同一(約 28 行の重複)。`@pytest.mark.parametrize("max_workers", [1, 4])` で 1 本に集約する。§2.1 の `Plan` 導入後は pass2 導出コピペも消えるため、あわせて書き直すこと。

## 3. 修正不要と判断した項目(記録)

- **mtime の strict `>` 比較**: 「外部プロセスが node 書き込みと同一秒内に dep を書き換えると stale を見逃す」という指摘があったが、`>=` にすると通常実行で同一秒に書かれた dep/node ペアが毎回再計算される(恒久的 false-positive)ため、spec §4 の判断どおり `>` を維持する。同一秒外部書き換えの盲点は mtime 方式固有の制約として spec §4 に注記を 1 行追加すること(将来の `version` フィールド拡張が正道)。
- **エラーシャットダウン中に get_ready() から取り出されたノードの破棄**: abort 後は必ず `PipelineError` を raise するため観測可能な影響はない。§1.1/§2.3 のループ一本化で構造ごと消える。
- **`_run_parallel` の lock**: 共有 dict はすべてメインスレッドからしか触られておらず lock は実質不要だが、spec §5 が明示的に要求しているため維持する(ワーカーに状態更新を移す変更をしない限り外さないこと)。
- **`Node.__post_init__` が裸の `ValueError` を送出する点**: spec §2.1 が「すべて `ValueError`」と明示しているため意図どおり。

## 4. 作業順序

1. §1.1 + §1.2 + §2.3(サブグラフ化とループ一本化は同一変更として実施、回帰テスト込み)
2. §1.4(完了キュー化)、§1.3(引数検証)
3. §2.1(Plan 導入、test_memory 書き直し = §2.5 込み)
4. §2.2、§2.4
5. 全段で `uv run pytest` と `uv run ty check` をパスさせること。§1.1 の回帰テストが修正前の実装で fail することを先に確認する(red → green)。
