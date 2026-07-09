# レビューノート: moktan 固有の既知バグクラスと確定済み設計判断

deep-review(汎用レビュースキル/エージェント)がスコープ確定時に読み込む、このリポジトリ固有の知見。
レビューで新しいバグクラスや設計判断が確定するたびに追記する。

## 既知バグクラス(優先的に疑う)

過去 2 ラウンドのレビューで実際に検出されたもの:

- `TopologicalSorter.get_ready()` は**スナップショット**を返す。ループ内の `done()` で ready になったノードは取得済みタプルに現れない → 取りこぼし/ストール(rev1 §1.1: 並列レジュームの KeyError クラッシュ)。
- `ThreadPoolExecutor.__exit__` は `shutdown(wait=True)` で **cancel_futures しない**。ループから例外(KeyboardInterrupt 含む)が脱出するとキュー済み未開始タスクの完走を待つ(rev2 §1.1: Ctrl-C ハング)。
- `concurrent.futures.wait()` の `done` は**順序不定 set** → 「最初の失敗」が非決定(rev1 §1.4)。完了順は `add_done_callback` + `SimpleQueue` で取る(現行実装)。
- `Node` は identity hash(`eq=False`)。**set/frozenset をイテレートして順序依存構造を作るとプロセス間で非決定**(rev2 §1.2)。順序が要る箇所は必ず `graph.order` を基準にする。
- ログ契約(spec §8: ノードごとに厳密 1 行、computed/loaded/skipped)は複数コード経路にまたがる。経路の追加・統合のたびに全経路×全バーブで検証(rev1 §1.2: skipped 二重出力)。
- 参照カウント/cache の不変条件は「Pass 2 で実際に消費されるエッジ」の定義に依存。エッジ集合の構成箇所(`Counter` の初期化)と減算箇所(`_finish_node`)の対応を確認。
- 逐次/並列の挙動一致が test_parallel の前提。共有ロジックのコピー分岐に注意(スケジューラループはまだ 2 本ある)。
- 旧コードの短絡が消える効率リグレッション(rev2 §1.3: force=True の stat 無駄撃ち)。

## 確定済み設計判断(再指摘しない)

- **mtime の strict `>` 比較**: `>=` にすると同一秒に書かれた dep/node ペアが毎回再計算される恒久 false-positive になる。同一秒外部書き換えの盲点は受容(spec §4 に注記済み)。
- **`_run_parallel` の lock**: 現状すべての cache/counts 変更はメインスレッドだが、spec §5 が明示要求するため維持(コード内コメント済み)。`_finish_node` をワーカー側コールバックへ移す場合の前提条件でもある。
- **逐次/並列で `__notes__` の有無が違う点**: 逐次は最初の失敗で停止するため構造的に notes が生じない。本質的な非対称であり修正不要。
- **stat エラー面(`FileNotFoundError` のみ捕捉)**: 「旧 `exists()` は他の OSError で False に倒れていた」という指摘は実測で棄却。Python 3.12 の `Path.exists()` は EACCES で自ら raise し、ENOTDIR も旧実装は無駄計算後に遅延エラーだった。現行の Pass 1 での fail-fast が正しい。
- **Pass 1 中の外部書き換え TOCTOU**: mtime 方式固有の受容済み制約(spec §4 注記済み)。`version` フィールド拡張が正道(spec §10)。

## 検証環境

- テスト: `uv run pytest`(PBT: `uv run pytest tests/test_properties.py --hypothesis-seed=random`)
- 型: `uv run ty check`
- 改修指示書の置き場所: `designdoc/flume_review_fixes_rev<N>.md`
