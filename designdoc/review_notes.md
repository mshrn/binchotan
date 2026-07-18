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
- **`structlog.wrap_logger()` は `wrapper_class`/`context_class` を明示しないとアプリの `structlog.configure()` を継承する**(rev3 §1.1)。ライブラリが「グローバル設定から独立」を謳うなら両方を明示的に固定すること。省略すると (a) アプリが `make_filtering_bound_logger` 等を設定した場合にこちらの DEBUG イベントが静かに消え、(b) アプリが `.log(level, event, **kw)` と互換性のない wrapper_class を設定した場合は毎回 TypeError でクラッシュする。
- **ハンドラ未設定のロガーでも WARNING 以上は `logging.lastResort` 経由で stderr に漏れる**。「アプリがハンドラを設定しなければ沈黙する」ことを謳うライブラリロガーは、import 時に `logging.NullHandler()` を明示的に付けること(rev3 §1.4)。この種の沈黙テストは pytest 内で `capsys` だけを使うと検出できない ── pytest 自身のロギングプラグインがセッション全体でルートロガーにハンドラを保持し続けるため `lastResort` がそもそも発火しない。実際のリークを検出するには子プロセス(`subprocess.run`)で再現し、その子プロセスの stdout/stderr を検証する。
- **「同一契約の多所実装で 1 箇所だけ漏れる」クラス**: 失敗イベント発行(rev2 §2.1 → rev2 §1.2 で再発 → rev3 §1.2 で 3 度目)、run 開始/終了イベントのペアリング(rev3 §1.3)など、"N 箇所どれからでも到達できる状態" を保証する契約は、経路を追加するたびに全経路で再検証するか、経路自体を 1 本に統合する(例: fresh-root 専用の早期リターンを廃止し、通常の Pass 2 経路に一本化)のが根本対策。
- **`configure_logging` 系のヘルパー関数は再呼び出しに対して非冪等になりがち**(rev3 §1.5): ハンドラを無条件 `addHandler` すると、notebook のセル再実行や多重 import で出力が重複する。モジュール側でインストールしたハンドラを記録し、再設定時に先に取り除く実装にする。さらに、その記録用リストの読み書き自体もロック保護しないと、2 スレッドがほぼ同時に呼んだ際にハンドラが二重登録されるレースが残る(rev4 §3.1)。`_registry`/`_registry_lock` のような他の共有可変状態と対称に保護されているか確認する。
- **バックワードコンパット用の「裸トークン」位置(`split()[:2]` で読む legacy verb の path など)は、他フィールドと同じ引用符エスケープが効かない**(rev4 §1.2/§1.3): スペースを含む値を渡されると構造的に契約が壊れる(既知の受容済み制約として spec に明記する以外にない)が、改行だけは「1 イベント1行」の不変条件を壊すので個別にエスケープする。同様に、リスト値(`deps` など)の要素は常にクォートされる前提のフィールドなので、要素ごとに `"`/改行のみを json.dumps 形式にフォールバックさせ、スペースだけの場合は元の repr() 形式(ゴールデン例と一致)を保つ、という「要素ごとの部分エスケープ」が両立可能な落とし所になる。
- **例外安全性のギャップは「成功パスの外側」に置かれた発行コードで起きがち**(rev4 §1.1): `run()` の `run_finished` 発行が try/except の外にあると、イベント発行自体の失敗(壊れたカスタムシンクなど)が、既にデータ生成に成功したパイプラインを見かけ上失敗させてしまう。発行専用の例外処理は「発行失敗を warning ログに落とすだけで、実行結果には影響させない」ように意図的に非対称にする。

## 確定済み設計判断(再指摘しない)

- **mtime の strict `>` 比較**: `>=` にすると同一秒に書かれた dep/node ペアが毎回再計算される恒久 false-positive になる。同一秒外部書き換えの盲点は受容(spec §4 に注記済み)。
- **`_run_parallel` の lock**: 現状すべての cache/counts 変更はメインスレッドだが、spec §5 が明示要求するため維持(コード内コメント済み)。`_finish_node` をワーカー側コールバックへ移す場合の前提条件でもある。
- **逐次/並列で `__notes__` の有無が違う点**: 逐次は最初の失敗で停止するため構造的に notes が生じない。本質的な非対称であり修正不要。
- **stat エラー面(`FileNotFoundError` のみ捕捉)**: 「旧 `exists()` は他の OSError で False に倒れていた」という指摘は実測で棄却。Python 3.12 の `Path.exists()` は EACCES で自ら raise し、ENOTDIR も旧実装は無駄計算後に遅延エラーだった。現行の Pass 1 での fail-fast が正しい。
- **Pass 1 中の外部書き換え TOCTOU**: mtime 方式固有の受容済み制約(spec §4 注記済み)。`version` フィールド拡張が正道(spec §10)。
- **`Reason`/`Decision` が events.py にホストされ runner.py が逆方向 import している点**(rev4 §3.3): 実害はまだなく優先度は低いため rev4 では未対応のまま据え置いた意図的な判断。次にこのあたりを触るタイミングでまとめて runner.py 側に戻すことを検討する(再指摘不要、ただし未解決の負債として認識しておく)。

## 検証環境

- テスト: `uv run pytest`(PBT: `uv run pytest tests/test_properties.py --hypothesis-seed=random`)
- 型: `uv run ty check`
- 改修指示書の置き場所: `designdoc/flume_review_fixes_rev<N>.md`
