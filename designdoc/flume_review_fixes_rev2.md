# 改修指示書 rev2: rev1 修正コミットのレビュー指摘対応

## 0. 背景

rev1 指示書(`flume_review_fixes.md`)を実装したコミット 47f805c に対して、再度 high effort のコードレビュー(7 角度のファインダー + 個別検証エージェント、主要指摘は実環境で再現確認済み)を実施した。rev1 の主要修正(§1.1 サブグラフ化・§1.4 完了キュー・§2.1 Plan・§2.2 二重 toposort 削除・§2.4 stat メモ化・§2.5 parametrize)はすべて意図どおり実装されていることを検証済み。特にサブグラフ制限の正しさ(「recompute ノードの全 dep は pass2 に含まれる」「load target の dep はすべて fresh」)は証明レベルで確認した。

本レビューと同時に、テスト強化(§4)は**実施済み**である。本書の §1〜§3 が次の改修ラウンドで対応すべき残項目となる。

## 1. バグ修正(必須)

### 1.1 完了ループから例外が脱出すると全キュー済みタスクの完走を待つ 【再現済み】

**場所**: `runner.py` `_run_parallel` の `with ThreadPoolExecutor` ブロック

**問題**: `while pending:` ループから例外が脱出すると(現実的なのは `completed.get()` 待機中の `KeyboardInterrupt`、または `_finish_node`/`sorter.done` の予期せぬバグ)、executor の `__exit__` が `cancel_futures` なしの `shutdown(wait=True)` を呼ぶため、**キュー済み未開始のタスクも含めて全タスクが実行完了するまでプロセスがブロックする**。再現実験: 遅いノード 8 件 / `max_workers=2` でループ中に例外を発生させたところ、0.5 秒で例外が出た後も 8 件全てが実行されるまで(2 秒超)戻らなかった。1 ノード数時間のパイプラインでは Ctrl-C がハングに見え、その間の計算結果は破棄される。

**修正方針**: `while pending:` ループを `try/except BaseException` で包み、脱出時に `for f in pending: f.cancel()` してから re-raise する(または executor を手動管理し `shutdown(cancel_futures=True)`)。通常の失敗パス(`PipelineError`)は `pending` を全てドレインしてからループを抜けるため cancel 対象が残っておらず、spec §6 の「実行中 future の完了を待つ」セマンティクスは壊れない(検証済み)。

**テスト**: 遅い独立ノード N 件を submit 後、`_finish_node` 相当の位置で例外を注入し、未開始タスクが実行されずに例外が伝播することを経過時間または呼び出し回数で検証する。

### 1.2 サブグラフ sorter の実行順が非決定的 【再現済み】

**場所**: `graph.py` `Graph.sorter()` の `subset` イテレーション

**問題**: `predecessors` を `subset`(identity-hash の `frozenset[Node]`)のイテレーションで構築するため、`TopologicalSorter` への挿入順=同一レベルの `get_ready()` タイブレーク順がメモリアドレス依存になり、**プロセスごとに実行順・ログ順・submit 順が変わる**(再現実験: 30 リーフの DAG で 3 プロセス連続実行し、毎回異なる順序を確認)。計算結果には影響しないが、ログ比較・デバッグ再現性を損ない、複数同時失敗時にどれが「最初の失敗」になるかの submit 順前提も揺らぐ。旧実装(全グラフ)は挿入順 dict をイテレートしていたため決定的だった。

**修正方針**: subset 構築を `graph.order` ベースにする:

```python
predecessors = {node: self.predecessors[node] & subset for node in self.order if node in subset}
```

この形で別プロセス間でも順序が安定することを検証済み。

**テスト**: 同一 DAG のサブグラフ sorter を 2 回構築し `get_ready()` 系列が一致すること(同一プロセス内では identity hash が同じため、テストは「orderベースの構築になっていること」の回帰保護として全 ready 系列の再現一致を確認する程度でよい)。

### 1.3 `force=True` で全ノードの stat を無駄に発行(効率リグレッション)

**場所**: `runner.py` `_determine_stale`

**問題**: `node_mtime = mtime(node)` が `force` 判定より先に評価されるため、force 実行時に全ノード分の stat syscall と memo が無駄になる。旧実装は `if force or not node.path.exists()` の短絡で stat ゼロだった。

**修正方針**: 関数冒頭で `if force: return {node: True for node in graph.order}` と早期リターンし、ループから force を消す。mtime 機構が意味を持つパスだけに限定され、可読性も上がる。

## 2. リファクタリング(推奨)

### 2.1 fresh-root 早期リターンの load 経路統合と例外包装方針の決定

`run()` の fresh-root 早期リターンは `_compute_or_load` の load 分岐(`pl.read_parquet` + `loaded` ログ)の逐語コピー。さらに例外面が非対称: 同じ root parquet が破損している場合、全 fresh なら生の `pl.ComputeError`、下流 stale なら `PipelineError` として届き、呼び出し側の `except PipelineError` が実行状態依存で効いたり効かなかったりする。

**指示**: `return _compute_or_load(root, False, {})` に置き換えた上で、「ロード失敗を PipelineError に包むか生で流すか」を一度だけ決めて全経路に適用する(推奨: ロード失敗も `PipelineError` に包む。node 属性で特定でき、呼び出し側の except が一本化される。spec §2.3 の文言を「ノード実行またはロードの失敗」に改訂)。

### 2.2 `Plan.load_targets` フィールドの削除

構築後に一度も読まれないデッドフィールド(grep で確認済み)。`_plan` 内のローカル変数に降格し、docstring の該当文も削る。

### 2.3 `Graph.sorter(nodes)` の前提条件の明文化

subset 制限は「subset 内のどのノードも除外ノードの成果物に依存しない」という runner 側の pass2 構成不変条件に暗黙依存しており、除外ノード越しの推移的順序は保存されない(chain a→b→c で subset={a,c} なら a と c が同時 ready)。docstring は汎用サブセット対応を謳っているため、将来の「指定ノードだけ実行」的な呼び出し元が黙って壊れる。

**指示**: docstring に前提条件(「除外ノードを跨ぐ順序は保存されない。subset 内ノードが除外ノードの出力を必要としない場合のみ有効」)を明記する。§1.2 の修正と同時に行うこと。

### 2.4 失敗後の `_finish_node` スキップ

失敗検知後もインフライト future の成功結果を cache に格納し参照カウント処理を行っており、raise までの間 DataFrame を無駄に保持する。完了処理の冒頭に `if failures: continue` を入れて即破棄する(parquet への checkpoint は `_compute_or_load` 内で書き込み済みなので、レジューム性は損なわれない)。

### 2.5 lock がセレモニーであることの記録

`_run_parallel` の lock が守る状態(cache/counts)は現在すべてメインスレッドからしか変更されない。spec §5 が明示要求するため維持するが、その事実が rev1 指示書 §3 にしか書かれておらずコードから見えない。lock 定義箇所に「spec §5 準拠のための保持。全変更はメインスレッドで実施されており、_finish_node をワーカー側コールバックに移す場合はこの lock が前提になる」というコメントを付ける。

### 2.6 小規模な整理(まとめて 1 コミットでよい)

1. `_determine_stale` の walrus + `is not None` ガード(コメント自身が到達不能と認めるデッド分岐)→ fresh ノードの mtime を `dict[Node, float]` に記録する形に変えれば `Optional` ごと消える。
2. `graph.py` `sorter()` の `isinstance(nodes, (set, frozenset))` 分岐 → 無条件 `frozenset(nodes)`(CPython は frozenset をそのまま返すため無コピー。可変 set のエイリアシングも防げる)。§1.2 の修正で書き換わる箇所なので同時に。
3. `consumer_counts` → 呼び出し元で `collections.Counter(chain.from_iterable(edges.values()))` に置換して関数ごと削除(ゼロ初期化エントリは一度も読まれないことを確認済み)。
4. テストの共通ビルダー整理: `make_a`/`make_b` 系が計 6 箇所、線形 3 ノードチェーンが 2 箇所に重複。`tests/conftest.py` にノードファクトリと `_linear_three` を移す。
5. rev1 §2.3 の完全形(スケジューラループ一本化)は未達のまま(共通ヘルパー抽出止まり)。逐次/並列の 2 ループ構造は残っており、PipelineError への変換も 2 箇所にある。`_raise_failures(failures)` ヘルパーの共有から始め、可能ならループ一本化まで進める。優先度は低(現状の 2 ループは §1.1/§1.2 修正後も正しく動く)。

## 3. ドキュメント(rev1 からの持ち越し・未実施)

- **spec §4 への注記追加(rev1 §3 の指示、コミット 47f805c で未実施)**: 「外部プロセスが node 書き込みと同一秒内(または mtime 分解能内)に dep を書き換えた場合の見逃しは mtime 方式固有の既知の制約。将来の `version` フィールド拡張が正道」の 1〜2 行を `flume_spec.md` §4 に追加する。あわせて、stat メモ化により「dep の mtime は Pass 1 の訪問時点のスナップショットであり、Pass 1 実行中の外部書き換えは次回 run まで検出されない」ことも同じ注記に含めてよい(同じ受容済み制約クラス)。

## 4. 本ラウンドで実施済みのテスト強化(参考記録)

以下は本レビューと同時に実装・実行済み。次ラウンドでの対応は不要。

1. **ログ不変条件テストの厳密化**: `test_each_node_logs_exactly_one_line` — fresh-root 再実行とレジュームの両経路 × max_workers 1/2 で、全ノードの verb 対応表(computed/loaded/skipped)を完全一致で検証(旧テストは 1 ノードの 'skipped' 行数のみだった)。
2. **`max_workers<1` の fresh root ケース追加**(rev1 §1.3 の「fresh/stale 両方で」を充足)。
3. **レジューム回帰テストの逐次レッグ追加**(rev1 §1.1 の「逐次でも同一ケースを流すこと」を充足)。
4. **複数失敗テストの notes 検証を直接比較に強化**(removeprefix による逆算をやめ `set(err.__notes__) == {期待文字列}` に)。
5. **PBT スイート新設 `tests/test_properties.py`**(hypothesis、ランダム DAG ≤15 ノード、全ノード root 到達可能):
   - レジューム閉包性質: 任意のファイル削除部分集合に対し、再計算されるのは「削除ノード + その推移的消費者」に厳密一致(closure 外の f は呼ばれない)、成果物値は算術オラクルと一致、ログはノードごとに厳密 1 行かつ verb が期待どおり。逐次/並列両方。
   - 失敗再開性質: 任意のノードを 1 回失敗させると `PipelineError`(node 一致・tmp なし・成果物なし)、再実行後は全ノードの f 呼び出し回数が「失敗ノード 2 回・他 1 回」に厳密一致(checkpoint 無再作業の本質)。
   - メモリ解放性質: 任意の削除部分集合に対し Pass 2 後の cache が {root} のみ。
   - 閉路性質: 任意の位置へのバックエッジ挿入で `CycleError`、f は一切呼ばれない。

**実行結果**: 30 tests passed(既存 25 相当 + 新規)、`ty check` パス。PBT はランダムシードで 5 回再実行し全パス(潜在バグの新規検出なし — §1.1/§1.2 は性質テストでは捉えにくいクラス: シグナルタイミングとプロセス間順序)。

## 5. 修正不要と判断した項目(記録)

- **stat エラー面の変化(FileNotFoundError のみ捕捉)**: 「旧 `exists()` は PermissionError 等で False を返し再計算に倒れていたのに、新実装は生 OSError を出す」という指摘は検証の結果**棄却**。Python 3.12 の `Path.exists()` は EACCES で自ら PermissionError を送出し(旧実装も Pass 1 でクラッシュ)、ENOTDIR ケースは旧実装だと f を無駄に実行した後 `_atomic_write` の mkdir で PipelineError になっていた。新実装の「Pass 1 で即・生エラー」はむしろ fail-fast であり、OSError を stale 扱いにすると無駄な計算と分かりにくい遅延エラーを再導入する。現状維持。
- **逐次/並列の notes 非対称**: 逐次は最初の失敗で停止するため `__notes__` が構造的に生じない。両モードとも「失敗が 1 件なら notes なし」で形は一致しており、非対称は本質的。対応不要。
- **mtime メモ化による TOCTOU 窓の拡大**: Pass 1 実行中の外部書き換えは従来も次の比較時点までしか捉えられず、同一クラスの受容済み制約。§3 の spec 注記に含めて記録するのみ。

## 6. 作業順序

1. §1.1(BaseException 時の cancel)+ §1.2(order ベース subset 構築、§2.3 の docstring・§2.6-2 の frozenset 化を同時に)
2. §1.3(force 早期リターン)+ §2.2(load_targets 削除)+ §2.4(失敗後 skip)+ §2.5(lock コメント)
3. §2.1(load 経路統合と例外包装方針 — spec 改訂を伴うため独立コミット)
4. §3(spec §4 注記)+ §2.6 残り
5. 全段で `uv run pytest`(PBT 含む)と `uv run ty check` をパスさせること。§1.1 は red→green を確認する(修正前にテストが失敗することを先に見る)。
