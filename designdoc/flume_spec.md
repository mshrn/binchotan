# 実装指示書: ファイルベース checkpoint 付き DataFrame パイプライン

## 0. 背景と目的

DataFrame 変換の DAG を宣言し、各ノードの結果を parquet として保存する軽量パイプラインランナーを実装する。旧 raft（Checkpoint 基底クラス + マーカーファイル方式）の後継。設計思想は以下の通り。

- ユーザーはクラスを書かない。`Node`（宣言）を組み合わせるだけ。実行ロジックはすべてランナー側。
- ノードの処理は `f(**dep_dfs, **kwargs) -> pl.DataFrame` に制限する。保存はランナーが握る。
- マーカーファイルは使わない。atomic write（tmp → rename）により「parquet が存在する = 成功」を成立させる。
- 成果物ファイル自体が checkpoint。実行済みノードはスキップされ、途中から再開できる。

## 1. 技術スタック・規約

- Python 3.12+、uv プロジェクト（src layout）
- Polars のみ（pandas 禁止）
- 型チェック: ty。全公開 API に型注釈必須。`Any` は `kwargs` の値以外で使わない。
- テスト: pytest + hypothesis
- 外部依存は polars のみ。標準ライブラリで完結させる（graphlib, concurrent.futures, threading, dataclasses）。
- パッケージ名は仮に `moktan` とする（変更可）。

```
moktan/
├── pyproject.toml
├── src/moktan/
│   ├── __init__.py      # 公開API: Node, run, PipelineError, CycleError の re-export
│   ├── node.py          # Node 定義
│   ├── graph.py         # DAG 収集・toposort・閉路検出・消費者カウント
│   └── runner.py        # 実行エンジン（stale判定・並列実行・メモリ解放）
└── tests/
```

## 2. 公開 API

### 2.1 Node

```python
@dataclass(frozen=True)
class Node:
    path: Path
    f: Callable[..., pl.DataFrame]
    deps: Mapping[str, "Node"] = field(default_factory=dict)
    kwargs: Mapping[str, Any] = field(default_factory=dict)
```

- frozen dataclass。ノードの同一性は `id()` ベース（`eq=False` を指定し、hash はデフォルトの identity hash を使う）。同一 `path` を持つ別インスタンスは別ノードとして扱ってよいが、後述のバリデーションで検出して拒否する。
- `deps` のキーは `f` のパラメータ名に対応する。実行時に `f(**{k: df_k}, **kwargs)` として呼ばれる。順序ではなく名前で対応する。

複数 dep を受け取るノードの例:

```python
def join_orders(users: pl.DataFrame, orders: pl.DataFrame, on: str) -> pl.DataFrame:
    return users.join(orders, on=on)

users = Node(Path("out/users.parquet"), fetch_users)
orders = Node(Path("out/orders.parquet"), fetch_orders)
joined = Node(
    Path("out/joined.parquet"),
    join_orders,
    deps={"users": users, "orders": orders},  # キー = join_orders の引数名
    kwargs={"on": "user_id"},
)
```

`deps["users"]` の成果物 df が `users` 引数に、`deps["orders"]` が `orders` 引数に渡される。

#### `__post_init__` でのバリデーション（すべて `ValueError`、オプション化しない）

1. `deps` と `kwargs` のキー衝突。
2. シグネチャ整合チェック: `inspect.signature(f)` を取得し、`f(**deps_keys, **kwargs_keys)` が bind 可能であることを `signature.bind(**{k: None for k in ...})` 相当で検証する（typo・引数過不足を構築時に検出）。ただし:
   - `f` が `**kwargs`（VAR_KEYWORD）を受ける場合、余剰キーは許容される（bind が自然にそう振る舞う）。
   - `signature()` が `ValueError`/`TypeError` を送出する callable（一部の builtin 等）は検査をスキップする。
   - チェックは Node 構築時に 1 回だけ実行され、実行時コストはない。

- 保存形式は parquet 固定。`path` の拡張子はチェックしない（ユーザー責任）。

### 2.2 run

```python
def run(
    root: Node,
    *,
    force: bool = False,
    max_workers: int = 1,
    keep_intermediate: bool = False,
) -> pl.DataFrame:
```

- `root` から到達可能な全ノードを実行（またはスキップ）し、`root` の DataFrame を返す。
- `force=True`: 全ノードを無条件に再計算。
- `max_workers`: 並列実行数。1 なら逐次（ThreadPoolExecutor を使わないパスにしてデバッグ容易性を確保）。
- `keep_intermediate=True`: メモリ解放（§5）を無効化し、実行後に全ノードの df を保持した `dict[Node, pl.DataFrame]` を返す…のは戻り値型が壊れるので却下。代わりに `run` は常に root の df のみ返し、中間結果が欲しい場合はユーザーが該当 Node を個別に `run` する（ファイルから読むだけなので安価）。**`keep_intermediate` 引数は実装しない。** ここに書いたのは設計判断の記録として。

### 2.3 例外

- `CycleError(ValueError)`: 閉路検出時。閉路に含まれるノードの path 一覧をメッセージに含める。
- `DuplicatePathError(ValueError)`: 異なるノードが同一 `path` を持つとき。
- `PipelineError(RuntimeError)`: ノード実行またはロードの失敗。`node: Node` 属性と `__cause__` に元例外を持つ。複数ノードが並列に失敗した場合は最初の 1 件を raise し、残りは `__notes__` に path を追記する（ExceptionGroup は使わない。呼び出し側の except が煩雑になるため）。

## 3. グラフ処理（graph.py）

### 3.1 収集とバリデーション

`root` から DFS で全ノードを収集する。この時点で:

- 閉路検出: 訪問中/訪問済みの 2 状態管理（前回実装した visiting/done 方式）。閉路発見時は `CycleError`。再帰でなくスタックベースの反復実装にすること（深い DAG での RecursionError 回避）。
- 同一 path 検出: `dict[Path, Node]` を構築し、`path.resolve()` が同じで `id` が異なるノードがあれば `DuplicatePathError`。

### 3.2 toposort

`graphlib.TopologicalSorter` を使う。自前実装しない。`TopologicalSorter` は `prepare()` 後に `get_ready()` / `done()` で ready-queue として使えるため、レベル分けせずにそのまま並列スケジューラの中核になる（§6）。

閉路検出は 3.1 で先に行うため、`graphlib.CycleError` は原理的に発生しないが、発生した場合は `CycleError` に変換して再送出する。

### 3.3 消費者カウント

メモリ解放（§5）のため、各ノードの出次数（そのノードを dep として参照するノード数）を `dict[Node, int]` として計算する。root は消費者 0 だが解放対象外。

## 4. 実行判定（stale 判定込み）

ノード `n` は以下のいずれかで **再計算** となる。判定は toposort 順に行うため、上流の判定結果は確定済み。

1. `force=True`
2. `n.path` が存在しない
3. いずれかの dep が今回の実行で再計算された（**推移的 stale**。in-memory の再計算結果を使うので mtime 比較より確実）
4. いずれかの dep について `dep.path.stat().st_mtime > n.path.stat().st_mtime`（前回実行以降に上流ファイルが手動更新/別プロセス更新されたケース）

条件 3 があるため、条件 4 は「今回の run の外」で起きた変更の検出専用。mtime の分解能問題（同一秒内の連続書き込み）は条件 3 が吸収するので、`>=` でなく `>` でよい。

既知の制約: 外部プロセスが `n` の書き込みと同一秒内（または mtime の分解能内）に `dep` を書き換えた場合、条件 4 はこれを検出できない（`>` を `>=` にすると通常実行のたびに恒久的な false-positive 再計算を招くため、この盲点は許容する）。Pass 1 は各ノードの mtime を訪問時点で 1 回だけ取得してメモ化するため、この盲点は「Pass 1 実行中の外部書き換え」にも同様に及ぶ。将来 `Node` に `version: str = ""` を足して判定に混ぜる拡張（§10 参照）が正道。

再計算とならないノードは `pl.read_parquet(n.path)` でロードする。ただし **下流に再計算ノードが 1 つもない場合はロード自体を省略できる**。実装方針: 判定パスを 2 段に分ける。

- Pass 1（逐次・軽量）: toposort 順に全ノードの再計算要否 `needs_compute: dict[Node, bool]` を確定。
- Pass 2: `needs_compute[root]` が False なら `pl.read_parquet(root.path)` を返して終了。そうでなければ、再計算ノードの依存として実際に必要なノード集合（再計算ノードの deps の閉包 ∩ 非再計算ノード）だけをロード対象にする。

## 5. メモリ解放

参照カウント方式。Pass 2 の実行中:

- 各ノードの df を `cache: dict[Node, pl.DataFrame]` に保持。
- ノード `n` の実行（またはロード）完了後、`n` の各 dep のカウントをデクリメント。0 になった dep を `cache` から `pop` する。
- root は常に保持。
- 実行スキップにより Pass 2 でロードされないノードは、カウント計算の対象からも除外する（カウントは「Pass 2 で実際に消費されるエッジ数」で初期化する）。

並列実行時は `cache` とカウントの更新を単一の `threading.Lock` で保護する。粒度を細かくする必要はない（df の計算自体は lock 外、dict 操作のみ lock 内）。

## 6. 並列実行

- `TopologicalSorter.prepare()` → メインループ: `get_ready()` で ready ノードを取得し、`ThreadPoolExecutor(max_workers)` に submit。future 完了ごとに `done(node)` を呼び、次の ready を submit する。`concurrent.futures.wait(..., return_when=FIRST_COMPLETED)` ベースのループで実装する。
- `needs_compute=False` かつロード対象のノードも同じスケジューラに乗せる（read_parquet も I/O なので並列化の恩恵がある）。ロード対象外のノードは submit せず即 `done()` を呼ぶ。
- **失敗時のセマンティクス**: 最初の失敗を検知したら新規 submit を停止し、実行中の future の完了を待ってから `PipelineError` を raise。future の cancel は試みる（未開始のものだけ効く）。
- スレッドベースで十分（Polars が GIL を解放して内部並列するため、プロセスプールは不要。df のプロセス間転送コストも回避できる）。
- `max_workers=1` のときは Executor を使わず同期ループで実行する。スタックトレースを素直に保つため。

## 7. atomic write

```python
tmp = n.path.with_name(n.path.name + ".tmp")
df.write_parquet(tmp)
tmp.replace(n.path)  # rename でなく replace（既存ファイル上書きの明示）
```

- `n.path.parent.mkdir(parents=True, exist_ok=True)` を書き込み前に実行。
- 例外時は tmp を best-effort で削除（`missing_ok=True`）。
- 同一ディレクトリ内 rename なので POSIX では atomic。Windows の挙動差は関知しない（docstring に一言書く）。

## 8. ログ

`logging.getLogger("moktan")` に対して、ノードごとに 1 行: `computed <path> (X.XXs)` / `loaded <path>` / `skipped <path>`。print 禁止。

## 9. テスト

pytest、`tmp_path` fixture でファイル I/O を実体で行う（モックしない）。

必須ケース:

1. 線形 3 ノード: 初回全計算 → 2 回目全スキップ（`f` の呼び出し回数を closure のカウンタで検証）。
2. ダイヤモンド DAG: 共有 dep の `f` が 1 回だけ呼ばれる。
3. 閉路: `CycleError`。frozen dataclass で閉路を作るには `object.__setattr__` か dict の後差しが必要なので、テストヘルパーで生成する。
4. 同一 path の別ノード: `DuplicatePathError`。
5. stale 伝播（条件 3）: 中間ノードのファイルを削除して再実行 → そのノードと下流のみ再計算、上流はロードのみ。
6. mtime stale（条件 4）: 上流 parquet を `os.utime` で未来に更新 → 下流が再計算される。
7. root 既存 & 全 fresh: `f` が一切呼ばれず、root のロード 1 回だけで返る（Pass 2 の省略ロードの検証）。
8. 失敗ノード: `PipelineError` が raise され、`__cause__` が元例外、失敗ノードの tmp ファイルが残っていない、既存の成果物が破壊されていない。
9. 失敗後の再開: 失敗より上流の成功ノードは 2 回目にスキップされる（checkpoint 再開の本質の検証）。
10. 並列 (`max_workers=4`): 幅 4 の独立ノード群で結果が逐次実行と一致。hypothesis でランダム DAG（ノード数 ≤ 20）を生成し、逐次と並列で全成果物ファイルの内容が一致することを確認。
11. メモリ解放: 実行完了時点で cache に root 以外が残っていないことを内部 API 経由で検証（runner がテスト用に最終 cache サイズを返すか、ログ/フックで観測）。
12. `deps` と `kwargs` のキー衝突: `ValueError`。
13. シグネチャ不整合: 存在しない引数名の dep キー → 構築時 `ValueError`。必須引数の不足 → 構築時 `ValueError`。`f` が `**kwargs` を受ける場合は余剰キーでもエラーにならない。`signature()` が取れない callable では検査がスキップされ構築が成功する。
14. 複数 deps の名前対応: §2.1 の join 例と同型のケースで、`deps` の辞書挿入順を入れ替えても結果が同一（名前対応であることの検証）。

## 10. スコープ外（実装しない）

- kwargs / 関数コードのハッシュによる invalidation（将来拡張。設計だけ意識: Node に `version: str = ""` フィールドを足して mtime 判定に混ぜる余地を残すが、今回は実装しない）
- リトライ、fail 後の部分継続
- parquet 以外のフォーマット、LazyFrame 対応
- 分散実行、プロセスプール
