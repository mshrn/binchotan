# moktan

ファイルベース checkpoint 付き DataFrame パイプラインランナー。

DataFrame 変換の DAG を `Node` で宣言し、`run()` に渡すと各ノードの結果を parquet
として永続化しながら実行する。成果物ファイル自体が checkpoint となり、実行済み
ノードはスキップされ、失敗した run も途中から再開できる。詳細な設計は
[designdoc/flume_spec.md](designdoc/flume_spec.md) を参照。

## 使い方

```python
from pathlib import Path

import polars as pl

from moktan import Node, run


def fetch_users() -> pl.DataFrame:
    return pl.DataFrame({"user_id": [1, 2], "name": ["a", "b"]})


def fetch_orders() -> pl.DataFrame:
    return pl.DataFrame({"user_id": [1, 2], "amount": [10, 20]})


def join_orders(users: pl.DataFrame, orders: pl.DataFrame, on: str) -> pl.DataFrame:
    return users.join(orders, on=on)


users = Node(Path("out/users.parquet"), fetch_users)
orders = Node(Path("out/orders.parquet"), fetch_orders)
joined = Node(
    Path("out/joined.parquet"),
    join_orders,
    deps={"users": users, "orders": orders},
    kwargs={"on": "user_id"},
)

df = run(joined)
```

## 開発

```sh
uv sync
uv run pytest
uv run ty check
```
