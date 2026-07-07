import polars as pl
import pytest

from moktan import Node


def test_deps_kwargs_key_collision_raises(tmp_path):
    def f(a: pl.DataFrame, on: str) -> pl.DataFrame:
        return a

    dep = Node(tmp_path / "dep.parquet", lambda: pl.DataFrame({"x": [1]}))
    with pytest.raises(ValueError):
        Node(tmp_path / "out.parquet", f, deps={"on": dep}, kwargs={"on": "x"})


def test_unknown_dep_key_raises(tmp_path):
    def f(a: pl.DataFrame) -> pl.DataFrame:
        return a

    dep = Node(tmp_path / "dep.parquet", lambda: pl.DataFrame({"x": [1]}))
    with pytest.raises(ValueError):
        Node(tmp_path / "out.parquet", f, deps={"a": dep, "z": dep})


def test_missing_required_arg_raises(tmp_path):
    def f(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return a

    dep = Node(tmp_path / "dep.parquet", lambda: pl.DataFrame({"x": [1]}))
    with pytest.raises(ValueError):
        Node(tmp_path / "out.parquet", f, deps={"a": dep})


def test_var_keyword_absorbs_extra_keys(tmp_path):
    def f(**kwargs: pl.DataFrame) -> pl.DataFrame:
        return next(iter(kwargs.values()))

    dep = Node(tmp_path / "dep.parquet", lambda: pl.DataFrame({"x": [1]}))
    node = Node(tmp_path / "out.parquet", f, deps={"anything": dep, "goes": dep})
    assert dict(node.deps) == {"anything": dep, "goes": dep}


def test_uninspectable_callable_skips_signature_check(tmp_path, monkeypatch):
    def f(**kwargs: pl.DataFrame) -> pl.DataFrame:
        raise AssertionError("not called")

    def raise_value_error(_callable: object) -> None:
        raise ValueError("no signature found")

    monkeypatch.setattr("moktan.node.inspect.signature", raise_value_error)

    dep = Node(tmp_path / "dep.parquet", lambda: pl.DataFrame({"x": [1]}))
    # `f` doesn't really accept `nonsense`, but since signature introspection is
    # unavailable, the bind check is skipped and construction still succeeds.
    node = Node(tmp_path / "out.parquet", f, deps={"nonsense": dep})
    assert dict(node.deps) == {"nonsense": dep}


def test_multiple_deps_named_by_key_not_order(tmp_path):
    def fetch_users() -> pl.DataFrame:
        return pl.DataFrame({"user_id": [1, 2], "name": ["a", "b"]})

    def fetch_orders() -> pl.DataFrame:
        return pl.DataFrame({"user_id": [1, 2], "amount": [10, 20]})

    def join_orders(users: pl.DataFrame, orders: pl.DataFrame, on: str) -> pl.DataFrame:
        return users.join(orders, on=on).sort("user_id")

    from moktan import run

    users = Node(tmp_path / "users.parquet", fetch_users)
    orders = Node(tmp_path / "orders.parquet", fetch_orders)

    joined_a = Node(
        tmp_path / "joined_a.parquet",
        join_orders,
        deps={"users": users, "orders": orders},
        kwargs={"on": "user_id"},
    )
    joined_b = Node(
        tmp_path / "joined_b.parquet",
        join_orders,
        deps={"orders": orders, "users": users},  # insertion order swapped
        kwargs={"on": "user_id"},
    )

    df_a = run(joined_a)
    df_b = run(joined_b)
    assert df_a.equals(df_b)
