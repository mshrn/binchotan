import polars as pl
import pytest

from moktan import CycleError, DuplicatePathError, Node, run


def test_cycle_detected(tmp_path):
    def fa() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def fb(x: pl.DataFrame) -> pl.DataFrame:
        return x

    a = Node(tmp_path / "a.parquet", fa)
    b = Node(tmp_path / "b.parquet", fb, deps={"x": a})
    # frozen dataclass: create the cycle by replacing `a`'s deps mapping after
    # construction, as suggested by the design doc.
    object.__setattr__(a, "deps", {"x": b})

    with pytest.raises(CycleError):
        run(b)


def test_duplicate_path_rejected(tmp_path):
    def f() -> pl.DataFrame:
        return pl.DataFrame({"x": [1]})

    def g(a: pl.DataFrame, b: pl.DataFrame) -> pl.DataFrame:
        return a

    shared_path = tmp_path / "shared.parquet"
    a = Node(shared_path, f)
    b = Node(shared_path, f)  # same path, distinct Node instance
    root = Node(tmp_path / "root.parquet", g, deps={"a": a, "b": b})

    with pytest.raises(DuplicatePathError):
        run(root)
