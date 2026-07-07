"""Node: declarative description of a single pipeline step."""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl


@dataclass(frozen=True, eq=False)
class Node:
    """One DAG node: where to persist ``f``'s result and what feeds it.

    Identity is by ``id()`` (``eq=False`` keeps the default identity-based
    ``__hash__``), so two ``Node`` instances that happen to share a ``path`` are
    still distinct nodes -- caught as a ``DuplicatePathError`` once both are
    reachable from a ``run()`` root.

    ``deps`` keys correspond to ``f``'s parameter names: at execution time ``f``
    is called as ``f(**{k: df_k for k, node_k in deps.items()}, **kwargs)``.
    """

    path: Path
    f: Callable[..., pl.DataFrame]
    deps: Mapping[str, Node] = field(default_factory=dict)
    kwargs: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        dep_keys = set(self.deps.keys())
        kwarg_keys = set(self.kwargs.keys())
        overlap = dep_keys & kwarg_keys
        if overlap:
            raise ValueError(f"deps and kwargs share keys: {sorted(overlap)}")

        try:
            signature = inspect.signature(self.f)
        except (ValueError, TypeError):
            # Some callables (certain builtins, etc.) don't expose a signature;
            # skip the bind check rather than rejecting a valid Node.
            return

        try:
            signature.bind(**dict.fromkeys(dep_keys | kwarg_keys))
        except TypeError as exc:
            raise ValueError(
                f"{self.f!r} is not callable with deps={sorted(dep_keys)} "
                f"kwargs={sorted(kwarg_keys)}: {exc}"
            ) from exc
