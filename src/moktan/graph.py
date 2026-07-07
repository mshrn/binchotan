"""DAG collection, cycle / duplicate-path validation, toposort and consumer counting."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from graphlib import CycleError as _GraphlibCycleError
from graphlib import TopologicalSorter
from pathlib import Path

from moktan.node import Node


class CycleError(ValueError):
    """Raised when the dependency graph contains a cycle."""


class DuplicatePathError(ValueError):
    """Raised when two distinct Node instances share the same output path."""


def _register_path(paths: dict[Path, Node], node: Node) -> None:
    resolved = node.path.resolve()
    existing = paths.get(resolved)
    if existing is not None and existing is not node:
        raise DuplicatePathError(f"multiple nodes write to {resolved}")
    paths[resolved] = node


def _extract_cycle(stack: list[tuple[Node, Iterator[Node]]], repeated: Node) -> list[Node]:
    nodes = [n for n, _ in stack]
    idx = nodes.index(repeated)
    return [*nodes[idx:], repeated]


def _collect_nodes(root: Node) -> list[Node]:
    """Iterative DFS collecting every node reachable from ``root`` via ``deps``.

    Uses visiting/done coloring to detect cycles and a stack-based (non-recursive)
    walk to avoid RecursionError on deep DAGs. Also validates that no two distinct
    nodes share a resolved output path.
    """
    paths: dict[Path, Node] = {}
    _register_path(paths, root)

    visiting: set[Node] = {root}
    done: set[Node] = set()
    order: list[Node] = []
    stack: list[tuple[Node, Iterator[Node]]] = [(root, iter(root.deps.values()))]

    while stack:
        node, dep_iter = stack[-1]
        advanced = False
        for dep in dep_iter:
            if dep in visiting:
                cycle = _extract_cycle(stack, dep)
                trail = " -> ".join(str(n.path) for n in cycle)
                raise CycleError(f"cycle detected: {trail}")
            if dep in done:
                continue
            _register_path(paths, dep)
            visiting.add(dep)
            stack.append((dep, iter(dep.deps.values())))
            advanced = True
            break
        if not advanced:
            stack.pop()
            visiting.discard(node)
            done.add(node)
            order.append(node)

    return order


def consumer_counts(
    nodes: Iterable[Node], predecessors: Mapping[Node, Iterable[Node]]
) -> dict[Node, int]:
    """出次数 (このノードを dep として参照するノード数) を ``nodes`` の範囲で計算する。"""
    counts = {node: 0 for node in nodes}
    for deps in predecessors.values():
        for dep in deps:
            counts[dep] += 1
    return counts


@dataclass(frozen=True)
class Graph:
    order: list[Node]
    predecessors: dict[Node, frozenset[Node]]

    def sorter(self) -> TopologicalSorter[Node]:
        """A fresh, prepared TopologicalSorter over the full graph.

        Cycles are already ruled out by ``build_graph``; the conversion below is a
        defensive net in case graphlib's own detection disagrees.
        """
        ts: TopologicalSorter[Node] = TopologicalSorter(self.predecessors)
        try:
            ts.prepare()
        except _GraphlibCycleError as exc:  # pragma: no cover - guarded by build_graph
            raise CycleError(str(exc)) from exc
        return ts


def build_graph(root: Node) -> Graph:
    """Collect and validate all nodes reachable from ``root``, then toposort them."""
    nodes = _collect_nodes(root)
    predecessors = {node: frozenset(node.deps.values()) for node in nodes}
    try:
        order = list(TopologicalSorter(predecessors).static_order())
    except _GraphlibCycleError as exc:  # pragma: no cover - guarded by _collect_nodes
        raise CycleError(str(exc)) from exc
    return Graph(order=order, predecessors=predecessors)
