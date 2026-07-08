"""DAG collection, cycle / duplicate-path validation and toposort."""

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
    nodes share a resolved output path. The returned order is already a valid
    topological order: a node is appended only once every one of its deps is
    ``done``, so no separate toposort pass over the result is needed.
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


@dataclass(frozen=True)
class Graph:
    order: list[Node]
    predecessors: dict[Node, frozenset[Node]]

    def sorter(self, nodes: Iterable[Node] | None = None) -> TopologicalSorter[Node]:
        """A fresh, prepared TopologicalSorter.

        With no argument, schedules the full graph. Pass a node subset (e.g. the
        Pass 2 execution set) to get a sorter restricted to just those nodes and
        the edges between them -- nodes outside the subset are never yielded by
        ``get_ready()``.

        Precondition for subsets: no node in ``nodes`` may need the output of a
        node outside ``nodes`` -- edges crossing the subset boundary are simply
        dropped, so any ordering through an excluded node is not preserved. This
        holds for Pass 2's ``plan.pass2`` (every dep of a recompute node is
        itself in ``pass2``, by construction of ``Plan``) but is not safe for an
        arbitrary caller-chosen subset.

        Construction walks ``self.order`` rather than the subset directly, so
        that ``TopologicalSorter``'s insertion order -- and therefore
        ``get_ready()``'s tie-break order among same-depth nodes -- stays
        deterministic across processes. Iterating a bare ``set``/``frozenset``
        of ``Node`` would vary run to run, since ``Node`` hashes by identity
        (object address).

        Cycles are already ruled out by ``build_graph``; the conversion below is a
        defensive net in case graphlib's own detection disagrees.
        """
        predecessors: Mapping[Node, Iterable[Node]]
        if nodes is None:
            predecessors = self.predecessors
        else:
            subset = frozenset(nodes)
            predecessors = {
                node: self.predecessors[node] & subset for node in self.order if node in subset
            }
        ts: TopologicalSorter[Node] = TopologicalSorter(predecessors)
        try:
            ts.prepare()
        except _GraphlibCycleError as exc:  # pragma: no cover - guarded by build_graph
            raise CycleError(str(exc)) from exc
        return ts


def build_graph(root: Node) -> Graph:
    """Collect and validate all nodes reachable from ``root``."""
    nodes = _collect_nodes(root)
    predecessors = {node: frozenset(node.deps.values()) for node in nodes}
    return Graph(order=nodes, predecessors=predecessors)
