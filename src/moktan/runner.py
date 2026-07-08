"""Execution engine: stale判定・並列実行・メモリ解放・atomic write."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from graphlib import TopologicalSorter
from pathlib import Path
from queue import SimpleQueue

import polars as pl

from moktan.graph import Graph, build_graph, consumer_counts
from moktan.node import Node

logger = logging.getLogger("moktan")


class PipelineError(RuntimeError):
    """A node's ``f`` (or its write) raised. ``node`` identifies the failed node,
    ``__cause__`` holds the original exception."""

    def __init__(self, node: Node) -> None:
        super().__init__(f"failed to compute node: {node.path}")
        self.node = node


@dataclass(frozen=True)
class Plan:
    """Pass 1 output: which nodes are stale and which ones Pass 2 must touch.

    ``recompute`` is every node whose ``f`` must run. ``load_targets`` is the
    non-stale nodes that feed a recompute node and must therefore be read from
    disk. ``pass2`` is their union -- everything the Pass 2 scheduler submits.
    Nodes outside ``pass2`` are never touched: not recomputed, not loaded.
    """

    needs_compute: dict[Node, bool]
    recompute: frozenset[Node]
    load_targets: frozenset[Node]
    pass2: frozenset[Node]


def run(root: Node, *, force: bool = False, max_workers: int = 1) -> pl.DataFrame:
    """Run (or resume) the pipeline rooted at ``root`` and return its DataFrame.

    Nodes whose parquet already reflects their current inputs are skipped or
    merely loaded; everything else is (re)computed and atomically written.
    """
    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers}")

    graph = build_graph(root)
    plan = _plan(graph, force=force)

    for node in graph.order:
        if node is not root and node not in plan.pass2:
            logger.info("skipped %s", node.path)

    if not plan.needs_compute[root]:
        df = pl.read_parquet(root.path)
        logger.info("loaded %s", root.path)
        return df

    cache = _execute_pass2(graph, plan, root, max_workers=max_workers)
    return cache[root]


def _plan(graph: Graph, *, force: bool) -> Plan:
    needs_compute = _determine_stale(graph, force=force)
    recompute = frozenset(node for node in graph.order if needs_compute[node])
    load_targets = frozenset(
        dep for node in recompute for dep in node.deps.values() if not needs_compute[dep]
    )
    return Plan(
        needs_compute=needs_compute,
        recompute=recompute,
        load_targets=load_targets,
        pass2=recompute | load_targets,
    )


def _determine_stale(graph: Graph, *, force: bool) -> dict[Node, bool]:
    """Pass 1: sequentially decide, in topological order, which nodes are stale.

    Each node's mtime is stat'd at most once (memoized), regardless of how many
    consumers it has.
    """
    needs_compute: dict[Node, bool] = {}
    mtimes: dict[Node, float | None] = {}

    def mtime(node: Node) -> float | None:
        if node not in mtimes:
            try:
                mtimes[node] = node.path.stat().st_mtime
            except FileNotFoundError:
                mtimes[node] = None
        return mtimes[node]

    for node in graph.order:
        node_mtime = mtime(node)
        if force or node_mtime is None:
            needs_compute[node] = True
            continue
        if any(needs_compute[dep] for dep in node.deps.values()):
            needs_compute[node] = True
            continue
        # By this point every dep has needs_compute[dep] is False, so its file
        # exists and mtime(dep) is never None -- checked anyway for typing.
        if any(
            (dep_mtime := mtime(dep)) is not None and dep_mtime > node_mtime
            for dep in node.deps.values()
        ):
            needs_compute[node] = True
            continue
        needs_compute[node] = False
    return needs_compute


def _atomic_write(path: Path, df: pl.DataFrame) -> None:
    """Write ``df`` to ``path`` via tmp-then-replace so a partial write never
    clobbers a previously valid parquet. Atomic on POSIX; not guaranteed on
    Windows."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    try:
        df.write_parquet(tmp)
        tmp.replace(path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def _compute_or_load(
    node: Node, needs_compute_flag: bool, kwargs: dict[str, pl.DataFrame]
) -> pl.DataFrame:
    if needs_compute_flag:
        start = time.perf_counter()
        df = node.f(**kwargs, **node.kwargs)
        _atomic_write(node.path, df)
        logger.info("computed %s (%.2fs)", node.path, time.perf_counter() - start)
    else:
        df = pl.read_parquet(node.path)
        logger.info("loaded %s", node.path)
    return df


def _execute_pass2(
    graph: Graph, plan: Plan, root: Node, *, max_workers: int
) -> dict[Node, pl.DataFrame]:
    # The sorter is restricted to plan.pass2: nodes outside it are never yielded
    # by get_ready(), so the execution loops below don't need to special-case
    # (and separately log) skipping them -- run() already did that once.
    sorter = graph.sorter(plan.pass2)
    edges = {node: node.deps.values() for node in plan.recompute}
    counts = consumer_counts(plan.pass2, edges)
    cache: dict[Node, pl.DataFrame] = {}

    if max_workers == 1:
        _run_sequential(sorter, plan, cache, counts, root)
    else:
        _run_parallel(sorter, plan, cache, counts, root, max_workers)
    return cache


def _prepare_kwargs(
    node: Node, plan: Plan, cache: dict[Node, pl.DataFrame]
) -> dict[str, pl.DataFrame]:
    if not plan.needs_compute[node]:
        return {}
    return {name: cache[dep] for name, dep in node.deps.items()}


def _finish_node(
    node: Node,
    df: pl.DataFrame,
    plan: Plan,
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
) -> None:
    cache[node] = df
    if not plan.needs_compute[node]:
        return  # loaded nodes never touched their own deps' cache entries
    for dep in node.deps.values():
        counts[dep] -= 1
        if counts[dep] == 0 and dep is not root:
            del cache[dep]


def _run_sequential(
    sorter: TopologicalSorter[Node],
    plan: Plan,
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
) -> None:
    while sorter.is_active():
        for node in sorter.get_ready():
            kwargs = _prepare_kwargs(node, plan, cache)
            try:
                df = _compute_or_load(node, plan.needs_compute[node], kwargs)
            except Exception as exc:
                raise PipelineError(node) from exc
            _finish_node(node, df, plan, cache, counts, root)
            sorter.done(node)


def _run_parallel(
    sorter: TopologicalSorter[Node],
    plan: Plan,
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
    max_workers: int,
) -> None:
    lock = threading.Lock()
    # A completion queue (fed by add_done_callback) preserves actual completion
    # order, unlike concurrent.futures.wait()'s unordered `done` set -- needed
    # so "the first failure" (spec) is deterministic when several futures fail
    # in the same scheduling window.
    completed: SimpleQueue[Future[pl.DataFrame]] = SimpleQueue()
    pending: dict[Future[pl.DataFrame], Node] = {}
    failures: list[tuple[Node, BaseException]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        def submit_ready() -> None:
            for node in sorter.get_ready():
                with lock:
                    kwargs = _prepare_kwargs(node, plan, cache)
                future = executor.submit(_compute_or_load, node, plan.needs_compute[node], kwargs)
                pending[future] = node
                future.add_done_callback(completed.put)

        submit_ready()
        while pending:
            future = completed.get()
            node = pending.pop(future)
            if future.cancelled():
                continue
            exc = future.exception()
            if exc is not None:
                failures.append((node, exc))
                for other in pending:
                    other.cancel()
                continue
            with lock:
                _finish_node(node, future.result(), plan, cache, counts, root)
            sorter.done(node)
            if not failures:
                submit_ready()

    if failures:
        first_node, first_exc = failures[0]
        err = PipelineError(first_node)
        for other_node, _ in failures[1:]:
            err.add_note(f"also failed: {other_node.path}")
        raise err from first_exc
