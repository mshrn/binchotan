"""Execution engine: stale判定・並列実行・メモリ解放・atomic write."""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from graphlib import TopologicalSorter
from pathlib import Path

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


def run(root: Node, *, force: bool = False, max_workers: int = 1) -> pl.DataFrame:
    """Run (or resume) the pipeline rooted at ``root`` and return its DataFrame.

    Nodes whose parquet already reflects their current inputs are skipped or
    merely loaded; everything else is (re)computed and atomically written.
    """
    graph = build_graph(root)
    needs_compute = _determine_stale(graph, force=force)

    recompute_nodes = {node for node in graph.order if needs_compute[node]}
    load_targets = {
        dep
        for node in recompute_nodes
        for dep in node.deps.values()
        if not needs_compute[dep]
    }
    pass2_nodes = recompute_nodes | load_targets

    for node in graph.order:
        if node is not root and node not in pass2_nodes:
            logger.info("skipped %s", node.path)

    if not needs_compute[root]:
        df = pl.read_parquet(root.path)
        logger.info("loaded %s", root.path)
        return df

    cache = _execute_pass2(graph, pass2_nodes, needs_compute, root, max_workers=max_workers)
    return cache[root]


def _determine_stale(graph: Graph, *, force: bool) -> dict[Node, bool]:
    """Pass 1: sequentially decide, in topological order, which nodes are stale."""
    needs_compute: dict[Node, bool] = {}
    for node in graph.order:
        if force or not node.path.exists():
            needs_compute[node] = True
            continue
        if any(needs_compute[dep] for dep in node.deps.values()):
            needs_compute[node] = True
            continue
        node_mtime = node.path.stat().st_mtime
        if any(dep.path.stat().st_mtime > node_mtime for dep in node.deps.values()):
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
    graph: Graph,
    pass2_nodes: set[Node],
    needs_compute: dict[Node, bool],
    root: Node,
    *,
    max_workers: int,
) -> dict[Node, pl.DataFrame]:
    sorter = graph.sorter()
    edges = {node: node.deps.values() for node in pass2_nodes if needs_compute[node]}
    counts = consumer_counts(pass2_nodes, edges)
    cache: dict[Node, pl.DataFrame] = {}

    if max_workers == 1:
        _run_sequential(sorter, pass2_nodes, needs_compute, cache, counts, root)
    else:
        _run_parallel(sorter, pass2_nodes, needs_compute, cache, counts, root, max_workers)
    return cache


def _release_deps(
    node: Node,
    needs_compute: dict[Node, bool],
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
) -> None:
    if not needs_compute[node]:
        return  # loaded nodes never touched their own deps' cache entries
    for dep in node.deps.values():
        counts[dep] -= 1
        if counts[dep] == 0 and dep is not root:
            del cache[dep]


def _run_sequential(
    sorter: TopologicalSorter[Node],
    pass2_nodes: set[Node],
    needs_compute: dict[Node, bool],
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
) -> None:
    while sorter.is_active():
        for node in sorter.get_ready():
            if node not in pass2_nodes:
                logger.info("skipped %s", node.path)
                sorter.done(node)
                continue
            kwargs = (
                {name: cache[dep] for name, dep in node.deps.items()}
                if needs_compute[node]
                else {}
            )
            try:
                df = _compute_or_load(node, needs_compute[node], kwargs)
            except Exception as exc:
                raise PipelineError(node) from exc
            cache[node] = df
            sorter.done(node)
            _release_deps(node, needs_compute, cache, counts, root)


def _run_parallel(
    sorter: TopologicalSorter[Node],
    pass2_nodes: set[Node],
    needs_compute: dict[Node, bool],
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
    max_workers: int,
) -> None:
    lock = threading.Lock()
    errors: list[tuple[Node, BaseException]] = []
    futures: dict[Future[pl.DataFrame], Node] = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:

        def submit_ready() -> None:
            for node in sorter.get_ready():
                if node not in pass2_nodes:
                    logger.info("skipped %s", node.path)
                    sorter.done(node)
                    continue
                if errors:
                    continue
                with lock:
                    kwargs = (
                        {name: cache[dep] for name, dep in node.deps.items()}
                        if needs_compute[node]
                        else {}
                    )
                future = executor.submit(_compute_or_load, node, needs_compute[node], kwargs)
                futures[future] = node

        submit_ready()
        while futures:
            done, _ = wait(list(futures), return_when=FIRST_COMPLETED)
            for future in done:
                node = futures.pop(future)
                if future.cancelled():
                    continue
                exc = future.exception()
                if exc is not None:
                    with lock:
                        errors.append((node, exc))
                        for pending in futures:
                            pending.cancel()
                    continue
                with lock:
                    cache[node] = future.result()
                    _release_deps(node, needs_compute, cache, counts, root)
                sorter.done(node)
                submit_ready()

    if errors:
        first_node, first_exc = errors[0]
        err = PipelineError(first_node)
        for other_node, _ in errors[1:]:
            err.add_note(f"also failed: {other_node.path}")
        raise err from first_exc
