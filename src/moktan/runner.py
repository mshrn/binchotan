"""Execution engine: stale判定・並列実行・メモリ解放・atomic write・構造化ログ."""

from __future__ import annotations

import logging
import threading
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from graphlib import TopologicalSorter
from pathlib import Path
from queue import SimpleQueue

import polars as pl

from moktan.events import Decision, Reason, RunContext, _emit, _listening, new_run_id
from moktan.graph import Graph, build_graph
from moktan.node import Node


class PipelineError(RuntimeError):
    """A node's ``f`` (or its write) raised. ``node`` identifies the first
    failed node, ``__cause__`` holds its original exception. ``failed`` lists
    every node that failed in this run (in completion-processed order);
    ``failed[0] is node`` always holds."""

    def __init__(self, node: Node, failed: list[Node] | None = None) -> None:
        super().__init__(f"failed to compute node: {node.path}")
        self.node = node
        self.failed = failed if failed is not None else [node]


@dataclass(frozen=True)
class Plan:
    """Pass 1 output: which nodes are stale (and why) and which ones Pass 2
    must touch.

    ``recompute`` is every node whose ``f`` must run. ``pass2`` additionally
    includes the non-stale nodes that feed a recompute node and must
    therefore be read from disk. Nodes outside ``pass2`` are never touched:
    not recomputed, not loaded.
    """

    needs_compute: dict[Node, bool]
    reasons: dict[Node, Reason]
    recompute: frozenset[Node]
    pass2: frozenset[Node]


def run(root: Node, *, force: bool = False, max_workers: int = 1) -> pl.DataFrame:
    """Run (or resume) the pipeline rooted at ``root`` and return its DataFrame.

    Nodes whose parquet already reflects their current inputs are skipped or
    merely loaded; everything else is (re)computed and atomically written.
    Emits the structured event stream described in
    designdoc/flume_logging_spec.md §3 -- see that spec for the full event
    catalogue and console/JSON Lines formats. ``run_started`` always pairs
    with exactly one of ``run_finished`` / ``run_failed`` (§8): graph
    validation happens before ``run_started`` is emitted (a bad DAG means the
    run never started, so it gets no events at all), and everything from
    ``run_started`` onward that can affect whether the *pipeline* succeeds is
    wrapped so any exception -- not just ``PipelineError`` -- closes the run
    with ``run_failed``. Event emission itself never raises due to a broken
    sink: a sink whose ``.events.append()`` raises is caught and warned about
    per-event inside ``events._dispatch`` (rev5 §1.1), so no ``_emit`` call
    anywhere in this function can turn pipeline success into ``run_failed``,
    or leave ``run_started`` dangling unpaired -- only a genuine pipeline
    failure changes which of the two closing events fires.
    """
    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers}")

    graph = build_graph(root)  # CycleError/DuplicatePathError here: run never started, no events

    ctx = RunContext(run_id=new_run_id())
    start = time.perf_counter()
    _emit(ctx, "run_started", logging.INFO, root=str(root.path), force=force, max_workers=max_workers)

    try:
        plan_start = time.perf_counter()
        plan = _plan(graph, root, force=force)
        plan_duration = time.perf_counter() - plan_start

        decisions = {node: _decision(node, plan) for node in graph.order}
        n_compute = sum(1 for d in decisions.values() if d == "compute")
        n_load = sum(1 for d in decisions.values() if d == "load")
        n_skip = len(graph.order) - n_compute - n_load
        _emit(
            ctx,
            "plan_computed",
            logging.INFO,
            n_nodes=len(graph.order),
            n_compute=n_compute,
            n_load=n_load,
            n_skip=n_skip,
            duration_s=plan_duration,
        )

        if _listening(logging.DEBUG):
            # Skip building a deps=[...] list (a Path-to-str conversion per
            # edge) for every node when nobody could observe node_planned
            # anyway -- same guard _emit itself uses internally, hoisted here
            # so the per-node work is skipped too, not just the emission.
            for node in graph.order:
                _emit(
                    ctx,
                    "node_planned",
                    logging.DEBUG,
                    node=node,
                    decision=decisions[node],
                    reason=plan.reasons[node],
                    deps=[str(dep.path) for dep in node.deps.values()],
                )

        for node in graph.order:
            if decisions[node] == "skip":
                _emit(ctx, "node_skipped", logging.INFO, node=node)

        # root always ends up in plan.pass2 (see _plan), so it's always
        # handled by the normal Pass 2 machinery -- including node_failed
        # emission on a load failure. No separate fresh-root shortcut.
        cache = _execute_pass2(ctx, graph, plan, root, max_workers=max_workers)
        df = cache[root]
    except PipelineError as exc:
        _emit_run_failed(ctx, start, failed=[str(n.path) for n in exc.failed])
        raise
    except BaseException as exc:
        # Anything other than PipelineError (a bug in _plan, an OSError from
        # stat(), KeyboardInterrupt, ...): still close the run so run_started
        # never dangles unpaired (§8).
        _emit_run_failed(ctx, start, failed=[], exc=exc)
        raise

    # The pipeline has already succeeded (df is ready) by this point.
    _emit(
        ctx,
        "run_finished",
        logging.INFO,
        status="ok",
        duration_s=time.perf_counter() - start,
        n_computed=n_compute,
        n_loaded=n_load,
        n_skipped=n_skip,
    )
    return df


def _emit_run_failed(
    ctx: RunContext, start: float, *, failed: list[str], exc: BaseException | None = None
) -> None:
    duration_s = time.perf_counter() - start
    if exc is not None:
        _emit(
            ctx,
            "run_failed",
            logging.ERROR,
            status="failed",
            duration_s=duration_s,
            failed=failed,
            error=type(exc).__name__,
            message=str(exc),
        )
    else:
        _emit(ctx, "run_failed", logging.ERROR, status="failed", duration_s=duration_s, failed=failed)


def _decision(node: Node, plan: Plan) -> Decision:
    if node in plan.recompute:
        return "compute"
    if node in plan.pass2:
        return "load"
    return "skip"


def _plan(graph: Graph, root: Node, *, force: bool) -> Plan:
    reasons = _determine_stale(graph, force=force)
    needs_compute = {node: reason != "fresh" for node, reason in reasons.items()}
    recompute = frozenset(node for node in graph.order if needs_compute[node])
    load_targets = {
        dep for node in recompute for dep in node.deps.values() if not needs_compute[dep]
    }
    pass2 = recompute | load_targets
    if root not in pass2:
        # root has no consumers (it's the sink), so it can never be a load
        # target for anything else -- but a fresh root must still be loaded
        # and returned. Folding it into pass2 here means run() has exactly
        # one execution path (_execute_pass2) instead of a separate
        # fresh-root shortcut that would need its own node_failed handling.
        pass2 = pass2 | {root}
    return Plan(
        needs_compute=needs_compute,
        reasons=reasons,
        recompute=recompute,
        pass2=pass2,
    )


def _determine_stale(graph: Graph, *, force: bool) -> dict[Node, Reason]:
    """Pass 1: sequentially decide, in topological order, why each node is (or
    isn't) stale. Each node's file is stat'd at most once, regardless of how
    many consumers it has: ``fresh_mtimes`` records a node's mtime only once
    it's decided fresh, and only fresh nodes are ever looked up there (a
    stale dep always short-circuits its consumer via the recompute-
    propagation check first).
    """
    if force:
        return dict.fromkeys(graph.order, "forced")

    reasons: dict[Node, Reason] = {}
    fresh_mtimes: dict[Node, float] = {}

    for node in graph.order:
        try:
            node_mtime = node.path.stat().st_mtime
        except FileNotFoundError:
            reasons[node] = "missing"
            continue
        if any(reasons[dep] != "fresh" for dep in node.deps.values()):
            reasons[node] = "dep_stale"
            continue
        if any(fresh_mtimes[dep] > node_mtime for dep in node.deps.values()):
            reasons[node] = "dep_newer"
            continue
        reasons[node] = "fresh"
        fresh_mtimes[node] = node_mtime
    return reasons


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


def _file_size(path: Path) -> int | None:
    try:
        return path.stat().st_size
    except OSError:
        # Best-effort: the write already succeeded (checkpoint is durable on
        # disk) by the time this runs. A stat failure here (external removal,
        # a flaky network FS) shouldn't turn a successful compute into a
        # node_failed/PipelineError -- report the size as unknown instead.
        return None


def _compute_or_load(
    ctx: RunContext, node: Node, needs_compute_flag: bool, kwargs: dict[str, pl.DataFrame]
) -> pl.DataFrame:
    if needs_compute_flag:
        start = time.perf_counter()
        df = node.f(**kwargs, **node.kwargs)
        _atomic_write(node.path, df)
        _emit(
            ctx,
            "node_computed",
            logging.INFO,
            node=node,
            duration_s=time.perf_counter() - start,
            rows=df.height,
            columns=df.width,
            bytes=_file_size(node.path),
        )
    else:
        start = time.perf_counter()
        df = pl.read_parquet(node.path)
        _emit(
            ctx,
            "node_loaded",
            logging.INFO,
            node=node,
            duration_s=time.perf_counter() - start,
            rows=df.height,
        )
    return df


def _execute_pass2(
    ctx: RunContext, graph: Graph, plan: Plan, root: Node, *, max_workers: int
) -> dict[Node, pl.DataFrame]:
    # The sorter is restricted to plan.pass2: nodes outside it are never yielded
    # by get_ready(), so the execution loops below don't need to special-case
    # (and separately log) skipping them -- run() already did that once.
    sorter = graph.sorter(plan.pass2)
    counts = Counter(dep for node in plan.recompute for dep in node.deps.values())
    cache: dict[Node, pl.DataFrame] = {}

    # No point spinning up a ThreadPoolExecutor when there's nothing to
    # compute (e.g. the all-fresh resume case, where pass2 is just {root}
    # being loaded) -- go sequential regardless of max_workers.
    if max_workers == 1 or not plan.recompute:
        _run_sequential(ctx, sorter, plan, cache, counts, root)
    else:
        _run_parallel(ctx, sorter, plan, cache, counts, root, max_workers)
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
    ctx: RunContext,
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
                df = _compute_or_load(ctx, node, plan.needs_compute[node], kwargs)
            except Exception as exc:
                _emit(
                    ctx, "node_failed", logging.ERROR, node=node,
                    error=type(exc).__name__, message=str(exc),
                )
                raise PipelineError(node) from exc
            _finish_node(node, df, plan, cache, counts, root)
            sorter.done(node)


def _run_parallel(
    ctx: RunContext,
    sorter: TopologicalSorter[Node],
    plan: Plan,
    cache: dict[Node, pl.DataFrame],
    counts: dict[Node, int],
    root: Node,
    max_workers: int,
) -> None:
    # Guards cache/counts, which spec section 5 requires to be lock-protected
    # during parallel execution. In the current implementation every mutation
    # of cache/counts actually happens on the main thread (worker threads only
    # run _compute_or_load, which touches neither) -- the lock is a no-op today
    # but is the precondition that would make it safe to move that bookkeeping
    # into a worker-thread callback later. Do not remove it on the grounds that
    # it's currently redundant.
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
                _emit(ctx, "node_submitted", logging.DEBUG, node=node)
                future = executor.submit(
                    _compute_or_load, ctx, node, plan.needs_compute[node], kwargs
                )
                pending[future] = node
                future.add_done_callback(completed.put)

        submit_ready()
        try:
            while pending:
                future = completed.get()
                node = pending.pop(future)
                if future.cancelled():
                    _emit(ctx, "node_cancelled", logging.DEBUG, node=node)
                    continue
                exc = future.exception()
                if exc is not None:
                    _emit(
                        ctx, "node_failed", logging.ERROR, node=node,
                        error=type(exc).__name__, message=str(exc),
                    )
                    failures.append((node, exc))
                    for other in pending:
                        other.cancel()
                    continue
                if failures:
                    continue  # already aborting -- drop the result, don't cache it
                with lock:
                    _finish_node(node, future.result(), plan, cache, counts, root)
                sorter.done(node)
                submit_ready()
        except BaseException:
            # An escape here (KeyboardInterrupt while blocked in completed.get(),
            # or a bug elsewhere in the loop) would otherwise hit the `with`
            # block's shutdown(wait=True) and block until every already-queued
            # task finishes, even ones that never started. Cancel what we can
            # (only not-yet-started futures are actually cancellable) before
            # letting the exception propagate; this doesn't affect the normal
            # failure path above, which always drains `pending` to empty first.
            for pending_future in pending:
                pending_future.cancel()
            raise

    if failures:
        first_node, first_exc = failures[0]
        err = PipelineError(first_node, failed=[n for n, _ in failures])
        for other_node, _ in failures[1:]:
            err.add_note(f"also failed: {other_node.path}")
        raise err from first_exc
