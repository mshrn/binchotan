"""RunRecorder: an event sink that turns the structured event stream (§3) into
Mermaid diagrams and Markdown reports (flume_logging_spec.md §7).

Unlike the logging path, a RunRecorder receives every event regardless of the
stdlib logger's configured level (moktan.events._dispatch fans out to it
directly) -- so DAG structure (from DEBUG-only node_planned events) is always
available for visualization even when the application logs at INFO or above.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from moktan.events import _LEGACY_VERBS, TERMINAL_NODE_EVENTS, _register, _unregister

_CLASSDEFS = (
    "    classDef computed fill:#dcfce7,stroke:#16a34a\n"
    "    classDef loaded   fill:#dbeafe,stroke:#2563eb\n"
    "    classDef skipped  fill:#f3f4f6,stroke:#9ca3af,color:#6b7280\n"
    "    classDef failed   fill:#fee2e2,stroke:#dc2626\n"
    "    classDef cancelled fill:#fef9c3,stroke:#ca8a04"
)


@dataclass(eq=False)
class RunRecorder:
    """Collects raw event dicts (§3) and renders them as Mermaid/Markdown.

    ``eq=False`` (like :class:`moktan.node.Node`) so the registry can
    unregister by identity: two recorders that happen to hold
    value-equal ``events`` lists (e.g. both still empty) must never be
    confused with each other.
    """

    events: list[dict[str, Any]] = field(default_factory=list)

    @contextmanager
    def attach(self) -> Iterator[RunRecorder]:
        """Register this recorder for the duration of the ``with`` block.
        Nestable, and independent of any other attached recorder."""
        _register(self)
        try:
            yield self
        finally:
            _unregister(self)

    def _events_for_run(self, run_id: str | None) -> tuple[str, list[dict[str, Any]]]:
        if not self.events:
            raise ValueError("no events recorded")
        target = run_id if run_id is not None else self.events[-1]["run_id"]
        return target, [e for e in self.events if e["run_id"] == target]

    def to_mermaid(self, run_id: str | None = None) -> str:
        _target, events = self._events_for_run(run_id)
        order, deps = _graph_view(events)
        terminal_by_node = _terminal_by_node(events)
        labels = _labels(order)
        return _render_mermaid(order, deps, labels, terminal_by_node)

    def to_markdown(self, run_id: str | None = None) -> str:
        target, events = self._events_for_run(run_id)
        # One snapshot (`events`), scanned once (`terminal_by_node`), shared
        # by the summary counts, the mermaid diagram, and the node table --
        # not three independent re-filters/re-scans of self.events. This
        # also means the embedded diagram can never disagree with the table
        # even if more events land mid-run (attach() still active).
        order, deps = _graph_view(events)
        terminal_by_node = _terminal_by_node(events)
        labels = _labels(order)
        mermaid = _render_mermaid(order, deps, labels, terminal_by_node)
        planned = {e["node"]: e for e in events if e["event"] == "node_planned"}

        final = next((e for e in events if e["event"] in ("run_finished", "run_failed")), None)
        status = final["status"] if final else "unknown"
        duration = final["duration_s"] if final else 0.0
        event_counts = Counter(e["event"] for e in events)

        lines = [
            f"# Run {target}",
            "",
            f"- status: {status}",
            f"- duration: {duration:.2f}s",
            f"- computed: {event_counts['node_computed']} / "
            f"loaded: {event_counts['node_loaded']} / skipped: {event_counts['node_skipped']}",
            "",
            "```mermaid",
            mermaid,
            "```",
            "",
            "| node | decision | reason | duration_s | rows | bytes |",
            "|---|---|---|---|---|---|",
        ]
        for path in order:
            plan_event = planned.get(path, {})
            decision = plan_event.get("decision", "—")
            reason = plan_event.get("reason", "—")
            terminal_event = terminal_by_node.get(path)
            duration_cell = (
                f"{terminal_event['duration_s']:.2f}"
                if terminal_event and "duration_s" in terminal_event
                else "—"
            )
            rows_cell = str(terminal_event["rows"]) if terminal_event and "rows" in terminal_event else "—"
            bytes_value = terminal_event.get("bytes") if terminal_event else None
            bytes_cell = str(bytes_value) if bytes_value is not None else "—"
            lines.append(f"| {path} | {decision} | {reason} | {duration_cell} | {rows_cell} | {bytes_cell} |")

        failure = _failure_section(events)
        if failure:
            lines += ["", failure]

        return "\n".join(lines)

    def write_report(self, path: Path, run_id: str | None = None) -> None:
        path.write_text(self.to_markdown(run_id))


def _graph_view(events: list[dict[str, Any]]) -> tuple[list[str], dict[str, list[str]]]:
    order: list[str] = []
    deps: dict[str, list[str]] = {}
    for event in events:
        if event["event"] == "node_planned":
            node = event["node"]
            order.append(node)
            deps[node] = list(event["deps"])
    return order, deps


def _labels(order: list[str]) -> dict[str, str]:
    paths = {p: Path(p) for p in order}
    name_counts = Counter(path.name for path in paths.values())
    labels: dict[str, str] = {}
    for p, path in paths.items():
        if name_counts[path.name] > 1:
            labels[p] = f"{path.parent.name}/{path.name}"
        else:
            labels[p] = path.name
    return labels


def _terminal_by_node(events: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Each node's terminal (§3 TERMINAL_NODE_EVENTS) event, in one O(events)
    pass -- first match wins, matching the previous per-node-rescan semantics."""
    result: dict[str, dict[str, Any]] = {}
    for event in events:
        node = event.get("node")
        if node is not None and node not in result and event["event"] in TERMINAL_NODE_EVENTS:
            result[node] = event
    return result


def _summarize(event: dict[str, Any] | None) -> tuple[str, str | None]:
    """(label summary, classDef state) for one node's terminal event."""
    if event is None:
        return "not started", None
    kind = event["event"]
    if kind in _LEGACY_VERBS:  # node_computed / node_loaded / node_skipped
        verb = _LEGACY_VERBS[kind]
        if kind == "node_skipped":
            return verb, verb
        return f"{verb} {event['duration_s']:.2f}s, {event['rows']} rows", verb
    if kind == "node_failed":
        return "FAILED", "failed"
    return "cancelled", "cancelled"  # node_cancelled


def _render_mermaid(
    order: list[str],
    deps: dict[str, list[str]],
    labels: dict[str, str],
    terminal_by_node: dict[str, dict[str, Any]],
) -> str:
    node_ids = {path: f"n{i}" for i, path in enumerate(order)}
    consumers: dict[str, list[str]] = {path: [] for path in order}
    for path in order:
        for dep in deps[path]:
            consumers[dep].append(path)

    lines = ["flowchart LR"]
    for path in order:
        summary, state = _summarize(terminal_by_node.get(path))
        suffix = f":::{state}" if state else ""
        definition = f'{node_ids[path]}["{labels[path]}<br/>{summary}"]{suffix}'
        targets = consumers[path]
        if targets:
            for target_path in targets:
                lines.append(f"    {definition} --> {node_ids[target_path]}")
        else:
            lines.append(f"    {definition}")
    return "\n".join(lines) + "\n" + _CLASSDEFS


def _failure_section(events: list[dict[str, Any]]) -> str | None:
    failed_events = [e for e in events if e["event"] == "node_failed"]
    if not failed_events:
        return None
    first, *rest = failed_events
    lines = [
        "## Failure",
        "",
        f"- node: {first['node']}",
        f"- error: {first['error']}",
        f"- message: {first['message']}",
    ]
    if rest:
        lines += ["", "Also failed:", ""]
        lines += [f"- {e['node']} ({e['error']}: {e['message']})" for e in rest]
    cancelled = [e["node"] for e in events if e["event"] == "node_cancelled"]
    if cancelled:
        lines += ["", "Cancelled (not started):", ""]
        lines += [f"- {node}" for node in cancelled]
    return "\n".join(lines)
