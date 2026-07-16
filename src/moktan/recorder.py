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

from moktan.events import _register, _unregister

_TERMINAL_VERBS: dict[str, str] = {
    "node_computed": "computed",
    "node_loaded": "loaded",
}

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
        node_ids = {path: f"n{i}" for i, path in enumerate(order)}
        labels = _labels(order)
        consumers: dict[str, list[str]] = {path: [] for path in order}
        for path in order:
            for dep in deps[path]:
                consumers[dep].append(path)

        lines = ["flowchart LR"]
        for path in order:
            summary, state = _terminal(events, path)
            suffix = f":::{state}" if state else ""
            definition = f'{node_ids[path]}["{labels[path]}<br/>{summary}"]{suffix}'
            targets = consumers[path]
            if targets:
                for target_path in targets:
                    lines.append(f"    {definition} --> {node_ids[target_path]}")
            else:
                lines.append(f"    {definition}")
        return "\n".join(lines) + "\n" + _CLASSDEFS

    def to_markdown(self, run_id: str | None = None) -> str:
        target, events = self._events_for_run(run_id)
        order, _deps = _graph_view(events)
        planned = {e["node"]: e for e in events if e["event"] == "node_planned"}

        final = next((e for e in events if e["event"] in ("run_finished", "run_failed")), None)
        status = final["status"] if final else "unknown"
        duration = final["duration_s"] if final else 0.0
        n_computed = sum(1 for e in events if e["event"] == "node_computed")
        n_loaded = sum(1 for e in events if e["event"] == "node_loaded")
        n_skipped = sum(1 for e in events if e["event"] == "node_skipped")

        lines = [
            f"# Run {target}",
            "",
            f"- status: {status}",
            f"- duration: {duration:.2f}s",
            f"- computed: {n_computed} / loaded: {n_loaded} / skipped: {n_skipped}",
            "",
            "```mermaid",
            self.to_mermaid(target),
            "```",
            "",
            "| node | decision | reason | duration_s | rows | bytes |",
            "|---|---|---|---|---|---|",
        ]
        for path in order:
            plan_event = planned.get(path, {})
            decision = plan_event.get("decision", "—")
            reason = plan_event.get("reason", "—")
            terminal_event = _terminal_event(events, path)
            duration_cell = (
                f"{terminal_event['duration_s']:.2f}"
                if terminal_event and "duration_s" in terminal_event
                else "—"
            )
            rows_cell = str(terminal_event["rows"]) if terminal_event and "rows" in terminal_event else "—"
            bytes_cell = (
                str(terminal_event["bytes"]) if terminal_event and "bytes" in terminal_event else "—"
            )
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


def _terminal_event(events: list[dict[str, Any]], node: str) -> dict[str, Any] | None:
    for event in events:
        if event.get("node") == node and event["event"] in (
            "node_computed",
            "node_loaded",
            "node_skipped",
            "node_failed",
            "node_cancelled",
        ):
            return event
    return None


def _terminal(events: list[dict[str, Any]], node: str) -> tuple[str, str | None]:
    event = _terminal_event(events, node)
    if event is None:
        return "not started", None
    kind = event["event"]
    if kind in _TERMINAL_VERBS:
        verb = _TERMINAL_VERBS[kind]
        summary = f"{verb} {event['duration_s']:.2f}s, {event['rows']} rows"
        return summary, verb
    if kind == "node_skipped":
        return "skipped", "skipped"
    if kind == "node_failed":
        return "FAILED", "failed"
    return "cancelled", "cancelled"  # node_cancelled


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
