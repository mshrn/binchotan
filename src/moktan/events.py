"""Structured event emission: the single source of truth for run observability.

``_emit`` is the only place moktan writes log output (flume_logging_spec.md §2).
It fans out to two independent consumers:

- the stdlib ``"moktan"`` logger, via a private structlog-wrapped logger. This is
  subject to normal ``logging`` configuration (level, handlers) -- silent unless
  the application (or :func:`configure_logging`) attaches a handler.
- any attached :class:`~moktan.recorder.RunRecorder`-like sink, via the module
  registry below. This bypasses logging levels entirely: sinks always receive
  every event (including DEBUG-only ones like ``node_planned``), because
  visualization must not silently break when an application raises the
  ``"moktan"`` logger's level.

moktan never calls ``structlog.configure()`` (that's global, and would clobber
an application's own structlog setup); it builds one private wrapped logger
with :func:`structlog.wrap_logger` instead.
"""

from __future__ import annotations

import json
import logging
import threading
import uuid
from collections.abc import MutableMapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import structlog

from moktan.node import Node

logger = logging.getLogger("moktan")


@dataclass(frozen=True)
class RunContext:
    """Threaded explicitly through run()/_execute_pass2/_compute_or_load so that
    events emitted from worker threads carry the same run_id as the main
    thread (spec §5 -- contextvars don't survive ThreadPoolExecutor.submit)."""

    run_id: str


def new_run_id() -> str:
    return uuid.uuid4().hex[:12]


# --- console rendering (§6.1) -----------------------------------------------

# The 3 pre-existing verbs keep their bare "<verb> <path>" first two tokens
# for backward compatibility (§6.1, §9-9); every other event renders its
# `event` name as the first token and all fields (including `node`/`root`) as
# key=value, per §12.0.
_LEGACY_VERBS: dict[str, str] = {
    "node_computed": "computed",
    "node_loaded": "loaded",
    "node_skipped": "skipped",
}


def _format_value(key: str, value: object) -> str:
    if key == "duration_s":
        return f"{value:.2f}"
    if isinstance(value, list):
        return repr(value)
    if isinstance(value, str) and " " in value:
        return f'"{value}"'
    return str(value)


def _render_console_message(
    _logger: object, _method_name: str, event_dict: MutableMapping[str, Any]
) -> tuple[Any, ...]:
    """Final structlog processor: turn the event dict into the exact console
    line text (§6.1/§12.0), and stash the raw dict on the LogRecord (via
    ``extra``) so a JSON formatter can recover it losslessly (§6.2)."""
    raw = dict(event_dict)
    event = event_dict["event"]
    rest = {k: v for k, v in event_dict.items() if k not in ("event", "timestamp")}

    if event in _LEGACY_VERBS:
        verb = _LEGACY_VERBS[event]
        node = rest.pop("node", None)
        duration = rest.pop("duration_s", None)
        head = verb if node is None else f"{verb} {node}"
        if duration is not None:
            head = f"{head} ({duration:.2f}s)"
    else:
        head = event

    tail = " ".join(f"{k}={_format_value(k, v)}" for k, v in rest.items())
    message = f"{head} {tail}" if tail else head
    return (message,), {"extra": {"moktan_event": raw}}


_struct_logger = structlog.wrap_logger(logger, processors=[_render_console_message])


# --- RunRecorder registry (§2, §7) ------------------------------------------


class _EventSink(Protocol):
    events: list[dict[str, Any]]


_registry: list[_EventSink] = []
_registry_lock = threading.Lock()


def _register(sink: _EventSink) -> None:
    with _registry_lock:
        _registry.append(sink)


def _unregister(sink: _EventSink) -> None:
    with _registry_lock:
        # identity-based removal: RunRecorder is eq=False (like Node) so that
        # two recorders with equal-by-value .events lists (e.g. both still
        # empty) can never be confused with each other here.
        for i, existing in enumerate(_registry):
            if existing is sink:
                del _registry[i]
                return


def _dispatch(event: dict[str, Any]) -> None:
    with _registry_lock:
        sinks = list(_registry)
    for sink in sinks:
        sink.events.append(event)


# --- emission ----------------------------------------------------------------


def _emit(
    ctx: RunContext, event: str, level: int, *, node: Node | None = None, **fields: object
) -> None:
    ordered: dict[str, Any] = {"event": event}
    if node is not None:
        ordered["node"] = str(node.path)
    ordered.update(fields)
    ordered["thread"] = threading.current_thread().name
    ordered["run_id"] = ctx.run_id
    ordered["timestamp"] = datetime.now(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    _dispatch(dict(ordered))
    _struct_logger.log(level, event, **{k: v for k, v in ordered.items() if k != "event"})


# --- configure_logging (§6.2) -------------------------------------------------


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = getattr(record, "moktan_event", None)
        if event is None:  # pragma: no cover - defensive, all our records set it
            return super().format(record)
        return json.dumps(event, default=str)


def configure_logging(
    level: int = logging.INFO, *, console: bool = True, json_path: Path | None = None
) -> None:
    """Opt-in helper an application can call to see moktan's log output.

    Never called internally by moktan itself -- without this (or an
    application's own handler on the ``"moktan"`` logger), moktan stays
    silent (§9-10).
    """
    target = logging.getLogger("moktan")
    target.setLevel(level)
    if console:
        handler: logging.Handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(message)s"))
        target.addHandler(handler)
    if json_path is not None:
        json_handler = logging.FileHandler(json_path)
        json_handler.setFormatter(_JSONFormatter())
        target.addHandler(json_handler)
