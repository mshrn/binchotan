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
from typing import Any, Literal, Protocol

import structlog

from moktan.node import Node

# --- event vocabulary (§3, §4) -----------------------------------------------
# events.py is the single owner of the event schema: event names, their field
# vocabulary, and the Literal types that classify specific fields. runner.py
# and recorder.py import from here rather than each keeping their own copy,
# so adding/renaming an event only requires editing one module.

Reason = Literal["missing", "forced", "dep_stale", "dep_newer", "fresh"]
Decision = Literal["compute", "load", "skip"]

# Which events represent a node's terminal (one-per-node) outcome for a run,
# per the §8 contract ("every reachable node gets exactly one of these, or
# node_failed/node_cancelled on a failed run").
TERMINAL_NODE_EVENTS = frozenset(
    {"node_computed", "node_loaded", "node_skipped", "node_failed", "node_cancelled"}
)

logger = logging.getLogger("moktan")
# Standard library etiquette for a library logger: without this, a record at
# WARNING or above (e.g. node_failed/run_failed, both ERROR) falls through to
# logging.lastResort and prints to stderr even when the application never
# configured any handler -- breaking the "silent by default" contract (§1,
# §9-10). NullHandler makes "no handler configured" mean what it says.
logger.addHandler(logging.NullHandler())


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


def _format_duration(value: float) -> str:
    return f"{value:.2f}"


# Single source of truth for "which characters break the one-event-one-line
# console contract" (rev5 §1.3: `_needs_quoting` and the bare-token escape
# below used to each hardcode `\n` separately, and both silently missed `\r`
# as a result -- a lone carriage return still split a line for the same
# reason `\n` does).
_LINE_BREAK_ESCAPES: dict[str, str] = {"\n": "\\n", "\r": "\\r"}


def _needs_quoting(value: str) -> bool:
    # Any of these break the whitespace-delimited "key=value ..." tail (or,
    # for `"`/a line-break character, the "one event, one line" console
    # contract itself) if left bare -- e.g. a node_failed message from
    # `str(exc)`.
    return any(c in value for c in (" ", '"', "=", *_LINE_BREAK_ESCAPES))


def _escape_bare_token(value: str) -> str:
    # For a value that must appear as a BARE (unquoted) token per the
    # split()[:2] back-compat contract (§6.1) -- the legacy-verb head's node
    # path. It can't be wrapped in quotes without breaking that contract, so
    # a space still splits it into extra tokens (known, accepted limitation,
    # documented in the spec). A line-break character is different: left
    # alone it would corrupt the "one event, one line" invariant regardless
    # of quoting, so those are neutralized here.
    for raw, escaped in _LINE_BREAK_ESCAPES.items():
        value = value.replace(raw, escaped)
    return value


def _format_list_element(value: object) -> str:
    # List elements are always quoted; switch from repr()'s single quotes to
    # json.dumps()'s escaping double quotes only when needed. A space in an
    # element stays unescaped -- accepted limitation, see spec §6.1.
    if isinstance(value, str) and _needs_quoting(value):
        return json.dumps(value)
    return repr(value)


def _format_value(value: object) -> str:
    if isinstance(value, float):
        # Dispatch on type, not the key name "duration_s": any future float
        # field gets the same 2-decimal rendering instead of falling through
        # to full-precision repr.
        return _format_duration(value)
    if isinstance(value, list):
        return "[" + ", ".join(_format_list_element(v) for v in value) + "]"
    if isinstance(value, str) and _needs_quoting(value):
        return json.dumps(value)  # also escapes embedded `"` and newlines
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
        if isinstance(node, str):
            node = _escape_bare_token(node)
        head = verb if node is None else f"{verb} {node}"
        if duration is not None:
            head = f"{head} ({_format_duration(duration)}s)"
    else:
        head = event

    tail = " ".join(f"{k}={_format_value(v)}" for k, v in rest.items())
    message = f"{head} {tail}" if tail else head
    return (message,), {"extra": {"moktan_event": raw}}


_struct_logger = structlog.wrap_logger(
    logger,
    processors=[_render_console_message],
    # Pinned explicitly: wrap_logger() falls back to the *application's*
    # global structlog.configure() wrapper_class/context_class when these are
    # omitted, breaking the "independent of global config" contract this
    # module documents above. Left implicit, an app configuring e.g.
    # make_filtering_bound_logger(INFO) silently drops moktan's DEBUG events,
    # and an app configuring the generic structlog.BoundLogger crashes every
    # _emit call (its .log() signature doesn't match ours).
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
)


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
        # list.remove() uses __eq__, which must be identity-based here so two
        # sinks with equal-by-value .events (e.g. both still empty) can never
        # be confused with each other: RunRecorder is eq=False (like Node),
        # and a plain class with no __eq__ override already gets identity
        # equality from object. Any future _EventSink implementer must keep
        # that property.
        _registry.remove(sink)


def _dispatch(event: dict[str, Any]) -> None:
    with _registry_lock:
        sinks = list(_registry)
    for sink in sinks:
        try:
            sink.events.append(event)
        except Exception as exc:  # noqa: BLE001 - deliberately broad, see below
            # rev5 §1.1 (root fix for the recurring "same contract, N emission
            # sites, one patched" bug class): a broken sink must never be able
            # to affect what run() returns or raises, no matter which of the
            # ~11 _emit call sites it chokes on -- isolating it here (instead
            # of wrapping individual call sites in runner.py) makes that true
            # by construction everywhere at once, and a broken sink no longer
            # starves sinks registered after it in the loop. KeyboardInterrupt
            # / SystemExit deliberately are NOT caught: an operator's Ctrl-C
            # must still abort the run even if it lands inside a sink's
            # .append().
            logger.warning(
                "moktan: a sink failed to record event %r (run_id=%s): %r",
                event.get("event"),
                event.get("run_id"),
                exc,
            )


# --- emission ----------------------------------------------------------------


def _listening(level: int) -> bool:
    """Would anything actually observe an event emitted at ``level`` right
    now -- either a sink (which bypasses stdlib levels entirely, spec §2) or
    the stdlib ``"moktan"`` logger itself? Shared by :func:`_emit`'s early
    return and by callers (e.g. runner.py's node_planned loop) that want to
    skip building expensive event fields before ever calling :func:`_emit`."""
    return bool(_registry) or logger.isEnabledFor(level)


def _emit(
    ctx: RunContext, event: str, level: int, *, node: Node | None = None, **fields: object
) -> None:
    if not _listening(level):
        # Nobody's listening: skip building the event dict, formatting the
        # timestamp, and rendering the console string. structlog's processor
        # chain runs *before* the stdlib level check, so without this the
        # full render cost is paid (and thrown away) for every event on every
        # run, even with logging fully unconfigured.
        return
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
    if logger.isEnabledFor(level):
        # A sink-only listener (RunRecorder attached, stdlib logger left
        # unconfigured) already got its event via _dispatch above; skip
        # rendering the console line and pushing it through structlog only to
        # have the stdlib logger discard it at its own level check.
        _struct_logger.log(level, event, **{k: v for k, v in ordered.items() if k != "event"})


def moktan_event(record: logging.LogRecord) -> dict[str, Any] | None:
    """Recover the full structured event dict moktan attached to a LogRecord
    it emitted (the same dict handed to RunRecorder sinks). Returns ``None``
    for a record moktan didn't emit. This is the one accessor for the
    ``extra`` attribute ``_render_console_message`` sets -- used by
    :class:`_JSONFormatter` and available for an application's own custom
    formatter/handler (§6.2)."""
    return getattr(record, "moktan_event", None)


# --- configure_logging (§6.2) -------------------------------------------------


class _JSONFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        event = moktan_event(record)
        if event is not None:
            return json.dumps(event, default=str)
        # A plain stdlib record on the "moktan" logger that didn't go through
        # _emit -- currently only _dispatch's best-effort "a sink failed"
        # warning (§1.1). Still emit valid, self-describing JSON rather than
        # falling back to plain text: the §6.2 contract is "every line in the
        # file independently parses with json.loads", and that must hold even
        # on the one run where observability itself already hiccupped.
        return json.dumps(
            {
                "event": "log_message",
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
        )


_installed_handlers: list[logging.Handler] = []
_configure_lock = threading.Lock()


def configure_logging(
    level: int = logging.INFO, *, console: bool = True, json_path: Path | None = None
) -> None:
    """Opt-in helper an application can call to see moktan's log output.

    Never called internally by moktan itself -- without this (or an
    application's own handler on the ``"moktan"`` logger), moktan stays
    silent (§9-10).

    Idempotent: calling this again *replaces* the previous configuration
    (removes and closes whatever handlers a prior call installed) rather than
    stacking duplicate handlers, so re-running setup code (a notebook cell, a
    module imported twice) doesn't multiply every log line.
    """
    target = logging.getLogger("moktan")
    with _configure_lock:
        # Guards the whole read-clear-append sequence on _installed_handlers,
        # matching _registry's _registry_lock protection: without this, two
        # near-simultaneous calls can both read the same old list before
        # either clears it, double-installing handlers.
        for handler in _installed_handlers:
            target.removeHandler(handler)
            handler.close()
        _installed_handlers.clear()

        target.setLevel(level)
        if console:
            console_handler: logging.Handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter("%(message)s"))
            target.addHandler(console_handler)
            _installed_handlers.append(console_handler)
        if json_path is not None:
            json_handler: logging.Handler = logging.FileHandler(json_path)
            json_handler.setFormatter(_JSONFormatter())
            target.addHandler(json_handler)
            _installed_handlers.append(json_handler)
