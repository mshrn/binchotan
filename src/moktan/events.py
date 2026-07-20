"""Structured event emission: the single source of truth for run observability.

``_emit`` is the only place moktan emits events (flume_logging_spec.md §2).
It fans out to two independent consumers:

- the stdlib ``"moktan"`` logger, via a private structlog-wrapped logger. This is
  subject to normal ``logging`` configuration (level, handlers) -- silent unless
  the application (or :func:`configure_logging`) attaches a handler.
- any attached :class:`~moktan.recorder.RunRecorder`-like sink, via the module
  registry below. This bypasses logging levels entirely: sinks always receive
  every event (including DEBUG-only ones like ``node_planned``), because
  visualization must not silently break when an application raises the
  ``"moktan"`` logger's level.

The sole non-event write to the "moktan" logger is ``_dispatch``'s broken-sink
warning -- a plain record, covered by ``_JSONFormatter``'s fallback.

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


def _iso_timestamp(dt: datetime) -> str:
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _safe_str(obj: object) -> str:
    """str(obj) -- unless obj's __str__ itself raises, in which case a
    placeholder is returned instead of letting the exception escape.
    Every exception-to-message conversion feeding an event field MUST go
    through this: plain str(exc) at an _emit call site is evaluated before
    any of _emit's guards run, so a user exception with a broken __str__
    would otherwise replace PipelineError with the __str__ error (rev7 §1.1).
    KeyboardInterrupt/SystemExit from __str__ still propagate.
    """
    try:
        return str(obj)
    except Exception:  # noqa: BLE001
        return f"<unprintable {type(obj).__name__}>"


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


# Single source of truth for characters that break the one-event-one-line
# console contract: exactly the set str.splitlines() treats as line
# boundaries (rev6 §1.4 -- the rationale is splitlines-safety, so the set
# must match it). _needs_quoting and _escape_bare_token both derive from
# this dict. Quoted positions are safe via json.dumps escaping; the
# bare-token position uses the escape text on the right.
_LINE_BREAK_ESCAPES: dict[str, str] = {
    "\n": "\\n",
    "\r": "\\r",
    "\v": "\\x0b",
    "\f": "\\x0c",
    "\x1c": "\\x1c",
    "\x1d": "\\x1d",
    "\x1e": "\\x1e",
    "\x85": "\\x85",
    "\u2028": "\\u2028",  # \u30bd\u30fc\u30b9\u4e0a\u306f\u5fc5\u305a\u3053\u306e ASCII \u30a8\u30b9\u30b1\u30fc\u30d7\u8868\u8a18\u3067\u66f8\u304f
    "\u2029": "\\u2029",  # \u540c\u4e0a(\u751f\u306e\u4e0d\u53ef\u8996\u6587\u5b57\u3092\u57cb\u3081\u8fbc\u307e\u306a\u3044 -- rev7 \u00a71.2)
}

_QUOTE_TRIGGERS: tuple[str, ...] = (" ", '"', "=", *_LINE_BREAK_ESCAPES)


def _needs_quoting(value: str) -> bool:
    # Any of these break the whitespace-delimited "key=value ..." tail (or,
    # for `"`/a line-break character, the "one event, one line" console
    # contract itself) if left bare -- e.g. a node_failed message from
    # `str(exc)`.
    return any(c in value for c in _QUOTE_TRIGGERS)


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
        except Exception as exc:  # noqa: BLE001 - sink isolation, see comment
            # Sink isolation (rev5 §1.1): a broken sink must never affect what
            # run() returns/raises, nor starve sinks later in the loop.
            # KeyboardInterrupt/SystemExit deliberately propagate -- Ctrl-C
            # must still abort even mid-append.
            try:
                logger.warning(
                    "moktan: sink %s failed to record event %r (run_id=%s): %r",
                    type(sink).__name__,
                    event.get("event"),
                    event.get("run_id"),
                    exc,
                    extra={"moktan_run_id": event.get("run_id")},
                )
            except Exception:  # noqa: BLE001
                # The warning channel itself (app logging config on the
                # "moktan" logger) is broken too -- same reasoning as _emit's
                # stdlib-path guard below: drop it.
                pass


# --- emission ----------------------------------------------------------------


def _listening(level: int) -> bool:
    """True if a sink is attached (sinks bypass stdlib levels, spec §2) or the
    stdlib ``"moktan"`` logger would accept ``level``. Callers may use it to
    skip building expensive event fields before calling :func:`_emit`."""
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
    ordered["timestamp"] = _iso_timestamp(datetime.now(UTC))
    _dispatch(dict(ordered))
    if logger.isEnabledFor(level):
        # A sink-only listener (RunRecorder attached, stdlib logger left
        # unconfigured) already got its event via _dispatch above; skip
        # rendering the console line and pushing it through structlog only to
        # have the stdlib logger discard it at its own level check.
        try:
            _struct_logger.log(level, event, **{k: v for k, v in ordered.items() if k != "event"})
        except Exception:  # noqa: BLE001
            # Either the app's logging setup on the "moktan" logger raised
            # (stdlib absorbs only conforming Handler.emit errors -- see
            # review_notes.md rev6 §1.1), or moktan's own render processor
            # did (it runs inside this call). Both are notification-channel
            # failures with nothing sane to notify through: drop it. Sinks
            # already got the event via _dispatch. KeyboardInterrupt/
            # SystemExit still propagate.
            pass


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
        payload: dict[str, Any] = {
            "event": "log_message",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "timestamp": _iso_timestamp(datetime.fromtimestamp(record.created, UTC)),
        }
        run_id = getattr(record, "moktan_run_id", None)
        if run_id is not None:
            payload["run_id"] = run_id
        return json.dumps(payload)


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
