"""Shared test helpers."""

from __future__ import annotations

import logging
from typing import Any


def moktan_event(record: logging.LogRecord) -> dict[str, Any]:
    """The full structured event dict moktan attaches to each LogRecord it
    emits (see moktan.events._render_console_message). Reading it through
    this typed helper (instead of `record.moktan_event` directly) avoids
    depending on the dynamically-added `extra` attribute at the type level:
    `getattr` with a dynamic name is typed `Any` rather than an attribute
    error.
    """
    return getattr(record, "moktan_event")
