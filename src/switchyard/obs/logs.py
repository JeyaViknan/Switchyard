"""Structured logging.

Standard library only. The requirement here is consistent key/value events and
two output formats, both of which `logging` does with a formatter -- a
dependency would buy nothing.

Two formats, because the two audiences are different. `text` is the default and
is meant to be read by a person watching a terminal during a demo; `json` is one
object per line for anything that ships logs somewhere. Set with
`SWITCHYARD_LOG_FORMAT`.

Levels carry meaning here rather than being decoration:

  INFO   things an operator would want to notice -- a request refused, a
         failover, a breaker opening, a budget clamped, a drain starting.
  DEBUG  one line per completed request. Useful when chasing a specific
         request, far too noisy to leave on under load.

So the default output is quiet and every line in it is worth reading.

What is never logged: prompt or response content, API keys, and message text.
Requests are identified by id and tenant, and described by counts and
durations -- enough to reconstruct what happened without recording what was
said. A test asserts this.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from typing import Any

FIELDS_ATTR = "switchyard_fields"
FORMAT_ENV = "SWITCHYARD_LOG_FORMAT"
LEVEL_ENV = "SWITCHYARD_LOG_LEVEL"

# Field names that must never carry content, whatever a caller passes.
FORBIDDEN_FIELDS = frozenset({
    "prompt", "messages", "content", "response", "completion", "text",
    "api_key", "key", "authorization", "token_text",
})


class RedactionError(ValueError):
    """A log call tried to record something that must not be recorded."""


def _fields(record: logging.LogRecord) -> dict[str, Any]:
    return getattr(record, FIELDS_ATTR, {}) or {}


class TextFormatter(logging.Formatter):
    """Compact single line, for a person watching a terminal."""

    def format(self, record: logging.LogRecord) -> str:
        stamp = time.strftime("%H:%M:%S", time.localtime(record.created))
        pairs = " ".join(f"{k}={v}" for k, v in _fields(record).items())
        line = f"{stamp} {record.levelname:<5} {record.getMessage():<24} {pairs}".rstrip()
        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)
        return line


class JsonFormatter(logging.Formatter):
    """One object per line, for anything that ships logs elsewhere."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "event": record.getMessage(),
            **_fields(record),
        }
        if record.exc_info:
            payload["error"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def formatter_for(name: str) -> logging.Formatter:
    return JsonFormatter() if name.lower() == "json" else TextFormatter()


def configure(fmt: str | None = None, level: str | None = None,
              stream: Any = None) -> None:
    """Install a handler on the `switchyard` logger. Idempotent."""
    logger = logging.getLogger("switchyard")
    logger.handlers.clear()
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(formatter_for(fmt or os.environ.get(FORMAT_ENV, "text")))
    logger.addHandler(handler)
    logger.setLevel((level or os.environ.get(LEVEL_ENV, "INFO")).upper())
    # The gateway's logs are its own; letting them propagate would duplicate
    # every line through uvicorn's root handler.
    logger.propagate = False


def ensure_configured() -> None:
    """Set up logging only if nothing already has.

    The gateway calls this at startup so that running it directly produces
    readable output. It deliberately does not overwrite an existing setup: a
    library that reconfigures logging out from under the application embedding
    it is badly behaved, and it makes the embedding application's own choice of
    format and destination silently ineffective.
    """
    if not logging.getLogger("switchyard").handlers:
        configure()


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"switchyard.{name}")


def event(logger: logging.Logger, name: str, level: int = logging.INFO,
          **fields: Any) -> None:
    """Emit a structured event.

    Rejects field names that would carry request or credential content. This is
    a guard against a careless call site rather than a security boundary, but it
    turns "someone logged the prompt" from a silent leak into a loud failure.
    """
    forbidden = FORBIDDEN_FIELDS & set(fields)
    if forbidden:
        raise RedactionError(
            f"refusing to log field(s) {sorted(forbidden)} in event {name!r}: "
            f"prompts, responses and credentials are never logged"
        )
    if logger.isEnabledFor(level):
        logger.log(level, name, extra={FIELDS_ATTR: fields})
