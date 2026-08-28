"""Terminal rendering helpers.

Plain ANSI rather than a rendering library. The views here are a handful of
aligned columns and a few colours; pulling in a dependency to draw them would
add install weight to a tool whose main selling point is that it runs
immediately with nothing to set up.
"""

from __future__ import annotations

import os
import shutil
import sys

RESET = "\033[0m"
BOLD = "\033[1m"
DIM = "\033[2m"
RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"

CLEAR_SCREEN = "\033[H\033[J"
HIDE_CURSOR = "\033[?25l"
SHOW_CURSOR = "\033[?25h"


def colour_enabled() -> bool:
    """Honour NO_COLOR and disable colour when piped, so captures stay clean."""
    if os.environ.get("NO_COLOR"):
        return False
    return sys.stdout.isatty()


class Style:
    """Colour helpers that become no-ops when colour is off."""

    def __init__(self, enabled: bool | None = None) -> None:
        self.enabled = colour_enabled() if enabled is None else enabled

    def _wrap(self, code: str, text: str) -> str:
        return f"{code}{text}{RESET}" if self.enabled else text

    def bold(self, t: str) -> str:
        return self._wrap(BOLD, t)

    def dim(self, t: str) -> str:
        return self._wrap(DIM, t)

    def red(self, t: str) -> str:
        return self._wrap(RED, t)

    def green(self, t: str) -> str:
        return self._wrap(GREEN, t)

    def yellow(self, t: str) -> str:
        return self._wrap(YELLOW, t)

    def blue(self, t: str) -> str:
        return self._wrap(BLUE, t)

    def cyan(self, t: str) -> str:
        return self._wrap(CYAN, t)


def compact(n: float) -> str:
    """Human-readable counts: 1.9M rather than 1900000."""
    for limit, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= limit:
            return f"{n / limit:.1f}{suffix}"
    return f"{n:.0f}"


def bar(fraction: float, width: int = 12, style: Style | None = None) -> str:
    """A simple utilisation bar. Reads instantly in a screen recording."""
    style = style or Style()
    fraction = max(0.0, min(1.0, fraction))
    filled = round(fraction * width)
    body = "#" * filled + "." * (width - filled)
    if fraction >= 0.9:
        return style.red(body)
    if fraction >= 0.6:
        return style.yellow(body)
    return style.green(body)


def terminal_width(default: int = 100) -> int:
    return shutil.get_terminal_size((default, 24)).columns
