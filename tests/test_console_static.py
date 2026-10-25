"""The served frontend, checked without a browser.

There is no Node toolchain in this project by choice, so there is no JS test
runner. These tests cover what can be verified from the files themselves --
and the most valuable of those is CSP compliance, because a violation is
invisible until a browser silently refuses to apply a style or run a script.
"""

from __future__ import annotations

import re
from pathlib import Path

import httpx
import pytest

STATIC = Path(__file__).resolve().parents[1] / "src" / "switchyard" / "console" / "static"
INDEX = (STATIC / "index.html").read_text()


def test_the_static_files_exist():
    for name in ("index.html", "app.css", "app.js", "map.js"):
        assert (STATIC / name).is_file(), name


# -- content security policy -----------------------------------------------
#
# The console sends `default-src 'self'` with no 'unsafe-inline'. A browser
# will quietly drop anything that violates it, so the page has to be clean.


def test_no_inline_style_blocks():
    assert "<style" not in INDEX.lower()


def test_no_inline_style_attributes():
    """`style="..."` is blocked by style-src; setting .style from JS is not."""
    assert not re.search(r"<[^>]+\sstyle\s*=", INDEX, re.I)


def test_no_inline_scripts():
    """Every script tag must load a file; an inline body would be blocked."""
    for tag in re.findall(r"<script\b[^>]*>(.*?)</script>", INDEX, re.S | re.I):
        assert not tag.strip(), "inline script body would be refused by the CSP"
    assert '<script type="module" src="/static/app.js">' in INDEX


def test_no_event_handler_attributes():
    """onclick= and friends are inline script under the CSP."""
    assert not re.search(r"<[^>]+\son[a-z]+\s*=", INDEX, re.I)


def test_every_asset_is_same_origin():
    """No CDN, no font service, no analytics -- so the policy can stay strict."""
    for url in re.findall(r'(?:src|href)="([^"]+)"', INDEX):
        if url.startswith(("#", "/static/", "data:")):
            continue
        assert url.startswith("https://github.com/"), f"unexpected external asset: {url}"


# -- accessibility and honesty ---------------------------------------------


def test_status_is_never_colour_alone():
    """Every breaker state carries an icon and a word in the JS, not a colour."""
    map_js = (STATIC / "map.js").read_text()
    for state, word in (("closed", "Closed"), ("half_open", "Half-open"), ("open", "Open")):
        assert state in map_js and word in map_js


def test_reduced_motion_is_respected():
    css = (STATIC / "app.css").read_text()
    assert "prefers-reduced-motion" in css
    assert "prefers-reduced-motion" in (STATIC / "map.js").read_text()


def test_the_legend_does_not_claim_a_dot_is_a_request():
    """A particle stands for a measured rate. Saying otherwise would be a lie
    at 25 rps and unreadable at 300."""
    assert "not an individual request" in INDEX


def test_dark_and_light_are_both_defined():
    css = (STATIC / "app.css").read_text()
    assert "prefers-color-scheme: dark" in css
    assert 'name="color-scheme"' in INDEX


# -- served correctly -------------------------------------------------------


async def test_the_console_serves_the_page_and_its_assets(console):
    async with httpx.AsyncClient(base_url=console.base_url, timeout=10.0) as c:
        index = await c.get("/")
        assert index.status_code == 200
        assert index.headers["content-type"].startswith("text/html")
        assert "Switchyard Console" in index.text

        for name, prefix in (("app.css", "text/css"),
                             ("app.js", "text/javascript"),
                             ("map.js", "text/javascript")):
            response = await c.get(f"/static/{name}")
            assert response.status_code == 200, name
            assert response.headers["content-type"].startswith(prefix), name


@pytest.mark.parametrize("path", ["/", "/static/app.js", "/static/app.css"])
async def test_security_headers_cover_the_page_and_its_assets(console, path):
    async with httpx.AsyncClient(base_url=console.base_url, timeout=10.0) as c:
        headers = (await c.get(path)).headers
    assert "frame-ancestors 'none'" in headers["content-security-policy"]
    assert headers["x-content-type-options"] == "nosniff"


async def test_every_element_the_frontend_looks_up_exists_in_the_page():
    """Catches an id renamed in one file and not the other.

    The JS reaches for elements by id. A typo gives a silent no-op, which on
    a live map looks like "the data is not arriving" rather than a bug.
    """
    app_js = (STATIC / "app.js").read_text()
    ids = set(re.findall(r'\$\("([a-z0-9-]+)"\)', app_js))
    ids |= set(re.findall(r'getElementById\("([a-z0-9-]+)"\)', app_js))
    ids |= set(re.findall(r'getElementById\("([a-z0-9-]+)"\)',
                          (STATIC / "map.js").read_text()))
    present = set(re.findall(r'id="([^"]+)"', INDEX))
    missing = sorted(ids - present)
    assert not missing, f"frontend looks up ids that the page does not define: {missing}"
