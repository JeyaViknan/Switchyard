"""Process harness for experiments.

Every experiment runs the fleet and the gateway as separate subprocesses and
drives load from the runner process. That separation is not incidental: an
earlier version shared one event loop between the generator and the gateway, and
the generator's own CPU work delayed the system it was measuring.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time

import httpx


def free_port() -> int:
    """Reserve a port by binding and releasing it.

    Racy in principle; in practice the window is microseconds, and the
    alternative -- parsing a port out of a subprocess's log output -- is worse.
    """
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


class Service:
    """A uvicorn subprocess."""

    def __init__(self, app: str, port: int | None = None,
                 env: dict[str, str] | None = None, factory: bool = False) -> None:
        self.port = port or free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self._proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", app,
                "--host", "127.0.0.1", "--port", str(self.port),
                "--log-level", "error", "--no-access-log",
                *(["--factory"] if factory else []),
            ],
            env={**os.environ, **(env or {})},
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

    async def wait_healthy(self, timeout_s: float = 30.0) -> None:
        deadline = time.perf_counter() + timeout_s
        async with httpx.AsyncClient(timeout=1.0) as c:
            while time.perf_counter() < deadline:
                if self._proc.poll() is not None:
                    err = self._proc.stderr.read().decode() if self._proc.stderr else ""
                    raise RuntimeError(f"{self.base_url} exited during startup:\n{err}")
                try:
                    if (await c.get(f"{self.base_url}/health")).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(0.05)
        raise RuntimeError(f"{self.base_url} did not become healthy in {timeout_s}s")

    def stop(self) -> None:
        self._proc.terminate()
        try:
            self._proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self._proc.kill()
            self._proc.wait(timeout=5)


class Stack:
    """A fleet and a gateway, started together and stopped together."""

    def __init__(self, gateway_env: dict[str, str] | None = None) -> None:
        self.fleet = Service("switchyard.synthetic.app:app")
        self.gateway = Service(
            "switchyard.gateway.app:create_app", factory=True,
            env={"SWITCHYARD_FLEET_URL": self.fleet.base_url, **(gateway_env or {})},
        )

    async def start(self, run_seed: int | None = None) -> None:
        await self.fleet.wait_healthy()
        await self.gateway.wait_healthy()
        if run_seed is not None:
            async with httpx.AsyncClient(timeout=5.0) as c:
                await c.put(f"{self.fleet.base_url}/control/seed", json={"run_seed": run_seed})

    @property
    def completions_url(self) -> str:
        return f"{self.gateway.base_url}/v1/chat/completions"

    def stop(self) -> None:
        self.gateway.stop()
        self.fleet.stop()
