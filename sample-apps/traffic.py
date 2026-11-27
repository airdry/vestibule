#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.12"
# dependencies = ["httpx>=0.28,<1"]
# ///
"""Continuous traffic generator for the vestibule sample stack.

The script sends ``GET /events`` and ``GET /transactions`` requests to the
FastAPI, Fastify, and Fiber sample applications, so the rate limiter can be
observed under load with its own Admin Plane metrics.

Every knob is an environment variable.

Per-service traffic mix (the three main knobs)::

    FASTAPI_TRAFFIC=events=5,transactions=1
    FASTIFY_TRAFFIC=events=5,transactions=1
    FIBER_TRAFFIC=events=5,transactions=1

A mix is a comma-separated list of ``kind`` or ``kind=weight`` entries, where
``kind`` is ``events`` or ``transactions``. ``off`` disables a service. The
weighted list is cycled deterministically, so ``events=5,transactions=1``
sends five event requests for every transaction request.

Per-service environments (cycled per round)::

    FASTAPI_ENVIRONMENTS=stage,production
    FASTIFY_ENVIRONMENTS=preprod,stage,production
    FIBER_ENVIRONMENTS=preprod,stage,production

Global knobs::

    TRAFFIC_INTERVAL=1.0            seconds between rounds
    TRAFFIC_COUNT=1                 items per request (1..1000)
    TRAFFIC_DURATION=0              run time in seconds; 0 runs until Ctrl+C
    TRAFFIC_TIMEOUT=10.0            per-request timeout in seconds
    FASTAPI_URL=http://127.0.0.1:8001
    FASTIFY_URL=http://127.0.0.1:8002
    FIBER_URL=http://127.0.0.1:8003

Run it with ``uv run traffic.py`` or ``mise run sample-apps-traffic``.
"""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from dataclasses import dataclass
from typing import Final

import httpx

KINDS: Final = ("events", "transactions")
DISABLED_MIXES: Final = frozenset({"off", "none", "disabled"})
DEFAULT_MIX: Final = "events=5,transactions=1"


@dataclass(frozen=True)
class Service:
    """One sample application and the requests it should receive."""

    name: str
    base_url: str
    raw_mix: str
    mix: tuple[str, ...]
    raw_environments: str
    environments: tuple[str, ...]

    @property
    def enabled(self) -> bool:
        return bool(self.mix)

    def kind_for(self, round_index: int) -> str:
        return self.mix[round_index % len(self.mix)]

    def environment_for(self, round_index: int) -> str:
        return self.environments[round_index % len(self.environments)]


@dataclass(frozen=True)
class Config:
    """Effective configuration parsed from the environment."""

    services: tuple[Service, ...]
    interval: float
    count: int
    duration: float
    timeout: float

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            services=(
                service_from_env(
                    "fastapi",
                    "FASTAPI_URL",
                    "FASTAPI_TRAFFIC",
                    "FASTAPI_ENVIRONMENTS",
                    "http://127.0.0.1:8001",
                    "stage,production",
                ),
                service_from_env(
                    "fastify",
                    "FASTIFY_URL",
                    "FASTIFY_TRAFFIC",
                    "FASTIFY_ENVIRONMENTS",
                    "http://127.0.0.1:8002",
                    "preprod,stage,production",
                ),
                service_from_env(
                    "fiber",
                    "FIBER_URL",
                    "FIBER_TRAFFIC",
                    "FIBER_ENVIRONMENTS",
                    "http://127.0.0.1:8003",
                    "preprod,stage,production",
                ),
            ),
            interval=env_float("TRAFFIC_INTERVAL", 1.0, minimum=0.01),
            count=env_int("TRAFFIC_COUNT", 1, minimum=1, maximum=1000),
            duration=env_float("TRAFFIC_DURATION", 0.0, minimum=0.0),
            timeout=env_float("TRAFFIC_TIMEOUT", 10.0, minimum=0.1),
        )


@dataclass
class Stats:
    """Per-service request counters for the final summary."""

    requests: int = 0
    succeeded: int = 0
    failed: int = 0
    items: int = 0

    def record_success(self, items: int) -> None:
        self.requests += 1
        self.succeeded += 1
        self.items += items

    def record_failure(self) -> None:
        self.requests += 1
        self.failed += 1


def service_from_env(
    name: str,
    url_var: str,
    mix_var: str,
    environments_var: str,
    default_url: str,
    default_environments: str,
) -> Service:
    raw_mix = env(mix_var, DEFAULT_MIX)
    raw_environments = env(environments_var, default_environments)
    return Service(
        name=name,
        base_url=env(url_var, default_url).rstrip("/"),
        raw_mix=raw_mix,
        mix=parse_mix(mix_var, raw_mix),
        raw_environments=raw_environments,
        environments=parse_environments(environments_var, raw_environments),
    )


def parse_mix(variable: str, raw: str) -> tuple[str, ...]:
    """Expands ``events=5,transactions=1`` into a weighted kind cycle."""
    if raw.strip().lower() in DISABLED_MIXES:
        return ()
    cycle: list[str] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry:
            continue
        kind, _, weight_raw = entry.partition("=")
        kind = kind.strip().lower()
        if kind not in KINDS:
            raise SystemExit(
                f"{variable}: unknown traffic kind {kind!r}; "
                f"expected {', '.join(KINDS)}, or off"
            )
        cycle.extend([kind] * parse_weight(variable, weight_raw))
    if not cycle:
        raise SystemExit(f"{variable}: empty traffic mix {raw!r}")
    return tuple(cycle)


def parse_weight(variable: str, raw: str) -> int:
    if not raw.strip():
        return 1
    try:
        weight = int(raw)
    except ValueError as error:
        raise SystemExit(
            f"{variable}: weight must be an integer, got {raw!r}"
        ) from error
    if weight < 1:
        raise SystemExit(f"{variable}: weight must be >= 1, got {weight}")
    return weight


def parse_environments(variable: str, raw: str) -> tuple[str, ...]:
    environments = tuple(entry.strip() for entry in raw.split(",") if entry.strip())
    if not environments:
        raise SystemExit(f"{variable}: empty environment list {raw!r}")
    return environments


def env(name: str, default: str) -> str:
    value = os.environ.get(name, "")
    return value if value.strip() else default


def env_float(name: str, default: float, *, minimum: float) -> float:
    raw = env(name, str(default))
    try:
        value = float(raw)
    except ValueError as error:
        raise SystemExit(f"{name}: expected a number, got {raw!r}") from error
    if value < minimum:
        raise SystemExit(f"{name}: expected >= {minimum}, got {value}")
    return value


def env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = env(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise SystemExit(f"{name}: expected an integer, got {raw!r}") from error
    if not minimum <= value <= maximum:
        raise SystemExit(f"{name}: expected {minimum}..{maximum}, got {value}")
    return value


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


async def send_request(
    client: httpx.AsyncClient,
    service: Service,
    kind: str,
    environment: str,
    config: Config,
    stats: Stats,
) -> None:
    started = time.perf_counter()
    try:
        response = await client.get(
            f"{service.base_url}/{kind}",
            params={"count": config.count, "environment": environment},
        )
        response.raise_for_status()
        payload = response.json()
    except (httpx.HTTPError, ValueError) as error:
        stats.record_failure()
        log(
            f"{service.name:<8} {kind:<12} {environment:<11} "
            f"ERROR {describe_error(error)}"
        )
        return

    elapsed_ms = (time.perf_counter() - started) * 1000
    items = len(payload.get("event_ids") or payload.get("trace_ids") or [])
    stats.record_success(items)
    log(
        f"{service.name:<8} {kind:<12} {environment:<11} {response.status_code} "
        f"items={items:<3} flushed={str(payload.get('flushed')).lower():<5} "
        f"{elapsed_ms:7.1f}ms"
    )


def describe_error(error: Exception) -> str:
    if isinstance(error, httpx.HTTPStatusError):
        return f"HTTP {error.response.status_code}"
    if isinstance(error, httpx.HTTPError):
        return f"{type(error).__name__}: {error}"
    return f"invalid response: {error}"


async def run_round(
    client: httpx.AsyncClient,
    config: Config,
    round_index: int,
    stats: dict[str, Stats],
) -> None:
    requests = [
        send_request(
            client,
            service,
            service.kind_for(round_index),
            service.environment_for(round_index),
            config,
            stats[service.name],
        )
        for service in config.services
        if service.enabled
    ]
    if requests:
        await asyncio.gather(*requests)


def describe_services(config: Config) -> str:
    return " ".join(
        f"{service.name}={service.raw_mix if service.enabled else 'off'}"
        for service in config.services
    )


def describe_environments(config: Config) -> str:
    return " ".join(
        f"{service.name}={service.raw_environments}" for service in config.services
    )


def print_summary(stats: dict[str, Stats]) -> None:
    print("--- traffic summary ---", flush=True)
    for name, service_stats in stats.items():
        print(
            f"{name:<8} requests={service_stats.requests} "
            f"ok={service_stats.succeeded} failed={service_stats.failed} "
            f"items={service_stats.items}",
            flush=True,
        )


async def run(config: Config) -> int:
    stats = {service.name: Stats() for service in config.services}
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    deadline = time.monotonic() + config.duration if config.duration > 0 else None
    round_index = 0

    log(f"traffic: {describe_services(config)}")
    log(f"environments: {describe_environments(config)}")
    log(f"target: count={config.count} interval={config.interval}s")

    async with httpx.AsyncClient(timeout=config.timeout) as client:
        while not stop.is_set():
            round_started = time.monotonic()
            await run_round(client, config, round_index, stats)
            round_index += 1

            if deadline is not None and time.monotonic() >= deadline:
                break

            delay = max(0.0, config.interval - (time.monotonic() - round_started))
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except TimeoutError:
                pass

    print_summary(stats)
    return 0


def main() -> int:
    config = Config.from_env()
    try:
        return asyncio.run(run(config))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
