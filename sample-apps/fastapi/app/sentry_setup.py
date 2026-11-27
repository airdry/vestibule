"""Sentry SDK wiring shared by the FastAPI sample application."""

import logging
import os
from typing import Any

import sentry_sdk
from sentry_sdk.integrations.dedupe import DedupeIntegration
from sentry_sdk.integrations.fastapi import FastApiIntegration

logger = logging.getLogger("sample.fastapi.sentry")

DEFAULT_DSN = "http://public@host.docker.internal:8080/1"
DEFAULT_ENVIRONMENT = "sample-apps"
DEFAULT_RELEASE = "sample-apps@local"


def _apply_environment_override(event: dict[str, Any]) -> None:
    """Promotes the per-request environment tag to the event's top-level field."""

    tags = event.get("tags") or {}
    environment = tags.get("sample_environment")
    if environment:
        event["environment"] = environment


def _log_payload(kind: str, event: dict[str, Any]) -> None:
    payload = dict(event)
    # The installed-packages map is large and not useful for traffic checks.
    payload.pop("modules", None)
    logger.debug(
        "sentry.payload",
        extra={
            "sentry_item": kind,
            "event_id": event.get("event_id"),
            "payload": payload,
        },
    )


def _before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any]:
    _apply_environment_override(event)
    _log_payload("event", event)
    return event


def _before_send_transaction(
    event: dict[str, Any], hint: dict[str, Any]
) -> dict[str, Any]:
    _apply_environment_override(event)
    _log_payload("transaction", event)
    return event


def flush_sentry(timeout: float = 5.0) -> bool:
    """Waits for the SDK transport to finish and reports success."""

    try:
        sentry_sdk.flush(timeout=timeout)
    except Exception:
        logger.exception("sentry.flush.failed")
        return False
    return True


def init_sentry() -> None:
    dsn = os.getenv("SENTRY_DSN", DEFAULT_DSN)
    environment = os.getenv("SENTRY_ENVIRONMENT", DEFAULT_ENVIRONMENT)
    release = os.getenv("SENTRY_RELEASE", DEFAULT_RELEASE)

    sentry_sdk.init(
        dsn=dsn,
        environment=environment,
        release=release,
        debug=True,
        attach_stacktrace=True,
        # Enables tracing so manual transactions can force ``sampled=True``.
        # Auto-instrumented request transactions are still dropped (rate 0).
        traces_sample_rate=0.0,
        integrations=[FastApiIntegration()],
        disabled_integrations=[DedupeIntegration],
        before_send=_before_send,
        before_send_transaction=_before_send_transaction,
    )

    logger.info(
        "sentry.initialized",
        extra={"dsn": dsn, "environment": environment, "release": release},
    )
