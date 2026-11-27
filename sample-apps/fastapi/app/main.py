"""FastAPI sample application used to exercise sentry-ratelimiter.

The application intentionally uses the Sentry Python SDK version released for
Sentry 25.3 (2025-03-17) and exposes two easy-to-call endpoints:

* ``GET /events``       - captures error events
* ``GET /transactions`` - finishes manually sampled transactions

Both endpoints accept ``count`` and ``environment`` query parameters. Every
log line, including Sentry SDK debug output, is emitted as JSON.
"""

import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Annotated, AsyncIterator, Literal

import sentry_sdk
import uvicorn
from fastapi import FastAPI, Query, Request
from pydantic import BaseModel

from .logging_config import configure_logging
from .sentry_setup import DEFAULT_ENVIRONMENT, flush_sentry, init_sentry

APP_NAME = "fastapi"
configure_logging(APP_NAME)
init_sentry()

logger = logging.getLogger("sample.fastapi")

CountQuery = Annotated[int, Query(ge=1, le=1000)]
EnvironmentQuery = Annotated[str | None, Query()]


class HealthResponse(BaseModel):
    status: str
    app: str


class EventResponse(BaseModel):
    app: str
    kind: Literal["event"]
    count: int
    environment: str
    request_id: str
    event_ids: list[str]
    flushed: bool


class TransactionResponse(BaseModel):
    app: str
    kind: Literal["transaction"]
    count: int
    environment: str
    request_id: str
    trace_ids: list[str]
    flushed: bool


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    logger.info(
        "app.started",
        extra={"port": int(os.getenv("PORT", "8000"))},
    )
    yield
    flushed = flush_sentry()
    logger.info("app.stopping", extra={"sentry_flushed": flushed})


app = FastAPI(title="sentry-ratelimiter FastAPI sample", lifespan=lifespan)


@app.middleware("http")
async def request_logging(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    request.state.request_id = request_id
    started = time.perf_counter()

    logger.debug(
        "http.request.started",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "query": request.url.query,
        },
    )

    response = await call_next(request)
    response.headers["x-request-id"] = request_id
    logger.debug(
        "http.request.finished",
        extra={
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": round((time.perf_counter() - started) * 1000, 3),
        },
    )
    return response


@app.get("/health")
def health() -> HealthResponse:
    return HealthResponse(status="ok", app=APP_NAME)


@app.get("/events")
def events(
    request: Request,
    count: CountQuery = 1,
    environment: EnvironmentQuery = None,
) -> EventResponse:
    request_id = request.state.request_id
    target_environment = environment or os.getenv(
        "SENTRY_ENVIRONMENT", DEFAULT_ENVIRONMENT
    )
    event_ids: list[str] = []

    logger.debug(
        "sample.events.requested",
        extra={
            "request_id": request_id,
            "count": count,
            "environment": target_environment,
        },
    )

    for index in range(count):
        with sentry_sdk.new_scope() as scope:
            scope.set_tag("sample_app", APP_NAME)
            scope.set_tag("sample_environment", target_environment)
            scope.set_tag("sample_request_id", request_id)
            scope.set_extra("event_index", index)
            event_id = sentry_sdk.capture_exception(
                RuntimeError(
                    f"sample {APP_NAME} event {index} request_id={request_id}"
                )
            )

        event_ids.append(event_id or "")
        logger.debug(
            "sentry.capture.event",
            extra={
                "request_id": request_id,
                "event_id": event_id,
                "event_index": index,
                "environment": target_environment,
            },
        )

    flushed = flush_sentry()
    logger.debug(
        "sentry.flush",
        extra={
            "request_id": request_id,
            "item": "event",
            "count": count,
            "flushed": flushed,
        },
    )

    return EventResponse(
        app=APP_NAME,
        kind="event",
        count=count,
        environment=target_environment,
        request_id=request_id,
        event_ids=event_ids,
        flushed=flushed,
    )


@app.get("/transactions")
def transactions(
    request: Request,
    count: CountQuery = 1,
    environment: EnvironmentQuery = None,
) -> TransactionResponse:
    request_id = request.state.request_id
    target_environment = environment or os.getenv(
        "SENTRY_ENVIRONMENT", DEFAULT_ENVIRONMENT
    )
    trace_ids: list[str] = []

    logger.debug(
        "sample.transactions.requested",
        extra={
            "request_id": request_id,
            "count": count,
            "environment": target_environment,
        },
    )

    for index in range(count):
        with sentry_sdk.new_scope():
            with sentry_sdk.start_transaction(
                name=f"sample.transaction.{APP_NAME}",
                op="sample.task",
                sampled=True,
            ) as transaction:
                transaction.set_tag("sample_app", APP_NAME)
                transaction.set_tag("sample_environment", target_environment)
                transaction.set_tag("sample_request_id", request_id)
                transaction.set_data("event_index", index)
                time.sleep(0.001)

        trace_ids.append(transaction.trace_id)
        logger.debug(
            "sentry.capture.transaction",
            extra={
                "request_id": request_id,
                "trace_id": transaction.trace_id,
                "span_id": transaction.span_id,
                "event_index": index,
                "environment": target_environment,
                "sampled": transaction.sampled,
            },
        )

    flushed = flush_sentry()
    logger.debug(
        "sentry.flush",
        extra={
            "request_id": request_id,
            "item": "transaction",
            "count": count,
            "flushed": flushed,
        },
    )

    return TransactionResponse(
        app=APP_NAME,
        kind="transaction",
        count=count,
        environment=target_environment,
        request_id=request_id,
        trace_ids=trace_ids,
        flushed=flushed,
    )


def main() -> None:
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        log_config=None,
        log_level="debug",
        access_log=True,
    )


if __name__ == "__main__":
    main()
