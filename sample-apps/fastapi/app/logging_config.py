"""JSON logging for the FastAPI sample application."""

import json
import logging
import os
import sys
import traceback
from datetime import datetime, timezone

TRACE_LEVEL = 5
logging.addLevelName(TRACE_LEVEL, "TRACE")

# LogRecord attributes that are not user-supplied structured fields.
_STANDARD_RECORD_FIELDS = frozenset(
    {
        "args",
        "asctime",
        "created",
        "exc_info",
        "exc_text",
        "filename",
        "funcName",
        "levelname",
        "levelno",
        "lineno",
        "module",
        "msecs",
        "message",
        "msg",
        "name",
        "pathname",
        "process",
        "processName",
        "relativeCreated",
        "stack_info",
        "thread",
        "threadName",
        "taskName",
    }
)


class JsonFormatter(logging.Formatter):
    """Renders every log record as one JSON object per line."""

    def __init__(self, app_name: str) -> None:
        super().__init__()
        self._app_name = app_name

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname.lower(),
            "app": self._app_name,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key in _STANDARD_RECORD_FIELDS or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exception"] = "".join(
                traceback.format_exception(*record.exc_info)
            )
        return json.dumps(payload, default=str, ensure_ascii=False)


def _resolve_level(raw: str) -> int:
    if raw.lower() == "trace":
        return TRACE_LEVEL
    return getattr(logging, raw.upper(), logging.DEBUG)


def configure_logging(app_name: str) -> None:
    """Installs the JSON formatter on the root logger.

    Uvicorn is started with ``log_config=None`` so its loggers propagate here
    instead of installing their own plain-text handlers.
    """

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(app_name))

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(_resolve_level(os.getenv("LOG_LEVEL", "debug")))

    for logger_name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(logging.NOTSET)

    # Importing sentry_sdk installs its own plain-text debug handler before
    # this configuration runs. Drop it so every SDK line is emitted as JSON
    # through the root handler.
    sentry_logger = logging.getLogger("sentry_sdk.errors")
    sentry_logger.handlers.clear()
    sentry_logger.propagate = True
    sentry_logger.setLevel(logging.NOTSET)
