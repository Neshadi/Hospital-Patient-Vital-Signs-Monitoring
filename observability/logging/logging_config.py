"""
Shared structured-logging setup.

Emits JSON-formatted log lines so they can be shipped to any log aggregator
(e.g. Loki, ELK, CloudWatch) in a real deployment. For the mini-project,
these lines can simply be captured from `docker compose logs` and included
as evidence of observability in the report/demo.
"""
import json
import logging
import sys
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "stage": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload)


def get_logger(stage_name: str) -> logging.Logger:
    logger = logging.getLogger(stage_name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
