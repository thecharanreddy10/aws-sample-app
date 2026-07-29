import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis


class Settings:
    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "kubewise-notification")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.namespace = os.getenv("POD_NAMESPACE", "kubewise")
        self.pod_name = os.getenv("POD_NAME", "unknown")
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        self.interval_seconds = int(os.getenv("NOTIFICATION_INTERVAL_SECONDS", "30"))
        self.redis_key_input = os.getenv("REDIS_KEY_EVENTS", "kubewise:load_events")
        self.redis_key_output = os.getenv("REDIS_KEY_NOTIFICATION", "kubewise:notification_events")


settings = Settings()

logging.basicConfig(level=settings.log_level, format="%(message)s")
logger = logging.getLogger(settings.app_name)


def log_event(level: str, message: str, duration_ms: float = 0.0, extra: dict[str, Any] | None = None) -> None:
    payload: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": settings.app_name,
        "level": level,
        "requestId": "-",
        "namespace": settings.namespace,
        "pod": settings.pod_name,
        "message": message,
        "duration": round(duration_ms, 2),
    }
    if extra:
        payload.update(extra)
    logger.log(getattr(logging, level, logging.INFO), json.dumps(payload))


redis_client = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True,
)


async def process_notifications() -> None:
    recent_events = await redis_client.lrange(settings.redis_key_input, -5, -1)
    if not recent_events:
        log_event("WARNING", "no recent load events found for notification processing")

    processed_marker = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source_events_seen": len(recent_events),
    }
    await redis_client.rpush(settings.redis_key_output, str(processed_marker))
    await redis_client.ltrim(settings.redis_key_output, -500, -1)


async def main() -> None:
    log_event("INFO", "startup notification service booting")
    try:
        await redis_client.ping()
        log_event("INFO", "startup redis connectivity check successful")
    except Exception as exc:
        log_event("ERROR", f"startup redis connectivity failed: {exc}")

    while True:
        started = time.perf_counter()
        try:
            await process_notifications()
            log_event("INFO", "processed notification", duration_ms=(time.perf_counter() - started) * 1000)
        except Exception as exc:
            log_event("ERROR", f"notification cycle failed: {exc}")

        await asyncio.sleep(settings.interval_seconds)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_event("INFO", "shutdown notification service interrupted")
    finally:
        try:
            asyncio.run(redis_client.aclose())
        except Exception:
            pass
