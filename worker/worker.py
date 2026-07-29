import asyncio
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis


class Settings:
    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "kubewise-worker")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.namespace = os.getenv("POD_NAMESPACE", "kubewise")
        self.pod_name = os.getenv("POD_NAME", "unknown")
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        self.interval_seconds = int(os.getenv("WORKER_INTERVAL_SECONDS", "10"))
        self.redis_key_input = os.getenv("REDIS_KEY_EVENTS", "kubewise:load_events")
        self.redis_key_output = os.getenv("REDIS_KEY_WORKER", "kubewise:worker_events")


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


def synthetic_calculation(seed: int) -> float:
    total = 0.0
    for idx in range(1, 10000):
        total += math.sqrt(idx + seed) * math.cos(idx / 3)
    return round(total, 4)


async def process_cycle() -> dict[str, Any]:
    recent_events = await redis_client.lrange(settings.redis_key_input, -10, -1)
    if not recent_events:
        log_event("WARNING", "no recent load events found in redis")

    result = await asyncio.to_thread(synthetic_calculation, len(recent_events) + 1)

    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "observed_load_events": len(recent_events),
        "calculation_result": result,
    }

    await redis_client.rpush(settings.redis_key_output, str(event))
    await redis_client.ltrim(settings.redis_key_output, -500, -1)
    return event


async def main() -> None:
    log_event("INFO", "startup worker service booting")
    try:
        await redis_client.ping()
        log_event("INFO", "startup redis connectivity check successful")
    except Exception as exc:
        log_event("ERROR", f"startup redis connectivity failed: {exc}")

    while True:
        started = time.perf_counter()
        try:
            event = await process_cycle()
            log_event(
                "INFO",
                "processing worker cycle complete",
                duration_ms=(time.perf_counter() - started) * 1000,
                extra={"observedLoadEvents": event.get("observed_load_events", 0)},
            )
        except Exception as exc:
            log_event("ERROR", f"processing worker cycle failed: {exc}")

        await asyncio.sleep(settings.interval_seconds)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log_event("INFO", "shutdown worker service interrupted")
    finally:
        try:
            asyncio.run(redis_client.aclose())
        except Exception:
            pass
