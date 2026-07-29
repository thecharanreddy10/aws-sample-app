from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from redis.asyncio import Redis

from analyzer import build_recommendations
from config import Settings


settings = Settings()

logging.basicConfig(
    level=settings.log_level,
    format="%(message)s",
)
logger = logging.getLogger(settings.app_name)


def log_event(
    *,
    level: str,
    message: str,
    request_id: str = "-",
    duration_ms: float = 0.0,
    extra: dict[str, Any] | None = None,
) -> None:
    payload: dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": settings.app_name,
        "level": level,
        "requestId": request_id,
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
analysis_task: asyncio.Task[Any] | None = None


async def analyze_once() -> dict[str, Any]:
    raw_cache = await redis_client.get(settings.redis_key_monitoring_cache)
    if not raw_cache:
        raise RuntimeError("Monitoring cache is not ready")

    snapshot = json.loads(raw_cache)
    pods = snapshot.get("pods", {}).get("items", [])
    deployments = snapshot.get("deployments", {}).get("items", [])

    recommendations_list = build_recommendations(
        pods=pods,
        deployments=deployments,
        cpu_price_per_m_hour=settings.price_per_millicore_hour,
        mem_price_per_mib_hour=settings.price_per_mib_hour,
        hours_per_month=settings.hours_per_month,
    )

    high = len([item for item in recommendations_list if str(item.get("severity", "")).lower() == "high"])
    medium = len([item for item in recommendations_list if str(item.get("severity", "")).lower() == "medium"])
    low = len([item for item in recommendations_list if str(item.get("severity", "")).lower() == "low"])

    monthly_saving = 0.0
    annual_saving = 0.0
    for item in recommendations_list:
        monthly_saving += float(str(item.get("estimatedMonthlySaving", "$0.00")).replace("$", ""))
        annual_saving += float(str(item.get("estimatedAnnualSaving", "$0.00")).replace("$", ""))

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "count": len(recommendations_list),
        "summary": {
            "high": high,
            "medium": medium,
            "low": low,
            "potentialMonthlySavings": round(monthly_saving, 2),
            "potentialAnnualSavings": round(annual_saving, 2),
        },
        "recommendations": recommendations_list,
    }


async def analysis_loop() -> None:
    while True:
        started = time.perf_counter()
        try:
            event = await analyze_once()
            await redis_client.set(settings.redis_key_recommendations, json.dumps(event))
            elapsed = (time.perf_counter() - started) * 1000
            log_event(
                level="INFO",
                message="recommendation generation complete",
                duration_ms=elapsed,
                extra={"count": event.get("count", 0)},
            )
        except Exception as exc:
            log_event(level="WARNING", message=f"recommendation analysis skipped: {exc}")

        await asyncio.sleep(settings.analysis_interval_seconds)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global analysis_task
    log_event(level="INFO", message="startup recommendation engine booting")
    try:
        await redis_client.ping()
        log_event(level="INFO", message="redis connectivity check successful")
    except Exception as exc:
        log_event(level="WARNING", message=f"redis connectivity unavailable at startup: {exc}")

    analysis_task = asyncio.create_task(analysis_loop())

    yield

    if analysis_task:
        analysis_task.cancel()
        try:
            await analysis_task
        except asyncio.CancelledError:
            pass
    log_event(level="INFO", message="shutdown recommendation engine stopping")
    await redis_client.aclose()


app = FastAPI(title="Kubewise Recommendation Engine", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    start = time.perf_counter()
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    log_event(level="INFO", message=f"request start {request.method} {request.url.path}", request_id=request_id)
    try:
        response = await call_next(request)
    except Exception as exc:
        log_event(level="ERROR", message=f"request failed path={request.url.path} error={exc}", request_id=request_id)
        raise

    elapsed_ms = (time.perf_counter() - start) * 1000
    response.headers["x-request-id"] = request_id
    log_event(
        level="INFO",
        message=f"request complete path={request.url.path} status={response.status_code}",
        request_id=request_id,
        duration_ms=elapsed_ms,
    )
    return response


@app.get("/health")
async def health() -> dict[str, str]:
    log_event(level="INFO", message="health probe")
    try:
        await redis_client.ping()
        await redis_client.get(settings.redis_key_monitoring_cache)
        return {"status": "healthy"}
    except Exception as exc:
        log_event(level="WARNING", message=f"health degraded: {exc}")
        return {"status": "degraded"}


@app.get("/recommendations")
async def recommendations() -> dict[str, Any]:
    try:
        cached = await redis_client.get(settings.redis_key_recommendations)
        if not cached:
            raise RuntimeError("Recommendations cache is not ready")
        return json.loads(cached)
    except Exception as exc:
        log_event(level="WARNING", message=f"recommendations cache unavailable: {exc}")
        raise HTTPException(status_code=503, detail="Recommendations cache not ready") from exc
