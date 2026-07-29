from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Request
from redis.asyncio import Redis


class Settings:
    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "kubewise-backend")
        self.app_version = os.getenv("APP_VERSION", "2.1.0")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.namespace = os.getenv("POD_NAMESPACE", "kubewise")
        self.pod_name = os.getenv("POD_NAME", "unknown")

        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))

        self.load_duration_seconds = int(os.getenv("LOAD_DURATION_SECONDS", "30"))
        self.stress_duration_seconds = int(os.getenv("STRESS_DURATION_SECONDS", "60"))
        self.http_timeout_seconds = float(os.getenv("HTTP_TIMEOUT_SECONDS", "10"))

        self.monitoring_base_url = os.getenv("MONITORING_BASE_URL", "http://monitoring-service:8080")
        self.recommendation_base_url = os.getenv("RECOMMENDATION_BASE_URL", "http://recommendation-engine:8080")

        self.redis_key_events = os.getenv("REDIS_KEY_EVENTS", "kubewise:load_events")
        self.redis_key_worker = os.getenv("REDIS_KEY_WORKER", "kubewise:worker_events")
        self.redis_key_notification = os.getenv("REDIS_KEY_NOTIFICATION", "kubewise:notification_events")
        self.redis_key_generate_load_count = os.getenv("REDIS_KEY_GENERATE_LOAD_COUNT", "kubewise:generate_load_count")
        self.redis_key_stress_count = os.getenv("REDIS_KEY_STRESS_COUNT", "kubewise:stress_count")


settings = Settings()

logging.basicConfig(level=settings.log_level, format="%(message)s")
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

http_client: httpx.AsyncClient | None = None
started_at = datetime.now(timezone.utc)
request_count = 0


def consume_cpu_for_duration(duration_seconds: int) -> dict[str, Any]:
    end_time = time.monotonic() + duration_seconds
    iterations = 0
    accumulator = 0.0

    while time.monotonic() < end_time:
        for value in range(1, 5000):
            accumulator += math.sqrt(value) * math.sin(value)
        iterations += 1

    return {
        "iterations": iterations,
        "accumulator": round(accumulator, 4),
    }


async def fetch_service_json(base_url: str, path: str) -> dict[str, Any]:
    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client unavailable")

    response = await http_client.get(f"{base_url}{path}")
    response.raise_for_status()
    return response.json()


def score_from_snapshot(
    summary: dict[str, Any],
    pods: list[dict[str, Any]],
    deployments: list[dict[str, Any]],
    recommendations_payload: dict[str, Any],
) -> dict[str, int]:
    cpu_util = float(summary.get("cpuUtilizationPercent", 0.0) or 0.0)
    memory_util = float(summary.get("memoryUtilizationPercent", 0.0) or 0.0)

    pending_pods = len([pod for pod in pods if str(pod.get("status", "")).lower() == "pending"])
    failed_pods = len([pod for pod in pods if str(pod.get("status", "")).lower() == "failed"])
    restart_total = sum(int(pod.get("restartCount", 0) or 0) for pod in pods)

    missing_limits = len(
        [
            deployment
            for deployment in deployments
            if int(deployment.get("cpuLimitMillicores", 0) or 0) == 0
            or int(deployment.get("memoryLimitMiB", 0) or 0) == 0
        ]
    )

    overprovisioned = 0
    for pod in pods:
        cpu_request = int(pod.get("cpuRequestMillicores", 0) or 0)
        mem_request = int(pod.get("memoryRequestMiB", 0) or 0)
        cpu_usage = int(pod.get("cpuUsageMillicores", 0) or 0)
        mem_usage = int(pod.get("memoryUsageMiB", 0) or 0)

        cpu_low = cpu_request > 0 and cpu_usage < (cpu_request * 0.3)
        mem_low = mem_request > 0 and mem_usage < (mem_request * 0.3)
        if cpu_low or mem_low:
            overprovisioned += 1

    high_recs = int(recommendations_payload.get("summary", {}).get("high", 0) or 0)

    health_penalty = min(25, restart_total) + (pending_pods * 2) + (failed_pods * 4)
    utilization_penalty = max(0.0, abs(cpu_util - 65) * 0.2) + max(0.0, abs(memory_util - 70) * 0.2)
    cluster_health = max(0, min(100, int(100 - health_penalty - utilization_penalty)))

    total_deployments = max(1, len(deployments))
    limit_penalty = min(30, int((missing_limits / total_deployments) * 30))
    overprov_penalty = min(40, int((overprovisioned / max(1, len(pods))) * 40))
    cost_efficiency = max(0, min(100, int(100 - limit_penalty - overprov_penalty)))

    optimization_penalty = min(50, high_recs * 8) + min(20, pending_pods + failed_pods)
    optimization_score = max(0, min(100, int((cluster_health * 0.5 + cost_efficiency * 0.5) - optimization_penalty)))

    return {
        "clusterHealth": cluster_health,
        "costEfficiency": cost_efficiency,
        "optimizationScore": optimization_score,
    }


def top_consumers(pods: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scored = []
    for pod in pods:
        cpu = int(pod.get("cpuUsageMillicores", 0) or 0)
        memory = int(pod.get("memoryUsageMiB", 0) or 0)
        score = cpu + (memory * 3)
        scored.append(
            {
                "namespace": pod.get("namespace", "default"),
                "podName": pod.get("podName", "unknown"),
                "deployment": pod.get("deployment", "unknown"),
                "cpuUsageMillicores": cpu,
                "memoryUsageMiB": memory,
                "score": score,
            }
        )
    return sorted(scored, key=lambda item: item["score"], reverse=True)[:5]


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client
    log_event(level="INFO", message="startup backend service booting")
    http_client = httpx.AsyncClient(timeout=settings.http_timeout_seconds)
    try:
        await redis_client.ping()
        log_event(level="INFO", message="startup redis connectivity check successful")
    except Exception as exc:
        log_event(level="WARNING", message=f"startup redis connectivity failed: {exc}")
    yield
    log_event(level="INFO", message="shutdown backend service stopping")
    if http_client is not None:
        await http_client.aclose()
    await redis_client.aclose()


app = FastAPI(title="Kubewise Backend Gateway", version=settings.app_version, lifespan=lifespan)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    global request_count
    request_count += 1
    request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
    started = time.perf_counter()
    path = request.url.path

    log_event(level="INFO", message=f"request start {request.method} {path}", request_id=request_id)
    try:
        response = await call_next(request)
    except Exception as exc:
        log_event(level="ERROR", message=f"request failed path={path} error={exc}", request_id=request_id)
        raise

    duration_ms = (time.perf_counter() - started) * 1000
    response.headers["x-request-id"] = request_id

    log_event(
        level="INFO",
        message=f"request complete path={path} status={response.status_code}",
        request_id=request_id,
        duration_ms=duration_ms,
    )
    return response


@app.get("/health")
async def health() -> dict[str, str]:
    log_event(level="INFO", message="health probe")
    return {"status": "healthy"}


@app.get("/metrics")
async def metrics() -> dict[str, Any]:
    uptime_seconds = int((datetime.now(timezone.utc) - started_at).total_seconds())
    redis_connectivity = "unreachable"
    generate_load_count = 0
    stress_test_count = 0

    try:
        await redis_client.ping()
        redis_connectivity = "healthy"
        generate_load_count = int(await redis_client.get(settings.redis_key_generate_load_count) or "0")
        stress_test_count = int(await redis_client.get(settings.redis_key_stress_count) or "0")
    except Exception as exc:
        log_event(level="WARNING", message=f"metrics redis read failed: {exc}")

    return {
        "service": settings.app_name,
        "applicationVersion": settings.app_version,
        "uptimeSeconds": uptime_seconds,
        "requestCount": request_count,
        "stressTestCount": stress_test_count,
        "generateLoadCount": generate_load_count,
        "redisConnectivity": redis_connectivity,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/generate-load")
async def generate_load() -> dict[str, Any]:
    started = time.perf_counter()
    try:
        work = await asyncio.to_thread(consume_cpu_for_duration, settings.load_duration_seconds)
        timestamp = datetime.now(timezone.utc).isoformat()
        await redis_client.rpush(settings.redis_key_events, timestamp)
        await redis_client.ltrim(settings.redis_key_events, -500, -1)
        await redis_client.incr(settings.redis_key_generate_load_count)
        log_event(level="INFO", message="load generation complete", duration_ms=(time.perf_counter() - started) * 1000)
        return {
            "status": "success",
            "message": f"CPU load generated for {settings.load_duration_seconds} seconds",
            "timestamp": timestamp,
            "work": work,
        }
    except Exception as exc:
        log_event(level="ERROR", message=f"generate-load failed: {exc}")
        raise HTTPException(status_code=500, detail="Failed to generate load") from exc


@app.post("/stress")
async def stress(duration: int | None = None) -> dict[str, Any]:
    duration_seconds = duration if duration is not None else settings.stress_duration_seconds
    if duration_seconds < 1 or duration_seconds > 600:
        raise HTTPException(status_code=400, detail="Duration must be between 1 and 600 seconds")

    started = time.perf_counter()
    try:
        work = await asyncio.to_thread(consume_cpu_for_duration, duration_seconds)
        timestamp = datetime.now(timezone.utc).isoformat()
        await redis_client.rpush(settings.redis_key_events, timestamp)
        await redis_client.ltrim(settings.redis_key_events, -500, -1)
        await redis_client.incr(settings.redis_key_stress_count)
        log_event(level="WARNING", message="stress test complete", duration_ms=(time.perf_counter() - started) * 1000)
        return {
            "status": "success",
            "message": f"Stress test completed for {duration_seconds} seconds",
            "timestamp": timestamp,
            "work": work,
        }
    except Exception as exc:
        log_event(level="ERROR", message=f"stress failed: {exc}")
        raise HTTPException(status_code=500, detail="Stress test failed") from exc


async def fetch_dashboard_components() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    try:
        summary, nodes, pods, deployments = await asyncio.gather(
            fetch_service_json(settings.monitoring_base_url, "/summary"),
            fetch_service_json(settings.monitoring_base_url, "/nodes"),
            fetch_service_json(settings.monitoring_base_url, "/pods"),
            fetch_service_json(settings.monitoring_base_url, "/deployments"),
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Monitoring service unavailable: {exc}") from exc

    try:
        recommendations = await fetch_service_json(settings.recommendation_base_url, "/recommendations")
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Recommendation engine unavailable: {exc}") from exc

    return summary, nodes, pods, deployments, recommendations


@app.get("/summary")
async def summary() -> dict[str, Any]:
    summary_payload = await fetch_service_json(settings.monitoring_base_url, "/summary")
    return summary_payload


@app.get("/nodes")
async def nodes() -> dict[str, Any]:
    return await fetch_service_json(settings.monitoring_base_url, "/nodes")


@app.get("/pods")
async def pods() -> dict[str, Any]:
    return await fetch_service_json(settings.monitoring_base_url, "/pods")


@app.get("/deployments")
async def deployments() -> dict[str, Any]:
    return await fetch_service_json(settings.monitoring_base_url, "/deployments")


@app.get("/recommendations")
async def recommendations() -> dict[str, Any]:
    return await fetch_service_json(settings.recommendation_base_url, "/recommendations")


@app.get("/cluster-score")
async def cluster_score() -> dict[str, int]:
    summary_payload, _nodes, pods_payload, deployments_payload, recs_payload = await fetch_dashboard_components()
    return score_from_snapshot(
        summary=summary_payload,
        pods=pods_payload.get("items", []),
        deployments=deployments_payload.get("items", []),
        recommendations_payload=recs_payload,
    )


@app.get("/dashboard")
async def dashboard() -> dict[str, Any]:
    summary_payload, nodes_payload, pods_payload, deployments_payload, recs_payload = await fetch_dashboard_components()
    pods_items = pods_payload.get("items", [])
    deployments_items = deployments_payload.get("items", [])

    scores = score_from_snapshot(
        summary=summary_payload,
        pods=pods_items,
        deployments=deployments_items,
        recommendations_payload=recs_payload,
    )

    recommendation_summary = recs_payload.get("summary", {})

    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "summary": summary_payload,
        "clusterScore": scores,
        "potentialMonthlySavings": recommendation_summary.get("potentialMonthlySavings", 0.0),
        "potentialAnnualSavings": recommendation_summary.get("potentialAnnualSavings", 0.0),
        "recommendationSummary": {
            "high": recommendation_summary.get("high", 0),
            "medium": recommendation_summary.get("medium", 0),
            "low": recommendation_summary.get("low", 0),
            "total": recs_payload.get("count", 0),
        },
        "topResourceConsumers": top_consumers(pods_items),
        "nodes": nodes_payload.get("items", []),
        "nodeMetrics": nodes_payload.get("metrics", []),
        "pods": pods_items,
        "deployments": deployments_items,
        "recommendations": recs_payload.get("recommendations", []),
    }
