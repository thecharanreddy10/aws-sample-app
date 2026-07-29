from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from kubernetes import client, config
from kubernetes.client import ApiException
from redis.asyncio import Redis

from config import Settings
from k8s_helpers import (
    deployment_name_from_owner,
    extract_container_resource_totals,
    parse_cpu_to_millicores,
    parse_memory_to_mib,
)


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


class KubeClients:
    def __init__(self) -> None:
        self.core_api = client.CoreV1Api()
        self.apps_api = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()


kube_clients: KubeClients | None = None
redis_client = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True,
)
collector_task: asyncio.Task[Any] | None = None


def load_kube_config() -> None:
    try:
        config.load_incluster_config()
        log_event(level="INFO", message="startup using in-cluster Kubernetes config")
        return
    except Exception:
        log_event(level="WARNING", message="in-cluster config unavailable, trying local kube config")

    kube_path = settings.kube_config_path or None
    config.load_kube_config(config_file=kube_path)
    log_event(level="INFO", message="startup using local kube config")


def list_nodes() -> list[dict[str, Any]]:
    assert kube_clients is not None
    response = kube_clients.core_api.list_node()
    nodes: list[dict[str, Any]] = []

    for item in response.items:
        capacity = item.status.capacity or {}
        allocatable = item.status.allocatable or {}
        nodes.append(
            {
                "name": item.metadata.name,
                "capacity_cpu": capacity.get("cpu", "0"),
                "capacity_memory": capacity.get("memory", "0"),
                "allocatable_cpu": allocatable.get("cpu", "0"),
                "allocatable_memory": allocatable.get("memory", "0"),
            }
        )

    return nodes


def list_node_metrics() -> list[dict[str, Any]]:
    assert kube_clients is not None
    metrics = kube_clients.custom_api.list_cluster_custom_object(
        group="metrics.k8s.io",
        version="v1beta1",
        plural="nodes",
    )

    items = metrics.get("items", [])
    return [
        {
            "name": entry.get("metadata", {}).get("name", "unknown"),
            "cpu_usage_millicores": parse_cpu_to_millicores(entry.get("usage", {}).get("cpu")),
            "memory_usage_mib": parse_memory_to_mib(entry.get("usage", {}).get("memory")),
        }
        for entry in items
    ]


def list_pod_metrics() -> dict[str, dict[str, int]]:
    assert kube_clients is not None
    metrics = kube_clients.custom_api.list_cluster_custom_object(
        group="metrics.k8s.io",
        version="v1beta1",
        plural="pods",
    )

    result: dict[str, dict[str, int]] = {}
    for item in metrics.get("items", []):
        namespace = item.get("metadata", {}).get("namespace", "default")
        pod_name = item.get("metadata", {}).get("name", "unknown")
        key = f"{namespace}/{pod_name}"

        total_cpu = 0
        total_memory = 0
        for container in item.get("containers", []):
            usage = container.get("usage", {})
            total_cpu += parse_cpu_to_millicores(usage.get("cpu"))
            total_memory += parse_memory_to_mib(usage.get("memory"))

        result[key] = {
            "cpu_usage_millicores": total_cpu,
            "memory_usage_mib": total_memory,
        }

    return result


def list_pods() -> list[dict[str, Any]]:
    assert kube_clients is not None
    pod_metrics = list_pod_metrics()
    response = kube_clients.core_api.list_pod_for_all_namespaces()

    pods: list[dict[str, Any]] = []
    for pod in response.items:
        namespace = pod.metadata.namespace
        pod_name = pod.metadata.name
        key = f"{namespace}/{pod_name}"

        restart_count = 0
        statuses = pod.status.container_statuses or []
        for status in statuses:
            restart_count += status.restart_count

        owner_refs = []
        if pod.metadata.owner_references:
            owner_refs = [
                {
                    "kind": owner.kind,
                    "name": owner.name,
                }
                for owner in pod.metadata.owner_references
            ]

        totals = extract_container_resource_totals(
            [
                {
                    "resources": {
                        "requests": container.resources.requests if container.resources else {},
                        "limits": container.resources.limits if container.resources else {},
                    }
                }
                for container in pod.spec.containers
            ]
        )

        pods.append(
            {
                "namespace": namespace,
                "podName": pod_name,
                "deployment": deployment_name_from_owner(owner_refs),
                "status": pod.status.phase,
                "restartCount": restart_count,
                "cpuUsageMillicores": pod_metrics.get(key, {}).get("cpu_usage_millicores", 0),
                "memoryUsageMiB": pod_metrics.get(key, {}).get("memory_usage_mib", 0),
                "cpuRequestMillicores": totals["cpu_request_millicores"],
                "memoryRequestMiB": totals["memory_request_mib"],
                "cpuLimitMillicores": totals["cpu_limit_millicores"],
                "memoryLimitMiB": totals["memory_limit_mib"],
            }
        )

    return pods


def list_deployments() -> list[dict[str, Any]]:
    assert kube_clients is not None
    response = kube_clients.apps_api.list_deployment_for_all_namespaces()
    deployments: list[dict[str, Any]] = []

    for deployment in response.items:
        containers = []
        for container in deployment.spec.template.spec.containers:
            containers.append(
                {
                    "resources": {
                        "requests": container.resources.requests if container.resources else {},
                        "limits": container.resources.limits if container.resources else {},
                    }
                }
            )

        totals = extract_container_resource_totals(containers)
        deployments.append(
            {
                "namespace": deployment.metadata.namespace,
                "name": deployment.metadata.name,
                "replicas": deployment.spec.replicas or 0,
                "availableReplicas": deployment.status.available_replicas or 0,
                "cpuRequestMillicores": totals["cpu_request_millicores"],
                "memoryRequestMiB": totals["memory_request_mib"],
                "cpuLimitMillicores": totals["cpu_limit_millicores"],
                "memoryLimitMiB": totals["memory_limit_mib"],
            }
        )

    return deployments


def list_namespaces() -> list[str]:
    assert kube_clients is not None
    response = kube_clients.core_api.list_namespace()
    return [item.metadata.name for item in response.items]


def list_services() -> list[dict[str, Any]]:
    assert kube_clients is not None
    response = kube_clients.core_api.list_service_for_all_namespaces()
    return [
        {
            "namespace": service.metadata.namespace,
            "name": service.metadata.name,
            "type": service.spec.type,
            "clusterIP": service.spec.cluster_ip,
        }
        for service in response.items
    ]


def summarize() -> dict[str, Any]:
    nodes = list_nodes()
    pods = list_pods()
    deployments = list_deployments()
    namespaces = list_namespaces()
    services = list_services()

    node_metrics = list_node_metrics()

    total_cpu_capacity = 0
    total_memory_capacity = 0
    total_cpu_usage = 0
    total_memory_usage = 0

    nodes_by_name = {node["name"]: node for node in nodes}

    for metric in node_metrics:
        node_name = metric["name"]
        node_info = nodes_by_name.get(node_name)
        if not node_info:
            continue

        total_cpu_usage += metric["cpu_usage_millicores"]
        total_memory_usage += metric["memory_usage_mib"]
        total_cpu_capacity += parse_cpu_to_millicores(node_info["allocatable_cpu"])
        total_memory_capacity += parse_memory_to_mib(node_info["allocatable_memory"])

    cpu_utilization = 0.0
    memory_utilization = 0.0

    if total_cpu_capacity > 0:
        cpu_utilization = (total_cpu_usage / total_cpu_capacity) * 100

    if total_memory_capacity > 0:
        memory_utilization = (total_memory_usage / total_memory_capacity) * 100

    return {
        "nodes": len(nodes),
        "pods": len(pods),
        "deployments": len(deployments),
        "namespaces": len(namespaces),
        "services": len(services),
        "cpuUtilization": f"{cpu_utilization:.2f}%",
        "memoryUtilization": f"{memory_utilization:.2f}%",
        "cpuUtilizationPercent": round(cpu_utilization, 2),
        "memoryUtilizationPercent": round(memory_utilization, 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def build_snapshot() -> dict[str, Any]:
    started = time.perf_counter()
    nodes = list_nodes()
    node_metrics = list_node_metrics()
    pods = list_pods()
    deployments = list_deployments()
    namespaces = list_namespaces()
    services = list_services()
    summary = summarize()

    snapshot = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "nodes": {"items": nodes, "metrics": node_metrics},
        "pods": {"items": pods},
        "deployments": {"items": deployments},
        "metrics": {
            "nodeMetrics": node_metrics,
            "podMetrics": {
                f"{pod['namespace']}/{pod['podName']}": {
                    "cpu_usage_millicores": pod["cpuUsageMillicores"],
                    "memory_usage_mib": pod["memoryUsageMiB"],
                }
                for pod in pods
            },
        },
        "namespaces": {"items": namespaces},
        "services": {"items": services},
        "summary": summary,
    }
    elapsed = (time.perf_counter() - started) * 1000
    log_event(
        level="INFO",
        message="resource analysis cached in redis",
        duration_ms=elapsed,
        extra={"pods": len(pods), "deployments": len(deployments), "nodes": len(nodes)},
    )
    return snapshot


async def cache_refresh_loop() -> None:
    while True:
        try:
            snapshot = await build_snapshot()
            await redis_client.set(settings.redis_key_cache, json.dumps(snapshot))
        except Exception as exc:
            log_event(level="ERROR", message=f"cache refresh failed: {exc}")

        await asyncio.sleep(settings.refresh_interval_seconds)


async def read_cached_snapshot() -> dict[str, Any]:
    data = await redis_client.get(settings.redis_key_cache)
    if not data:
        raise HTTPException(status_code=503, detail="Monitoring cache not ready")
    return json.loads(data)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task, kube_clients
    log_event(level="INFO", message="startup monitoring service booting")
    load_kube_config()
    kube_clients = KubeClients()
    collector_task = asyncio.create_task(cache_refresh_loop())
    log_event(level="INFO", message="startup monitoring cache scheduler ready")
    yield
    if collector_task:
        collector_task.cancel()
        with suppress(asyncio.CancelledError):
            await collector_task
    await redis_client.aclose()
    log_event(level="INFO", message="shutdown monitoring service stopping")


app = FastAPI(title="Kubewise Monitoring Service", version="1.0.0", lifespan=lifespan)


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
async def health() -> dict[str, Any]:
    log_event(level="INFO", message="health probe")
    try:
        await redis_client.ping()
        await read_cached_snapshot()
        return {"status": "healthy", "cache": "ready"}
    except (ApiException, HTTPException) as exc:
        log_event(level="WARNING", message=f"health degraded: {exc}")
        return {"status": "degraded", "cache": "warming", "detail": str(exc)}


@app.get("/nodes")
async def nodes() -> dict[str, Any]:
    snapshot = await read_cached_snapshot()
    return snapshot["nodes"]


@app.get("/pods")
async def pods() -> dict[str, Any]:
    snapshot = await read_cached_snapshot()
    return snapshot["pods"]


@app.get("/deployments")
async def deployments() -> dict[str, Any]:
    snapshot = await read_cached_snapshot()
    return snapshot["deployments"]


@app.get("/metrics")
async def metrics() -> dict[str, Any]:
    snapshot = await read_cached_snapshot()
    return snapshot["metrics"]


@app.get("/summary")
async def summary() -> dict[str, Any]:
    snapshot = await read_cached_snapshot()
    return snapshot["summary"]
