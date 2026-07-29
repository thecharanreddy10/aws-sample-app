from __future__ import annotations

import asyncio
import json
import logging
import time
import traceback
import uuid
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from kubernetes import client, config
from kubernetes.client import ApiException
from redis.asyncio import Redis

try:
    from kubernetes.config import ConfigException
except ImportError:  # pragma: no cover - compatibility fallback
    from kubernetes.config.config_exception import ConfigException

from config import Settings
from k8s_helpers import (
    deployment_name_from_owner,
    extract_container_resource_totals,
    parse_cpu_to_millicores,
    parse_memory_to_mib,
    safe_dict,
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
kubernetes_available = False
redis_client = Redis(
    host=settings.redis_host,
    port=settings.redis_port,
    db=settings.redis_db,
    decode_responses=True,
)
collector_task: asyncio.Task[Any] | None = None


def _safe_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return []


def _is_kubernetes_config_error(exc: Exception) -> bool:
    return isinstance(exc, (ConfigException, FileNotFoundError, OSError, TypeError, ValueError))


def initialize_kubernetes_clients() -> None:
    global kubernetes_available, kube_clients

    log_event(level="INFO", message="Starting Monitoring Service...")
    log_event(level="INFO", message="Trying in-cluster Kubernetes configuration...")

    try:
        config.load_incluster_config()
    except Exception as exc:
        if not _is_kubernetes_config_error(exc):
            raise
        log_event(level="WARNING", message="In-cluster configuration unavailable.")
    else:
        kubernetes_available = True
        kube_clients = KubeClients()
        log_event(level="INFO", message="Using in-cluster Kubernetes configuration.")
        log_event(level="INFO", message="Monitoring Service Ready.")
        return

    log_event(level="INFO", message="Trying kubeconfig...")
    kube_path = settings.kube_config_path or None
    try:
        config.load_kube_config(config_file=kube_path)
    except Exception as exc:
        if not _is_kubernetes_config_error(exc):
            raise
        kubernetes_available = False
        kube_clients = None
        log_event(level="WARNING", message="No Kubernetes configuration found. Running in local development mode.")
        log_event(level="INFO", message="Monitoring Service Ready.")
        return

    kubernetes_available = True
    kube_clients = KubeClients()
    log_event(level="INFO", message="Using kubeconfig Kubernetes configuration.")
    log_event(level="INFO", message="Monitoring Service Ready.")


def build_local_development_snapshot() -> dict[str, Any]:
    summary = {
        "nodes": 0,
        "pods": 0,
        "deployments": 0,
        "namespaces": 0,
        "services": 0,
        "cpuUtilization": "0.00%",
        "memoryUtilization": "0.00%",
        "cpuUtilizationPercent": 0.0,
        "memoryUtilizationPercent": 0.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": "local-development",
        "clusterConnected": False,
        "message": "No Kubernetes cluster available.",
    }
    return {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "nodes": {"items": [], "metrics": []},
        "pods": {"items": []},
        "deployments": {"items": []},
        "metrics": {"nodeMetrics": [], "podMetrics": {}},
        "namespaces": {"items": []},
        "services": {"items": []},
        "summary": summary,
    }


def list_nodes() -> list[dict[str, Any]]:
    if not kubernetes_available or kube_clients is None:
        return []
    response = kube_clients.core_api.list_node()
    nodes: list[dict[str, Any]] = []

    for item in _safe_items(getattr(response, "items", [])):
        try:
            status = getattr(item, "status", None)
            metadata = getattr(item, "metadata", None)
            capacity = safe_dict(getattr(status, "capacity", None))
            allocatable = safe_dict(getattr(status, "allocatable", None))
            name = getattr(metadata, "name", None) or "unknown"

            nodes.append(
                {
                    "name": name,
                    "capacity_cpu": str(capacity.get("cpu", "0")),
                    "capacity_memory": str(capacity.get("memory", "0")),
                    "allocatable_cpu": str(allocatable.get("cpu", "0")),
                    "allocatable_memory": str(allocatable.get("memory", "0")),
                }
            )
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed node object: {exc}")

    return nodes


def list_node_metrics() -> list[dict[str, Any]]:
    if not kubernetes_available or kube_clients is None:
        return []
    metrics = kube_clients.custom_api.list_cluster_custom_object(
        group="metrics.k8s.io",
        version="v1beta1",
        plural="nodes",
    )

    items = _safe_items(safe_dict(metrics).get("items"))
    sanitized: list[dict[str, Any]] = []
    for entry in items:
        try:
            entry_map = safe_dict(entry)
            metadata = safe_dict(entry_map.get("metadata"))
            usage = safe_dict(entry_map.get("usage"))
            sanitized.append(
                {
                    "name": str(metadata.get("name", "unknown")),
                    "cpu_usage_millicores": parse_cpu_to_millicores(usage.get("cpu")),
                    "memory_usage_mib": parse_memory_to_mib(usage.get("memory")),
                }
            )
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed node metric: {exc}")
    return sanitized


def list_pod_metrics() -> dict[str, dict[str, int]]:
    if not kubernetes_available or kube_clients is None:
        return {}
    metrics = kube_clients.custom_api.list_cluster_custom_object(
        group="metrics.k8s.io",
        version="v1beta1",
        plural="pods",
    )

    result: dict[str, dict[str, int]] = {}
    for item in _safe_items(safe_dict(metrics).get("items")):
        try:
            item_map = safe_dict(item)
            metadata = safe_dict(item_map.get("metadata"))
            namespace = str(metadata.get("namespace", "default"))
            pod_name = str(metadata.get("name", "unknown"))
            key = f"{namespace}/{pod_name}"

            total_cpu = 0
            total_memory = 0
            for container in _safe_items(item_map.get("containers")):
                usage = safe_dict(safe_dict(container).get("usage"))
                total_cpu += parse_cpu_to_millicores(usage.get("cpu"))
                total_memory += parse_memory_to_mib(usage.get("memory"))

            result[key] = {
                "cpu_usage_millicores": total_cpu,
                "memory_usage_mib": total_memory,
            }
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed pod metric: {exc}")

    return result


def list_pods() -> list[dict[str, Any]]:
    if not kubernetes_available or kube_clients is None:
        return []
    pod_metrics = list_pod_metrics()
    response = kube_clients.core_api.list_pod_for_all_namespaces()

    pods: list[dict[str, Any]] = []
    for pod in _safe_items(getattr(response, "items", [])):
        try:
            metadata = getattr(pod, "metadata", None)
            status = getattr(pod, "status", None)
            spec = getattr(pod, "spec", None)

            namespace = getattr(metadata, "namespace", None) or "default"
            pod_name = getattr(metadata, "name", None) or "unknown"
            key = f"{namespace}/{pod_name}"

            restart_count = 0
            statuses = _safe_items(getattr(status, "container_statuses", []))
            for container_status in statuses:
                restart_count += int(getattr(container_status, "restart_count", 0) or 0)

            owner_refs: list[dict[str, Any]] = []
            for owner in _safe_items(getattr(metadata, "owner_references", [])):
                owner_refs.append(
                    {
                        "kind": getattr(owner, "kind", "") or "",
                        "name": getattr(owner, "name", "") or "",
                    }
                )

            containers_payload: list[dict[str, Any]] = []
            for container in _safe_items(getattr(spec, "containers", [])):
                resources = getattr(container, "resources", None)
                containers_payload.append(
                    {
                        "resources": {
                            "requests": safe_dict(getattr(resources, "requests", None)),
                            "limits": safe_dict(getattr(resources, "limits", None)),
                        }
                    }
                )

            totals = extract_container_resource_totals(containers_payload)
            pod_metric = safe_dict(pod_metrics.get(key))

            pods.append(
                {
                    "namespace": namespace,
                    "podName": pod_name,
                    "deployment": deployment_name_from_owner(owner_refs),
                    "status": getattr(status, "phase", None) or "Unknown",
                    "restartCount": restart_count,
                    "cpuUsageMillicores": int(pod_metric.get("cpu_usage_millicores", 0) or 0),
                    "memoryUsageMiB": int(pod_metric.get("memory_usage_mib", 0) or 0),
                    "cpuRequestMillicores": totals["cpu_request_millicores"],
                    "memoryRequestMiB": totals["memory_request_mib"],
                    "cpuLimitMillicores": totals["cpu_limit_millicores"],
                    "memoryLimitMiB": totals["memory_limit_mib"],
                }
            )
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed pod object: {exc}")

    return pods


def list_deployments() -> list[dict[str, Any]]:
    if not kubernetes_available or kube_clients is None:
        return []
    response = kube_clients.apps_api.list_deployment_for_all_namespaces()
    deployments: list[dict[str, Any]] = []

    for deployment in _safe_items(getattr(response, "items", [])):
        try:
            metadata = getattr(deployment, "metadata", None)
            spec = getattr(deployment, "spec", None)
            status = getattr(deployment, "status", None)
            template = getattr(spec, "template", None)
            pod_spec = getattr(template, "spec", None)

            containers: list[dict[str, Any]] = []
            for container in _safe_items(getattr(pod_spec, "containers", [])):
                resources = getattr(container, "resources", None)
                containers.append(
                    {
                        "resources": {
                            "requests": safe_dict(getattr(resources, "requests", None)),
                            "limits": safe_dict(getattr(resources, "limits", None)),
                        }
                    }
                )

            totals = extract_container_resource_totals(containers)
            deployments.append(
                {
                    "namespace": getattr(metadata, "namespace", None) or "default",
                    "name": getattr(metadata, "name", None) or "unknown",
                    "replicas": int(getattr(spec, "replicas", 0) or 0),
                    "availableReplicas": int(getattr(status, "available_replicas", 0) or 0),
                    "cpuRequestMillicores": totals["cpu_request_millicores"],
                    "memoryRequestMiB": totals["memory_request_mib"],
                    "cpuLimitMillicores": totals["cpu_limit_millicores"],
                    "memoryLimitMiB": totals["memory_limit_mib"],
                }
            )
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed deployment object: {exc}")

    return deployments


def list_namespaces() -> list[str]:
    if not kubernetes_available or kube_clients is None:
        return []
    response = kube_clients.core_api.list_namespace()
    names: list[str] = []
    for item in _safe_items(getattr(response, "items", [])):
        try:
            metadata = getattr(item, "metadata", None)
            name = getattr(metadata, "name", None)
            if name:
                names.append(str(name))
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed namespace object: {exc}")
    return names


def list_services() -> list[dict[str, Any]]:
    if not kubernetes_available or kube_clients is None:
        return []
    response = kube_clients.core_api.list_service_for_all_namespaces()
    services: list[dict[str, Any]] = []
    for service in _safe_items(getattr(response, "items", [])):
        try:
            metadata = getattr(service, "metadata", None)
            spec = getattr(service, "spec", None)
            services.append(
                {
                    "namespace": getattr(metadata, "namespace", None) or "default",
                    "name": getattr(metadata, "name", None) or "unknown",
                    "type": getattr(spec, "type", None) or "Unknown",
                    "clusterIP": getattr(spec, "cluster_ip", None) or "None",
                }
            )
        except Exception as exc:
            log_event(level="WARNING", message=f"skipping malformed service object: {exc}")
    return services


def summarize() -> dict[str, Any]:
    if not kubernetes_available:
        return {
            "nodes": 0,
            "pods": 0,
            "deployments": 0,
            "namespaces": 0,
            "services": 0,
            "cpuUtilization": "0.00%",
            "memoryUtilization": "0.00%",
            "cpuUtilizationPercent": 0.0,
            "memoryUtilizationPercent": 0.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": "local-development",
            "clusterConnected": False,
            "message": "No Kubernetes cluster available.",
        }

    try:
        nodes = list_nodes()
    except Exception as exc:
        log_event(level="WARNING", message=f"summary nodes collection failed: {exc}")
        nodes = []

    try:
        pods = list_pods()
    except Exception as exc:
        log_event(level="WARNING", message=f"summary pods collection failed: {exc}")
        pods = []

    try:
        deployments = list_deployments()
    except Exception as exc:
        log_event(level="WARNING", message=f"summary deployments collection failed: {exc}")
        deployments = []

    try:
        namespaces = list_namespaces()
    except Exception as exc:
        log_event(level="WARNING", message=f"summary namespaces collection failed: {exc}")
        namespaces = []

    try:
        services = list_services()
    except Exception as exc:
        log_event(level="WARNING", message=f"summary services collection failed: {exc}")
        services = []

    try:
        node_metrics = list_node_metrics()
    except Exception as exc:
        log_event(level="WARNING", message=f"summary node metrics collection failed: {exc}")
        node_metrics = []

    total_cpu_capacity = 0
    total_memory_capacity = 0
    total_cpu_usage = 0
    total_memory_usage = 0

    nodes_by_name = {
        str(node.get("name", "unknown")): node
        for node in nodes
        if isinstance(node, dict)
    }

    for metric in node_metrics:
        if not isinstance(metric, dict):
            continue
        node_name = str(metric.get("name", "unknown"))
        node_info = nodes_by_name.get(node_name)
        if not node_info:
            continue

        total_cpu_usage += int(metric.get("cpu_usage_millicores", 0) or 0)
        total_memory_usage += int(metric.get("memory_usage_mib", 0) or 0)
        total_cpu_capacity += parse_cpu_to_millicores(node_info.get("allocatable_cpu"))
        total_memory_capacity += parse_memory_to_mib(node_info.get("allocatable_memory"))

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
    if not kubernetes_available:
        return build_local_development_snapshot()

    started = time.perf_counter()

    try:
        nodes = list_nodes()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot nodes collection failed: {exc}")
        nodes = []

    try:
        node_metrics = list_node_metrics()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot node metrics collection failed: {exc}")
        node_metrics = []

    try:
        pods = list_pods()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot pods collection failed: {exc}")
        pods = []

    try:
        deployments = list_deployments()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot deployments collection failed: {exc}")
        deployments = []

    try:
        namespaces = list_namespaces()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot namespaces collection failed: {exc}")
        namespaces = []

    try:
        services = list_services()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot services collection failed: {exc}")
        services = []

    try:
        summary = summarize()
    except Exception as exc:
        log_event(level="WARNING", message=f"snapshot summary generation failed: {exc}")
        summary = {
            "nodes": len(nodes),
            "pods": len(pods),
            "deployments": len(deployments),
            "namespaces": len(namespaces),
            "services": len(services),
            "cpuUtilization": "0.00%",
            "memoryUtilization": "0.00%",
            "cpuUtilizationPercent": 0.0,
            "memoryUtilizationPercent": 0.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    snapshot = {
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "nodes": {"items": nodes, "metrics": node_metrics},
        "pods": {"items": pods},
        "deployments": {"items": deployments},
        "metrics": {
            "nodeMetrics": node_metrics,
            "podMetrics": {
                f"{pod.get('namespace', 'default')}/{pod.get('podName', 'unknown')}": {
                    "cpu_usage_millicores": int(pod.get("cpuUsageMillicores", 0) or 0),
                    "memory_usage_mib": int(pod.get("memoryUsageMiB", 0) or 0),
                }
                for pod in pods
                if isinstance(pod, dict)
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
            log_event(
                level="ERROR",
                message=f"cache refresh failed: {exc}",
                extra={"traceback": traceback.format_exc()},
            )

        await asyncio.sleep(settings.refresh_interval_seconds)


async def read_cached_snapshot() -> dict[str, Any]:
    data = await redis_client.get(settings.redis_key_cache)
    if data:
        try:
            payload = json.loads(data)
            snapshot = safe_dict(payload)
            if not snapshot:
                raise ValueError("Cached payload is not a JSON object")

            return {
                "generatedAt": snapshot.get("generatedAt", datetime.now(timezone.utc).isoformat()),
                "nodes": safe_dict(snapshot.get("nodes")) or {"items": [], "metrics": []},
                "pods": safe_dict(snapshot.get("pods")) or {"items": []},
                "deployments": safe_dict(snapshot.get("deployments")) or {"items": []},
                "metrics": safe_dict(snapshot.get("metrics")) or {"nodeMetrics": [], "podMetrics": {}},
                "namespaces": safe_dict(snapshot.get("namespaces")) or {"items": []},
                "services": safe_dict(snapshot.get("services")) or {"items": []},
                "summary": safe_dict(snapshot.get("summary")) or {
                    "nodes": 0,
                    "pods": 0,
                    "deployments": 0,
                    "namespaces": 0,
                    "services": 0,
                    "cpuUtilization": "0.00%",
                    "memoryUtilization": "0.00%",
                    "cpuUtilizationPercent": 0.0,
                    "memoryUtilizationPercent": 0.0,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            }
        except Exception as exc:
            log_event(
                level="WARNING",
                message=f"invalid monitoring cache payload: {exc}",
                extra={"traceback": traceback.format_exc()},
            )
    if not kubernetes_available:
        return build_local_development_snapshot()
    raise HTTPException(status_code=503, detail="Monitoring cache not ready")


@asynccontextmanager
async def lifespan(_: FastAPI):
    global collector_task, kube_clients
    initialize_kubernetes_clients()
    try:
        await redis_client.ping()
        log_event(level="INFO", message="Redis initialized.")
    except Exception as exc:
        log_event(level="WARNING", message=f"Redis unavailable during startup: {exc}")
    collector_task = asyncio.create_task(cache_refresh_loop())
    log_event(level="INFO", message="Scheduler started.")
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
    payload: dict[str, Any] = {
        "status": "healthy",
        "clusterConnected": kubernetes_available,
        "cache": "ready",
        "mode": "local-development" if not kubernetes_available else "kubernetes",
    }

    try:
        await redis_client.ping()
    except Exception as exc:
        log_event(level="WARNING", message=f"health redis check failed: {exc}")
        payload["redis"] = "unavailable"
        return payload

    payload["redis"] = "healthy"
    if not kubernetes_available:
        payload["message"] = "No Kubernetes cluster available."
        return payload

    try:
        await read_cached_snapshot()
    except (ApiException, HTTPException) as exc:
        log_event(level="WARNING", message=f"health degraded: {exc}")
        payload["cache"] = "warming"
        payload["detail"] = str(exc)

    return payload


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
