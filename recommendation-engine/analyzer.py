from __future__ import annotations

from typing import Any


def to_int(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    return int(float(str(value)))


def severity_from_savings(savings_percent: float) -> str:
    if savings_percent >= 60:
        return "High"
    if savings_percent >= 30:
        return "Medium"
    return "Low"


def monthly_cost(cpu_m: int, memory_mib: int, cpu_price_per_m_hour: float, mem_price_per_mib_hour: float, hours_per_month: int) -> float:
    return (cpu_m * cpu_price_per_m_hour + memory_mib * mem_price_per_mib_hour) * hours_per_month


def build_recommendations(
    *,
    pods: list[dict[str, Any]],
    deployments: list[dict[str, Any]],
    cpu_price_per_m_hour: float,
    mem_price_per_mib_hour: float,
    hours_per_month: int,
) -> list[dict[str, str]]:
    recommendations: list[dict[str, str]] = []
    pods_by_deployment: dict[str, list[dict[str, Any]]] = {}

    for pod in pods:
        deployment = pod.get("deployment", "unknown")
        pods_by_deployment.setdefault(deployment, []).append(pod)

    for deployment in deployments:
        name = deployment.get("name", "unknown")
        deployment_pods = pods_by_deployment.get(name, [])

        cpu_request = to_int(deployment.get("cpuRequestMillicores", 0))
        memory_request = to_int(deployment.get("memoryRequestMiB", 0))
        cpu_limit = to_int(deployment.get("cpuLimitMillicores", 0))
        memory_limit = to_int(deployment.get("memoryLimitMiB", 0))
        replicas = to_int(deployment.get("replicas", 0))

        if replicas <= 0:
            continue

        namespace = str(deployment.get("namespace", "default"))

        if cpu_request <= 0 and memory_request <= 0:
            continue

        pod_count = len(deployment_pods)
        if pod_count == 0:
            continue

        total_cpu_usage = sum(to_int(pod.get("cpuUsageMillicores", 0)) for pod in deployment_pods)
        total_memory_usage = sum(to_int(pod.get("memoryUsageMiB", 0)) for pod in deployment_pods)
        total_restarts = sum(to_int(pod.get("restartCount", 0)) for pod in deployment_pods)

        avg_cpu_usage = total_cpu_usage / max(pod_count, 1)
        avg_memory_usage = total_memory_usage / max(pod_count, 1)

        per_pod_cpu_request = cpu_request / max(replicas, 1)
        per_pod_memory_request = memory_request / max(replicas, 1)

        cpu_utilization_pct = 0.0
        memory_utilization_pct = 0.0

        if per_pod_cpu_request > 0:
            cpu_utilization_pct = (avg_cpu_usage / per_pod_cpu_request) * 100

        if per_pod_memory_request > 0:
            memory_utilization_pct = (avg_memory_usage / per_pod_memory_request) * 100

        recommended_cpu = int(per_pod_cpu_request)
        recommended_memory = int(per_pod_memory_request)

        cpu_reason = ""
        memory_reason = ""

        if per_pod_cpu_request > 0 and cpu_utilization_pct < 40:
            recommended_cpu = max(50, int(avg_cpu_usage * 1.7))
            cpu_reason = f"CPU request is overprovisioned with {cpu_utilization_pct:.1f}% utilization"
        elif per_pod_cpu_request > 0 and cpu_utilization_pct > 90:
            recommended_cpu = int(avg_cpu_usage * 1.25)
            cpu_reason = f"CPU request is near saturation with {cpu_utilization_pct:.1f}% utilization"

        if per_pod_memory_request > 0 and memory_utilization_pct < 40:
            recommended_memory = max(64, int(avg_memory_usage * 1.8))
            memory_reason = f"Memory request is overprovisioned with {memory_utilization_pct:.1f}% utilization"
        elif per_pod_memory_request > 0 and memory_utilization_pct > 92:
            recommended_memory = int(avg_memory_usage * 1.25)
            memory_reason = f"Memory request is near saturation with {memory_utilization_pct:.1f}% utilization"

        if cpu_reason or memory_reason or total_restarts > 5 or cpu_limit <= 0 or memory_limit <= 0:
            current_cpu_total = int(per_pod_cpu_request * max(replicas, 1))
            current_memory_total = int(per_pod_memory_request * max(replicas, 1))
            target_cpu_total = int(recommended_cpu * max(replicas, 1))
            target_memory_total = int(recommended_memory * max(replicas, 1))

            cpu_reduction = max(0, current_cpu_total - target_cpu_total)
            memory_reduction = max(0, current_memory_total - target_memory_total)

            monthly_now = monthly_cost(
                current_cpu_total,
                current_memory_total,
                cpu_price_per_m_hour,
                mem_price_per_mib_hour,
                hours_per_month,
            )
            monthly_new = monthly_cost(
                target_cpu_total,
                target_memory_total,
                cpu_price_per_m_hour,
                mem_price_per_mib_hour,
                hours_per_month,
            )
            monthly_saving = max(0.0, monthly_now - monthly_new)
            annual_saving = monthly_saving * 12

            saving_pct = (monthly_saving / monthly_now * 100) if monthly_now > 0 else 0
            severity = severity_from_savings(saving_pct)
            if total_restarts > 5:
                severity = "High"

            reasons = [reason for reason in [cpu_reason, memory_reason] if reason]
            if cpu_limit <= 0 or memory_limit <= 0:
                reasons.append("Missing CPU or memory limit")
            if total_restarts > 5:
                reasons.append(f"Container restarts are high ({total_restarts})")

            recommendation_text = "Tune resource requests based on observed usage"
            if cpu_reason and not memory_reason:
                recommendation_text = (
                    f"Reduce CPU request from {int(per_pod_cpu_request)}m to {recommended_cpu}m per pod"
                    if recommended_cpu < per_pod_cpu_request
                    else f"Increase CPU request from {int(per_pod_cpu_request)}m to {recommended_cpu}m per pod"
                )
            if memory_reason and not cpu_reason:
                recommendation_text = (
                    f"Reduce memory request from {int(per_pod_memory_request)}Mi to {recommended_memory}Mi per pod"
                    if recommended_memory < per_pod_memory_request
                    else f"Increase memory request from {int(per_pod_memory_request)}Mi to {recommended_memory}Mi per pod"
                )

            recommendations.append(
                {
                    "service": name,
                    "namespace": namespace,
                    "severity": severity,
                    "reason": "; ".join(reasons) if reasons else "Resource tuning opportunity detected",
                    "recommendation": recommendation_text,
                    "currentCpuRequest": f"{int(per_pod_cpu_request)}m",
                    "recommendedCpuRequest": f"{int(recommended_cpu)}m",
                    "currentMemoryRequest": f"{int(per_pod_memory_request)}Mi",
                    "recommendedMemoryRequest": f"{int(recommended_memory)}Mi",
                    "estimatedCpuReduction": f"{cpu_reduction}m",
                    "estimatedMemoryReduction": f"{memory_reduction}Mi",
                    "estimatedMonthlySaving": f"${monthly_saving:.2f}",
                    "estimatedAnnualSaving": f"${annual_saving:.2f}",
                }
            )

    return recommendations
