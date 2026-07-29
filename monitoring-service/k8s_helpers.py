from __future__ import annotations

from typing import Any


CPU_FACTORS = {
    "n": 1 / 1_000_000,
    "u": 1 / 1_000,
    "m": 1,
}

MEMORY_BINARY_FACTORS = {
    "Ki": 1 / 1024,
    "Mi": 1,
    "Gi": 1024,
    "Ti": 1024 * 1024,
    "Pi": 1024 * 1024 * 1024,
    "Ei": 1024 * 1024 * 1024 * 1024,
}

MEMORY_DECIMAL_FACTORS = {
    "K": 1000 / (1024 * 1024),
    "M": 1_000_000 / (1024 * 1024),
    "G": 1_000_000_000 / (1024 * 1024),
    "T": 1_000_000_000_000 / (1024 * 1024),
}


def parse_cpu_to_millicores(value: str | None) -> int:
    if not value:
        return 0

    raw = value.strip()
    for suffix, factor in CPU_FACTORS.items():
        if raw.endswith(suffix):
            number = float(raw[: -len(suffix)] or "0")
            return int(number * factor)

    return int(float(raw) * 1000)


def parse_memory_to_mib(value: str | None) -> int:
    if not value:
        return 0

    raw = value.strip()

    for suffix, factor in MEMORY_BINARY_FACTORS.items():
        if raw.endswith(suffix):
            number = float(raw[: -len(suffix)] or "0")
            return int(number * factor)

    for suffix, factor in MEMORY_DECIMAL_FACTORS.items():
        if raw.endswith(suffix):
            number = float(raw[: -len(suffix)] or "0")
            return int(number * factor)

    return int(float(raw) / (1024 * 1024))


def extract_container_resource_totals(containers: list[dict[str, Any]]) -> dict[str, int]:
    cpu_request = 0
    memory_request = 0
    cpu_limit = 0
    memory_limit = 0

    for container in containers:
        resources = container.get("resources", {})
        requests = resources.get("requests", {})
        limits = resources.get("limits", {})

        cpu_request += parse_cpu_to_millicores(requests.get("cpu"))
        memory_request += parse_memory_to_mib(requests.get("memory"))
        cpu_limit += parse_cpu_to_millicores(limits.get("cpu"))
        memory_limit += parse_memory_to_mib(limits.get("memory"))

    return {
        "cpu_request_millicores": cpu_request,
        "memory_request_mib": memory_request,
        "cpu_limit_millicores": cpu_limit,
        "memory_limit_mib": memory_limit,
    }


def deployment_name_from_owner(owner_references: list[dict[str, Any]]) -> str:
    if not owner_references:
        return "unknown"

    owner = owner_references[0]
    kind = owner.get("kind", "")
    name = owner.get("name", "")

    if kind == "ReplicaSet" and name.count("-") >= 1:
        return "-".join(name.split("-")[:-1])

    return name or "unknown"
