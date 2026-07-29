import os


class Settings:
    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "kubewise-monitoring")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.namespace = os.getenv("POD_NAMESPACE", "kubewise")
        self.pod_name = os.getenv("POD_NAME", "unknown")
        self.namespace_scope = os.getenv("NAMESPACE_SCOPE", "")
        self.kube_config_path = os.getenv("KUBECONFIG_PATH", "")
        self.request_timeout_seconds = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "8"))
        self.refresh_interval_seconds = int(os.getenv("REFRESH_INTERVAL_SECONDS", "30"))
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        self.redis_key_cache = os.getenv("REDIS_KEY_MONITORING_CACHE", "kubewise:monitoring:cache")
