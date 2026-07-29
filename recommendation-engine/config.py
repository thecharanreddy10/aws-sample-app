import os


class Settings:
    def __init__(self) -> None:
        self.app_name = os.getenv("APP_NAME", "kubewise-recommendation")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.namespace = os.getenv("POD_NAMESPACE", "kubewise")
        self.pod_name = os.getenv("POD_NAME", "unknown")
        self.analysis_interval_seconds = int(os.getenv("ANALYSIS_INTERVAL_SECONDS", "30"))
        self.price_per_millicore_hour = float(os.getenv("PRICE_PER_MILLICORE_HOUR", "0.000011"))
        self.price_per_mib_hour = float(os.getenv("PRICE_PER_MIB_HOUR", "0.000007"))
        self.hours_per_month = int(os.getenv("HOURS_PER_MONTH", "730"))
        self.redis_host = os.getenv("REDIS_HOST", "redis")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        self.redis_key_monitoring_cache = os.getenv("REDIS_KEY_MONITORING_CACHE", "kubewise:monitoring:cache")
        self.redis_key_recommendations = os.getenv("REDIS_KEY_RECOMMENDATIONS", "kubewise:recommendations")
