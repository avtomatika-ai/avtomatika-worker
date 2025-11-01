from _socket import gaierror, gethostbyname, gethostname
from json import JSONDecodeError, loads
from os import getenv
from typing import Any
from uuid import uuid4


class WorkerConfig:
    """A class for centralized management of worker configuration.
    Reads parameters from environment variables and provides default values.
    """

    def __init__(self):
        # --- Basic worker information ---
        self.worker_id: str = getenv("WORKER_ID", f"worker-{uuid4()}")
        self.worker_type: str = getenv("WORKER_TYPE", "generic-cpu-worker")
        self.worker_port: int = int(getenv("WORKER_PORT", "8083"))
        self.hostname: str = gethostname()
        try:
            self.ip_address: str = gethostbyname(self.hostname)
        except gaierror:
            self.ip_address: str = "127.0.0.1"

        # --- Orchestrator settings ---
        self.orchestrators: list[dict[str, Any]] = self._get_orchestrators_config()

        # --- Security ---
        self.worker_token: str = getenv(
            "WORKER_INDIVIDUAL_TOKEN",
            getenv("WORKER_TOKEN", "your-secret-worker-token"),
        )

        # --- Resources and performance ---
        self.cost_per_second: float = float(getenv("WORKER_COST_PER_SECOND", "0.01"))
        self.max_concurrent_tasks: int = int(getenv("MAX_CONCURRENT_TASKS", "10"))
        self.resources: dict[str, Any] = {
            "cpu_cores": int(getenv("CPU_CORES", "4")),
            "gpu_info": self._get_gpu_info(),
        }

        # --- Installed software and models (read as JSON strings) ---
        self.installed_software: dict[str, str] = self._load_json_from_env(
            "INSTALLED_SOFTWARE",
            default={"python": "3.9"},
        )
        self.installed_models: list[dict[str, str]] = self._load_json_from_env(
            "INSTALLED_MODELS",
            default=[],
        )

        # --- Tuning parameters ---
        self.heartbeat_interval: float = float(getenv("HEARTBEAT_INTERVAL", "15"))
        self.result_max_retries: int = int(getenv("RESULT_MAX_RETRIES", "5"))
        self.result_retry_initial_delay: float = float(
            getenv("RESULT_RETRY_INITIAL_DELAY", "1.0"),
        )
        self.heartbeat_debounce_delay: float = float(getenv("WORKER_HEARTBEAT_DEBOUNCE_DELAY", 0.1))
        self.task_poll_timeout: float = float(getenv("TASK_POLL_TIMEOUT", "30"))
        self.task_poll_error_delay: float = float(
            getenv("TASK_POLL_ERROR_DELAY", "5.0"),
        )
        self.idle_poll_delay: float = float(getenv("IDLE_POLL_DELAY", "0.01"))
        self.enable_websockets: bool = getenv("WORKER_ENABLE_WEBSOCKETS", "false").lower() == "true"
        self.multi_orchestrator_mode: str = getenv("MULTI_ORCHESTRATOR_MODE", "FAILOVER")

    def _get_orchestrators_config(self) -> list[dict[str, Any]]:
        """
        Loads orchestrator configuration from the ORCHESTRATORS_CONFIG environment variable.
        For backward compatibility, if it is not set, it uses ORCHESTRATOR_URL.
        """
        orchestrators_json = getenv("ORCHESTRATORS_CONFIG")
        if orchestrators_json:
            try:
                orchestrators = loads(orchestrators_json)
                for o in orchestrators:
                    if "priority" not in o:
                        o["priority"] = 10
                orchestrators.sort(key=lambda x: (x.get("priority", 10), x.get("url")))
                return orchestrators
            except JSONDecodeError:
                print("Warning: Could not decode JSON from ORCHESTRATORS_CONFIG. Falling back to default.")

        orchestrator_url = getenv("ORCHESTRATOR_URL", "http://localhost:8080")
        return [{"url": orchestrator_url, "priority": 1}]

    def _get_gpu_info(self) -> dict[str, Any] | None:
        """Collects GPU information from environment variables.
        Returns None if GPU is not configured.
        """
        gpu_model = getenv("GPU_MODEL")
        if not gpu_model:
            return None

        return {
            "model": gpu_model,
            "vram_gb": int(getenv("GPU_VRAM_GB", "0")),
        }

    def _load_json_from_env(self, key: str, default: Any) -> Any:
        """Safely loads a JSON string from an environment variable."""
        value = getenv(key)
        if value:
            try:
                return loads(value)
            except JSONDecodeError:
                print(
                    f"Warning: Could not decode JSON from environment variable {key}.",
                )
                return default
        return default
