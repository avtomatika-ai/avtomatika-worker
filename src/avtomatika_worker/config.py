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
        self.WORKER_ID: str = getenv("WORKER_ID", f"worker-{uuid4()}")
        self.WORKER_TYPE: str = getenv("WORKER_TYPE", "generic-cpu-worker")
        self.WORKER_PORT: int = int(getenv("WORKER_PORT", "8083"))
        self.HOSTNAME: str = gethostname()
        try:
            self.IP_ADDRESS: str = gethostbyname(self.HOSTNAME)
        except gaierror:
            self.IP_ADDRESS: str = "127.0.0.1"

        # --- Orchestrator settings ---
        self.ORCHESTRATORS: list[dict[str, Any]] = self._get_orchestrators_config()

        # --- Security ---
        self.WORKER_TOKEN: str = getenv(
            "WORKER_INDIVIDUAL_TOKEN",
            getenv("WORKER_TOKEN", "your-secret-worker-token"),
        )

        # --- Resources and performance ---
        self.COST_PER_SKILL: dict[str, float] = self._load_json_from_env("COST_PER_SKILL", default={})
        self.MAX_CONCURRENT_TASKS: int = int(getenv("MAX_CONCURRENT_TASKS", "10"))
        self.RESOURCES: dict[str, Any] = {
            "cpu_cores": int(getenv("CPU_CORES", "4")),
            "gpu_info": self._get_gpu_info(),
        }

        # --- Installed software and models (read as JSON strings) ---
        self.INSTALLED_SOFTWARE: dict[str, str] = self._load_json_from_env(
            "INSTALLED_SOFTWARE",
            default={"python": "3.9"},
        )
        self.INSTALLED_MODELS: list[dict[str, str]] = self._load_json_from_env(
            "INSTALLED_MODELS",
            default=[],
        )

        # --- S3 Settings for payload offloading ---
        self.TASK_FILES_DIR: str = getenv("TASK_FILES_DIR", "/tmp/payloads")
        self.S3_ENDPOINT_URL: str | None = getenv("S3_ENDPOINT_URL")
        self.S3_ACCESS_KEY: str | None = getenv("S3_ACCESS_KEY")
        self.S3_SECRET_KEY: str | None = getenv("S3_SECRET_KEY")
        self.S3_DEFAULT_BUCKET: str = getenv("S3_DEFAULT_BUCKET", "avtomatika-payloads")

        # --- Tuning parameters ---
        self.HEARTBEAT_INTERVAL: float = float(getenv("HEARTBEAT_INTERVAL", "15"))
        self.RESULT_MAX_RETRIES: int = int(getenv("RESULT_MAX_RETRIES", "5"))
        self.RESULT_RETRY_INITIAL_DELAY: float = float(
            getenv("RESULT_RETRY_INITIAL_DELAY", "1.0"),
        )
        self.HEARTBEAT_DEBOUNCE_DELAY: float = float(getenv("WORKER_HEARTBEAT_DEBOUNCE_DELAY", 0.1))
        self.TASK_POLL_TIMEOUT: float = float(getenv("TASK_POLL_TIMEOUT", "30"))
        self.TASK_POLL_ERROR_DELAY: float = float(
            getenv("TASK_POLL_ERROR_DELAY", "5.0"),
        )
        self.IDLE_POLL_DELAY: float = float(getenv("IDLE_POLL_DELAY", "0.01"))
        self.ENABLE_WEBSOCKETS: bool = getenv("WORKER_ENABLE_WEBSOCKETS", "false").lower() == "true"
        self.MULTI_ORCHESTRATOR_MODE: str = getenv("MULTI_ORCHESTRATOR_MODE", "FAILOVER")

    def _get_orchestrators_config(self) -> list[dict[str, Any]]:
        """
        Loads orchestrator configuration from the ORCHESTRATORS_CONFIG environment variable.
        For backward compatibility, if it is not set, it uses ORCHESTRATOR_URL.
        """
        if orchestrators_json := getenv("ORCHESTRATORS_CONFIG"):
            try:
                orchestrators = loads(orchestrators_json)
                if getenv("ORCHESTRATOR_URL"):
                    print("Info: Both ORCHESTRATORS_CONFIG and ORCHESTRATOR_URL are set. Using ORCHESTRATORS_CONFIG.")
                for o in orchestrators:
                    if "priority" not in o:
                        o["priority"] = 10
                    if "weight" not in o:
                        o["weight"] = 1
                orchestrators.sort(key=lambda x: (x.get("priority", 10), x.get("url")))
                return orchestrators
            except JSONDecodeError:
                print("Warning: Could not decode JSON from ORCHESTRATORS_CONFIG. Falling back to default.")

        orchestrator_url = getenv("ORCHESTRATOR_URL", "http://localhost:8080")
        return [{"url": orchestrator_url, "priority": 1, "weight": 1}]

    @staticmethod
    def _get_gpu_info() -> dict[str, Any] | None:
        """Collects GPU information from environment variables.
        Returns None if GPU is not configured.
        """
        if gpu_model := getenv("GPU_MODEL"):
            return {
                "model": gpu_model,
                "vram_gb": int(getenv("GPU_VRAM_GB", "0")),
            }
        else:
            return None

    @staticmethod
    def _load_json_from_env(key: str, default: Any) -> Any:
        """Safely loads a JSON string from an environment variable."""
        if value := getenv(key):
            try:
                return loads(value)
            except JSONDecodeError:
                print(
                    f"Warning: Could not decode JSON from environment variable {key}.",
                )
                return default
        return default
