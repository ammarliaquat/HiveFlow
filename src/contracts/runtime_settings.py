# src/contracts/runtime_settings.py
"""
Runtime settings contract for centralizing timeout and reaping configurations.
"""
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Mapping

if TYPE_CHECKING:
    from src.contracts.deployment_profile import DeploymentProfile

@dataclass(frozen=True)
class RuntimeSettings:
    """Consolidated execution timeout and reaping settings."""
    dispatch_lease_timeout_sec: int
    worker_heartbeat_timeout_sec: int
    publication_timeout_sec: int
    target_lock_timeout_ms: int

    @classmethod
    def from_deployment_profile(cls, profile: "DeploymentProfile") -> "RuntimeSettings":
        """Loads settings from the authoritative deployment configuration."""
        return cls.from_config_path(profile.config_path)

    @classmethod
    def from_config_path(cls, config_path: str) -> "RuntimeSettings":
        """Loads settings from the authoritative YAML configuration file."""
        import yaml
        from src.exceptions import ExecutionProfileInvalidError

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)
        except Exception as e:
            raise ExecutionProfileInvalidError(f"Failed to read config file {config_path}: {e}") from e

        pipeline_config = (config_data or {}).get("pipeline", {})
        runtime_config = pipeline_config.get("runtime_settings", {})
        if not isinstance(runtime_config, Mapping):
            raise ExecutionProfileInvalidError(
                "pipeline.runtime_settings must be a mapping in the deployment config."
            )
        return cls.from_mapping(runtime_config)

    @classmethod
    def from_mapping(cls, runtime_config: Mapping[str, Any]) -> "RuntimeSettings":
        """Builds RuntimeSettings from an explicit mapping; every key is mandatory."""
        from src.exceptions import ExecutionProfileInvalidError

        required = (
            "dispatch_lease_timeout_sec",
            "worker_heartbeat_timeout_sec",
            "publication_timeout_sec",
            "target_lock_timeout_ms",
        )
        missing = [key for key in required if key not in runtime_config]
        if missing:
            raise ExecutionProfileInvalidError(
                f"Missing pipeline.runtime_settings key(s): {', '.join(missing)}"
            )
        values = {}
        for key in required:
            try:
                value = int(runtime_config[key])
            except (TypeError, ValueError) as exc:
                raise ExecutionProfileInvalidError(
                    f"pipeline.runtime_settings.{key} must be an integer."
                ) from exc
            if value <= 0:
                raise ExecutionProfileInvalidError(
                    f"pipeline.runtime_settings.{key} must be greater than zero."
                )
            values[key] = value
        return cls(
            dispatch_lease_timeout_sec=values["dispatch_lease_timeout_sec"],
            worker_heartbeat_timeout_sec=values["worker_heartbeat_timeout_sec"],
            publication_timeout_sec=values["publication_timeout_sec"],
            target_lock_timeout_ms=values["target_lock_timeout_ms"],
        )
