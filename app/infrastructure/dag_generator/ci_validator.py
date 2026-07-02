from __future__ import annotations

import yaml


class CiValidator:
    """Validates Pipeline YAML configurations statically without runtime dependencies."""

    def validate_yaml(self, yaml_content: str) -> list[str]:
        """Validates a YAML string and returns a list of error messages. Empty if valid."""
        try:
            doc = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            return [f"Invalid YAML: {e}"]

        if not isinstance(doc, dict) or "pipeline" not in doc:
            return ["YAML must contain a 'pipeline' root key."]

        return self._validate_sensor_timeout(doc)

    def _validate_sensor_timeout(self, doc: dict) -> list[str]:
        errors = []
        exec_timeout = (
            doc.get("pipeline", {}).get("airflow", {}).get("execution_timeout_minutes", 120)
        )
        for obj in doc.get("pipeline", {}).get("source", {}).get("objects", []):
            sensor = obj.get("sensor") or {}
            if sensor.get("query"):
                sensor_timeout = sensor.get("timeout_minutes", 60)
                if sensor_timeout > exec_timeout:
                    errors.append(
                        f"sensor.timeout_minutes ({sensor_timeout}) > "
                        f"execution_timeout_minutes ({exec_timeout}) "
                        f"for object_id={obj.get('object_id')!r}"
                    )
        return errors
