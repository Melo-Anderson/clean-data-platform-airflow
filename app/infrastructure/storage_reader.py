from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class StorageReader:
    """Utility to read JSON or schema metadata from storage paths (local or bucket)."""

    def read_json(self, file_path: str | None) -> dict[str, Any]:
        if not file_path:
            return {}

        path = Path(file_path)
        if not path.exists():
            return {}

        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
            return {}
        except Exception:
            return {}
