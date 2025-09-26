import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict


class DesktopConfig:
    """Lightweight JSON backed configuration store for the Odoo uploader client."""

    DEFAULTS: Dict[str, Any] = {
        "odoo": {
            "url": "http://localhost:8069",
            "database": "",
            "username": "",
            "password": "",
        },
        "paths": {
            "path1": str(Path.home()),
            "path2": str(Path.home()),
            "move_path1": str(Path.home() / "archive"),
            "move_path2": str(Path.home() / "errors"),
            "source_pdf": str(Path.home()),
            "move_pdf": str(Path.home()),
        },
        "monitoring": {
            "interval": 30,
            "delete_source": False,
            "auto_upload": True,
        },
        "client": {
            "default_type_code": "",
            "last_directory": str(Path.home()),
        },
        "emails": {
            "recipients": "[]",
        },
    }

    def __init__(
        self,
        config_file: Path | str | None = None,
        defaults: Dict[str, Any] | None = None,
    ) -> None:
        self.config_path = Path(config_file or Path.home() / ".odoo_csv_uploader.json")
        base_defaults = deepcopy(self.DEFAULTS)
        if defaults:
            self._deep_update(base_defaults, defaults)
        self._data: Dict[str, Any] = base_defaults
        self.load()

    # ------------------------------------------------------------------ basic IO
    def load(self) -> None:
        if self.config_path.exists():
            try:
                loaded = json.loads(self.config_path.read_text(encoding="utf-8"))
                self._deep_update(self._data, loaded)
            except Exception:
                pass

    def save(self) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(json.dumps(self._data, indent=2), encoding="utf-8")

    # ------------------------------------------------------------------ accessors
    def get(self, section: str, key: str, default: Any = "") -> Any:
        return self._data.get(section, {}).get(key, default)

    def get_section(self, section: str) -> Dict[str, Any]:
        return dict(self._data.get(section, {}))

    def set(self, section: str, key: str, value: Any) -> None:
        self._data.setdefault(section, {})[key] = value
        self.save()

    def set_section(self, section: str, values: Dict[str, Any]) -> None:
        self._data[section] = values
        self.save()

    def update_section(self, section: str, values: Dict[str, Any]) -> None:
        target = self._data.setdefault(section, {})
        for key, value in values.items():
            target[key] = value
        self.save()

    # ------------------------------------------------------------------ utils
    @staticmethod
    def _deep_update(target: Dict[str, Any], source: Dict[str, Any]) -> None:
        for key, value in source.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                DesktopConfig._deep_update(target[key], value)
            else:
                target[key] = value
