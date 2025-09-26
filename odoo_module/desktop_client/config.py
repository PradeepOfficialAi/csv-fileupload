from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from config.config_manager import DesktopConfig


class OdooDesktopConfig(DesktopConfig):
    """Configuration profile tailored for the Odoo module desktop uploader."""

    def __init__(
        self,
        config_file: Path | str | None = None,
        extra_defaults: Dict[str, Any] | None = None,
    ) -> None:
        env_defaults: Dict[str, Any] = {
            "odoo": {
                "url": os.getenv("ODOO_URL", "http://localhost:8069"),
                "database": os.getenv("ODOO_DB", ""),
                "username": os.getenv("ODOO_USER", ""),
                "password": os.getenv("ODOO_PASSWORD", ""),
            },
            "postgres": {
                "host": os.getenv("PGHOST", "localhost"),
                "port": int(os.getenv("PGPORT", "5432")),
                "database": os.getenv("PGDATABASE", ""),
                "username": os.getenv("PGUSER", ""),
                "password": os.getenv("PGPASSWORD", ""),
            },
        }
        if extra_defaults:
            env_defaults.update(extra_defaults)

        target_path = config_file or Path.home() / ".odoo_module_csv_uploader.json"
        super().__init__(config_file=target_path, defaults=env_defaults)
