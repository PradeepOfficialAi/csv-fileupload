from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

MODULE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = MODULE_ROOT.parents[1]

if str(MODULE_ROOT) not in sys.path:
    sys.path.insert(0, str(MODULE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if __package__:
    from .config import OdooDesktopConfig  # type: ignore[import-not-found]
else:
    spec = importlib.util.spec_from_file_location(
        "odoo_module.desktop_client.config",
        MODULE_ROOT / "config.py",
    )
    if spec and spec.loader:
        config_module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = config_module
        spec.loader.exec_module(config_module)
        OdooDesktopConfig = config_module.OdooDesktopConfig  # type: ignore[attr-defined]
    else:  # pragma: no cover - defensive fallback
        raise ImportError("Unable to load Odoo desktop configuration module")

from main import MainApplication  # noqa: E402  (import after sys.path tweaks)


def create_application() -> MainApplication:
    config = OdooDesktopConfig()
    return MainApplication(config_store=config, logger_name="OdooModuleMain")


def run() -> None:
    app = create_application()
    app.mainloop()


if __name__ == "__main__":
    run()
