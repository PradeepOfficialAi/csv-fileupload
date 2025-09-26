from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Dict, Optional

from .logger import Logger
from .uploader import OdooCsvUploader, UploadError
from config.config_manager import DesktopConfig


class FolderMonitor:
    def __init__(
        self,
        config: DesktopConfig,
        uploader: OdooCsvUploader,
        logger: Logger,
    ) -> None:
        self.config = config
        self.uploader = uploader
        self.logger = logger
        self.running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._processed: set[str] = set()

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self.running:
            return
        self.running = True
        interval = int(self.config.get("monitoring", "interval", 30)) or 30
        self.logger.info(f"Starting folder monitor (interval={interval}s)")
        self._process_existing_files()
        self._thread = threading.Thread(target=self._run, args=(interval,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self.running:
            return
        self.logger.info("Stopping folder monitor")
        self.running = False
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)
        self._thread = None

    # ------------------------------------------------------------------ core loop
    def _run(self, interval: int) -> None:
        while self.running:
            try:
                self._process_existing_files()
            except Exception as exc:  # pragma: no cover - safety net
                self.logger.exception(f"Unexpected error during monitoring: {exc}")
            time.sleep(interval)

    def _process_existing_files(self) -> None:
        path_config = self.config.get_section("paths")
        monitor_map: Dict[str, Dict[str, Optional[Path]]] = {
            "path1": {
                "source": self._prepare_directory(path_config.get("path1")),
                "archive": self._prepare_directory(path_config.get("move_path1")),
                "error": self._prepare_directory(path_config.get("move_path2")),
            },
            "path2": {
                "source": self._prepare_directory(path_config.get("path2")),
                "archive": self._prepare_directory(path_config.get("move_path2")),
                "error": self._prepare_directory(path_config.get("move_path2")),
            },
            "pdf": {
                "source": self._prepare_directory(path_config.get("source_pdf")),
                "archive": self._prepare_directory(path_config.get("move_pdf")),
                "error": self._prepare_directory(path_config.get("move_pdf")),
            },
        }

        for entry in monitor_map.values():
            source = entry.get("source")
            if not source or not source.exists():
                continue

            for file_path in sorted(source.glob("*")):
                if not file_path.is_file():
                    continue
                if file_path.suffix.lower() not in {".csv", ".txt"}:
                    continue
                self._process_file(
                    file_path=file_path,
                    archive_dir=entry.get("archive"),
                    error_dir=entry.get("error"),
                )

    # ------------------------------------------------------------------ per-file handling
    def _process_file(self, file_path: Path, archive_dir: Optional[Path], error_dir: Optional[Path]) -> None:
        with self._lock:
            signature = str(file_path.resolve())
            if signature in self._processed:
                return
            self._processed.add(signature)

        type_code = self._guess_type_code(file_path)
        delete_source = bool(self.config.get("monitoring", "delete_source", False))
        auto_upload = bool(self.config.get("monitoring", "auto_upload", True))

        try:
            self.logger.info(f"Processing file {file_path.name} -> type {type_code}")
            result = self.uploader.upload(
                file_path=file_path,
                type_code=type_code,
                source_system="folder-monitor",
                archive_dir=archive_dir,
                error_dir=error_dir,
                auto_register_lines=auto_upload,
                delete_source=delete_source,
            )
            self.logger.info(
                "Upload completed: id=%s rows=%s duplicates=%s errors=%s"
                % (
                    result.get("upload_id"),
                    result.get("rows"),
                    result.get("duplicates"),
                    result.get("errors"),
                )
            )
        except UploadError as exc:
            self.logger.error(f"Upload failed for {file_path.name}: {exc}")
        except Exception as exc:  # pragma: no cover - defensive guard
            self.logger.exception(f"Unexpected failure for {file_path.name}: {exc}")
        finally:
            with self._lock:
                self._processed.discard(signature)

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _prepare_directory(value: Optional[str]) -> Optional[Path]:
        if not value:
            return None
        path = Path(value).expanduser()
        try:
            path.mkdir(parents=True, exist_ok=True)
        except Exception:
            return None
        return path

    def _guess_type_code(self, file_path: Path) -> str:
        stem = file_path.stem.upper()
        parts = stem.replace("-", "_").split("_")
        if parts:
            return parts[0]
        return stem[:32] or "UNKNOWN"
