import logging
from pathlib import Path
from datetime import datetime


class Logger:
    def __init__(self, name: str, log_dir: Path | str = "logs") -> None:
        log_folder = Path(log_dir)
        log_folder.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(name)
        if not self.logger.handlers:
            self.logger.setLevel(logging.DEBUG)

            log_file = log_folder / f"{datetime.now().strftime('%Y-%m-%d')}.log"
            file_handler = logging.FileHandler(log_file, encoding="utf-8")
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def exception(self, message: str) -> None:
        self.logger.exception(message)
