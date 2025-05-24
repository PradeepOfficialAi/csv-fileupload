from abc import ABC, abstractmethod
from pathlib import Path

class BaseProcessor(ABC):
    def __init__(self, db_service, email_notifier, logger):
        self.db_service = db_service  # تغییر از db_handler به db_service
        self.email_notifier = email_notifier
        self.logger = logger
    
    @abstractmethod
    def get_table_name(self) -> str:
        """Return the database table name for this processor"""
        pass
    
    @abstractmethod
    def process(self, file_path: Path, move_dir: Path) -> bool:
        pass