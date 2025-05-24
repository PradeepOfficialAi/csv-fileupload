from processors.base_processor import BaseProcessor
from pathlib import Path

class FRAMESCUTTINGProcessor(BaseProcessor):
    def process(self, file_path: Path, move_dir: Path) -> bool:
        try:
            self.logger.info(f"Processing FRAMESCUTTING file: {file_path}")
            # پردازش خاص برای فایل‌های FRAMESCUTTING
            # ...
            return True
        except Exception as e:
            self.logger.error(f"Error processing FRAMESCUTTING file: {str(e)}")
            return False