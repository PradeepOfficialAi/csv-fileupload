import importlib
from pathlib import Path

class FileProcessorFactory:
    def __init__(self, db_handler, email_notifier, logger):
        self.db_handler = db_handler
        self.email_notifier = email_notifier
        self.logger = logger
    
    def get_processor(self, file_name: str):
        try:
            # استخراج نام پردازشگر از نام فایل
            processor_name = self._extract_processor_name(file_name)
            
            # ایمپورت پویای ماژول پردازشگر
            module = importlib.import_module(f"processors.{processor_name}")
            processor_class = getattr(module, f"{processor_name}Processor")
            
            return processor_class(
                self.db_handler,
                self.email_notifier,
                self.logger
            )
        except (ImportError, AttributeError) as e:
            self.logger.error(f"Processor not found for {file_name}: {str(e)}")
            raise ValueError(f"No processor found for file: {file_name}")
    
    def _extract_processor_name(self, filename):
        """استخراج نام پردازشگر از نام فایل"""
        base_name = Path(filename).stem
        return base_name.split('_')[0].upper()