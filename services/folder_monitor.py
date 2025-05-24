import os
import time
import threading
import filecmp
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from services.database_service import DatabaseService
from services.email_notifier import EmailNotifier
from services.logger import Logger
from abc import ABC, abstractmethod
import importlib
from processors.file_processor_factory import FileProcessorFactory
import shutil

class FolderMonitor:
    def __init__(self, config_manager):
        self.logger = Logger("FolderMonitor")
        self.config_manager = config_manager
        self.running = False
        self.observer = Observer()
        
        # Initialize paths
        self.path_settings = {
            'input1': {
                'input': config_manager.get_setting('paths', 'path1'),
                'move': config_manager.get_setting('paths', 'move_path1')
            },
            'input2': {
                'input': config_manager.get_setting('paths', 'path2'),
                'move': config_manager.get_setting('paths', 'move_path2')
            },
            'pdf': {
                'input': config_manager.get_setting('paths', 'source_pdf'),
                'move': config_manager.get_setting('paths', 'move_pdf')
            }
        }

        # Validate paths
        for name, paths in self.path_settings.items():
            if not paths['input'] or not paths['move']:
                raise ValueError(f"Path configuration incomplete for {name}")

        # Initialize services
        self.db_handler = DatabaseService(
            host=config_manager.get_setting('mysql', "mysql_server"),
            database=config_manager.get_setting('mysql', "mysql_db"),
            user=config_manager.get_setting('mysql', "mysql_user"),
            password=config_manager.get_setting('mysql', "mysql_pass"),
            port=config_manager.get_setting('mysql', "mysql_port", 3306)
        )
        
        self.email_notifier = EmailNotifier(
            smtp_server=os.getenv("SMTP_SERVER"),
            smtp_port=int(os.getenv("SMTP_PORT", 587)),
            sender_email=os.getenv("SENDER_EMAIL"),
            sender_password=os.getenv("SENDER_PASSWORD")
        )

        self.processor_factory = FileProcessorFactory(
            self.db_handler,
            self.email_notifier,
            self.logger
        )

    def start(self, interval=30):
        """Start monitoring service"""
        self.running = True
        self.logger.info("Starting folder monitoring service")
        
        event_handler = FileSystemEventHandler()
        event_handler.on_created = self._on_file_created
        
        # Setup observers for each input path
        for paths in self.path_settings.values():
            input_dir = Path(paths['input'])
            input_dir.mkdir(parents=True, exist_ok=True)
            self.observer.schedule(event_handler, str(input_dir), recursive=False)
        
        self.observer.start()
        
        monitor_thread = threading.Thread(
            target=self._monitor_folders,
            args=(interval,),
            daemon=True
        )
        monitor_thread.start()

    def stop(self):
        """Stop monitoring service"""
        self.running = False
        if hasattr(self, 'observer') and self.observer:
            self.observer.stop()
            self.observer.join()
        self.logger.info("Folder monitoring service stopped")

    def _on_file_created(self, event):
        """Handle new file creation events"""
        if not event.is_directory:
            file_path = Path(event.src_path)
            if file_path.suffix.lower() == '.csv':
                for path_name, paths in self.path_settings.items():
                    input_path = Path(paths['input'])
                    if str(file_path).startswith(str(input_path)):
                        self._process_file(file_path, Path(paths['move']), path_name)
                        break

    def _monitor_folders(self, interval):
        """Periodic folder monitoring"""
        while self.running:
            try:
                self.logger.info("Checking folders for new files...")
                
                for paths in self.path_settings.values():
                    input_dir = Path(paths['input'])
                    move_dir = Path(paths['move'])
                    
                    input_dir.mkdir(parents=True, exist_ok=True)
                    move_dir.mkdir(parents=True, exist_ok=True)
                    
                    for csv_file in input_dir.glob('*.[cC][sS][vV]'):
                        self._process_file(csv_file, move_dir, 'periodic_check')
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Error in folder monitoring: {str(e)}")
                time.sleep(60)

    def _process_file(self, file_path, move_dir, path_name):
        try:
            processor = self.processor_factory.get_processor(file_path.name)
            
            # Process the file (upload to DB and handle duplicates)
            success = processor.process(file_path, move_dir)
            
            if not success:
                return False
                
            # Prepare destination path
            dest_path = self._get_unique_filename(file_path, move_dir)
            
            # Handle file transfer with fallback strategies
            transfer_success, needs_cleanup = self._transfer_file_with_fallback(file_path, dest_path)
            
            if needs_cleanup:
                self._register_cleanup(file_path)
                
            return transfer_success
            
        except ValueError as e:
            self.logger.error(str(e))
            return False
        except Exception as e:
            self.logger.error(f"Error processing file: {str(e)}")
        return False

    def _get_unique_filename(self, src_path, dest_dir):
        """Generate unique filename if destination exists"""
        base_name = src_path.name
        dest_path = dest_dir / base_name
        counter = 1
        
        while dest_path.exists():
            stem = src_path.stem
            suffix = src_path.suffix
            dest_path = dest_dir / f"{stem}_{counter}{suffix}"
            counter += 1
            
        return dest_path

    def _transfer_file_with_fallback(self, src, dst):
        """Transfer file with multiple fallback strategies"""
        try:
            # Try direct copy first
            shutil.copy2(str(src), str(dst))
            
            # Verify copy
            if not (dst.exists() and dst.stat().st_size == src.stat().st_size):
                raise IOError("Copy verification failed")
            
            # Try to remove source
            try:
                src.unlink()
                return True, False
            except PermissionError:
                # Try renaming as fallback
                try:
                    processed_path = src.with_suffix(src.suffix + '.processed')
                    src.rename(processed_path)
                    return True, False
                except Exception:
                    self.logger.warning(f"Copied to {dst} but couldn't modify source")
                    return True, True
        
        except Exception as e:
            self.logger.error(f"File transfer failed: {str(e)}")
            return False, False

    def _register_cleanup(self, file_path):
        """Register file for later cleanup"""
        self.logger.info(f"Registered for cleanup: {file_path}")
        # Add your cleanup registration logic here

    




