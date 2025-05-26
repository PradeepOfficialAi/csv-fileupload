import os
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
from services.database_service import DatabaseService
from services.email_notifier import EmailNotifier
from services.logger import Logger
import shutil
from processors.file_processor_factory import FileProcessorFactory

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

        # Track processed files to prevent duplicate processing
        self.processed_files = set()
        self.lock = threading.Lock()

    def start(self, interval=None):
        """Start monitoring service with optional interval"""
        if interval is not None:
            self.interval = interval
        """Start monitoring service using only event-based system"""
        self.running = True
        self.logger.info("Starting folder monitoring service (event-based only)")
        
        event_handler = FileHandler(self)
        
        # Setup observers for each input path
        for paths in self.path_settings.values():
            input_dir = Path(paths['input'])
            input_dir.mkdir(parents=True, exist_ok=True)
            self.observer.schedule(event_handler, str(input_dir), recursive=False)
        
        self.observer.start()

    def stop(self):
        """Stop monitoring service"""
        self.running = False
        if hasattr(self, 'observer') and self.observer:
            self.observer.stop()
            self.observer.join()
        self.logger.info("Folder monitoring service stopped")

    def process_file(self, file_path):
        """Process a single file with thread-safe checks"""
        with self.lock:
            # Skip if already processed or processing
            if str(file_path) in self.processed_files:
                return False
            
            self.processed_files.add(str(file_path))
        
        try:
            # Determine which path group this file belongs to
            for path_name, paths in self.path_settings.items():
                input_path = Path(paths['input'])
                if str(file_path).startswith(str(input_path)):
                    move_dir = Path(paths['move'])
                    break
            else:
                self.logger.warning(f"File {file_path} not in any monitored directory")
                return False

            processor = self.processor_factory.get_processor(file_path.name)
            
            # Process the file (upload to DB and handle duplicates)
            success = processor.process(file_path, move_dir)
            
            if not success:
                return False
                
            # Prepare destination path
            dest_path = self._get_unique_filename(file_path, move_dir)
            
            # Handle file transfer with fallback strategies
            transfer_success = self._transfer_file_safely(file_path, dest_path)
            
            return transfer_success
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            return False
        finally:
            with self.lock:
                if str(file_path) in self.processed_files:
                    self.processed_files.remove(str(file_path))

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

    def _transfer_file_safely(self, src, dst):
        """Safe file transfer with verification"""
        try:
            # Create destination directory if not exists
            dst.parent.mkdir(parents=True, exist_ok=True)
            
            # First try atomic rename (fastest if possible)
            try:
                src.rename(dst)
                return True
            except OSError:
                pass
            
            # Fallback to copy + delete
            shutil.copy2(str(src), str(dst))
            
            # Verify copy
            if not (dst.exists() and dst.stat().st_size == src.stat().st_size):
                raise IOError("Copy verification failed")
            
            # Try to remove source
            try:
                src.unlink()
            except Exception as e:
                self.logger.warning(f"Could not delete source file {src}: {str(e)}")
                # Mark for manual cleanup
                processed_path = src.with_suffix(src.suffix + '.processed')
                try:
                    src.rename(processed_path)
                except Exception:
                    pass
            
            return True
        
        except Exception as e:
            self.logger.error(f"File transfer failed from {src} to {dst}: {str(e)}")
            # Clean up failed copy if exists
            if dst.exists():
                try:
                    dst.unlink()
                except Exception:
                    pass
            return False


class FileHandler(FileSystemEventHandler):
    """Custom event handler for file system events"""
    def __init__(self, folder_monitor):
        super().__init__()
        self.folder_monitor = folder_monitor
    
    def on_created(self, event):
        """Handle file creation events"""
        if not event.is_directory and event.src_path.lower().endswith('.csv'):
            file_path = Path(event.src_path)
            
            # Wait briefly to ensure file is fully written
            time.sleep(0.5)
            
            # Process in a new thread to avoid blocking
            threading.Thread(
                target=self.folder_monitor.process_file,
                args=(file_path,),
                daemon=True
            ).start()
    
    def on_moved(self, event):
        """Handle file move events (some systems report copy as move)"""
        if not event.is_directory and event.dest_path.lower().endswith('.csv'):
            self.on_created(event)