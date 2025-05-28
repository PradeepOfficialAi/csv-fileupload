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

        # Validate paths and resolve to absolute paths
        for name, paths in self.path_settings.items():
            if not paths['input'] or not paths['move']:
                raise ValueError(f"Path configuration incomplete for {name}")
            paths['input'] = str(Path(paths['input']).resolve())
            paths['move'] = str(Path(paths['move']).resolve())

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
        """Start monitoring service and process existing files"""
        self.running = True
        self.logger.info("Starting folder monitoring service (event-based and processing existing files)")
        
        event_handler = FileHandler(self)
        
        # Setup observers for each input path but don't start yet
        for path_name, paths in self.path_settings.items():
            input_dir = Path(paths['input'])
            input_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Monitoring directory: {input_dir}")
            self.observer.schedule(event_handler, str(input_dir), recursive=False)
        
        # Process existing .csv files before starting observer
        self.logger.info("Processing existing files before starting file system monitoring")
        self._process_existing_files()
        
        # Start observer after processing existing files
        self.logger.info("Starting file system observer")
        self.observer.start()

    def stop(self):
        """Stop monitoring service"""
        self.running = False
        if hasattr(self, 'observer') and self.observer:
            self.observer.stop()
            self.observer.join()
        self.logger.info("Folder monitoring service stopped")

    def _process_existing_files(self):
        """Process all existing .csv files in monitored directories"""
        self.logger.info("Scanning for existing .csv files in monitored directories")
        for path_name, paths in self.path_settings.items():
            input_dir = Path(paths['input'])
            if not input_dir.exists():
                self.logger.warning(f"Input directory {input_dir} does not exist")
                continue
            
            # Iterate over .csv files (case-insensitive)
            csv_pattern = '*.[cC][sS][vV]'
            files_found = False
            for file_path in input_dir.glob(csv_pattern):
                files_found = True
                if file_path.is_file():
                    try:
                        # Check file accessibility
                        if not os.access(file_path, os.R_OK):
                            self.logger.warning(f"File {file_path} is not readable, skipping")
                            continue
                        if file_path.stat().st_size == 0:
                            self.logger.warning(f"File {file_path} is empty, skipping")
                            continue
                        self.logger.info(f"Processing existing file: {file_path}")
                        # Process synchronously to avoid overlap with observer
                        self.process_file(file_path)
                    except Exception as e:
                        self.logger.error(f"Error accessing file {file_path}: {str(e)}")
                        continue
            
            if not files_found:
                self.logger.info(f"No .csv files found in directory {input_dir}")

    def process_file(self, file_path):
        """Process a single file with thread-safe checks"""
        with self.lock:
            # Skip if already processed or processing
            if str(file_path) in self.processed_files:
                self.logger.info(f"Skipping already processed file: {file_path}")
                return False
            
            # Check if file still exists
            if not file_path.exists():
                self.logger.warning(f"File {file_path} no longer exists, skipping")
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

            self.logger.info(f"Starting processing for file: {file_path}")
            processor = self.processor_factory.get_processor(file_path.name)
            
            # Process the file (upload to DB and handle duplicates)
            success = processor.process(file_path, move_dir)
            
            if not success:
                self.logger.error(f"Failed to process file {file_path}")
                return False
                
            # Prepare destination path
            dest_path = self._get_unique_filename(file_path, move_dir)
            
            # Handle file transfer with fallback strategies
            transfer_success = self._transfer_file_safely(file_path, dest_path)
            
            if transfer_success:
                self.logger.info(f"Successfully processed and transferred file: {file_path}")
            else:
                self.logger.error(f"Failed to transfer file: {file_path}")
            
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
                self.logger.info(f"Moved file to {dst}")
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
                self.logger.info(f"Moved file to {dst}")
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
            
            self.folder_monitor.logger.info(f"Detected new file creation: {file_path}")
            # Process in a new thread to avoid blocking
            threading.Thread(
                target=self.folder_monitor.process_file,
                args=(file_path,),
                daemon=True
            ).start()
    
    def on_moved(self, event):
        """Handle file move events, but skip if already processed"""
        if not event.is_directory and event.dest_path.lower().endswith('.csv'):
            file_path = Path(event.dest_path)
            with self.folder_monitor.lock:
                if str(file_path) in self.folder_monitor.processed_files:
                    self.folder_monitor.logger.info(f"Skipping moved file {file_path} as it was already processed")
                    return
            
            self.folder_monitor.logger.info(f"Detected moved file: {file_path}")
            # Wait briefly to ensure file is fully written
            time.sleep(0.5)
            
            # Process in a new thread to avoid blocking
            threading.Thread(
                target=self.folder_monitor.process_file,
                args=(file_path,),
                daemon=True
            ).start()