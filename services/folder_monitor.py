import os
import time
import threading
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

    def start(self, interval=30):
        """Start monitoring service with periodic polling"""
        self.running = True
        self.logger.info(f"Starting folder monitoring service (polling every {interval} seconds)")
        
        # Process existing .csv files at startup
        self.logger.info("Processing existing files at startup")
        self._process_existing_files()
        
        # Start polling in a separate thread
        polling_thread = threading.Thread(
            target=self._poll_directories,
            args=(interval,),
            daemon=True
        )
        polling_thread.start()

    def stop(self):
        """Stop monitoring service"""
        self.running = False
        self.logger.info("Folder monitoring service stopped")

    def _poll_directories(self, interval):
        """Periodically scan directories for new .csv files"""
        while self.running:
            self.logger.info("Polling directories for new .csv files")
            self._process_existing_files()
            time.sleep(interval)

    def _process_existing_files(self):
        """Process all unprocessed .csv files in monitored directories"""
        self.logger.info("Scanning for unprocessed .csv files in monitored directories")
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
                        # Check if file is still being written (optional)
                        if self._is_file_locked(file_path):
                            self.logger.info(f"File {file_path} is locked, skipping for now")
                            continue
                        self.logger.info(f"Processing file: {file_path}")
                        # Process synchronously to avoid race conditions
                        self.process_file(file_path)
                    except Exception as e:
                        self.logger.error(f"Error accessing file {file_path}: {str(e)}")
                        continue
            
            if not files_found:
                self.logger.info(f"No .csv files found in directory {input_dir}")

    def _is_file_locked(self, file_path):
        """Check if file is still being written (e.g., locked by another process)"""
        try:
            # Attempt to open the file in exclusive mode
            with open(file_path, 'a') as f:
                return False
        except IOError:
            return True

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
            
            # Process the file (upload to DB)
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