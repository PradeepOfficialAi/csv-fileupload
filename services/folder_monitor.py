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
                'move': config_manager.get_setting('paths', 'move_path1'),
                'is_network': True  # /mnt/share_folder/file_upload_linux is network share
            },
            'input2': {
                'input': config_manager.get_setting('paths', 'path2'),
                'move': config_manager.get_setting('paths', 'move_path2'),
                'is_network': False  # /home/ali/csv/S2 is local
            },
            'pdf': {
                'input': config_manager.get_setting('paths', 'source_pdf'),
                'move': config_manager.get_setting('paths', 'move_pdf'),
                'is_network': False
            }
        }

        # Validate paths
        for name, paths in self.path_settings.items():
            if not paths['input'] or not paths['move']:
                raise ValueError(f"Path configuration incomplete for {name}")
            try:
                input_path = Path(paths['input'])
                move_path = Path(paths['move'])
                input_path.mkdir(parents=True, exist_ok=True)
                move_path.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Validated path {name}: input={input_path}, move={move_path}")
            except Exception as e:
                self.logger.error(f"Failed to access/create path {paths['input']} or {paths['move']}: {str(e)}")
                raise

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
        self.poll_interval = 30  # Seconds

    def start(self, interval=None):
        """Start monitoring service with event-based and polling for network shares"""
        if interval is not None:
            self.poll_interval = interval
        
        self.running = True
        self.logger.info("Starting folder monitoring service (event-based + polling for network shares)")
        
        event_handler = FileHandler(self)
        
        # Setup observers for event-based monitoring
        for name, paths in self.path_settings.items():
            input_dir = Path(paths['input'])
            try:
                input_dir.mkdir(parents=True, exist_ok=True)
                self.observer.schedule(event_handler, str(input_dir), recursive=False)
                self.logger.info(f"Monitoring {name} at {input_dir} (event-based)")
            except Exception as e:
                self.logger.error(f"Failed to monitor {input_dir}: {str(e)}")

        # Start observer
        self.observer.start()

        # Start polling thread for network share (input1)
        if self.path_settings['input1']['is_network']:
            polling_thread = threading.Thread(
                target=self._poll_network_share,
                daemon=True
            )
            polling_thread.start()
            self.logger.info(f"Started polling for network share at {self.path_settings['input1']['input']}")

    def stop(self):
        """Stop monitoring service"""
        self.running = False
        if hasattr(self, 'observer') and self.observer:
            self.observer.stop()
            self.observer.join()
        self.logger.info("Folder monitoring service stopped")

    def _poll_network_share(self):
        """Poll network share folder for new CSV files"""
        network_input = Path(self.path_settings['input1']['input'])
        while self.running:
            try:
                self.logger.debug(f"Polling network share: {network_input}")
                for file_path in network_input.glob("*.csv"):
                    if file_path.is_file():
                        self.logger.debug(f"Found file during polling: {file_path}")
                        self.process_file(file_path)
                time.sleep(self.poll_interval)
            except Exception as e:
                self.logger.error(f"Error polling network share {network_input}: {str(e)}")
                time.sleep(self.poll_interval)

    def process_file(self, file_path):
        """Process a single file with thread-safe checks"""
        file_path = Path(file_path)
        file_key = str(file_path)

        with self.lock:
            if file_key in self.processed_files:
                self.logger.debug(f"Skipping already processed file: {file_path}")
                return False
            
            self.processed_files.add(file_key)
        
        try:
            # Determine path group
            for path_name, paths in self.path_settings.items():
                input_path = Path(paths['input'])
                if str(file_path).startswith(str(input_path)):
                    move_dir = Path(paths['move'])
                    is_network = paths['is_network']
                    break
            else:
                self.logger.warning(f"File {file_path} not in any monitored directory")
                return False

            # Wait longer for network shares
            wait_time = 2.0 if is_network else 0.5
            time.sleep(wait_time)

            # Verify file exists and is accessible
            if not file_path.exists():
                self.logger.error(f"File {file_path} no longer exists")
                return False

            # Check file properties
            try:
                file_size = file_path.stat().st_size
                file_mtime = time.ctime(file_path.stat().st_mtime)
                self.logger.debug(f"File {file_path}: size={file_size} bytes, modified={file_mtime}")
            except Exception as e:
                self.logger.error(f"Cannot access file {file_path}: {str(e)}")
                return False

            processor = self.processor_factory.get_processor(file_path.name)
            if not processor:
                self.logger.warning(f"No processor found for file: {file_path}")
                return False
            
            self.logger.info(f"Processing file: {file_path}")
            success = processor.process(file_path, move_dir)
            
            if not success:
                self.logger.error(f"Processing failed for {file_path}")
                return False
                
            dest_path = self._get_unique_filename(file_path, move_dir)
            transfer_success = self._transfer_file_safely(file_path, dest_path)
            
            return transfer_success
            
        except Exception as e:
            self.logger.error(f"Error processing file {file_path}: {str(e)}")
            return False
        finally:
            with self.lock:
                self.processed_files.discard(file_key)

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
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                src.rename(dst)
                self.logger.info(f"Moved file {src} to {dst}")
                return True
            except OSError:
                pass
            
            shutil.copy2(str(src), str(dst))
            if not (dst.exists() and dst.stat().st_size == src.stat().st_size):
                raise IOError("Copy verification failed")
            
            try:
                src.unlink()
                self.logger.info(f"Moved file {src} to {dst}")
            except Exception as e:
                self.logger.warning(f"Could not delete source file {src}: {str(e)}")
                processed_path = src.with_suffix(src.suffix + '.processed')
                try:
                    src.rename(processed_path)
                    self.logger.info(f"Marked file as processed: {processed_path}")
                except Exception:
                    pass
            
            return True
        
        except Exception as e:
            self.logger.error(f"File transfer failed from {src} to {dst}: {str(e)}")
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
            self.folder_monitor.logger.debug(f"Detected new file: {file_path}")
            time.sleep(0.5)
            threading.Thread(
                target=self.folder_monitor.process_file,
                args=(file_path,),
                daemon=True
            ).start()
    
    def on_moved(self, event):
        """Handle file move events"""
        if not event.is_directory and event.dest_path.lower().endswith('.csv'):
            self.on_created(event)