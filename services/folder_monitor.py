import os
import time
import threading
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from services.database_handler import DatabaseHandler
from services.email_notifier import EmailNotifier
from services.logger import Logger
from pathlib import Path
import csv  # برای کار با فایل‌های CSV
import traceback2 as traceback

class FolderMonitor:
    def __init__(self, config_manager):
        self.logger = Logger("FolderMonitor")
        self.config_manager = config_manager
        self.running = False
        self.observer = Observer()  # مقداردهی اولیه observer
        
        # دریافت مسیرها به صورت صحیح
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

        # اعتبارسنجی مسیرها
        for name, path in self.path_settings.items():
            if not path:
                raise ValueError(f"Path {name} is not configured")
        # اتصال به دیتابیس
        self.db_handler = DatabaseHandler(
            host=config_manager.get_setting('mysql',"mysql_server"),
            database=config_manager.get_setting('mysql',"mysql_db"),
            user=config_manager.get_setting('mysql',"mysql_user"),
            password=config_manager.get_setting('mysql',"mysql_pass"),
            port=config_manager.get_setting('mysql',"mysql_port", 3306)
        )
        
        # سرویس ایمیل
        self.email_notifier = EmailNotifier(
            smtp_server=os.getenv("SMTP_SERVER"),
            smtp_port=int(os.getenv("SMTP_PORT", 587)),
            sender_email=os.getenv("SENDER_EMAIL"),
            sender_password=os.getenv("SENDER_PASSWORD")
        )

    def start(self, interval=30):
        """شروع مانیتورینگ با بازه زمانی مشخص"""
        self.running = True
        self.logger.info("Starting folder monitoring service")
        
        event_handler = FileSystemEventHandler()
        event_handler.on_created = self._on_file_created
        
        # تنظیم observer برای هر پوشه ورودی
        for path_name, paths in self.path_settings.items():
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

    def _on_file_created(self, event):
        """هنگامی که فایل جدیدی ایجاد می‌شود"""
        if not event.is_directory:
            file_path = Path(event.src_path).resolve()
            file_ext = file_path.suffix.lower()
            
            if file_ext == '.csv':
                for path_name, paths in self.path_settings.items():
                    input_path = Path(paths['input']).resolve()
                    try:
                        if file_path.is_relative_to(input_path):
                            move_path = Path(paths['move'])
                            self._process_file(file_path, move_path, path_name)
                            break
                    except AttributeError:  # برای پایتون < 3.9
                        if str(file_path).startswith(str(input_path)):
                            move_path = Path(paths['move'])
                            self._process_file(file_path, move_path, path_name)
                            break


    def stop(self):
        """توقف سرویس مانیتورینگ"""
        self.running = False
        if hasattr(self, 'observer') and self.observer:
            self.observer.stop()
            self.observer.join()
        self.logger.info("Folder monitoring service stopped")

    def _monitor_folders(self, interval):
        """مانیتورینگ دوره‌ای پوشه‌ها"""
        while self.running:
            try:
                self.logger.info("Checking folders for new files...")
                
                for path_name, paths in self.path_settings.items():
                    input_dir = Path(paths['input'])
                    move_dir = Path(paths['move'])
                    
                    input_dir.mkdir(parents=True, exist_ok=True)
                    move_dir.mkdir(parents=True, exist_ok=True)
                    
                    csv_files = list(input_dir.glob('*.[cC][sS][vV]'))
                    self.logger.info(f"Found {len(csv_files)} CSV files in {input_dir}")
                    for file_path in csv_files:
                        self._process_file(file_path, move_dir, path_name)
                
                time.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"Error in folder monitoring: {str(e)}")
                time.sleep(60)

    def _process_file(self, file_path, move_dir, path_name):
        """Process file with robust CIFS share handling"""
        try:
            # Database upload logic
            table_name = self._extract_table_name(file_path.name)
            self.logger.info(f"Processing {file_path} for table {table_name}")
            
            if not self.db_handler.upload_csv_data(table_name, str(file_path), self.email_notifier):
                return False

            # Prepare destination with unique filename
            dest_path = self._get_unique_destination(file_path, move_dir)
            
            # File transfer handling
            success, needs_cleanup = self._transfer_file_cifs(file_path, dest_path)
            
            if needs_cleanup:
                self._register_for_cleanup(file_path)
                
            return success
            
        except Exception as e:
            self.logger.error(f"Processing failed: {str(e)}")
            return False

    def _transfer_file_cifs(self, src, dst):
        """Specialized file transfer for CIFS shares"""
        try:
            import shutil
            import os
            
            # 1. Attempt copy
            shutil.copy2(str(src), str(dst))
            
            # 2. Verify copy
            if not self._verify_copy(src, dst):
                raise IOError("Copy verification failed")
            
            # 3. Attempt source removal
            try:
                os.unlink(str(src))
                return True, False
            except PermissionError:
                # 4. If delete fails, try renaming
                try:
                    processed_path = src.with_name(f"{src.name}.processed")
                    os.rename(str(src), str(processed_path))
                    return True, False
                except:
                    self.logger.warning(f"Copied to {dst} but couldn't modify source")
                    return True, True
        
        except Exception as e:
            self.logger.error(f"Transfer failed: {str(e)}")
            return False, False

    def _verify_copy(self, src, dst):
        """Verify copied file integrity"""
        import filecmp
        return (dst.exists() and 
                dst.stat().st_size == src.stat().st_size and
                filecmp.cmp(src, dst, shallow=False))

    def _get_unique_destination(self, src_path, dest_dir):
        """Generate unique destination filename"""
        counter = 1
        dest_path = dest_dir / src_path.name
        while dest_path.exists():
            dest_path = dest_dir / f"{src_path.stem}_{counter}{src_path.suffix}"
            counter += 1
        return dest_path

    def _register_for_cleanup(self, file_path):
        """Register files needing manual cleanup"""
        cleanup_file = Path("/var/lib/folder_monitor/cleanup_list.txt")
        cleanup_file.parent.mkdir(exist_ok=True)
        
        with cleanup_file.open('a') as f:
            f.write(f"{file_path}\n")
        self.logger.info(f"Registered for cleanup: {file_path}")

    

    def _extract_table_name(self, filename):
        """استخراج نام جدول از نام فایل"""
        # حذف پسوند
        filename = os.path.splitext(filename)[0]
        
        # جدا کردن بخش اول قبل از _
        table_part = filename.split('_')[0]
        
        # تبدیل به حروف کوچک و حذف کاراکترهای غیرمجاز
        return ''.join(c for c in table_part.lower() if c.isalnum())