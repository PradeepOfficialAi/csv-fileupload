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
        """Process file and move it after successful upload"""
        try:
            table_name = self._extract_table_name(file_path.name)
            self.logger.info(f"Processing {file_path} for table {table_name}")
            
            # 1. Upload to database
            success = self.db_handler.upload_csv_data(
                table_name=table_name,
                csv_file_path=str(file_path),
                email_notifier=self.email_notifier
            )
            
            if not success:
                return False

            # 2. Prepare destination path
            dest_path = move_dir / file_path.name
            counter = 1
            while dest_path.exists():
                new_name = f"{file_path.stem}_{counter}{file_path.suffix}"
                dest_path = move_dir / new_name
                counter += 1

            # 3. File transfer strategies with fallbacks
            try:
                import shutil
                from pathlib import Path
                
                # First attempt: direct rename (same filesystem)
                try:
                    file_path.rename(dest_path)
                    self.logger.info(f"Direct rename successful to {dest_path}")
                    return True
                except OSError as e:
                    if e.errno != 18:  # Not cross-device error
                        raise
                    
                    # Second attempt: copy + delete with different permissions
                    try:
                        # Copy file
                        shutil.copy2(str(file_path), str(dest_path))
                        
                        # Attempt deletion with different approaches
                        try:
                            file_path.unlink()
                        except PermissionError:
                            # Try changing permissions temporarily
                            try:
                                file_path.chmod(0o777)
                                file_path.unlink()
                            except:
                                # Use sudo if configured
                                if self._try_sudo_delete(file_path):
                                    self.logger.warning(f"File deleted using sudo")
                                else:
                                    raise
                        
                        self.logger.info(f"Successful copy and delete to {dest_path}")
                        return True
                        
                    except Exception as copy_error:
                        self.logger.error(f"Copy/delete error: {copy_error}")
                        # Third approach: rename source file as processed marker
                        processed_mark = file_path.with_name(f"{file_path.name}.processed")
                        try:
                            file_path.rename(processed_mark)
                            self.logger.warning(f"Source file renamed to {processed_mark}")
                            return True
                        except:
                            # Fourth approach: create marker file
                            with open(str(file_path) + '.processed', 'w') as f:
                                f.write("processed")
                            return True
                            
            except Exception as final_error:
                self.logger.error(f"All transfer methods failed: {final_error}")
                return False
                
        except Exception as e:
            self.logger.error(f"General processing error: {str(e)}")
            return False

    def _try_sudo_delete(self, file_path):
        """Attempt file deletion using sudo"""
        try:
            import subprocess
            subprocess.run(['sudo', 'rm', '-f', str(file_path)], check=True)
            return True
        except:
            return False

    

    def _extract_table_name(self, filename):
        """استخراج نام جدول از نام فایل"""
        # حذف پسوند
        filename = os.path.splitext(filename)[0]
        
        # جدا کردن بخش اول قبل از _
        table_part = filename.split('_')[0]
        
        # تبدیل به حروف کوچک و حذف کاراکترهای غیرمجاز
        return ''.join(c for c in table_part.lower() if c.isalnum())