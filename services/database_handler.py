import mysql.connector
from mysql.connector import Error
import csv
from pathlib import Path
from services.logger import Logger

class DatabaseHandler:
    def __init__(self, host, database, user, password, port=3306):
        self.logger = Logger("DatabaseHandler")
        self.config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.connection = None

    def connect(self):
        try:
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                self.logger.info(f"Connected to MySQL database '{self.config['database']}'")
                return True
        except Error as e:
            self.logger.error(f"Error connecting to MySQL: {str(e)}")
            return False

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

    def create_table_from_csv(self, table_name, headers):
        """ایجاد جدول جدید بر اساس هدرها"""
        try:
            cursor = self.connection.cursor()
            
            columns = []
            for header in headers:
                clean_header = header.replace(' ', '_').replace('-', '_')
                # اگر فیلد order است، UNIQUE می‌کنیم
                if header.lower() == 'order':
                    columns.append(f"`{clean_header}` VARCHAR(255) UNIQUE")
                else:
                    columns.append(f"`{clean_header}` TEXT")
            
            create_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(columns)}
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            cursor.execute(create_query)
            self.connection.commit()
            self.logger.info(f"Created table '{table_name}' with columns: {headers}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table: {str(e)}")
            return False

    def upload_csv_data(self, table_name, csv_file_path, email_notifier=None):
        if not self.connect():
            return False
        
        try:
            cursor = self.connection.cursor()
            
            # خواندن فایل CSV و بررسی ساختار
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                try:
                    # ابتدا سعی می‌کنیم فایل را با هدر بخوانیم
                    csvreader = csv.DictReader(csvfile)
                    
                    # اگر فایل هدر نداشت، این خطا رخ می‌دهد
                    if not csvreader.fieldnames:
                        raise csv.Error("No headers detected")
                        
                    headers = [h.strip().replace(' ', '_') for h in csvreader.fieldnames]
                    
                except csv.Error:
                    # اگر فایل هدر نداشت، به صورت دستی هدر می‌سازیم
                    csvfile.seek(0)  # بازگشت به ابتدای فایل
                    first_row = csvfile.readline().strip()
                    num_columns = len(first_row.split(','))
                    
                    # ساخت هدرهای پیش‌فرض (column_1, column_2, ...)
                    headers = [""]
                    self.logger.warning(f"No headers found. Using auto-generated headers: {headers}")
                    
                    # بازگشت به ابتدای فایل برای خواندن دوباره
                    csvfile.seek(0)
                    csvreader = csv.DictReader(csvfile, fieldnames=headers)
                
                self.logger.info(f"Processing CSV with columns: {headers}")



                # بررسی/ایجاد جدول
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    if not self.create_table_from_csv(table_name, headers):
                        return False
                
                # پردازش سطرها
                new_rows = 0
                duplicate_rows = 0
                
                for row_num, row in enumerate(csvreader, 2):  # شماره‌گذاری از سطر 2 شروع می‌شود
                    try:
                        # پر کردن مقادیر خالی برای کلیدهای وجود نداشته
                        complete_row = {h: row.get(h, '') for h in headers}
                        
                        # بررسی تکراری بودن بر اساس فیلد order اگر وجود دارد
                        if 'order' in headers:
                            cursor.execute(f"SELECT 1 FROM `{table_name}` WHERE `order` = %s LIMIT 1", 
                                        (complete_row['order'],))
                            if cursor.fetchone():
                                duplicate_rows += 1
                                if email_notifier:
                                    order_value = complete_row.get('order', 'N/A')
                                    email_notifier.notify_duplicate(table_name, order_value, complete_row)
                                continue
                        
                        # ساخت و اجرای کوئری INSERT
                        columns = ', '.join([f'`{h}`' for h in headers])
                        placeholders = ', '.join(['%s'] * len(headers))
                        values = [complete_row[h] for h in headers]
                        
                        insert_query = f"""
                        INSERT INTO `{table_name}` ({columns})
                        VALUES ({placeholders})
                        """
                        
                        cursor.execute(insert_query, values)
                        new_rows += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error in row {row_num}: {str(e)}")
                        continue
            
            self.connection.commit()
            self.logger.info(f"Successfully inserted {new_rows} rows, {duplicate_rows} duplicates skipped")
            return True
            
        except Exception as e:
            self.logger.error(f"Upload failed: {str(e)}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()