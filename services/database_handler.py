import mysql.connector
from mysql.connector import Error
import csv
from pathlib import Path
from services.logger import Logger
import os

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

    def create_table_from_csv(self, table_name, headers=None):
        """ایجاد جدول جدید بر اساس هدرها یا ساختار پیش‌فرض"""
        try:
            cursor = self.connection.cursor()
            # تعیین هدرهای پیش‌فرض بر اساس نام جدول
            if headers[0] != 'order_date' or headers[0] != 'A':
                table_type = self._detect_table_type(table_name)
                headers = self._get_default_headers(table_type)
                self.logger.info(f"Using default headers for table '{table_name}': {headers}")
            
            columns = []
            for header in headers:
                clean_header = header.replace(' ', '_').replace('-', '_')
                # اگر فیلد order است، UNIQUE می‌کنیم
                if header.lower() == 'order':
                    columns.append(f"`{clean_header}` VARCHAR(255) UNIQUE")
                elif header.lower() == 'sealed_unit_id':
                    columns.append(f"`{clean_header}` VARCHAR(255) UNIQUE")
                elif header.lower() == 'f':
                    columns.append(f"`{clean_header}` VARCHAR(255) UNIQUE")
                elif header.lower() == 'j':
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


    def _detect_table_type(self, table_name):
        """تشخیص نوع جدول بر اساس نام آن"""
        table_name = table_name.lower()
        if 'glass' in table_name:
            return 'glass'
        elif 'glassreport' in table_name:
            return 'glass'
        elif 'frame' in table_name:
            return 'frame'
        elif 'framescutting' in table_name:
            return 'frame'
        elif 'rush' in table_name:
            return 'rush'
        else:
            return 'default'

    def _get_default_headers(self, table_type):
        """دریافت هدرهای پیش‌فرض بر اساس نوع جدول"""
        default_headers = {
            'glass': [
                'order_date', 'list_date', 'sealed_unit_id', 'ot', 'window_type', 'line1',
                'line2', 'line3', 'grills', 'spacer', 'dealer', 'glass_comment','tag', 'zones','u_value',
                'solar_heat_gain','visual_trasmittance','energy_rating','glass_type','order','width',
                'height', 'qty','description','note1','note2','rack_id','complete', 'shipping'
                ],

            'frame': [
                "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q",
                "R","S","T","U","V","W","X","Y","Z"
                ],
            #'rush': ['order', 'priority', 'due_date', 'description'],
            #'default': ['order', 'field1', 'field2', 'field3', 'quantity']
        }
        return default_headers.get(table_type)


    def upload_csv_data(self, table_name, csv_file_path, email_notifier=None):
        if not self.connect():
            return False
        
        try:
            # 1. ابتدا بررسی می‌کنیم آیا فایل هدر دارد یا خیر
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline().strip().split(",")[0]
                print("first_line",first_line)
                try:
                    # بررسی آیا خط اول می‌تواند هدر باشد (حاوی حروف است)
                    has_header = any(c.isalpha() for c in first_line)
                    if not has_header:
                        print("zzzzzzz")
                        # تشخیص نوع جدول برای دریافت هدرهای پیش‌فرض
                        table_type = self._detect_table_type(table_name)
                        headers = self._get_default_headers(table_type)
                        
                        if not headers:
                            self.logger.error("No default headers defined for this table type")
                            return False
                        
                        # خواندن تمام محتوای فایل
                        csvfile.seek(0)
                        content = csvfile.readlines()
                        
                        # اضافه کردن هدرها به ابتدای فایل و ذخیره موقت

                        import tempfile
                        temp_dir = tempfile.gettempdir()
                        temp_path = os.path.join(temp_dir, os.path.basename(csv_file_path) + ".tmp")

                        try:

                            with open(temp_path, 'w', newline='', encoding='utf-8') as temp_file:
                                # نوشتن هدرها
                                temp_file.write(','.join(headers) + '\n')
                                # نوشتن محتوای اصلی
                                temp_file.writelines(content)
                                # کپی فایل موقت به مسیر اصلی با دسترسی مناسب
                                import shutil
                                shutil.copy(temp_path, csv_file_path)
                                self.logger.warning(f"Added headers to CSV file: {headers}")
                        
                        except PermissionError as e:
                            self.logger.error(f"Permission denied when writing to {temp_path}: {str(e)}")
                            return False
                    #     # جایگزینی فایل اصلی با فایل موقت
                    #     import os
                    #     os.replace(temp_path, csv_file_path)
                        
                    #     self.logger.warning(f"Added headers to CSV file: {headers}")
                    # else:
                    #     print("has_header",has_header)
                except Exception as e:
                    self.logger.error(f"Error checking headers: {str(e)}")
                    return False
            
            # 2. حالا فایل را با هدرهای جدید پردازش می‌کنیم
            cursor = self.connection.cursor()
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.DictReader(csvfile)
                headers = [h.strip().replace(' ', '_') for h in csvreader.fieldnames]
                
                self.logger.info(f"Processing CSV with columns: {headers}")

                # بررسی/ایجاد جدول
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    if not self.create_table_from_csv(table_name, headers):
                        return False
                

                 # تعیین فیلد تاریخ بر اساس نوع جدول
                date_field = self._get_date_field(table_name, headers)
                # پردازش سطرها
                new_rows = 0
                duplicate_rows = 0
                duplicates = []  # لیست برای ذخیره موارد تکراری
                
                for row_num, row in enumerate(csvreader, 2):  # شماره‌گذاری از سطر 2 شروع می‌شود
                    try:
                        # پر کردن مقادیر خالی برای کلیدهای وجود نداشته
                        complete_row = {h: row.get(h, '') for h in headers}
                        
                        # بررسی تکراری بودن
                        duplicate_key = None
                        
                        # تعیین فیلد کلید بر اساس نوع جدول
                        if 'sealed_unit_id' in headers:
                            duplicate_key = ('sealed_unit_id', complete_row['sealed_unit_id'])
                        elif 'order' in headers:
                            duplicate_key = ('order', complete_row['order'])
                        elif 'F' in headers:
                            duplicate_key = ('F', complete_row['F'])
                        elif 'J' in headers:
                            duplicate_key = ('J', complete_row['J'])
                        
                        # اگر رکورد تکراری وجود دارد
                        if duplicate_key:
                            key_field, key_value = duplicate_key
                            query = f"""
                            SELECT {date_field} 
                            FROM `{table_name}` 
                            WHERE `{key_field}` = %s 
                            LIMIT 1
                            """
                            cursor.execute(query, (key_value,))
                            result = cursor.fetchone()
                            
                            if result:
                                duplicate_rows += 1
                                original_date = result[0]
                                duplicates.append((key_value, original_date))
                                continue
                        
                        # ساخت و اجرای کوئری INSERT
                        columns = ', '.join([f'`{h}`' for h in headers])
                        placeholders = ', '.join(['%s'] * len(headers))
                        values = [complete_row[h] for h in headers]
                        
                        insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_query, values)
                        new_rows += 1
                        
                    except Exception as e:
                        self.logger.error(f"Error in row {row_num}: {str(e)}")
                        continue
        
            self.connection.commit()
            self.logger.info(f"Successfully inserted {new_rows} rows, {duplicate_rows} duplicates skipped")
            
            # ارسال ایمیل برای موارد تکراری (اگر وجود داشتند)
            if duplicates and email_notifier:
                email_notifier.notify_duplicate(
                    table_name=table_name,
                    duplicates=duplicates,  # لیست تپل‌های (کلید, تاریخ)
                    key_field=duplicate_key[0] if duplicate_key else 'order'
                )
                
            return True
                
        except Exception as e:
            self.logger.error(f"Upload failed: {str(e)}")
            self.connection.rollback()
            return False
        finally:
            self.disconnect()

    def _get_date_field(self, table_name, headers):
        """تعیین فیلد تاریخ بر اساس نوع جدول"""
        table_type = self._detect_table_type(table_name)
        if table_type == 'frame':
            return 'Date'
        else:
            return 'list_date' 
        