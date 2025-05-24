from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import os

class FRAMESCUTTINGProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "framescutting"

    def connect(self):
        """Establish database connection"""
        try:
            self.config = {
                'host': self.config_manager.get_setting('mysql', 'mysql_server'),
                'database': self.config_manager.get_setting('mysql', 'mysql_db'),
                'user': self.config_manager.get_setting('mysql', 'mysql_user'),
                'password': self.config_manager.get_setting('mysql', 'mysql_pass'),
                'port': self.config_manager.get_setting('mysql', 'mysql_port'),
            }
            self.connection = mysql.connector.connect(**self.config)
            if self.connection.is_connected():
                self.logger.info(f"Connected to MySQL database '{self.config['database']}'")
                return True
        except Error as e:
            self.logger.error(f"Error connecting to MySQL: {str(e)}")
            return False

    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

    def process(self, file_path: Path, move_dir: Path) -> bool:
        try:
            self.logger.info(f"Processing FRAMESCUTTING file: {file_path}")
            
            if not self.connect():
                return False

            success = self.upload_csv_data(self.get_table_name(), str(file_path), self.email_notifier)
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing FRAMESCUTTING file: {str(e)}")
            return False
        finally:
            self.disconnect()

    def upload_csv_data(self, table_name, csv_file_path, email_notifier=None):
        """Upload CSV data with robust delimiter detection and proper email notification"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        cursor = None
        try:
            # 1. Define expected headers
            headers = [
                "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q",
                "R","S","T","U","V","W","X","Y","Z"
            ]
            
            # 2. Process CSV file - add headers if missing
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline().strip()
                has_header = False
                print("has_header",has_header)
                if not has_header:
                    # Create temp file with headers
                    import tempfile
                    temp_dir = tempfile.gettempdir()
                    temp_path = os.path.join(temp_dir, os.path.basename(csv_file_path) + ".tmp")
                    
                    try:
                        with open(temp_path, 'w', newline='', encoding='utf-8') as temp_file:
                            temp_file.write(','.join(headers) + '\n')
                            temp_file.write(first_line + '\n')  # Write the first line we read
                            temp_file.writelines(csvfile.readlines())  # Write the rest
                        
                        # Replace original file with temp file
                        import shutil
                        shutil.move(temp_path, csv_file_path)
                        self.logger.warning(f"Added headers to CSV file: {headers}")
                    except Exception as e:
                        self.logger.error(f"Error adding headers: {e}")
                        return False
            
            # 3. Now process the file with headers
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.DictReader(csvfile)
                actual_headers = [h.strip().replace(' ', '_') for h in csvreader.fieldnames]
                
                self.logger.info(f"Processing CSV with columns: {actual_headers}")

                # Check/create table
                cursor = self.connection.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    if not self._create_table(table_name, actual_headers):
                        return False

                # 4. Process rows
                new_rows = 0
                duplicate_rows = 0
                duplicates = []
                key_field = 'F'
                date_field = 'U'

                for row in csvreader:
                    try:
                        complete_row = {h: row.get(h, '') for h in actual_headers}
                        duplicate_key = None
                        
                        # Determine duplicate key
                        if 'F' in actual_headers and complete_row['F']:
                            duplicate_key = ('F', complete_row['F'])
                        elif 'J' in actual_headers and complete_row['J']:
                            duplicate_key = ('J', complete_row['J'])
                        
                        # Check for duplicates
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
                        
                        # Insert new record
                        columns = ', '.join([f'`{h}`' for h in actual_headers])
                        placeholders = ', '.join(['%s'] * len(actual_headers))
                        values = [complete_row[h] for h in actual_headers]
                        
                        insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_query, values)
                        new_rows += 1

                    except Exception as e:
                        #self.logger.error(f"Row processing error: {e}")
                        continue
                
                self.connection.commit()
                self.logger.info(f"Inserted {new_rows} rows, skipped {duplicate_rows} duplicates")
                
                # 5. Send email notification for duplicates
                if duplicates and email_notifier:
                    email_notifier.notify_duplicate(
                        table_name=table_name,
                        duplicates=duplicates,
                        key_field=key_field
                    )
                    
                return True
                
        except Exception as e:
            self.logger.error(f"Upload failed: {e}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def _create_table(self, table_name, headers):
        """Create table with appropriate structure"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            columns = []
            for header in headers:
                clean_header = header.replace(' ', '_')
                
                if header.lower() in ['order', 'sealed_unit_id', 'f', 'j']:
                    col_def = f"`{clean_header}` VARCHAR(255) UNIQUE"
                elif header.lower() in ['width', 'height', 'qty']:
                    col_def = f"`{clean_header}` DECIMAL(10,2)"
                elif any(x in header.lower() for x in ['date', 'time']):
                    col_def = f"`{clean_header}` DATE"
                else:
                    col_def = f"`{clean_header}` TEXT"
                
                columns.append(col_def)
            
            query = f"""
            CREATE TABLE `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                {','.join(columns)},
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            cursor.execute(query)
            self.connection.commit()
            self.logger.info(f"Created table '{table_name}' with {len(headers)} columns")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table: {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()

    def _table_exists(self, cursor, table_name):
        """Check if table exists in database"""
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None