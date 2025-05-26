from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import os

class GLASSREPORTProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "glassreport"

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
            self.logger.info(f"Processing GLASSREPORT file: {file_path}")
            
            if not self.connect():
                return False

            success = self.upload_csv_data(self.get_table_name(), str(file_path), self.email_notifier)
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing GLASSREPORT file: {str(e)}")
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
                'order_date', 'list_date', 'sealed_unit_id', 'ot', 'window_type', 'line1',
                'line2', 'line3', 'grills', 'spacer', 'dealer', 'glass_comment', 'tag', 'zones', 'u_value',
                'solar_heat_gain', 'visual_trasmittance', 'energy_rating', 'glass_type', 'order', 'width',
                'height', 'qty', 'description', 'note1', 'note2', 'rack_id', 'complete', 'shipping'
            ]
            
            # 2. Process CSV file - add headers if missing
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline().strip()
                has_header = any(c.isalpha() for c in first_line)
                
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
                resend_orders = []
                key_field = None
                date_field = 'list_date'

                for row in csvreader:
                    try:
                        complete_row = {h: row.get(h, '') for h in actual_headers}
                        order_id = complete_row.get('order', '')
                        sealed_unit_id = complete_row.get('sealed_unit_id', '')
                        is_duplicate = False

                        # Check for duplicates (order and sealed_unit_id match)
                        if order_id and sealed_unit_id:
                            query = """
                            SELECT list_date 
                            FROM `glassreport` 
                            WHERE `order` = %s AND `sealed_unit_id` = %s
                            LIMIT 1
                            """
                            cursor.execute(query, (order_id, sealed_unit_id))
                            result = cursor.fetchone()
                            
                            if result:
                                duplicate_rows += 1
                                original_date = result[0]
                                duplicates.append({
                                    'order': order_id,
                                    'sealed_unit_id': sealed_unit_id,
                                    'original_date': original_date,
                                    'type': 'DUPLICATE'
                                })
                                key_field = 'sealed_unit_id'
                                is_duplicate = True

                        # Check for resends (order match only, but not a duplicate)
                        if order_id and not is_duplicate:
                            query = """
                            SELECT list_date 
                            FROM `glassreport` 
                            WHERE `order` = %s
                            LIMIT 1
                            """
                            cursor.execute(query, (order_id,))
                            result = cursor.fetchone()
                            
                            if result:
                                duplicate_rows += 1
                                original_date = result[0]
                                resend_orders.append({
                                    'order': order_id,
                                    'original_date': original_date,
                                    'type': 'RE-SEND'
                                })
                                key_field = 'order'

                        # Insert the record (always insert, regardless of duplicate/resend)
                        columns = ', '.join([f'`{h}`' for h in actual_headers])
                        placeholders = ', '.join(['%s'] * len(actual_headers))
                        values = [complete_row[h] for h in actual_headers]
                        
                        insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                        cursor.execute(insert_query, values)
                        new_rows += 1

                    except Exception as e:
                        self.logger.error(f"Row processing error: {e}")
                        continue
                
                self.connection.commit()
                self.logger.info(f"Inserted {new_rows} rows, identified {duplicate_rows} duplicates/resends")
                
                # 5. Send email notifications
                if email_notifier:
                    if duplicates:
                        email_notifier.notify_duplicate(
                            table_name=table_name,
                            duplicates=duplicates,
                            key_field=key_field
                        )
                    if resend_orders:
                        email_notifier.notify_resend(
                            table_name=table_name,
                            resends=resend_orders,
                            key_field='order'
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

    def _table_exists(self, cursor, table_name):
        """Check if table exists in database"""
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None