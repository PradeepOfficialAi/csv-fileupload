from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import os
import shutil

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
            
            if success:
                # Move file to move_dir after successful processing
                try:
                    destination = move_dir / file_path.name
                    shutil.move(str(file_path), str(destination))
                    self.logger.info(f"Moved file to {destination}")
                except Exception as e:
                    self.logger.error(f"Failed to move file {file_path} to {move_dir}: {str(e)}")
                    return False
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing GLASSREPORT file {file_path}: {str(e)}")
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
            
            # 2. Check if CSV file already has the expected headers
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline().strip()
                first_line_headers = [h.strip() for h in first_line.split(',')]
                has_expected_headers = first_line_headers == headers
                
                if not has_expected_headers:
                    # Create temp file with headers
                    import tempfile
                    temp_dir = tempfile.gettempdir()
                    temp_path = os.path.join(temp_dir, os.path.basename(csv_file_path) + ".tmp")
                    
                    try:
                        with open(temp_path, 'w', newline='', encoding='utf-8') as temp_file:
                            temp_file.write(','.join(headers) + '\n')
                            temp_file.write(first_line + '\n')  # Write the first line as data
                            temp_file.writelines(csvfile.readlines())  # Write the rest
                        
                        # Replace original file with temp file
                        shutil.move(temp_path, csv_file_path)
                        self.logger.warning(f"Added headers to CSV file: {headers}")
                    except Exception as e:
                        self.logger.error(f"Error adding headers to {csv_file_path}: {str(e)}")
                        return False
            
            # 3. Read all rows and perform duplicate/resend checks
            rows_to_insert = []
            new_rows = 0
            duplicate_rows = 0
            duplicates = []
            resend_orders = []
            key_field = None
            date_field = 'list_date'

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

                # Collect all rows and check for duplicates/resends
                for row in csvreader:
                    try:
                        complete_row = {h: row.get(h, '') for h in actual_headers}
                        # Trim spaces for all columns
                        for header in actual_headers:
                            value = complete_row[header]
                            if value is not None:
                                # If the value is all whitespace, set to empty string
                                if value.strip() == '':
                                    complete_row[header] = ''
                                # Otherwise, trim leading and trailing spaces
                                elif value != value.strip():
                                    complete_row[header] = value.strip()

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
                                original_date = result[0] if result[0] else 'Unknown'
                                duplicates.append({
                                    'order': order_id,
                                    'sealed_unit_id': sealed_unit_id,
                                    'original_date': original_date,
                                    'type': 'DUPLICATE'
                                })
                                key_field = 'sealed_unit_id'
                                is_duplicate = True
                                self.logger.debug(f"Duplicate found: order={order_id}, sealed_unit_id={sealed_unit_id}, original_date={original_date}")

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
                                original_date = result[0] if result[0] else 'Unknown'
                                resend_orders.append({
                                    'order': order_id,
                                    'original_date': original_date,
                                    'type': 'RE-SEND'
                                })
                                key_field = 'order'
                                self.logger.debug(f"Resend found: order={order_id}, original_date={original_date}")

                        # Store row for later insertion
                        rows_to_insert.append(complete_row)
                        new_rows += 1

                    except Exception as e:
                        self.logger.error(f"Row processing error for row {complete_row}: {str(e)}")
                        continue

            # 4. Insert all rows in a single batch
            if rows_to_insert:
                try:
                    columns = ', '.join([f'`{h}`' for h in actual_headers])
                    placeholders = ', '.join(['%s'] * len(actual_headers))
                    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                    
                    # Batch insert all rows
                    for complete_row in rows_to_insert:
                        values = [complete_row[h] for h in actual_headers]
                        cursor.execute(insert_query, values)
                    
                    self.connection.commit()
                    self.logger.info(f"Inserted {new_rows} rows, identified {duplicate_rows} duplicates/resends")
                
                except Exception as e:
                    self.logger.error(f"Batch insert failed for {csv_file_path}: {str(e)}")
                    self.connection.rollback()
                    return False

            # 5. Send email notifications
            if email_notifier:
                if duplicates:
                    self.logger.info(f"Sending duplicate notification for {len(duplicates)} duplicates")
                    email_notifier.notify_duplicate(
                        table_name=table_name,
                        duplicates=duplicates,
                        key_field=key_field
                    )
                if resend_orders:
                    self.logger.info(f"Sending resend notification for {len(resend_orders)} resends")
                    email_notifier.notify_resend(
                        table_name=table_name,
                        resends=resend_orders,
                        key_field='order'
                    )
                    
            return True
                
        except Exception as e:
            self.logger.error(f"Upload failed for {csv_file_path}: {str(e)}")
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
                
                if header.lower() in ['order', 'sealed_unit_id']:
                    col_def = f"`{clean_header}` VARCHAR(255)"
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
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                INDEX `idx_order` (`order`),
                INDEX `idx_sealed_unit_id` (`sealed_unit_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            cursor.execute(query)
            self.connection.commit()
            self.logger.info(f"Created table '{table_name}' with {len(headers)} columns")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table '{table_name}': {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()

    def _table_exists(self, cursor, table_name):
        """Check if table exists in database"""
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        return cursor.fetchone() is not None