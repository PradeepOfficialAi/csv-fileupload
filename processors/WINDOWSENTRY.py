from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import shutil
import time

class WINDOWSENTRYProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "windowsentry"

    def connect(self):
        """Establish database connection with retry"""
        max_retries = 3
        retry_delay = 2  # seconds
        for attempt in range(max_retries):
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
                self.logger.error(f"Attempt {attempt + 1}/{max_retries} failed to connect to MySQL: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
        self.logger.error("Failed to connect to MySQL after retries")
        return False

    def disconnect(self):
        """Close database connection"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.logger.info("Database connection closed")

    def process(self, file_path: Path, move_dir: Path) -> bool:
        try:
            self.logger.info(f"Processing WINDOWSENTRY file: {file_path}")
            
            if not self.connect():
                return False

            success = self.upload_csv_data(self.get_table_name(), str(file_path))
            
            if success:
                try:
                    destination = move_dir / file_path.name
                    shutil.move(str(file_path), str(destination))
                    self.logger.info(f"Moved file to {destination}")
                except Exception as e:
                    self.logger.error(f"Failed to move file {file_path} to {move_dir}: {str(e)}")
                    return False
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error processing WINDOWSENTRY file {file_path}: {str(e)}")
            return False
        finally:
            self.disconnect()

    def upload_csv_data(self, table_name, csv_file_path):
        """Upload CSV data with proper duplicate handling and field updates"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False

        cursor = None
        try:
            # 1. Define expected headers
            headers = [
                'ORDER_NUMBER', 'QUOTATION_NUMBER', 'WINDOWS_QTY', 'LINE_QTY',
                'OPENING_QTY', 'USER_NAME', 'ORDER_DATE', 'SYSTEM', 'OUTPUT_DATE', 'DEALER NAME'
            ]

            # 2. Check CSV headers
            with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
                first_line = csvfile.readline().strip()
                first_line_headers = [h.strip() for h in first_line.split(',')]
                normalized_first_line_headers = [h.replace('\ufeff', '').replace('"', '').strip().upper() for h in first_line_headers]
                normalized_expected_headers = [h.strip().upper() for h in headers]
                
                if normalized_first_line_headers != normalized_expected_headers:
                    self.logger.error(f"CSV headers don't match. Found: {normalized_first_line_headers}, Expected: {normalized_expected_headers}")
                    return False

            # 3. Check table existence and schema
            cursor = self.connection.cursor(dictionary=True)
            if not self._table_exists(cursor, table_name):
                self.logger.info(f"Creating table '{table_name}'")
                if not self._create_table(table_name, headers):
                    return False
            else:
                if not self._verify_and_fix_schema(cursor, table_name, headers):
                    return False

            # 4. Process CSV rows
            rows_to_insert = []
            duplicates = []
            skipped_rows = 0
            total_rows = 0

            with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
                csvreader = csv.DictReader(csvfile, fieldnames=headers)
                next(csvreader)  # Skip header row
                
                for row in csvreader:
                    total_rows += 1
                    try:
                        csv_row = {h: (row.get(h) or '').strip() for h in headers}
                        order_number = csv_row['ORDER_NUMBER']
                        quotation_number = csv_row['QUOTATION_NUMBER']

                        # Skip if both identifiers are empty
                        if not order_number and not quotation_number:
                            skipped_rows += 1
                            continue

                        # Find existing records
                        query = """
                            SELECT * FROM `{}` 
                            WHERE (ORDER_NUMBER = %s AND ORDER_NUMBER != '')
                               OR (QUOTATION_NUMBER = %s AND QUOTATION_NUMBER != '')
                        """.format(table_name)
                        
                        cursor.execute(query, (order_number, quotation_number))
                        existing_records = cursor.fetchall()

                        if existing_records:
                            # Take the first matching record
                            existing = existing_records[0]  # Use the first record if multiple matches

                            # Prepare update for all fields with the new row data
                            update_cols = []
                            update_values = []
                            for field in headers:
                                update_cols.append(f"`{field}` = %s")
                                update_values.append(csv_row[field])
                            
                            update_values.append(existing['id'])  # Add id for WHERE clause
                            
                            update_query = f"""
                                UPDATE `{table_name}` 
                                SET {', '.join(update_cols)}
                                WHERE id = %s
                            """
                            
                            cursor.execute(update_query, update_values)
                            self.connection.commit()  # Commit the update immediately
                            rows_updated = cursor.rowcount
                            
                            duplicates.append({
                                'id': existing['id'],
                                'order_number': existing['ORDER_NUMBER'],
                                'quotation_number': existing['QUOTATION_NUMBER'],
                                'changed_fields': {field: {'old': existing.get(field, ''), 'new': csv_row[field]} for field in headers},
                                'type': 'UPDATED'
                            })
                            self.logger.info(f"Updated record ID {existing['id']} with new values: {csv_row}")
                        else:
                            rows_to_insert.append(csv_row)
                            self.logger.debug(f"New record to insert. Order: {order_number}, Quote: {quotation_number}")

                    except Exception as e:
                        self.logger.error(f"Error processing row {total_rows}: {str(e)}")
                        continue

            self.logger.info(f"Processing stats - Total: {total_rows}, Skipped: {skipped_rows}, New: {len(rows_to_insert)}, Updated: {len([d for d in duplicates if d['type'] == 'UPDATED'])}, Duplicates: {len([d for d in duplicates if d['type'] == 'DUPLICATE'])}")

            # 5. Insert new rows
            if rows_to_insert:
                try:
                    columns = ', '.join([f'`{h}`' for h in headers])
                    placeholders = ', '.join(['%s'] * len(headers))
                    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                    
                    batch_values = []
                    for row in rows_to_insert:
                        batch_values.append([row[h] for h in headers])
                    
                    cursor.executemany(insert_query, batch_values)
                    inserted_count = cursor.rowcount
                    self.connection.commit()
                    self.logger.info(f"Inserted {inserted_count} new records")
                except Exception as e:
                    self.logger.error(f"Failed to insert new records: {str(e)}")
                    self.connection.rollback()
                    return False

            # 6. Send notifications for updated records
            if duplicates:
                try:
                    updated_records = [d for d in duplicates if d['type'] == 'UPDATED']
                    if updated_records:
                        #self.email_notifier.notify_updates(table_name, updated_records)
                        self.logger.info(f"Sent notifications for {len(updated_records)} updated records")
                except Exception as e:
                    self.logger.error(f"Failed to send update notifications: {str(e)}")

            return True

        except Exception as e:
            self.logger.error(f"Upload failed: {str(e)}")
            if self.connection:
                self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def _create_table(self, table_name, headers):
        """Create table with proper structure"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            columns = ["`id` INT NOT NULL AUTO_INCREMENT PRIMARY KEY"]
            for header in headers:
                columns.append(f"`{header}` TEXT NOT NULL DEFAULT ''")
            
            # Add indexes
            columns.append("INDEX `idx_order_number` (`ORDER_NUMBER`(255))")
            columns.append("INDEX `idx_quotation_number` (`QUOTATION_NUMBER`(255))")
            
            create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns)})"
            cursor.execute(create_sql)
            
            self.connection.commit()
            self.logger.info(f"Created table '{table_name}'")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table '{table_name}': {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()

    def _table_exists(self, cursor, table_name):
        """Check if table exists"""
        try:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            return cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"Error checking table existence: {str(e)}")
            return False

    def _verify_and_fix_schema(self, cursor, table_name, expected_headers):
        """Verify and fix table schema"""
        try:
            cursor.execute(f"DESCRIBE `{table_name}`")
            current_columns = [row['Field'] for row in cursor.fetchall()]
            
            missing_columns = [h for h in expected_headers if h not in current_columns]
            
            for column in missing_columns:
                try:
                    cursor.execute(f"ALTER TABLE `{table_name}` ADD COLUMN `{column}` TEXT NOT NULL DEFAULT ''")
                    self.logger.info(f"Added missing column '{column}'")
                except Exception as e:
                    self.logger.error(f"Failed to add column '{column}': {str(e)}")
                    return False
            
            # Verify indexes
            try:
                cursor.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'idx_order_number'")
                if not cursor.fetchone():
                    cursor.execute(f"CREATE INDEX `idx_order_number` ON `{table_name}` (`ORDER_NUMBER`(255))")
                
                cursor.execute(f"SHOW INDEX FROM `{table_name}` WHERE Key_name = 'idx_quotation_number'")
                if not cursor.fetchone():
                    cursor.execute(f"CREATE INDEX `idx_quotation_number` ON `{table_name}` (`QUOTATION_NUMBER`(255))")
                
                self.connection.commit()
            except Exception as e:
                self.logger.warning(f"Could not create indexes: {str(e)}")
            
            return True

        except Exception as e:
            self.logger.error(f"Schema verification failed: {str(e)}")
            return False