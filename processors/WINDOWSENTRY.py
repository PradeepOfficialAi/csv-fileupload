from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import os
import shutil
import tempfile
from typing import List, Dict
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
        """Upload CSV data to the windowsentry table, using ORDER_NUMBER or QUOTATION_NUMBER for duplicates"""
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

            # 2. Check if CSV file has headers
            has_headers = False
            first_line_headers = []
            with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
                first_line = csvfile.readline().strip()
                first_line_headers = [h.strip() for h in first_line.split(',')]
                normalized_first_line_headers = [h.replace('\ufeff', '').replace('"', '').lower().strip() for h in first_line_headers]
                normalized_expected_headers = [h.lower().strip() for h in headers]
                has_headers = normalized_first_line_headers == normalized_expected_headers
                self.logger.info(f"CSV headers: {first_line_headers}, Normalized: {normalized_first_line_headers}, Expected: {headers}, Has headers: {has_headers}")

            if not has_headers:
                self.logger.error(f"CSV file {csv_file_path} does not have expected headers: {headers}")
                return False
            
            # 3. Check table existence and schema
            cursor = self.connection.cursor()
            if not self._table_exists(cursor, table_name):
                self.logger.info(f"Table '{table_name}' does not exist, attempting to create")
                if not self._create_table(table_name, headers):
                    self.logger.error(f"Failed to create table '{table_name}'")
                    return False
            else:
                if not self._verify_and_fix_schema(cursor, table_name, headers):
                    self.logger.error(f"Failed to verify or fix schema for '{table_name}'")
                    return False

            # 4. Collect all rows and check ORDER_NUMBER or QUOTATION_NUMBER against database
            rows_to_insert = []
            rows_to_update = []
            duplicates = []
            
            with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
                csvreader = csv.DictReader(csvfile, fieldnames=headers)
                next(csvreader)  # Skip header row
                self.logger.info(f"Processing CSV with columns: {headers}")

                for row in csvreader:
                    try:
                        complete_row = {h: row.get(h, '') or '' for h in headers}
                        order_number = complete_row.get('ORDER_NUMBER', '').strip()
                        quotation_number = complete_row.get('QUOTATION_NUMBER', '').strip()

                        if not order_number and not quotation_number:
                            self.logger.warning(f"Skipping row with missing ORDER_NUMBER and QUOTATION_NUMBER: {complete_row}")
                            continue

                        # Determine key field and value
                        key_field = 'ORDER_NUMBER' if order_number else 'QUOTATION_NUMBER'
                        key_value = order_number if order_number else quotation_number

                        # Check if key_value exists in database
                        cursor.execute(f"SELECT `{key_field}` FROM `{table_name}` WHERE `{key_field}` = %s", (key_value,))
                        exists = cursor.fetchone() is not None

                        if exists:
                            rows_to_update.append((complete_row, key_field))
                            duplicates.append({
                                'order': key_value,
                                key_field.lower(): key_value,
                                'original_date': complete_row.get('ORDER_DATE', 'Unknown'),
                                'type': f'DUPLICATE_{key_field.upper()}'
                            })
                            self.logger.info(f"Found duplicate {key_field}: {key_value}")
                        else:
                            rows_to_insert.append(complete_row)

                    except Exception as e:
                        self.logger.error(f"Row processing error for row {complete_row}: {str(e)}")
                        continue

            # 5. Update existing rows
            rows_updated = 0
            if rows_to_update:
                try:
                    for row, key_field in rows_to_update:
                        update_columns = [h for h in headers if h != key_field]
                        set_clause = ', '.join([f"`{h}` = %s" for h in update_columns])
                        update_query = f"UPDATE `{table_name}` SET {set_clause} WHERE `{key_field}` = %s"
                        values = [row[h] for h in update_columns] + [row[key_field]]
                        cursor.execute(update_query, values)
                        rows_updated += cursor.rowcount
                    
                    self.connection.commit()
                    self.logger.info(f"Updated {rows_updated} rows in {table_name}")
                
                except Exception as e:
                    self.logger.error(f"Batch update failed for {csv_file_path}: {str(e)}")
                    self.connection.rollback()
                    return False

            # 6. Insert new rows
            rows_inserted = 0
            if rows_to_insert:
                try:
                    db_columns = headers
                    columns = ', '.join([f'`{h}`' for h in db_columns])
                    placeholders = ', '.join(['%s'] * len(db_columns))
                    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                    batch_values = [[complete_row[h] for h in db_columns] for complete_row in rows_to_insert]
                    cursor.executemany(insert_query, batch_values)
                    rows_inserted = cursor.rowcount
                    self.connection.commit()
                    self.logger.info(f"Inserted {rows_inserted} rows into {table_name}")
                
                except Exception as e:
                    self.logger.error(f"Batch insert failed for {csv_file_path}: {str(e)}")
                    self.connection.rollback()
                    return False

            # 7. Send duplicate :notification if any duplicates were found
            if duplicates:
                try:
                    for dup in duplicates:
                        key_field = 'order_number' if 'order_number' in dup else 'quotation_number'
                        #self.email_notifier.notify_duplicate(table_name, [dup], key_field)
                    self.logger.info(f"Sent duplicate notification for {len(duplicates)} duplicate values")
                except Exception as e:
                    self.logger.error(f"Failed to send duplicate notification: {str(e)}")
                    for dup in duplicates:
                        key_field = 'order_number' if 'order_number' in dup else 'quotation_number'
                        self.logger.warning(f"Duplicate not notified: {key_field}={dup[key_field]}, Order={dup['order']}")

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
        """Create table with exact header names"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            type_mapping = {'id': 'INT NOT NULL AUTO_INCREMENT PRIMARY KEY'}
            for header in headers:
                if header not in type_mapping:
                    type_mapping[header] = 'TEXT NOT NULL DEFAULT ""'
            
            columns = ["id INT NOT NULL AUTO_INCREMENT PRIMARY KEY"]
            for header in headers:
                if header in type_mapping and header != 'id':
                    sql_type = type_mapping[header]
                    columns.append(f"`{header}` {sql_type}")
            
            create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns)})"
            self.logger.debug(f"Executing CREATE TABLE query: {create_sql}")
            cursor.execute(create_sql)
            
            self.connection.commit()
            self.logger.info(f"Created table '{table_name}' with columns: {headers}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to create table '{table_name}': {str(e)}")
            return False
        finally:
            if cursor:
                cursor.close()

    def _table_exists(self, cursor, table_name):
        """Check if table exists in database"""
        try:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            exists = cursor.fetchone() is not None
            self.logger.debug(f"Table '{table_name}' exists: {exists}")
            return exists
        except Exception as e:
            self.logger.error(f"Error checking table existence for '{table_name}': {str(e)}")
            return False

    def _verify_and_fix_schema(self, cursor, table_name, expected_headers):
        """Verify table schema and add missing columns"""
        try:
            cursor.execute(f"DESCRIBE `{table_name}`")
            current_columns = [row[0] for row in cursor.fetchall()]
            self.logger.debug(f"Current columns in '{table_name}': {current_columns}")

            missing_columns = [h for h in expected_headers if h not in current_columns]
            self.logger.debug(f"Missing columns in '{table_name}': {missing_columns}")

            for header in missing_columns:
                try:
                    alter_sql = f"ALTER TABLE `{table_name}` ADD `{header}` TEXT NOT NULL DEFAULT ''"
                    self.logger.debug(f"Executing ALTER TABLE query: {alter_sql}")
                    cursor.execute(alter_sql)
                    self.logger.info(f"Added column '{header}' to table '{table_name}'")
                except Exception as e:
                    self.logger.error(f"Failed to add column '{header}' to '{table_name}': {str(e)}")
                    return False

            self.connection.commit()
            return True

        except Exception as e:
            self.logger.error(f"Failed to verify/fix schema for '{table_name}': {str(e)}")
            return False