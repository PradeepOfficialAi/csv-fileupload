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

class URBANCUTTINGProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "urbancutting"

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
            self.logger.info(f"Processing URBANCUTTING file: {file_path}")
            
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
            self.logger.error(f"Error processing URBANCUTTING file {file_path}: {str(e)}")
            return False
        finally:
            self.disconnect()

    def upload_csv_data(self, table_name, csv_file_path):
        """Upload CSV data to the urb table, checking column O for duplicates"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        cursor = None
        try:
            # 1. Define expected headers (A to O)
            headers = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O']

            # 2. Check if CSV file has headers
            has_headers = False
            first_line_headers = []
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline().strip()
                first_line_headers = [h.strip() for h in first_line.split(',')]
                normalized_first_line_headers = [h.lower().strip() for h in first_line_headers]
                normalized_expected_headers = [h.lower().strip() for h in headers]
                has_headers = normalized_first_line_headers == normalized_expected_headers
                self.logger.info(f"CSV headers: {first_line_headers}, Expected: {headers}, Has headers: {has_headers}")

            if not has_headers:
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, os.path.basename(csv_file_path) + ".tmp")
                
                try:
                    with open(csv_file_path, 'r', encoding='utf-8') as infile, \
                        open(temp_path, 'w', newline='', encoding='utf-8') as outfile:
                        outfile.write(','.join(headers) + '\n')
                        outfile.writelines(infile.readlines())
                    
                    shutil.move(temp_path, csv_file_path)
                    self.logger.warning(f"Added headers to CSV file: {headers}")
                except Exception as e:
                    self.logger.error(f"Error adding headers to {csv_file_path}: {str(e)}")
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

            # 4. Collect all rows and check column O against existing database
            rows_to_insert = []
            duplicates = []
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.DictReader(csvfile)
                actual_headers = [h.strip() for h in csvreader.fieldnames]
                
                self.logger.info(f"Processing CSV with columns: {actual_headers}")
                self.logger.info("Assuming column G is the order number for duplicate notifications")

                for row in csvreader:
                    try:
                        complete_row = {h: row.get(h, '') or '' for h in actual_headers}
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

                        column_o_value = complete_row.get('O', '')

                        if not column_o_value:
                            self.logger.warning(f"Skipping row with missing column O value: {complete_row}")
                            continue

                        rows_to_insert.append(complete_row)
                    except Exception as e:
                        self.logger.error(f"Row processing error for row {complete_row}: {str(e)}")
                        continue

            # 5. Check column O values against existing database
            if rows_to_insert:
                column_o_values = [row['O'] for row in rows_to_insert]
                format_strings = ','.join(['%s'] * len(column_o_values))
                query = f"SELECT `O`, `A` FROM `{table_name}` WHERE `O` IN ({format_strings})"
                cursor.execute(query, column_o_values)
                existing_o_values = {row[0] for row in cursor.fetchall()}
                self.logger.debug(f"Existing column O values in database: {existing_o_values}")

                # Identify duplicates
                for row in rows_to_insert:
                    if row['O'] in existing_o_values:
                        duplicates.append({
                            'order': row.get('O', 'Unknown'),  # Adjust if order number is not G
                            'original_date': row.get('A', 'Unknown'),
                            'type': 'DUPLICATE'
                        })
                        self.logger.info(f"Found duplicate urban_id: {row['O']} for Order: {row.get('G', 'Unknown')}")

            # 6. Insert all rows
            rows_inserted = 0
            if rows_to_insert:
                try:
                    db_columns = [h for h in actual_headers]
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

            # 7. Send duplicate notification if any duplicates were found
            if duplicates:
                try:
                    self.email_notifier.notify_duplicate(table_name, duplicates, 'O')
                    self.logger.info(f"Sent duplicate notification for {len(duplicates)} urban_id values")
                except Exception as e:
                    self.logger.error(f"Failed to send duplicate notification: {str(e)}")
                    for dup in duplicates:
                        self.logger.warning(f"Duplicate not notified: urban_id={dup['urban_id']}, Order={dup['order']}")

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