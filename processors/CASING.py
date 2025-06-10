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

class CASINGProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "casing"

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
            self.logger.info(f"Processing CASING file: {file_path}")
            
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
            self.logger.error(f"Error processing CASING file {file_path}: {str(e)}")
            return False
        finally:
            self.disconnect()

    def upload_csv_data(self, table_name, csv_file_path):
        """Upload CSV data to the casing table, checking CASING_ID occurrences"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        cursor = None
        try:
            # 1. Define expected headers
            headers = [
                'SIZE', 'H AND W', 'BIN', 'LINE NUMBER', 'PROFILE TYPE', 'LABEL',
                'ORDER NUMBER', 'WINDOW_TYPE', 'WINDOW SIZE', 'WINDOW LINE', 'OT',
                'COLOUR IN', 'COLOUR OUT', 'RUBBER COLOUR', 'COMPANY NAME',
                'CUSTOMER PO', 'CASING_ID', 'DATE', 'TIME'
            ]

            # 2. Check if CSV file has the expected headers
            has_expected_headers = False
            first_line_headers = []
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                first_line = csvfile.readline().strip()
                first_line_headers = [h.strip() for h in first_line.split(',')]
                normalized_first_line_headers = [h.lower().strip() for h in first_line_headers]
                normalized_expected_headers = [h.lower().strip() for h in headers]
                has_expected_headers = normalized_first_line_headers == normalized_expected_headers
                self.logger.info(f"CSV headers: {first_line_headers}, Expected: {headers}, Match: {has_expected_headers}")

            if not has_expected_headers:
                temp_dir = tempfile.gettempdir()
                temp_path = os.path.join(temp_dir, os.path.basename(csv_file_path) + ".tmp")
                
                try:
                    with open(csv_file_path, 'r', encoding='utf-8') as infile, \
                        open(temp_path, 'w', newline='', encoding='utf-8') as outfile:
                        outfile.write(','.join(headers) + '\n')
                        infile.seek(0)
                        outfile.writelines(infile.readlines())
                    
                    shutil.move(temp_path, csv_file_path)
                    self.logger.warning(f"Added headers to CSV file: {headers}")
                except Exception as e:
                    self.logger.error(f"Error adding headers to {csv_file_path}: {str(e)}")
                    return False
            
            # 3. Create table if it doesn't exist
            cursor = self.connection.cursor()
            if not self._table_exists(cursor, table_name):
                self.logger.info(f"Table '{table_name}' does not exist, attempting to create")
                if not self._create_table(table_name, headers):
                    self.logger.error(f"Failed to create table '{table_name}'")
                    return False

            # 4. Collect all rows and count CASING_ID occurrences
            rows_to_insert = []
            casing_id_counts = {}  # Track new CASING_ID occurrences
            duplicates = []
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.DictReader(csvfile)
                actual_headers = [h.strip() for h in csvreader.fieldnames]
                
                self.logger.info(f"Processing CSV with columns: {actual_headers}")

                normalized_headers = [h.lower().strip() for h in headers]
                for row in csvreader:
                    row_values = [str(row.get(h, '')).lower().strip() for h in actual_headers]
                    if row_values == normalized_headers:
                        self.logger.warning(f"Skipping duplicate header row: {row_values}")
                        continue
                    
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

                        casing_id = complete_row.get('CASING_ID', '')
                        order_number = complete_row.get('ORDER NUMBER', '')

                        if not casing_id:
                            self.logger.warning(f"Skipping row with missing CASING_ID: {complete_row}")
                            continue

                        casing_id_counts[casing_id] = casing_id_counts.get(casing_id, 0) + 1
                        rows_to_insert.append(complete_row)

                    except Exception as e:
                        self.logger.error(f"Row processing error for row {complete_row}: {str(e)}")
                        continue

            # 5. Check existing CASING_ID counts in the database
            if casing_id_counts:
                casing_ids = list(casing_id_counts.keys())
                format_strings = ','.join(['%s'] * len(casing_ids))
                query = f"SELECT `CASING_ID`, COUNT(*), `DATE` FROM `{table_name}` WHERE `CASING_ID` IN ({format_strings}) GROUP BY `CASING_ID`"
                cursor.execute(query, casing_ids)
                existing_counts = {row[0]: row[1] for row in cursor.fetchall()}
                self.logger.debug(f"Existing CASING_ID counts: {existing_counts}")

                # Combine existing and new counts, and collect duplicates
                for casing_id, new_count in casing_id_counts.items():
                    total_count = new_count + existing_counts.get(casing_id, 0)
                    if total_count > 2:
                        # Find all rows with this CASING_ID
                        for row in rows_to_insert:
                            if row['CASING_ID'] == casing_id:
                                duplicates.append({
                                    'order': row.get('ORDER NUMBER', 'Unknown'),
                                    'casing_id': casing_id,
                                    'total_occurrences': total_count,
                                    'original_date': row.get('DATE', ''),
                                    'type': 'DUPLICATE'
                                })
                                self.logger.info(f"Found duplicate CASING_ID: {casing_id} for Order: {row.get('ORDER NUMBER', 'Unknown')} with {total_count} occurrences")

            # 6. Insert all rows, replacing existing CASING_IDs
            rows_inserted = 0
            rows_replaced = 0
            if rows_to_insert:
                try:
                    db_columns = [h for h in actual_headers]
                    columns = ', '.join([f'`{h}`' for h in db_columns])
                    placeholders = ', '.join(['%s'] * len(db_columns))
                    insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                    
                    # Batch insert new rows
                    batch_values = [[complete_row[h] for h in db_columns] for complete_row in rows_to_insert]
                    cursor.executemany(insert_query, batch_values)
                    rows_inserted = cursor.rowcount
                    self.connection.commit()
                    self.logger.info(f"Inserted {rows_inserted} rows into {table_name}, replaced {rows_replaced} duplicates")
                
                except Exception as e:
                    self.logger.error(f"Batch insert failed for {csv_file_path}: {str(e)}")
                    self.connection.rollback()
                    return False

            # 7. Send duplicate notification if any duplicates were found
            if duplicates:
                try:
                    self.email_notifier.notify_duplicate(table_name, duplicates, 'CASING_ID')
                    self.logger.info(f"Sent duplicate notification for {len(duplicates)} CASING_IDs")
                except Exception as e:
                    self.logger.error(f"Failed to send duplicate notification: {str(e)}")
                    for dup in duplicates:
                        self.logger.warning(f"Duplicate not notified: CASING_ID={dup['casing_id']}, Order={dup['order']}, Total={dup['total_occurrences']}")

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