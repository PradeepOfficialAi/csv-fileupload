from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import os
import shutil

class ORDERSUMMARYProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "ordersummary"

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
            self.logger.info(f"Processing ORDERSUMMARY file: {file_path}")
            
            if not self.connect():
                return False

            success = self.upload_csv_data(self.get_table_name(), str(file_path))
            
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
            self.logger.error(f"Error processing ORDERSUMMARY file {file_path}: {str(e)}")
            return False
        finally:
            self.disconnect()

    def upload_csv_data(self, table_name, csv_file_path):
        """Upload CSV data to the ordersummary table, updating or inserting based on ORDER#"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        cursor = None
        try:
            # 1. Define expected headers
            headers = [
                'ORDER#', 'CUST PO', 'COMPANY', 'ORDER DATE', 'DUE DATE', 'LIVE_TEST', 'AW-V', 'CAW-V',
                'CCS-L', 'CCS-R', 'CECS-L', 'CECS-R', 'CS-L', 'CS-R', 'CSHAPE', 'CV-F', 'DES', 'DESLO',
                'DWIND', 'SDWIND', 'SHO', 'SLO', 'SU', 'SU1', 'SUSHP', 'V-A', 'V-AO', 'V-B', 'V-BLO',
                'V-C', 'V-F', 'V-LCS', 'V-SF', 'V-SH', 'V-SHO', 'V-SLO', 'V-SLOO', 'V-SLOS', 'V-SSO',
                'V-SS', 'V-SLOR', 'V-SS-R', 'V-SSOR', 'VSLOSR', 'DES4', 'DESLO4', 'SH', 'SS', 'SS-R',
                'SSO', 'SLO-R', 'SSO-R', 'SLOO', 'SLOS', 'SLOSR', 'DH', 'SHP-SH', 'SHAPE', 'CV-SF',
                'WINDOW1', 'WINDOW2', 'WINDOW3', 'WINDOW4', 'WINDOW5', 'WINDOW6', 'WINDOW7', 'WINDOW8',
                'BRICKMOULD', 'EXT', 'CASING', 'ROSETTE', 'GRILL', 'SDL', 'COLOUR IN', 'COLOUR OUT',
                'RUBBER COLOUR', 'BAY', 'BOW', 'PATIO DOOR', 'PATIO DOOR OPTIONS', 'EX_COL1', 'EX_COL2',
                'EX_COL3', 'EX_COL4', 'EX_COL5', 'CORNER_DR', 'USER NAME', 'LIST DATE', 'COMPLETE',
                'STATUS', 'P_BOTTERO', 'P_URBAN', 'P_CASING', 'P_SCREEN', 'P_GLASSTOP', 'P_SLCOVERS',
                'P_EXTENSION', 'NOTE', 'BOOKING_DATE', 'COLOUR_BATCH_NO', 'COLOUR_CUT_DATE'
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
            
            # 3. Read all rows and process based on ORDER#
            rows_inserted = 0
            rows_updated = 0
            
            with open(csv_file_path, 'r', encoding='utf-8') as csvfile:
                csvreader = csv.DictReader(csvfile)
                actual_headers = [h.strip() for h in csvreader.fieldnames]
                
                self.logger.info(f"Processing CSV with columns: {actual_headers}")

                # Check/create table
                cursor = self.connection.cursor()
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                if not cursor.fetchone():
                    if not self._create_table(table_name, actual_headers):
                        return False

                db_columns = [h for h in actual_headers]
                protected_columns = []
                
                # Process each row
                for row in csvreader:
                    try:
                        # Convert None or empty values to empty strings and trim spaces
                        complete_row = {h: row.get(h, '') or '' for h in actual_headers}
                        order_id = complete_row.get('ORDER#', '')

                        # Trim spaces for all columns except protected ones
                        for header in actual_headers:
                            if header not in protected_columns:
                                value = complete_row[header]
                                if value is not None:
                                    # If the value is all whitespace, set to empty string
                                    if value.strip() == '':
                                        complete_row[header] = ''
                                    # Otherwise, trim leading and trailing spaces
                                    elif value != value.strip():
                                        complete_row[header] = value.strip()

                        if not order_id:
                            self.logger.warning(f"Skipping row with missing ORDER#: {complete_row}")
                            continue

                        # Check if ORDER# exists
                        query = f"SELECT {', '.join([f'`{col}`' for col in db_columns])} FROM `ordersummary` WHERE `ORDER#` = %s"
                        cursor.execute(query, (order_id,))
                        existing_row = cursor.fetchone()

                        if existing_row:
                            # Get existing row as dict with column names
                            existing_row_dict = {db_columns[i]: existing_row[i] for i in range(len(db_columns))}
                            
                            # Build update query only for columns where existing value is NULL or empty
                            update_columns = []
                            update_values = []
                            for col in db_columns:
                                if col not in protected_columns:
                                    existing_value = existing_row_dict.get(col)
                                    new_value = complete_row[col]
                                    # Update only if existing value is NULL or empty string
                                    if existing_value is None or existing_value == '':
                                        update_columns.append(col)
                                        update_values.append(new_value)

                            if update_columns:
                                set_clause = ', '.join([f"`{col}` = %s" for col in update_columns])
                                update_query = f"UPDATE `ordersummary` SET {set_clause} WHERE `ORDER#` = %s"
                                values = update_values + [order_id]
                                cursor.execute(update_query, values)
                                rows_updated += 1
                                self.logger.info(f"Updated row for ORDER#: {order_id} with {len(update_columns)} columns")
                            else:
                                self.logger.info(f"No columns to update for ORDER#: {order_id} (all non-empty)")
                        else:
                            # Insert new row
                            columns = ', '.join([f'`{h}`' for h in db_columns])
                            placeholders = ', '.join(['%s'] * len(db_columns))
                            insert_query = f"INSERT INTO `ordersummary` ({columns}) VALUES ({placeholders})"
                            values = [complete_row[h] for h in db_columns]
                            cursor.execute(insert_query, values)
                            rows_inserted += 1
                            self.logger.info(f"Inserted new row {order_id}")

                    except Exception as e:
                        self.logger.error(f"Row processing error for row {order_id}: {str(e)}")
                        continue

            # Commit transaction
            self.connection.commit()
            self.logger.info(f"Inserted {rows_inserted} new rows, updated {rows_updated} rows into {table_name}")
            
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
            
            # Map Python types to SQL types
            type_mapping = {
                'id': 'INT NOT NULL AUTO_INCREMENT PRIMARY KEY'
            }
            # All other columns are TEXT NOT NULL DEFAULT ""
            for header in headers:
                if header not in type_mapping:
                    type_mapping[header] = 'TEXT NOT NULL DEFAULT ""'
            
            # Build column definitions
            columns = []
            columns.append("id INT NOT NULL AUTO_INCREMENT PRIMARY KEY")  # Add ID column first
            
            for header in headers:
                if header in type_mapping and header != 'id':  # Skip id as it's already added
                    sql_type = type_mapping[header]
                    columns.append(f"`{header}` {sql_type}")
            
            # Create the table
            create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns)})"
            cursor.execute(create_sql)
            
            self.connection.commit()
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