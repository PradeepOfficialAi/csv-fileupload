from processors.base_processor import BaseProcessor
from pathlib import Path
from config.config import ConfigManager
import mysql.connector
from mysql.connector import Error
import csv
import os
import shutil

class CASINGCUTTINGProcessor(BaseProcessor):
    def __init__(self, db_handler, email_notifier, logger):
        super().__init__(db_handler, email_notifier, logger)
        self.config_manager = ConfigManager()
        self.connection = None

    def get_table_name(self):
        return "casingcutting"

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
            self.logger.info(f"Processing CASINGCUTTING file: {file_path}")
            
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
            self.logger.error(f"Error processing CASINGCUTTING file {file_path}: {str(e)}")
            return False
        finally:
            self.disconnect()

    def upload_csv_data(self, table_name, csv_file_path):
        """Upload CSV data to the casingcutting table, checking for resends"""
        if not self.connection or not self.connection.is_connected():
            if not self.connect():
                return False
        
        cursor = None
        try:
            # 1. Define expected headers
            headers = [
                'H_W', 'BIN', 'ORDER_LINE', 'MATERIAL', 'LABEL', 'ORDER', 'WINDOW',
                'WINDOWS_SIZE', 'ROSSETTE', 'CASING LINE', 'COMPANY', 'PO', 'DATE',
                'TIME', 'USER'
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
            
            # 3. Read all rows and process
            rows_inserted = 0
            resends = []
            
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
                
                # Prepare insert query
                columns = ', '.join([f'`{h}`' for h in db_columns])
                placeholders = ', '.join(['%s'] * len(db_columns))
                insert_query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
                
                # Process each row
                for row in csvreader:
                    try:
                        # Convert None or empty values to empty strings
                        complete_row = {h: row.get(h, '') or '' for h in actual_headers}
                        order_id = complete_row.get('ORDER', '')

                        if not order_id:
                            self.logger.warning(f"Skipping row with missing ORDER: {complete_row}")
                            continue

                        # Check if ORDER exists in database
                        query = "SELECT `ORDER`, `DATE` FROM `casingcutting` WHERE `ORDER` = %s LIMIT 1"
                        cursor.execute(query, (order_id,))
                        existing_order = cursor.fetchone()

                        if existing_order:
                            # Add to resends list (deduplication handled by notify_resend)
                            resends.append({
                                'order': order_id,
                                'original_date': existing_order[1] or ''  # Use DATE from database
                            })

                        # Insert the row
                        values = [complete_row[h] for h in db_columns]
                        cursor.execute(insert_query, values)
                        rows_inserted += 1
                        self.logger.info(f"Inserted row for ORDER: {order_id}")

                    except Exception as e:
                        self.logger.error(f"Row processing error for row {order_id}: {str(e)}")
                        continue

                # Send resend notification if any resends were found
                if resends:
                    try:
                        self.email_notifier.notify_resend(table_name, resends, 'ORDER')
                        self.logger.info(f"Sent resend notification for {len(resends)} orders")
                    except Exception as e:
                        self.logger.error(f"Failed to send resend notification: {str(e)}")

            # Commit transaction
            self.connection.commit()
            self.logger.info(f"Inserted {rows_inserted} rows into {table_name}")
            
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