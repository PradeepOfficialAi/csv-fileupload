import mysql.connector
from mysql.connector import Error
from pathlib import Path
from services.logger import Logger
import csv
import os
from typing import List, Dict, Optional, Any, Union


class DatabaseService:
    def __init__(self, host, database, user, password, port=3306):
        self.logger = Logger("DatabaseService")
        self.config = {
            'host': host,
            'database': database,
            'user': user,
            'password': password,
            'port': port
        }
        self.connection = None
        self.connect()

    def connect(self):
        """Establish database connection"""
        try:
            if self.connection and self.connection.is_connected():
                return True
                
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

    def execute_query(self, query, params=None):
        """Execute query with MariaDB-specific handling"""
        if not self.test_connection():
            self.logger.error("No database connection")
            return False
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            # Log the query before execution
            self.logger.debug(f"Executing query: {query}")
            
            cursor.execute(query, params or ())
            
            if query.strip().lower().startswith('select'):
                return cursor.fetchall()
                
            self.connection.commit()
            return cursor.rowcount
            
        except Error as e:
            self.logger.error(f"Query failed (MariaDB): {str(e)}")
            self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()

    def upload_csv_data(self, table_name, csv_file_path, email_notifier=None):
        """Upload CSV data with robust delimiter detection and proper email notification"""
        if not self.connect():
            return False
        
        cursor = None
        try:
            # 1. Detect table type and get default headers
            table_type = self._detect_table_type(table_name)
            default_headers = self._get_default_headers(table_type)
            
            # 2. Process CSV file with multiple delimiter fallbacks
            with open(csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
                sample = csvfile.read(2048)  # Read larger sample for better detection
                csvfile.seek(0)
                
                # Try common delimiters if auto-detection fails
                for delimiter in [',', ';', '\t', '|']:
                    try:
                        dialect = csv.Sniffer().sniff(sample, delimiters=delimiter)
                        has_header = csv.Sniffer().has_header(sample)
                        break
                    except csv.Error:
                        continue
                else:
                    # Final fallback to comma delimiter
                    dialect = csv.excel()
                    has_header = False
                    self.logger.warning("Using comma as fallback delimiter")
                
                if not has_header and not default_headers:
                    self.logger.error("CSV has no headers and no default headers")
                    return False
                
                # Create reader with detected settings
                if has_header:
                    reader = csv.DictReader(csvfile, dialect=dialect)
                    headers = [h.strip() for h in reader.fieldnames]
                else:
                    content = csvfile.readlines()
                    headers = default_headers
                    reader = csv.DictReader(content, fieldnames=headers, dialect=dialect)
            
            # 3. Create table if needed
            cursor = self.connection.cursor()
            if not self._table_exists(cursor, table_name):
                if not self._create_table(table_name, headers):
                    return False
            
            # 4. Process rows with duplicate checking
            new_rows = 0
            duplicate_rows = 0
            duplicates = []  # Store complete duplicate info
            
            key_field = self._get_key_field(headers)
            date_field = self._get_date_field(table_type)
            
            for row in reader:
                try:
                    clean_row = {k: v.strip() if v else None for k, v in row.items()}
                    
                    if key_field and clean_row.get(key_field):
                        key_value = clean_row[key_field]
                        existing = self._check_duplicate(cursor, table_name, key_field, key_value)
                        
                        if existing:
                            duplicate_rows += 1
                            original_date = existing
                            duplicates.append((key_value, original_date))
                            
                            continue
                    
                    # Insert new row
                    columns = [f"`{col}`" for col in headers]
                    placeholders = ['%s'] * len(headers)
                    values = [clean_row.get(col) for col in headers]
                    
                    query = f"""
                    INSERT INTO `{table_name}` ({','.join(columns)})
                    VALUES ({','.join(placeholders)})
                    """
                    cursor.execute(query, values)
                    new_rows += 1
                    
                except Exception as e:
                    self.logger.error(f"Row processing error: {str(e)}")
                    continue
            
            self.connection.commit()
            self.logger.info(f"Inserted {new_rows} rows, skipped {duplicate_rows} duplicates")
            
            # 5. Send proper email notification
            if duplicates and email_notifier:
                try:
                    email_notifier.notify_duplicate(
                        table_name=table_name,
                        file_name=os.path.basename(csv_file_path),
                        duplicate_info=duplicates,
                        key_field=key_field
                    )
                except Exception as e:
                    self.logger.error(f"Failed to send duplicate notification: {str(e)}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Upload failed: {str(e)}")
            self.connection.rollback()
            return False
        finally:
            if cursor:
                cursor.close()
            self.disconnect()

    def _create_table(self, table_name, headers):
        """Create table with appropriate structure"""
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            # Define column types based on field names
            columns = []
            for header in headers:
                clean_header = header.replace(' ', '_')
                
                # Special handling for key fields
                if header.lower() in ['order', 'sealed_unit_id', 'f', 'j']:
                    col_def = f"`{clean_header}` VARCHAR(255) UNIQUE"
                elif header.lower() in ['width', 'height', 'qty']:
                    col_def = f"`{clean_header}` DECIMAL(10,2)"
                elif any(x in header.lower() for x in ['date', 'time']):
                    col_def = f"`{clean_header}` DATE"
                else:
                    col_def = f"`{clean_header}` TEXT"
                
                columns.append(col_def)
            
            # Create table query
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

    def _check_duplicate(self, cursor, table_name, key_field, key_value):
        """Check for duplicate entry and return existing data"""
        query = f"SELECT * FROM `{table_name}` WHERE `{key_field}` = %s LIMIT 1"
        cursor.execute(query, (key_value,))
        result = cursor.fetchone()
        return dict(zip([col[0] for col in cursor.description], result)) if result else None

    def _get_key_field(self, headers):
        """Determine the appropriate key field for duplicate checking"""
        for field in ['sealed_unit_id', 'order', 'F', 'J']:
            if field in headers:
                return field
        return None

    def _get_date_field(self, table_type):
        """Get the appropriate date field based on table type"""
        return 'list_date' if table_type == 'glassreport' else 'U'

    def _detect_table_type(self, table_name):
        table_name = table_name.lower()
        if 'glassreport' in table_name:
            return 'glassreport'
        elif 'framescutting' in table_name:
            return 'framescutting'
        else:
            return 'default'
    
    def _get_default_headers(self, table_type):
        default_headers = {
            'glassreport': [
                'order_date', 'list_date', 'sealed_unit_id', 'ot', 'window_type', 'line1',
                'line2', 'line3', 'grills', 'spacer', 'dealer', 'glass_comment','tag', 'zones','u_value',
                'solar_heat_gain','visual_trasmittance','energy_rating','glass_type','order','width',
                'height', 'qty','description','note1','note2','rack_id','complete', 'shipping'
                ],

            'framescutting': [
                "A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q",
                "R","S","T","U","V","W","X","Y","Z"
                ],
            #'rush': ['order', 'priority', 'due_date', 'description'],
            #'default': ['order', 'field1', 'field2', 'field3', 'quantity']
        }
        return default_headers.get(table_type)

    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            if self.connection and self.connection.is_connected():
                return True
            return self.connect()
        except Error as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        if not self.test_connection():
            self.logger.error("Cannot get tables - no database connection")
            return []
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute("SHOW TABLES")
            tables = [table[0] for table in cursor.fetchall()]
            return tables
        except Error as e:
            self.logger.error(f"Failed to get tables: {str(e)}")
            return []
        finally:
            if cursor:
                cursor.close()

    def get_columns(self, table_name: str) -> List[str]:
        """Get list of columns for a specific table"""
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [column[0] for column in cursor.fetchall()]
            cursor.close()
            return columns
        except Error as e:
            self.logger.error(f"Failed to get columns for table {table_name}: {str(e)}")
            return []

    def get_table_data(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get sample data from a table"""
        try:
            cursor = self.connection.cursor(dictionary=True)
            
            # Build SELECT query
            if columns:
                cols = ", ".join([f"`{col}`" for col in columns])
            else:
                cols = "*"
                
            query = f"SELECT {cols} FROM `{table_name}` LIMIT %s"
            cursor.execute(query, (limit,))
            
            results = cursor.fetchall()
            cursor.close()
            return results
        except Error as e:
            self.logger.error(f"Failed to get data from {table_name}: {str(e)}")
            return []

    

    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get detailed information about a table"""
        info = {
            'columns': [],
            'row_count': 0,
            'create_syntax': ''
        }
        
        try:
            # Get column details
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
            info['columns'] = cursor.fetchall()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            info['row_count'] = cursor.fetchone()[0]
            
            # Get create syntax
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            info['create_syntax'] = cursor.fetchone()[1]
            
            cursor.close()
            return info
            
        except Error as e:
            self.logger.error(f"Failed to get table info: {str(e)}")
            return info

    def execute_script(self, sql_script: str) -> Dict[str, Any]:
        """
        Execute a multi-statement SQL script
        
        Returns:
            Dict with:
            - 'success': Boolean indicating overall success
            - 'executed_statements': Number of executed statements
            - 'failed_statements': Number of failed statements
            - 'errors': List of error messages
        """
        result = {
            'success': True,
            'executed_statements': 0,
            'failed_statements': 0,
            'errors': []
        }
        
        if not sql_script.strip():
            return result
            
        cursor = None
        try:
            cursor = self.connection.cursor()
            
            # Split script into individual statements
            statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
            
            for stmt in statements:
                try:
                    cursor.execute(stmt)
                    result['executed_statements'] += 1
                except Error as e:
                    result['failed_statements'] += 1
                    result['errors'].append(str(e))
                    self.logger.error(f"SQL statement failed: {str(e)}")
                    continue
            
            if result['failed_statements'] > 0:
                result['success'] = False
                
            self.connection.commit()
            return result
            
        except Error as e:
            self.logger.error(f"Script execution failed: {str(e)}")
            self.connection.rollback()
            result['success'] = False
            result['errors'].append(str(e))
            return result
        finally:
            if cursor:
                cursor.close()