import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
import datetime
import re
import logging
from typing import List, Optional, Tuple, Dict, Any

class Tab5(ttk.Frame):
    def __init__(self, parent, config_manager, db_handler=None):
        super().__init__(parent)
        self.config_manager = config_manager
        self.db_handler = db_handler
        self.selected_table = None
        self.column_vars = {}  # Dictionary to store column checkboxes variables
        self.create_widgets()
    
    def create_widgets(self):
        """Create all UI widgets"""
        self.create_title()
        self.create_table_selector()
        self.create_columns_frame()
        self.create_action_button()
    
    def create_title(self):
        """Create title label"""
        ttk.Label(self, text="Database Tools", font=('Tahoma', 12, 'bold')).pack(pady=10)
    
    def create_table_selector(self):
        """Create table selection dropdown"""
        table_frame = ttk.Frame(self)
        table_frame.pack(pady=10, padx=10, fill=tk.X)
        
        ttk.Label(table_frame, text="Select Table:").pack(side=tk.LEFT, padx=5)
        
        self.table_combobox = ttk.Combobox(table_frame, state='readonly')
        self.table_combobox.pack(side=tk.LEFT, expand=True, fill=tk.X, padx=5)
        self.table_combobox.bind('<<ComboboxSelected>>', self.on_table_selected)
        
        if self.db_handler:
            self.load_tables()
        else:
            self.table_combobox['values'] = ['No database connection']
    
    def create_columns_frame(self):
        """Create frame for column checkboxes"""
        self.columns_frame = ttk.LabelFrame(self, text="Select Columns")
        self.columns_frame.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
        
        # Frame to hold the grid of checkboxes
        self.columns_grid = ttk.Frame(self.columns_frame)
        self.columns_grid.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Message when no table is selected
        self.no_columns_msg = ttk.Label(self.columns_grid, text="Please select a table first")
        self.no_columns_msg.grid(row=0, column=0, columnspan=1, pady=10)
    
    def create_action_button(self):
        """Create the trim spaces button"""
        self.trim_button = ttk.Button(
            self, 
            text="Remove Extra Spaces", 
            command=self.trim_spaces,
            state=tk.DISABLED
        )
        self.trim_button.pack(pady=10)
    
    def load_tables(self):
        """Load available tables from database"""
        if not self.db_handler:
            self.table_combobox['values'] = ['No database handler']
            self.table_combobox.set('No database handler')
            return
            
        try:
            if not self.db_handler.test_connection():
                if not self.db_handler.connect():
                    raise ConnectionError("Failed to connect to database")
                    
            tables = self.db_handler.get_tables()
            if tables:
                self.table_combobox['values'] = tables
                self.table_combobox.set('Select a table')
            else:
                self.table_combobox['values'] = ['No tables found']
                self.table_combobox.set('No tables found')
        except Exception as e:
            logging.error(f"Error loading tables: {str(e)}")
            self.table_combobox['values'] = ['Error loading tables']
            self.table_combobox.set('Error loading tables')
    
    def on_table_selected(self, event):
        """Handle table selection event"""
        self.selected_table = self.table_combobox.get()
        if self.selected_table and self.selected_table not in ['No tables found', 'Error loading tables']:
            self.load_columns()
            self.trim_button['state'] = tk.NORMAL
        else:
            self.trim_button['state'] = tk.DISABLED
    
    def load_columns(self):
        """Load columns for selected table and create checkboxes in a grid with max 20 rows per column"""
        # Clear previous columns
        for widget in self.columns_grid.winfo_children():
            widget.destroy()
        
        if not self.selected_table:
            self.no_columns_msg = ttk.Label(self.columns_grid, text="Please select a table first")
            self.no_columns_msg.grid(row=0, column=0, columnspan=1, pady=10)
            return
        
        try:
            columns = self.db_handler.get_columns(self.selected_table)
            if not columns:
                ttk.Label(self.columns_grid, text="No columns found").grid(row=0, column=0, columnspan=1, pady=10)
                return
            
            self.column_vars = {}
            
            # Arrange checkboxes in a grid with max 20 rows per column
            max_rows = 20
            for index, col in enumerate(columns):
                var = tk.BooleanVar(value=False)
                self.column_vars[col] = var
                cb = ttk.Checkbutton(
                    self.columns_grid,
                    text=col,
                    variable=var,
                    onvalue=True,
                    offvalue=False
                )
                # Calculate row and column: each column holds up to 20 rows
                row = index % max_rows
                column = index // max_rows
                cb.grid(row=row, column=column, sticky=tk.W, padx=5, pady=2)
            
            # Add select all/none buttons below the grid
            # Calculate the number of columns needed
            num_columns = (len(columns) + max_rows - 1) // max_rows
            btn_frame = ttk.Frame(self.columns_grid)
            btn_frame.grid(row=max_rows, column=0, columnspan=num_columns, pady=5, sticky=tk.EW)
            
            ttk.Button(
                btn_frame,
                text="Select All",
                command=lambda: self.toggle_all_columns(True)
            ).pack(side=tk.LEFT, padx=5)
            
            ttk.Button(
                btn_frame,
                text="Deselect All",
                command=lambda: self.toggle_all_columns(False)
            ).pack(side=tk.LEFT, padx=5)
            
        except Exception as e:
            logging.error(f"Error loading columns: {str(e)}")
            ttk.Label(self.columns_grid, text=f"Error loading columns: {str(e)}").grid(row=0, column=0, columnspan=1, pady=10)
    
    def toggle_all_columns(self, state: bool):
        """Toggle all column checkboxes to specified state"""
        for var in self.column_vars.values():
            var.set(state)
    
    def trim_spaces(self):
        """Remove extra whitespace from selected columns with validation"""
        if not self.selected_table or not self.column_vars:
            messagebox.showerror("Error", "No table or columns selected")
            return
        
        selected_columns = [col for col, var in self.column_vars.items() if var.get()]
        
        if not selected_columns:
            messagebox.showerror("Error", "No columns selected")
            return
        
        available_columns = self.db_handler.get_columns(self.selected_table)
        invalid_columns = [col for col in selected_columns if col not in available_columns]
        if invalid_columns:
            messagebox.showerror("Error", f"The following columns were not found in the table: {', '.join(invalid_columns)}")
            return
        
        confirm = messagebox.askyesno(
            "Confirm",
            f"Do you want to remove extra spaces from {len(selected_columns)} columns in the table?"
        )
        
        if not confirm:
            return
        
        try:
            success_count = 0
            for column in selected_columns:
                try:
                    safe_table = self._escape_identifier(self.selected_table)
                    safe_column = self._escape_identifier(column)
                    
                    query_empty = f"""
                    UPDATE {safe_table} 
                    SET {safe_column} = ''
                    WHERE {safe_column} IS NOT NULL 
                    AND TRIM({safe_column}) = ''
                    """
                    
                    query_trim = f"""
                    UPDATE {safe_table} 
                    SET {safe_column} = TRIM({safe_column})
                    WHERE {safe_column} IS NOT NULL 
                    AND TRIM({safe_column}) != ''
                    AND ({safe_column} LIKE ' %' OR {safe_column} LIKE '% ')
                    """
                    
                    affected_rows_empty = self.db_handler.execute_query(query_empty)
                    if affected_rows_empty is not False:
                        logging.info(f"Column {column}: {affected_rows_empty} empty rows cleared")
                    
                    affected_rows_trim = self.db_handler.execute_query(query_trim)
                    if affected_rows_trim is not False:
                        success_count += 1
                        logging.info(f"Column {column}: {affected_rows_trim} rows with extra spaces trimmed")
                    
                except Exception as e:
                    logging.error(f"Error trimming column {column}: {str(e)}")
                    continue
            
            messagebox.showinfo(
                "Completed",
                f"Operation completed: {success_count}/{len(selected_columns)} columns processed"
            )
            
        except Exception as e:
            logging.error(f"Trim operation failed: {str(e)}")
            messagebox.showerror("Error", f"Operation failed: {str(e)}")

    def _escape_identifier(self, identifier):
        """Properly escape identifiers for MariaDB"""
        return f"`{identifier}`"