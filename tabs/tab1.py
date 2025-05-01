import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from pathlib import Path
import threading
import mysql.connector
from mysql.connector import Error

class Tab1(ttk.Frame):
    def __init__(self, parent, config_manager, db_handler=None, email_notifier=None):
        super().__init__(parent)
        self.config_manager = config_manager
        self.db_handler = db_handler  # اضافه کردن هندلر دیتابیس
        self.email_notifier = email_notifier  # اضافه کردن نوتیفایر ایمیل
        self.create_widgets()
        self.load_saved_paths()
    
    def create_widgets(self):
        """Create all widgets for the tab"""
        # Tab title
        ttk.Label(self, text="Path and Database Settings", font=('Tahoma', 12, 'bold')).grid(
            row=0, column=0, columnspan=3, pady=10)
        
        # Path settings section
        self.create_path_settings()
        # MySQL settings section
        self.create_mysql_settings()
        # Buttons section
        self.create_buttons()
    
    def create_path_settings(self):
        """Create path settings widgets"""
        ttk.Label(self, text="--- Path Settings ---", font=('Tahoma', 10)).grid(
            row=1, column=0, columnspan=3, pady=5)
        
        path_labels = [
            "Select Source Path 1:",
            "Select Source Path 2:",
            "Select Move Path 1:",
            "Select Move Path 2:",
            "Select Source PDF:",
            "Select Move PDF:",
        ]
        
        self.path_entry_vars = [tk.StringVar() for _ in range(6)]
        
        for i, label_text in enumerate(path_labels):
            ttk.Label(self, text=label_text).grid(
                row=i+2, column=0, padx=5, pady=5, sticky='e')
            
            ttk.Entry(self, textvariable=self.path_entry_vars[i], width=40).grid(
                row=i+2, column=1, padx=5, pady=2)
            
            ttk.Button(self, text="Browse", 
                     command=lambda idx=i: self.browse_directory(idx)).grid(
                row=i+2, column=2, padx=5, pady=2)
    
    def create_mysql_settings(self):
        """Create MySQL settings widgets"""
        ttk.Label(self, text="--- MySQL Database Settings ---", font=('Tahoma', 10)).grid(
            row=8, column=0, columnspan=3, pady=5)
        
        mysql_labels = [
            "1. MySQL Server URL/IP:",
            "2. Database Name:",
            "3. Username:",
            "4. Password:",
            "5. Port:"
        ]
        
        self.mysql_entry_vars = [tk.StringVar() for _ in range(5)]
        
        for i, label_text in enumerate(mysql_labels):
            ttk.Label(self, text=label_text).grid(
                row=i+9, column=0, padx=5, pady=2, sticky='e')
            
            entry = ttk.Entry(self, textvariable=self.mysql_entry_vars[i], width=40)
            if i == 3:  # Password field
                entry.config(show="*")
            entry.grid(row=i+9, column=1, padx=5, pady=2)
    
    def create_buttons(self):
        """Create action buttons"""
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=14, column=0, columnspan=3, pady=15)
        
        ttk.Button(btn_frame, text="Save Settings", 
                  command=self.save_settings).pack(side='left', padx=10)
        ttk.Button(btn_frame, text="Reset", 
                  command=self.reset_settings).pack(side='left', padx=10)
        ttk.Button(btn_frame, text="Test Connection", 
                  command=self.test_db_connection).pack(side='left', padx=10)
        
        # Connection status label
        self.connection_status = tk.StringVar()
        ttk.Label(self, textvariable=self.connection_status).grid(
            row=15, column=0, columnspan=3, pady=5)
    
    def browse_directory(self, index):
        """Open directory dialog and set entry value"""
        selected_path = filedialog.askdirectory()
        if selected_path:
            self.path_entry_vars[index].set(selected_path)
    
    def load_saved_paths(self):
        """Load saved paths from config"""
        # Load paths
        path_keys = ['path1', 'path2', 'move_path1', 'move_path2', 'source_pdf', 'move_pdf']
        for i, key in enumerate(path_keys):
            saved_value = self.config_manager.get_setting('paths', key)
            if saved_value:
                self.path_entry_vars[i].set(saved_value)
        
        # Load MySQL settings
        mysql_keys = ['mysql_server', 'mysql_db', 'mysql_user', 'mysql_pass', 'mysql_port']
        for i, key in enumerate(mysql_keys):
            saved_value = self.config_manager.get_setting('mysql', key)
            if saved_value:
                self.mysql_entry_vars[i].set(saved_value)
    
    def save_settings(self):
        """Save all settings to config"""
        try:
            # Save paths
            path_data = {
                'path1': self.path_entry_vars[0].get(),
                'path2': self.path_entry_vars[1].get(),
                'move_path1': self.path_entry_vars[2].get(),
                'move_path2': self.path_entry_vars[3].get(),
                'source_pdf': self.path_entry_vars[4].get(),
                'move_pdf': self.path_entry_vars[5].get()
            }
            
            # Save MySQL settings
            mysql_data = {
                'mysql_server': self.mysql_entry_vars[0].get(),
                'mysql_db': self.mysql_entry_vars[1].get(),
                'mysql_user': self.mysql_entry_vars[2].get(),
                'mysql_pass': self.mysql_entry_vars[3].get(),
                'mysql_port': self.mysql_entry_vars[4].get()
            }
            
            # Save all settings
            for key, value in path_data.items():
                self.config_manager.update_setting('paths', key, value)
            
            for key, value in mysql_data.items():
                self.config_manager.update_setting('mysql', key, value)
            
            messagebox.showinfo("Success", "Settings saved successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save settings: {str(e)}")
    
    def reset_settings(self):
        """Reset all settings to empty"""
        if messagebox.askyesno("Confirm", "Are you sure you want to reset all settings?"):
            for var in self.path_entry_vars + self.mysql_entry_vars:
                var.set('')
            messagebox.showinfo("Reset", "All settings have been reset")

    def test_db_connection(self):
        """Test database connection in background thread"""
        # Validate inputs first
        if not all(self.mysql_entry_vars[i].get() for i in range(4)):  # Skip port check
            messagebox.showwarning("Warning", "Please fill all MySQL settings first")
            return
        
        self.connection_status.set("Testing connection...")
        
        # Get connection parameters
        connection_params = {
            'host': self.mysql_entry_vars[0].get(),
            'database': self.mysql_entry_vars[1].get(),
            'user': self.mysql_entry_vars[2].get(),
            'password': self.mysql_entry_vars[3].get(),
            'port': self.mysql_entry_vars[4].get() or '3306'
        }
        
        # Run test in background
        threading.Thread(
            target=self._perform_connection_test,
            args=(connection_params,),
            daemon=True
        ).start()
    
    def _perform_connection_test(self, params):
        """Perform actual connection test"""
        try:
            connection = mysql.connector.connect(
                host=params['host'],
                database=params['database'],
                user=params['user'],
                password=params['password'],
                port=int(params['port'])
            )
            
            if connection.is_connected():
                server_info = (
                    f"Server: {connection.server_host}:{connection.server_port}\n"
                    f"Version: {connection.get_server_info()}"
                )
                self._show_connection_result(True, f"Connection successful!\n{server_info}")
            else:
                self._show_connection_result(False, "Connection failed")
        except Error as e:
            self._show_connection_result(False, f"Connection failed!\nError: {str(e)}")
        finally:
            if 'connection' in locals() and connection.is_connected():
                connection.close()
    
    def _show_connection_result(self, success, message):
        """Show connection result in UI thread"""
        self.after(0, lambda: self._update_connection_status(success, message))
    
    def _update_connection_status(self, success, message):
        """Update UI with connection status"""
        status = "Connected successfully" if success else "Connection failed"
        self.connection_status.set(status)
        
        if success:
            messagebox.showinfo("Success", message)
        else:
            messagebox.showerror("Error", message)