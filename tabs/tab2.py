import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path
import datetime
import re
import logging
from typing import List, Optional, Tuple

class Tab2(ttk.Frame):
    def __init__(self, parent, config_manager, db_handler=None):
        super().__init__(parent)
        self.config_manager = config_manager
        self.log_dir = Path("logs")  # Use Path object for better path handling
        self.current_log_file = None
        self.create_widgets()
        self.load_log_files()
    
    def create_widgets(self):
        """Create all UI widgets"""
        self.create_title()
        self.create_controls_frame()
        self.create_search_frame()
        self.create_log_table()
        self.create_status_bar()
    
    def create_title(self):
        """Create title label"""
        ttk.Label(self, text="Log Viewer", font=('Tahoma', 12, 'bold')).pack(pady=10)
    
    def create_controls_frame(self):
        """Create controls frame with date selection"""
        controls_frame = ttk.Frame(self)
        controls_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(controls_frame, text="Select Date:").pack(side='left', padx=5)
        
        self.date_var = tk.StringVar()
        self.date_dropdown = ttk.Combobox(
            controls_frame, 
            textvariable=self.date_var, 
            state='readonly',
            width=15
        )
        self.date_dropdown.pack(side='left', padx=5)
        self.date_dropdown.bind('<<ComboboxSelected>>', self.on_date_selected)
        
        ttk.Button(
            controls_frame, 
            text="Refresh", 
            command=self.refresh_logs
        ).pack(side='right', padx=5)
        
        ttk.Button(
            controls_frame,
            text="Export to CSV",
            command=self.export_to_csv
        ).pack(side='right', padx=5)
    
    def create_search_frame(self):
        """Create search frame"""
        search_frame = ttk.Frame(self)
        search_frame.pack(fill='x', padx=10, pady=5)
        
        ttk.Label(search_frame, text="Search:").pack(side='left', padx=5)
        
        self.search_var = tk.StringVar()
        self.search_var.trace("w", self.on_search_changed)
        
        search_entry = ttk.Entry(
            search_frame, 
            textvariable=self.search_var, 
            width=40
        )
        search_entry.pack(side='left', padx=5)
        search_entry.bind('<Return>', lambda e: self.on_search_changed())
    
    def create_log_table(self):
        """Create log table with treeview"""
        columns = ("Timestamp", "Module", "Level", "Message")
        self.tree = ttk.Treeview(
            self, 
            columns=columns, 
            show="headings",
            selectmode="extended",
            height=20
        )
        
        # Configure columns
        col_widths = {
            "Timestamp": 150,
            "Module": 100, 
            "Level": 80,
            "Message": 400
        }
        
        for col in columns:
            self.tree.heading(col, text=col, anchor='w')
            self.tree.column(col, width=col_widths[col], stretch=False)
        
        # Add scrollbars
        yscroll = ttk.Scrollbar(self, orient='vertical', command=self.tree.yview)
        xscroll = ttk.Scrollbar(self, orient='horizontal', command=self.tree.xview)
        self.tree.configure(yscrollcommand=yscroll.set, xscrollcommand=xscroll.set)
        
        # Layout
        self.tree.pack(side='left', fill='both', expand=True, padx=5, pady=5)
        yscroll.pack(side='right', fill='y')
        xscroll.pack(side='bottom', fill='x')
        
        # Configure tags for highlighting
        self.tree.tag_configure('ERROR', foreground='red')
        self.tree.tag_configure('WARNING', foreground='orange')
        self.tree.tag_configure('INFO', foreground='green')
        self.tree.tag_configure('match', background='lightyellow')
        
        # Setup context menu
        self.setup_context_menu()
    
    def create_status_bar(self):
        """Create status bar"""
        self.status_var = tk.StringVar(value="Ready")
        status_bar = ttk.Label(
            self,
            textvariable=self.status_var,
            relief='sunken',
            anchor='w',
            padding=5
        )
        status_bar.pack(fill='x', padx=5, pady=5)
    
    def setup_context_menu(self):
        """Setup right-click context menu"""
        self.context_menu = tk.Menu(self, tearoff=0)
        self.context_menu.add_command(
            label="Copy", 
            command=self.copy_selected,
            accelerator="Ctrl+C"
        )
        self.context_menu.add_separator()
        self.context_menu.add_command(
            label="Clear All", 
            command=self.clear_logs
        )
        
        self.tree.bind("<Button-3>", self.show_context_menu)
        self.bind_all("<Control-c>", lambda e: self.copy_selected())
    
    def show_context_menu(self, event):
        """Show context menu on right click"""
        item = self.tree.identify_row(event.y)
        if item:
            self.tree.selection_set(item)
            self.context_menu.post(event.x_root, event.y_root)
    
    def refresh_logs(self):
        """Refresh log files list"""
        self.load_log_files()
        self.status_var.set("Log list refreshed")
    
    def load_log_files(self):
        """Load available log files from log directory"""
        try:
            # Create log directory if it doesn't exist
            self.log_dir.mkdir(exist_ok=True)
            
            # Find all log files
            log_files = sorted(
                self.log_dir.glob("*.log"), 
                key=lambda f: f.stat().st_mtime, 
                reverse=True
            )
            
            # Extract dates from filenames
            dates = []
            for log_file in log_files:
                try:
                    date_str = log_file.stem
                    datetime.datetime.strptime(date_str, '%Y-%m-%d')
                    dates.append(date_str)
                except ValueError:
                    continue
            
            # Update dropdown
            self.date_dropdown['values'] = dates
            
            if dates:
                if not self.date_var.get() or self.date_var.get() not in dates:
                    self.date_var.set(dates[0])
                self.load_log_file(self.log_dir / f"{self.date_var.get()}.log")
            else:
                self.status_var.set("No valid log files found")
                
        except Exception as e:
            logging.error(f"Error loading log files: {e}")
            self.status_var.set(f"Error: {str(e)}")
    
    def load_log_file(self, log_file: Path):
        """Load log entries from specified file"""
        try:
            self.current_log_file = log_file
            self.tree.delete(*self.tree.get_children())
            
            with open(log_file, 'r', encoding='utf-8') as f:
                log_entries = f.readlines()
            
            for entry in log_entries:
                self.parse_and_add_log_entry(entry)
            
            self.status_var.set(
                f"Loaded {len(log_entries)} entries from {log_file.name}"
            )
            
        except Exception as e:
            logging.error(f"Error loading log file {log_file}: {e}")
            self.status_var.set(f"Error loading {log_file.name}")
    
    def parse_and_add_log_entry(self, entry: str) -> Optional[Tuple[str, str, str, str]]:
        """Parse a log entry and add to treeview"""
        # Improved log pattern to handle various formats
        pattern = r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:,\d{3})?) - (\w+) - (\w+) - (.*)$'
        match = re.match(pattern, entry.strip())
        
        if not match:
            return None
        
        timestamp, module, level, message = match.groups()
        # Clean up timestamp (remove milliseconds if present)
        timestamp = timestamp.split(',')[0]
        
        # Add to treeview with appropriate tag for level
        self.tree.insert(
            '', 
            'end', 
            values=(timestamp, module, level, message),
            tags=(level,)
        )
        
        return timestamp, module, level, message
    
    def on_date_selected(self, event=None):
        """Handle date selection change"""
        selected_date = self.date_var.get()
        if selected_date:
            log_file = self.log_dir / f"{selected_date}.log"
            self.load_log_file(log_file)
    
    def on_search_changed(self, *args):
        """Handle search text changes"""
        search_term = self.search_var.get().lower()
        
        if not search_term:
            for item in self.tree.get_children():
                self.tree.item(item, tags=(self.tree.item(item, 'values')[2],))
            return
        
        for item in self.tree.get_children():
            values = [str(v).lower() for v in self.tree.item(item, 'values')]
            if any(search_term in v for v in values):
                current_tag = self.tree.item(item, 'values')[2]
                self.tree.item(item, tags=(current_tag, 'match'))
            else:
                current_tag = self.tree.item(item, 'values')[2]
                self.tree.item(item, tags=(current_tag,))
    
    def copy_selected(self):
        """Copy selected log entries to clipboard"""
        selected_items = self.tree.selection()
        if not selected_items:
            return
        
        text_to_copy = ""
        for item in selected_items:
            values = self.tree.item(item, 'values')
            text_to_copy += " | ".join(values) + "\n"
        
        self.clipboard_clear()
        self.clipboard_append(text_to_copy.strip())
        self.status_var.set(f"Copied {len(selected_items)} entries to clipboard")
    
    def clear_logs(self):
        """Clear displayed log entries"""
        if messagebox.askyesno(
            "Confirm Clear", 
            "Are you sure you want to clear all displayed logs?"
        ):
            self.tree.delete(*self.tree.get_children())
            self.status_var.set("Log display cleared")
    
    def export_to_csv(self):
        """Export current logs to CSV file"""
        if not self.current_log_file:
            messagebox.showwarning("No Data", "No log file is currently loaded")
            return
        
        save_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV Files", "*.csv")],
            initialfile=f"{self.current_log_file.stem}.csv"
        )
        
        if not save_path:
            return
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                # Write header
                f.write("Timestamp,Module,Level,Message\n")
                
                # Write log entries
                for item in self.tree.get_children():
                    values = self.tree.item(item, 'values')
                    # Escape commas in message
                    message = values[3].replace('"', '""')
                    f.write(f'{values[0]},"{values[1]}","{values[2]}","{message}"\n')
            
            self.status_var.set(f"Exported logs to {Path(save_path).name}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {str(e)}")
            self.status_var.set("Export failed")