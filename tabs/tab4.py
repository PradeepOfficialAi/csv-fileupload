import tkinter as tk
from tkinter import ttk
from services.folder_monitor import FolderMonitor
from services.logger import Logger
from tkinter import ttk, messagebox

class Tab4(ttk.Frame):

    def __init__(self, parent, config_manager):
        super().__init__(parent)
        self.config_manager = config_manager
        self.logger = Logger("Tab4")
        self.monitor = None
        self.create_widgets()
        self.load_settings()
        self.start_monitoring()

    def create_widgets(self):
        # عنوان
        ttk.Label(self, text="Automatic Folder Monitoring", font=('Tahoma', 12, 'bold')).pack(pady=10)
        
        # وضعیت مانیتورینگ
        self.status_var = tk.StringVar(value="Status: Not running")
        ttk.Label(self, textvariable=self.status_var).pack(pady=5)
        
        # تنظیمات مانیتورینگ
        settings_frame = ttk.LabelFrame(self, text="Monitoring Settings")
        settings_frame.pack(pady=10, padx=10, fill='x')
        
        ttk.Label(settings_frame, text="Check Interval (seconds):").grid(row=0, column=0, padx=5, pady=5, sticky='e')
        self.interval_var = tk.IntVar(value=30)  # 5 دقیقه پیش‌فرض
        ttk.Entry(settings_frame, textvariable=self.interval_var, width=10).grid(row=0, column=1, padx=5, pady=5, sticky='w')
        
        # دکمه‌های کنترل
        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)
        
        self.start_btn = ttk.Button(btn_frame, text="Start Monitoring", command=self.start_monitoring)
        self.start_btn.pack(side='left', padx=5)
        
        self.stop_btn = ttk.Button(btn_frame, text="Stop Monitoring", command=self.stop_monitoring, state='disabled')
        self.stop_btn.pack(side='left', padx=5)
        
        # نمایش لاگ
        log_frame = ttk.LabelFrame(self, text="Monitoring Log")
        log_frame.pack(pady=10, padx=10, fill='both', expand=True)
        
        self.log_text = tk.Text(log_frame, height=10, state='disabled')
        self.log_text.pack(fill='both', expand=True, padx=5, pady=5)
        
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scrollbar.pack(side='right', fill='y')
        self.log_text['yscrollcommand'] = scrollbar.set

    def start_monitoring(self):
        if not self.monitor:
            try:
                # ذخیره تنظیمات interval
                self.config_manager.update_setting(
                    'monitoring', 'interval', 
                    str(self.interval_var.get()))
                
                # ایجاد نمونه FolderMonitor با تنظیمات صحیح
                self.monitor = FolderMonitor(self.config_manager)
                self.monitor.start(self.interval_var.get())
                
                # به‌روزرسانی UI
                self.start_btn.config(state='disabled')
                self.stop_btn.config(state='normal')
                self.status_var.set("Status: Running")
                self.log_message(f"Monitoring started (Interval: {self.interval_var.get()}s)")
                
            except Exception as e:
                self.log_message(f"ERROR: {str(e)}")
                messagebox.showerror(
                    "Start Failed",
                    f"Failed to start monitoring:\n{str(e)}"
                )

    def stop_monitoring(self):
        if self.monitor:
            self.monitor.stop()
            self.monitor = None
            
            self.start_btn.config(state='normal')
            self.stop_btn.config(state='disabled')
            self.status_var.set("Status: Stopped")
            
            self.log_message("Monitoring service stopped")

    def load_settings(self):
        """بارگذاری تنظیمات از config"""
        try:
            self.interval_var.set(
                int(self.config_manager.get_setting('monitoring', 'interval', 30)))
        except:
            self.interval_var.set(30)  # مقدار پیش‌فرض

    def save_settings(self):
        """ذخیره تنظیمات در config"""
        self.config_manager.update_setting(
            'monitoring', 'interval', 
            str(self.interval_var.get()))


    def log_message(self, message):
        self.log_text.config(state='normal')
        self.log_text.insert('end', message + "\n")
        self.log_text.see('end')
        self.log_text.config(state='disabled')