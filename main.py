import tkinter as tk
from tkinter import ttk, messagebox
from tabs.tab1 import Tab1
from tabs.tab2 import Tab2
from tabs.tab3 import Tab3
from tabs.tab4 import Tab4
from tabs.tab5 import Tab5
from config.config import ConfigManager
from services.database_service import DatabaseService
from services.email_notifier import EmailNotifier

class MainApplication(tk.Tk):
    def __init__(self):
        super().__init__()
        
        # ابتدا ConfigManager را مقداردهی کنید
        self.config_manager = ConfigManager()
        
        # تنظیمات اولیه پنجره
        self.title("CSV File Uploader")
        self.geometry("1000x700")
        self.minsize(800, 800)
        
        # مدیریت تنظیمات و سرویس‌ها
        self._initialize_services()
        
        # ایجاد رابط کاربری
        self._create_ui()
        
        # منوی برنامه
        self._create_menu()
    
    def _initialize_services(self):
        """مقداردهی اولیه سرویس‌ها با مدیریت خطا"""
        try:
            # تنظیمات MySQL
            mysql_config = {
                'host': self.config_manager.get_setting('mysql', 'mysql_server'),
                'database': self.config_manager.get_setting('mysql', 'mysql_db'),
                'user': self.config_manager.get_setting('mysql', 'mysql_user'),
                'password': self.config_manager.get_setting('mysql', 'mysql_pass'),
                'port': self.config_manager.get_setting('mysql', 'mysql_port')
            }
            self.db_handler = DatabaseService(**mysql_config)
            
            # تنظیمات SMTP/Email
            smtp_config = {
                'smtp_server': self.config_manager.get_setting('smtp', 'smtp_server'),
                'smtp_port': self.config_manager.get_setting('smtp', 'smtp_port'),
                'sender_email': self.config_manager.get_setting('smtp', 'sender_email'),
                'sender_password': self.config_manager.get_setting('smtp', 'sender_password')
            }
            self.email_notifier = EmailNotifier(**smtp_config)
            
        except Exception as e:
            messagebox.showerror(
                "Initialization Error",
                f"Failed to initialize services:\n{str(e)}"
            )
            raise
    
    def _create_ui(self):
        """ایجاد رابط کاربری اصلی"""
        # ایجاد نوت‌بوک (تب‌ها)
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(expand=True, fill='both', padx=10, pady=10)
        
        # ایجاد تب‌ها و انتقال سرویس‌های لازم به هر تب
        self.tabs = {
            'upload': Tab1(self.notebook, self.config_manager, self.db_handler, self.email_notifier),
            'log': Tab2(self.notebook, self.config_manager, self.db_handler),
            'mails': Tab3(self.notebook, self.config_manager, self.email_notifier),
            'monitoring': Tab4(self.notebook, self.config_manager),
            'options': Tab5(self.notebook, self.config_manager, self.db_handler)
        }
        
        # اضافه کردن تب‌ها به نوت‌بوک
        self.notebook.add(self.tabs['upload'], text="Upload CSV")
        self.notebook.add(self.tabs['log'], text="Log Viewer")
        self.notebook.add(self.tabs['mails'], text="Email Settings")
        self.notebook.add(self.tabs['monitoring'], text="System Monitoring")
        self.notebook.add(self.tabs['options'], text="Database Tools")
    
    def _create_menu(self):
        """ایجاد منوی برنامه"""
        menubar = tk.Menu(self)
        
        # منوی File
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Refresh", command=self._refresh_app)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.quit)
        menubar.add_cascade(label="File", menu=file_menu)
        
        # منوی Help
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self._show_about)
        menubar.add_cascade(label="Help", menu=help_menu)
        
        self.config(menu=menubar)
    
    def _refresh_app(self):
        """بارگذاری مجدد تنظیمات و رفرش برنامه"""
        self.config_manager.load_settings()
        for tab in self.tabs.values():
            if hasattr(tab, 'refresh'):
                tab.refresh()
    
    def _show_about(self):
        """نمایش اطلاعات برنامه"""
        about_window = tk.Toplevel(self)
        about_window.title("About")
        about_window.geometry("300x200")
        
        tk.Label(about_window, text="CSV File Uploader\n\nVersion 1.0").pack(pady=20)
        tk.Button(about_window, text="OK", command=about_window.destroy).pack(pady=10)

if __name__ == "__main__":
    app = MainApplication()
    app.mainloop()