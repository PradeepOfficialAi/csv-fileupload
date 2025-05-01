import configparser
from pathlib import Path
import json
class ConfigManager:
    def __init__(self, config_file='config/settings.ini'):
        self.config_file = Path(config_file)
        self.config = configparser.ConfigParser()
        
        # مقادیر پیش‌فرض
        self.defaults = {
            'paths': {
                'path1': str(Path.home() / ''),
                'path2': str(Path.home() / ''),
                'move_path1': str(Path.home() / ''),
                'move_path2': str(Path.home() / ''),
                'source_pdf': str(Path.home() / ''),
                'move_pdf': str(Path.home() / '')
            },
            'mysql': {
                'mysql_server': 'localhost',
                'mysql_db': 'mydatabase',
                'mysql_user': 'admin',
                'mysql_pass': 'password',
                'mysql_port': '3306'
            },
            'emails': {
                'emails': json.dumps([
                    {
                        "email": "admin@example.com",
                        "glass": True,
                        "frame": True,
                        "rush": True
                    }
                ])
            }
        }
        
        self.load_settings()

    def load_settings(self):
        """بارگذاری تنظیمات از فایل یا استفاده از مقادیر پیش‌فرض"""
        if self.config_file.exists():
            self.config.read(self.config_file)
        else:
            # اگر فایل وجود ندارد، مقادیر پیش‌فرض را تنظیم کنید
            self.config.read_dict(self.defaults)
            self.save_settings()

    def save_settings(self):
        """ذخیره تنظیمات در فایل"""
        self.config_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_file, 'w') as f:
            self.config.write(f)

    def get_setting(self, section, key, default=None):
        """دریافت مقدار تنظیم با section و key مشخص"""
        try:
            value = self.config.get(section, key)
            # تبدیل به int اگر مقدار عددی باشد
            if key == 'interval' or key.endswith('_port'):
                return int(value) if value.isdigit() else default
            return value
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default

    def update_setting(self, section, key, value):
        """به‌روزرسانی تنظیمات"""
        if not self.config.has_section(section):
            self.config.add_section(section)
        self.config.set(section, key, str(value))
        self.save_settings()