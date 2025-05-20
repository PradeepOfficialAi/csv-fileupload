import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from dotenv import load_dotenv
import os
import json
import configparser
from pathlib import Path
from services.logger import Logger

class EmailNotifier:
    def __init__(self, smtp_server=None, smtp_port=None, sender_email=None, sender_password=None):
        self.logger = Logger("EmailNotifier")
        load_dotenv()

        # خواندن تنظیمات SMTP از .env
        self.smtp_server = smtp_server or os.getenv('SMTP_SERVER')
        self.smtp_port = int(smtp_port or os.getenv('SMTP_PORT', 465))
        self.sender_email = sender_email or os.getenv('SENDER_EMAIL')
        self.sender_password = sender_password or os.getenv('SENDER_PASSWORD')

        # خواندن تنظیمات ایمیل‌ها از فایل settings.txt
        self.email_settings = self._load_email_settings()
        
        if not all([self.smtp_server, self.smtp_port, self.sender_email, self.sender_password]):
            self.logger.error("Incomplete SMTP configuration! Email notifications will be disabled.")
            self.enabled = False
        else:
            self.enabled = True

    def _load_email_settings(self):
        """بارگذاری تنظیمات ایمیل از فایل settings.ini"""
        try:
            # خواندن فایل تنظیمات
            config = configparser.ConfigParser()
            config.read(Path('config/settings.ini'))
            
            # بررسی وجود بخش emails
            if 'emails' in config and 'emails' in config['emails']:
                email_data = config['emails']['emails']
                
                # حذف فاصله‌های اضافی و خطوط جدید
                email_data = ' '.join(email_data.split())
                
                # تبدیل JSON به لیست
                try:
                    email_list = json.loads(email_data)
                    if not isinstance(email_list, list):
                        self.logger.warning("Email settings is not a list, converting to list")
                        email_list = [email_list]
                    return email_list
                except json.JSONDecodeError as e:
                    self.logger.error(f"Invalid JSON format in email settings: {str(e)}")
                    return []
            return []
        
        except Exception as e:
            self.logger.error(f"Failed to load email settings: {str(e)}")
            return []

    def notify_duplicate(self, table_name, duplicates_list,type):
        if not self.enabled:
            return

        recipients = self.get_recipients_for_table(table_name)
        if not recipients:
            self.logger.info(f"No recipients configured for table: {table_name}")
            return

        if table_name == 'glassreport':
            new_table = 'glass'
        elif table_name == 'framescutting':
            new_table = 'frame'
        if type == "id":
            subject = f"🔴 Alert! duplicate {new_table} order {datetime.now().strftime('[ %Y-%m-%d %I:%M %p] ')}"
        elif type == "order":
            subject = f"Alert! Re send {new_table} order {datetime.now().strftime('[ %Y-%m-%d %I:%M %p]')}"
        else:
            subject = f"Alert! 🔴"

        # تبدیل لیست به فرمت مورد نظر
        formatted_duplicates = ", ".join(
            f"[{item[0]}, {item[1]}]" for item in duplicates_list
        )
            
        body = f"""
        <html>
        <body>
            <p>
                {formatted_duplicates} {table_name} send to cut {datetime.now().strftime('%Y-%m-%d')}
            </p>
        </body>
        </html>
        """

        msg = MIMEMultipart('alternative')
        msg['From'] = f"VinylPro Notifications <{self.sender_email}>"
        msg['Subject'] = subject
        msg['X-Priority'] = '1'

        try:
            # روش امن‌تر با تشخیص خودکار پروتکل
            if self.smtp_port == 465:
                # استفاده از SSL
                with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                    server.login(self.sender_email, self.sender_password)
                    self._send_emails(server, msg, recipients,body)
            else:
                # استفاده از STARTTLS
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                    server.starttls()  # فعال‌سازی TLS
                    server.login(self.sender_email, self.sender_password)
                    self._send_emails(server, msg, recipients,body)
                    
        except Exception as e:
            self.logger.error(f"SMTP connection failed: {str(e)}")
            # برای دیباگ می‌توانید traceback کامل را هم لاگ کنید
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")

    def _send_emails(self, server, msg, recipients, body):
        """ارسال ایمیل‌ها به لیست دریافت‌کنندگان"""
        for recipient in recipients:
            try:
                # Create a new message for each recipient to avoid header issues
                recipient_msg = MIMEMultipart('alternative')
                recipient_msg['From'] = msg['From']
                recipient_msg['Subject'] = msg['Subject']
                recipient_msg['X-Priority'] = msg['X-Priority']
                recipient_msg['To'] = recipient
                
                # Attach the HTML body
                html_part = MIMEText(body, 'html')
                recipient_msg.attach(html_part)
                
                server.send_message(recipient_msg)
                self.logger.info(f"Notification sent to {recipient}")
            except Exception as e:
                self.logger.error(f"Failed to send email to {recipient}: {str(e)}")


    def get_recipients_for_table(self, table_name):
        """دریافت لیست ایمیل‌های مربوط به نوع جدول با لاگ‌گیری دقیق"""
        recipients = []
        table_type = self._determine_table_type(table_name)
        if not table_type:
            self.logger.warning(f"No table type matched for: {table_name}")
            return recipients
        
        self.logger.debug(f"Checking emails for table type: {table_type}")
        
        for email_config in self.email_settings:
            if email_config.get(table_type, False):
                recipients.append(email_config['email'])
                self.logger.debug(f"Added recipient: {email_config['email']} for {table_type}")
        
        if not recipients:
            self.logger.info(f"No recipients configured for table: {table_name} (type: {table_type})")
        else:
            self.logger.info(f"Found {len(recipients)} recipients for {table_name}")
        
        return recipients

    def _determine_table_type(self, table_name):
        """تعیین نوع جدول با تطابق دقیق‌تر"""
        table_name = table_name.lower().strip()
        
        # الگوهای تطابق
        patterns = {
            'frame': ['frame', 'framereport'],
            'glass': ['glass', 'glassreport', 'glazing'],
            'rush': ['rush', 'urgent']
        }
        
        for table_type, keywords in patterns.items():
            if any(keyword in table_name for keyword in keywords):
                return table_type
        
        return None