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

    def notify_duplicate(self, table_name, duplicates, key_field):
        """Notify about duplicate orders (both order and sealed_unit_id match)"""
        if not self.enabled:
            return

        recipients = self.get_recipients_for_table(table_name)
        if not recipients:
            self.logger.info(f"No recipients configured for table: {table_name}")
            return

        table_display_name = 'Glass' if 'glass' in table_name.lower() else 'Frame'
        
        subject = f"🔴 DUPLICATE {table_display_name} Order Alert {datetime.now().strftime('[%Y-%m-%d %I:%M %p]')}"
        
        # Format duplicates for email body
        formatted_duplicates = []
        for dup in duplicates:
            formatted_duplicates.append(
                f"Order: {dup['order']}, Sealed Unit ID: {dup['sealed_unit_id']}, "
                f"Original Date: {dup['original_date']}"
            )
            
        body = f"""
        <html>
        <body>
            <h2>Duplicate {table_display_name} Orders Detected</h2>
            <p>The following orders were identified as duplicates (both order number and sealed unit ID match existing records):</p>
            <ul>
                {"".join(f"<li>{item}</li>" for item in formatted_duplicates)}
            </ul>
            <p>Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </body>
        </html>
        """

        self._send_email(recipients, subject, body)

    def notify_resend(self, table_name, resends, key_field):
        """Notify about re-sent orders (order number match only)"""
        if not self.enabled:
            return

        recipients = self.get_recipients_for_table(table_name)
        if not recipients:
            self.logger.info(f"No recipients configured for table: {table_name}")
            return

        table_display_name = 'Glass' if 'glass' in table_name.lower() else 'Frame'
        
        subject = f"⚠️ RE-SENT {table_display_name} Order Notification {datetime.now().strftime('[%Y-%m-%d %I:%M %p]')}"
        
        # Format resends for email body
        formatted_resends = []
        for resend in resends:
            formatted_resends.append(
                f"Order: {resend['order']}, Original Date: {resend['original_date']}"
            )
            
        body = f"""
        <html>
        <body>
            <h2>Re-Sent {table_display_name} Orders Detected</h2>
            <p>The following orders were re-sent (order number matches existing records but with different sealed unit IDs):</p>
            <ul>
                {"".join(f"<li>{item}</li>" for item in formatted_resends)}
            </ul>
            <p>Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </body>
        </html>
        """

        self._send_email(recipients, subject, body)

    def _send_email(self, recipients, subject, body):
        """Send email to recipients"""
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
                    self._send_emails(server, msg, recipients, body)
            else:
                # استفاده از STARTTLS
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                    server.starttls()  # فعال‌سازی TLS
                    server.login(self.sender_email, self.sender_password)
                    self._send_emails(server, msg, recipients, body)
                    
        except Exception as e:
            self.logger.error(f"SMTP connection failed: {str(e)}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")

    def _send_emails(self, server, msg, recipients, body):
        """Send emails to list of recipients"""
        for recipient in recipients:
            try:
                recipient_msg = MIMEMultipart('alternative')
                recipient_msg['From'] = msg['From']
                recipient_msg['Subject'] = msg['Subject']
                recipient_msg['X-Priority'] = msg['X-Priority']
                recipient_msg['To'] = recipient
                
                html_part = MIMEText(body, 'html')
                recipient_msg.attach(html_part)
                
                server.send_message(recipient_msg)
                self.logger.info(f"Notification sent to {recipient}")
            except Exception as e:
                self.logger.error(f"Failed to send email to {recipient}: {str(e)}")

    def get_recipients_for_table(self, table_name):
        """Get email recipients for specific table type"""
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
        """Determine table type based on name"""
        table_name = table_name.lower().strip()
        
        patterns = {
            'frame': ['frame', 'framereport', 'framescutting'],
            'glass': ['glass', 'glassreport', 'glazing'],
            'rush': ['rush', 'urgent']
        }
        
        for table_type, keywords in patterns.items():
            if any(keyword in table_name for keyword in keywords):
                return table_type
        
        return None