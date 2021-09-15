import argparse
import json
import re
import smtplib
from email.mime.text import MIMEText
from email.header import Header

# 解析 arguments
parser = argparse.ArgumentParser()
parser.add_argument("-c", help="SMTP credential file path")
parser.add_argument("-log", help="log file path")
parser.add_argument("-regex", help="regex pattern")
parser.add_argument("-title", help="email title")
parser.add_argument("-receiver", help="receiver email address")
args = parser.parse_args()

with open(args.c, 'r', encoding='utf8') as credential_file:
    content = credential_file.read()
    credential = json.loads(content)
    if any(k not in credential for k in ('host', 'port', 'user', 'pass')):
        print("invalid credential file")
        exit(1)

error_content = ''
with open(args.log, 'r', encoding='utf8') as log_file:
    line = log_file.readline()
    while line:
        has_error = re.search(args.regex, line)
        if has_error:
            error_content = error_content + line
        line = log_file.readline()

if len(error_content) == 0:
    exit(0)

sender = 'stock_alert@red.com'
receivers = [args.receiver]
mail_msg = """
<h3>{log}</h3>
<p>{content}</p>
"""
message = MIMEText(mail_msg.format(log=args.log, content=error_content), 'html', 'utf-8')
message['Subject'] = Header(args.title, 'utf-8')

try:
    smtpObj = smtplib.SMTP_SSL(credential['host'], credential['port'])
    # smtpObj.set_debuglevel(True)
    smtpObj.login(credential['user'], credential['pass'])
    smtpObj.sendmail(sender, receivers, message.as_string())
    print('郵件發送成功')
except smtplib.SMTPException as e:
    print(e)
    print('郵件發送失敗')
    exit(1)
