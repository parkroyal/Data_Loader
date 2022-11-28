# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import json
import smtplib
import mimetypes
import os
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from package.logging_helper import logger 
import time

with open("config.json", "r") as f:
    config = json.load(f)

# %%
def send_email_by_mode(recepient, emailcc, attachment_paths, subject, mode="auto", file_type="files", content=None,
                       content_type="html"):
    if not isinstance(recepient, list):
        recepient = [recepient]
    if not isinstance(emailcc, list):
        emailcc = [emailcc]
    if not isinstance(attachment_paths, list):
        attachment_paths = [attachment_paths]

    emailfrom = config["emails"][mode]["user_name"]
    emailto = recepient + emailcc
    username = config["emails"][mode]["user_name"]
    password = config["emails"][mode]["password"]

    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(recepient)
    msg["Cc"] = ",".join(emailcc)
    msg["Subject"] = subject

    if content is not None:
        msg.attach(MIMEText(content, content_type))

    if file_type == "files":
        for file_path in attachment_paths:
            ctype, encoding = mimetypes.guess_type(file_path)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"
            maintype, subtype = ctype.split("/", 1)
            try:
                with open(file_path, 'rb') as f:
                    part = MIMEBase(maintype, subtype)
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                    # print os.path.basename(file_path)
                    logger.info(f"success: upload the file {file_path}")
                    msg.attach(part)
            except IOError:
                logger.exception(f"error: Can't open the file {file_path}")
                raise Exception(f"error: Can't open the file {file_path}")
    elif file_type == "zip":
        with open(attachment_paths, 'rb') as fin:
            data = fin.read()

            part = MIMEBase('application', 'octet-stream')
            part.set_payload(data)
            encoders.encode_base64(part)

            part.add_header('Content-Disposition', 'attachment; filename="%s"' % attachment_paths)
            msg.attach(part)

    if mode in ['opl']:
        server = smtplib.SMTP("smtpout.secureserver.net", 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
    # elif mode = 'pug':
    #     server = smtplib.SMTP("puprime-com.mail.protection.outlook.com")
    elif mode in ['pug']:
        smtp_server = "puprime-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        #server.login(username, password)
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())
        #server.quit()

    elif mode in ['vfx']:
        smtp_server = "vantagemarkets-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())
    elif mode in ['vt']:
        smtp_server = "vtmarkets-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ['iv']:
        smtp_server = "startrader-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ['test']:
        # smtp_server = "vantagemarkets-com.mail.protection.outlook.com"
        # smtp.mandrillapp.com
        smtp = smtplib.SMTP("smtp.mandrillapp.com", 587)  # googl的ping
        smtp.ehlo()  # 申請身分
        smtp.starttls()  # 加密文件，避免私密信息被截取
        smtp.login("HYTECH S TECHNOLOGY PTY LTD", "wYgWr_uzlH-4ufaT9Q8rYA")
        mime = MIMEText('test', "plain", "utf-8")
        # msg = mine
        smtp.sendmail('statistics@vantagemarkets.com', [emailto], mime.as_string())
        # smtp.sendmail('steven.su@unicornfintech.com', ['steven.su@unicornfintech.com'], msg.as_string())

    elif mode in ['test2']:
        email_text = 'test'
        emailfrom = 'statistics.su@unicornfintech.com'
        emailto = 'statistics@vantagemarkets.com'
        mime = MIMEText(email_text, "plain", "utf-8")  # 撰寫內文內容，以及指定格式為plain，語言為中文
        mime["Subject"] = "test"  # 撰寫郵件標題
        mime["From"] = emailfrom  # 撰寫你的暱稱或是信箱
        mime["To"] = emailto  # 撰寫你要寄的人
        msg = mime.as_string()  # 將msg將text轉成str
        smtp = smtplib.SMTP("smtp.mandrillapp.com", 587)  # googl的ping
        smtp.ehlo()  # 申請身分
        smtp.starttls()  # 加密文件，避免私密信息被截取
        smtp.login("HYTECH S TECHNOLOGY PTY LTD", "wYgWr_uzlH-4ufaT9Q8rYA")
        # smtp.login("statistics@unicornfintech.com", "Taipei1234")
        # from_addr = 'statistics@vantagemarkets.com'
        # to_addr = ['steven.su@unicornfintech.com']
        status = smtp.sendmail(emailfrom, [emailto], msg)
        if status == {}:
            print("郵件傳送成功!")
        else:
            print("郵件傳送失敗!")
        smtp.quit()
    else:
        # 原先登入帳密寄信方式
        server = smtplib.SMTP("smtp.office365.com", 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
    
    time.sleep(10)



def send_email_test(recepient, emailcc, attachment_paths, subject, mode="auto", file_type="files", content=None,
                       content_type="html"):
    if not isinstance(recepient, list):
        recepient = [recepient]
    if not isinstance(emailcc, list):
        emailcc = [emailcc]
    if not isinstance(attachment_paths, list):
        attachment_paths = [attachment_paths]

    emailfrom = config["emails"][mode]["user_name"]
    emailto = recepient + emailcc
    username = config["emails"][mode]["user_name"]
    password = config["emails"][mode]["password"]

    msg = MIMEMultipart()
    msg["From"] = emailfrom
    msg["To"] = ",".join(recepient)
    msg["Cc"] = ",".join(emailcc)
    msg["Subject"] = subject

    if content is not None:
        msg.attach(MIMEText(content, content_type))

    if file_type == "files":
        for file_path in attachment_paths:
            ctype, encoding = mimetypes.guess_type(file_path)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"
            maintype, subtype = ctype.split("/", 1)
            try:
                with open(file_path, 'rb') as f:
                    part = MIMEBase(maintype, subtype)
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(file_path))
                    # print os.path.basename(file_path)
                    logger.info(f"success: upload the file {file_path}")
                    msg.attach(part)
            except IOError:
                logger.exception(f"error: Can't open the file {file_path}")
                raise Exception(f"error: Can't open the file {file_path}")
    elif file_type == "zip":
        with open(attachment_paths, 'rb') as fin:
            data = fin.read()

            part = MIMEBase('application', 'octet-stream')
            part.set_payload(data)
            encoders.encode_base64(part)

            part.add_header('Content-Disposition', 'attachment; filename="%s"' % attachment_paths)
            msg.attach(part)

    if mode in ['opl']:
        server = smtplib.SMTP("smtpout.secureserver.net", 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
    # elif mode = 'pug':
    #     server = smtplib.SMTP("puprime-com.mail.protection.outlook.com")
    elif mode in ['pug']:
        smtp_server = "puprime-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        #server.login(username, password)
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())
        #server.quit()

    elif mode in ['vfx']:
        smtp_server = "vantagemarkets-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())
    elif mode in ['vt']:
        smtp_server = "vtmarkets-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())

    elif mode in ['iv']:
        smtp_server = "startrader-com.mail.protection.outlook.com"
        server = smtplib.SMTP(smtp_server)
        server.connect(smtp_server, 25)
        server.starttls()
        server.ehlo()
        server.sendmail(emailfrom, emailto, msg.as_string())
    
    elif mode in ['test']:
        # smtp_server = "vantagemarkets-com.mail.protection.outlook.com"
        # smtp.mandrillapp.com
        smtp = smtplib.SMTP("smtp.mandrillapp.com", 587)  
        smtp.ehlo()  
        smtp.starttls()  
        smtp.login("HYTECH S TECHNOLOGY PTY LTD", "wYgWr_uzlH-4ufaT9Q8rYA")
        server.sendmail('mt4@vantagemarkets.com', emailto, msg.as_string())
    
    else:
        # 原先登入帳密寄信方式
        server = smtplib.SMTP("smtp.office365.com", 587)
        server.starttls()
        server.login(username, password)
        server.sendmail(emailfrom, emailto, msg.as_string())
        server.quit()
    
    time.sleep(10)
