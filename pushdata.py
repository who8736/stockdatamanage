import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
import datetime as dt

import config
from initlog import initlog


def push(title, filename):
    """发送邮件
    :param title: 邮件标题
    :param filename: 附件名称，文件存放在data文件夹
    :return:
    """
    cf = config.Config()
    user = cf.mailUser
    receivers = [f'aa<{s}>' for s in cf.sendTo.split('|')]
    # receivers = [f'{s}' for s in cf.sendTo.split('|')]
    sendTime = dt.datetime.now().strftime('%Y%m%d %H:%M:%S')

    message = MIMEMultipart()
    message['From'] = f'aa<{user}>'
    message['To'] = ';'.join(receivers)
    message.attach(MIMEText(title, 'plain', 'utf-8'))

    subject = f'{title} {sendTime}'
    message['Subject'] = Header(subject, 'utf-8')

    # 增加附件
    f = open(os.path.join('data', filename), 'rb').read()
    att = MIMEText(f, 'base64', 'utf-8')
    att['Content-Type'] = 'application/octet-stream'
    att['Content-Disposition'] = f'attachment; filename="{filename}"'
    message.attach(att)

    server = cf.mailServer
    port = cf.mailPort
    password = cf.mailPassword
    try:
        serv = smtplib.SMTP(server, port)
        serv.login(user, password)
        serv.sendmail(user, receivers, message.as_string())
        # serv.sendmail(user, ';'.join(receivers), message.as_string())
        serv.quit()
        print('邮件发送成功')
        logging.debug(f'邮件发送成功: {title}')
    except Exception as e:
        print(e)


def push1(title, filename):
    """发送邮件
    :param title: 邮件标题
    :param filename: 附件名称，文件存放在data文件夹
    :return:
    """
    cf = config.Config()
    user = cf.mailUser
    receivers = [f'aa<{s}>' for s in cf.sendTo.split('|')]
    # receivers = [f'{s}' for s in cf.sendTo.split('|')]
    sendTime = dt.datetime.now().strftime('%Y%m%d %H:%M:%S')

    message = MIMEMultipart()
    message['From'] = f'aa<{user}>'
    message['To'] = ';'.join(receivers)
    message.attach(MIMEText(title, 'plain', 'utf-8'))

    subject = f'{title} {sendTime}'
    message['Subject'] = Header(subject, 'utf-8')

    # 增加附件
    f = open(os.path.join('data', filename), 'rb').read()
    att = MIMEText(f, 'base64', 'utf-8')
    att['Content-Type'] = 'application/octet-stream'
    att['Content-Disposition'] = f'attachment; filename="{filename}"'
    message.attach(att)

    server = cf.mailServer
    port = cf.mailPort
    password = cf.mailPassword
    try:
        serv = smtplib.SMTP(server, port)
        serv.login(user, password)
        serv.sendmail(user, receivers, message.as_string())
        # serv.sendmail(user, ';'.join(receivers), message.as_string())
        serv.quit()
        print('邮件发送成功')
        logging.debug(f'邮件发送成功: {title}')
    except Exception as e:
        print(e)
        logging.error('邮件发送失败: ', e)


if __name__ == '__main__':
    initlog()
    push('haha mail', 'valuations20200228.xlsx')
