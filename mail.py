#! /usr/bin/env python
#-*- coding:utf-8 -*-
#
# Author: ye.zhiqin@outlook.com
# Date  : 2017/8/25

import logging
import string
import smtplib   
from email.header import Header
from email.mime.text import MIMEText

class Mail:
    def __init__(self, receiver, subject, content):
        logging.basicConfig(level=logging.DEBUG)
        self.server = '127.0.0.1'
        self.port = 25
        self.username=''
        self.password=''
        self.sender='db2zk@domain.com'
        self.receiver = receiver
        self.subject = subject
        self.content = content
        self.body = ""

    def prepare(self):
        for item in self.content:
            self.body += "<p>%s</p>" % item
            self.body += "<br />"

    def send(self):
        self.prepare()

        message = MIMEText(self.body, 'html', 'utf-8')
        message['From'] = self.sender
        message['To'] = self.receiver
        message['Subject'] = Header(self.subject, 'utf-8')

        try:
            smtpSrv = smtplib.SMTP()
            smtpSrv.connect(self.server, self.port)
            smtpSrv.helo()
            #smtpSrv.login(self.username, self.password)
            smtpSrv.sendmail(self.sender, string.splitfields(self.receiver, ","), message.as_string())
            smtpSrv.quit()
            logging.info("Email Success")
        except smtplib.SMTPException:
            logging.info("Email Error")

# __main__
if __name__=='__main__':
    receiver = "yourname@domain.com"
    subject = "DB2ZK Warning"
    content = ["====1====", "====2====", "====3===="]
    mail = Mail(receiver, subject, content)
    mail.send()
