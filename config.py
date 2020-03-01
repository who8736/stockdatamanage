# -*- coding: utf-8 -*-
"""
Created on 2016年12月14日

@author: who8736
"""
import os
import configparser

SECRETKEY = 'Sm9obiBTY2hyb20ga2lja3MgYXNz'

SQLUSER = ''
SQLPASSWORD = ''
SQLIP = '127.0.0.1'
TUSHARETOKEN = ''

MAILSERVER = 'smtp.163.com'
MAILPORT = '25'  # 设为空表示不需指定端口
MAILUSER = 'test@163.com'
MAILPASSWORD = 'testpassword'
SENDTO = 'test@qq.com|test1@qq.com'


class Config:
    def __init__(self):
        self.secretKey = ''
        self.sqlUser = ''
        self.sqlPassword = ''
        self.sqlIp = ''
        self.tushareToken = ''
        self.mailServer = ''
        self.mailPort = ''
        self.mailUser = ''
        self.mailPassword = ''
        self.sendTo = ''
        self.cf = configparser.ConfigParser()

        if not os.path.isfile('stockdata.conf'):
            self.initConfig()
        self.readConfig()

    def initConfig(self):
        # add section / set option & key
        self.cf.add_section('main')
        self.cf.set('main', 'secretKey', SECRETKEY)

        self.cf.add_section('sql')
        self.cf.set('sql', 'sqlUser', SQLUSER)
        self.cf.set('sql', 'sqlPassword', SQLPASSWORD)
        self.cf.set('sql', 'sqlIp', SQLIP)
        self.cf.set('sql', 'tushareToken', TUSHARETOKEN)

        self.cf.add_section('mail')
        self.cf.set('mail', 'mailServer', MAILSERVER)
        self.cf.set('mail', 'mailPort', MAILPORT)
        self.cf.set('mail', 'mailUser', MAILUSER)
        self.cf.set('mail', 'mailPassword', MAILPASSWORD)
        self.cf.set('mail', 'sendTo', SENDTO)

        # write to file
        self.cf.write(open('stockdata.conf', 'w+'))

    def readConfig(self):
        self.cf.read('stockdata.conf')

        self.secretKey = self.cf.get('main', 'secretKey')
        self.sqlUser = self.cf.get('sql', 'sqlUser')
        self.sqlPassword = self.cf.get('sql', 'sqlPassword')
        self.sqlIp = self.cf.get('sql', 'sqlIp')
        self.tushareToken = self.cf.get('sql', 'tushareToken')
        self.mailServer = self.cf.get('mail', 'mailServer')
        self.mailPort = self.cf.get('mail', 'mailPort')
        self.mailUser = self.cf.get('mail', 'mailUser')
        self.mailPassword = self.cf.get('mail', 'mailPassword')
        self.sendTo = self.cf.get('mail', 'sendTo')

    def saveConf(self):
        # write to file
        self.cf.write(open('stockdata.conf', 'w+'))


if __name__ == '__main__':
    mycnf = Config()
    print(mycnf.sendTo)
