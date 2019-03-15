# -*- coding: utf-8 -*-
'''
Created on 2016年11月21日

@author: who8736
'''
import os
import configparser
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


class SQLConn():

    def __init__(self, parent=None):
        self.loadSQLConf()
        user = self.user
        password = self.password
        ip = self.ip
        connectStr = ('mysql://%(user)s:%(password)s@%(ip)s'
                      '/stockdata?charset=utf8' % locals())
        self.engine = create_engine(connectStr,
                                    strategy='threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))

    def loadSQLConf(self):
        if not os.path.isfile('sql.conf'):
            self.user = 'root'
            self.password = 'password'
            self.ip = '127.0.0.1'
            self.saveConf()
            return

        try:
            cf = configparser.ConfigParser()
            cf.read('sql.conf')
            if cf.has_option('main', 'user'):
                self.user = cf.get('main', 'user')
            if cf.has_option('main', 'password'):
                self.password = cf.get('main', 'password')
            if cf.has_option('main', 'ip'):
                self.ip = cf.get('main', 'ip')
        except Exception as e:
            print(e)
            logging.error('read conf file error.')

    def saveConf(self):
        cf = configparser.ConfigParser()
        # add section / set option & key
        cf.add_section('main')
        cf.set('main', 'user', self.user)
        cf.set('main', 'password', self.password)
        cf.set('main', 'ip', self.ip)

        # write to file
        cf.write(open('sql.conf', 'w+'))


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session

# logfilename = os.path.join(os.path.abspath(os.curdir), 'stockanalyse.log')
# formatStr = ('%(asctime)s %(filename)s[line:%(lineno)d] '
#              '%(levelname)s %(message)s')
# logging.basicConfig(level=logging.DEBUG,
#                     format=formatStr,
#                     # datefmt = '%Y-%m-%d %H:%M:%S +0000',
#                     filename=logfilename,
#                     filemode='a')

##########################################################################
# 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
# console = logging.StreamHandler()
# console.setLevel(logging.INFO)
# formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
# console.setFormatter(formatter)
# logging.getLogger('').addHandler(console)

# user = u'root'
# password = u'password'
# ip = '127.0.0.1'
# cf = ConfigParser.ConfigParser()
# if not os.path.isfile('sql.conf'):
#     cf = ConfigParser.ConfigParser()
#     # add section / set option & key
#     cf.add_section('main')
#     cf.set('main', 'user', user)
#     cf.set('main', 'password', password)
#     cf.set('main', 'ip', ip)
#     # write to file
#     cf.write(open('sql.conf', 'w+'))
#
# try:
#     cf.read('sql.conf')
#     if cf.has_option('main', 'user'):
#         user = cf.get('main', 'user')
#     if cf.has_option('main', 'password'):
#         password = cf.get('main', 'password')
#     if cf.has_option('main', 'ip'):
#         ip = cf.get('main', 'ip')
# except Exception, e:
#     print e
#     logging.error('read conf file error.')
#
# connectStr = (u'mysql://%(user)s:%(password)s@%(ip)s'
#               u'/stockdata?charset=utf8' % locals())
# engine = create_engine(connectStr,
#                        strategy=u'threadlocal', echo=False)
# Session = scoped_session(
#     sessionmaker(bind=engine, autoflush=False))
