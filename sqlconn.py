# -*- coding: utf-8 -*-
"""
Created on 2016年11月21日

@author: who8736
"""
import os
import configparser
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session


class SQLConn:

    def __init__(self):
        self.ip = '127.0.0.1'
        self.user = 'user'
        self.password = 'password'
        self.database = 'test'
        self.token = ''
        self.loadSQLConf()

        # connectStr = (f'mysql://{self.user}:{self.password}@{self.ip}'
        #               f'/{self.database}?charset=utf8mb4')
        # connectStr = (f'mysql+pymysql://{self.user}:{self.password}@{self.ip}'
        #               f'/{self.database}?charset=utf8mb4')
        connectStr = (f'mysql+mysqlconnector://{self.user}:{self.password}@{self.ip}'
                      f'/{self.database}?charset=utf8')
        self.engine = create_engine(connectStr,
                                    strategy='threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))
        self.saveConf()

    def loadSQLConf(self):
        if not os.path.isfile('sql.conf'):
            self.user = 'root'
            self.password = 'password'
            self.ip = '127.0.0.1'
            self.database = 'test'
            self.token = ''
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

            if cf.has_option('main', 'database'):
                self.database = cf.get('main', 'database')

            if cf.has_option('main', 'token'):
                self.token = cf.get('main', 'token')

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
        cf.set('main', 'token', self.token)
        cf.set('main', 'database', self.database)

        # write to file
        cf.write(open('sql.conf', 'w+'))


# sqlconn = SQLConn()
# engine = sqlconn.engine
# Session = sqlconn.Session
