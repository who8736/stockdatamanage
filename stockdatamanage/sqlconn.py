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
# from initlog import initlog

from . import config


class SQLConn():
    """建立mysql连接"""
    def __init__(self, parent=None):
        self.loadSQLConf()
        connectStr = (f'mysql://{self.user}:{self.password}@{self.ip}'
                      f'/stockdata?charset=utf8')
        self.engine = create_engine(connectStr,
                                    strategy='threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))

    def loadSQLConf(self):
        mycnf = config.Config()
        self.user = mycnf.sqlUser
        self.password = mycnf.sqlPassword
        self.ip = mycnf.sqlIp
        # self.token = mycnf.tusharetoken


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session

