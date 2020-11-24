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
# from initlog import initlog

from . import config


class SQLConn:
    """建立mysql连接"""
    def __init__(self):
        mycnf = config.Config()
        connectStr = (f'mysql://{mycnf.sqlUser}:{mycnf.sqlPassword}'
                      f'@{mycnf.sqlIp}/stockdata?charset=utf8')
        self.engine = create_engine(connectStr,
                                    strategy='threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session

