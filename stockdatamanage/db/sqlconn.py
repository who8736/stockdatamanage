# -*- coding: utf-8 -*-
"""
Created on 2016年11月21日

@author: who8736
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

from ..config import sqlHost, sqlUser, sqlPassword
# from initlog import initlog


class SQLConn:
    """建立mysql连接"""

    def __init__(self):
        connectStr = (f'mysql://{sqlUser}:{sqlPassword}'
                      f'@{sqlHost}/stockdata?charset=utf8')
        self.engine = create_engine(connectStr,
                                    strategy='threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session
