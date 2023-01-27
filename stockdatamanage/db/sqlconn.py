# -*- coding: utf-8 -*-
"""
Created on 2016年11月21日

@author: who8736
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.exc import NoResultFound

from ..config import SQLHOST, SQLUSER, SQLPASSWORD
# from initlog import initlog


class SQLConn:
    """建立mysql连接"""

    def __init__(self):
        connectStr = (f'mysql://{SQLUSER}:{SQLPASSWORD}'
                      f'@{SQLHOST}/stockdata?charset=utf8')
        self.engine = create_engine(connectStr, echo=False)
        # sqlalchemy升级后，strategy='threadlocal'参数失效
        # self.engine = create_engine(connectStr,
        #                             strategy='threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session


def executesql(sql, return_value=True):
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        # print(result)
        try:
            if return_value:
                return result.one()[0]
            else:
                return result.all()
        except NoResultFound:
            return None
