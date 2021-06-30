# -*- coding:utf-8 -*-
# author:who8736
# datetime:2021/6/30 20:18
import datetime as dt
import logging
import os
import socket
import time
import zipfile
from requests.exceptions import ReadTimeout, ConnectTimeout

import pandas as pd
import baostock as bs

from ..config import DATAPATH
from ..db.sqlconn import engine
from ..db.sqlrw import (
    readCal, readTableFields, writeClassifyMemberToSQL, writeClassifyNameToSQL,
    writeSQL, readUpdate, setUpdate,
)
from ..downloader.webRequest import WebRequest
from ..util.initlog import initlog
from ..util.misc import tsCode
from ..util.datatrans import longDate


def downTradeCalBaostock(startDate, endDate=None):
    """下载交易日历"""
    bs.login()

    if startDate is not None and isinstance(startDate, str) and len(
            startDate) == 8:
        startDate = longDate(startDate)
    if endDate is not None and isinstance(endDate, str) and len(endDate) == 8:
        endDate = longDate(endDate)

    #### 获取交易日信息 ####
    rs = bs.query_trade_dates(start_date=startDate, end_date=endDate)

    df = rs.get_data()
    if not df.empty:
        df.rename(columns={'calendar_date': 'cal_date',
                           'is_trading_day': 'is_open'}, inplace=True)
        df['exchange'] = 'SSE'
        writeSQL(df, 'trade_cal')
    bs.logout()


def downStockListBaostock():
    """ 更新股票列表与行业列表
    """
    bs.login()

    rs = bs.query_stock_basic()
    df = rs.get_data()
    if not df.empty:
        # 转换为tushare列标题
        df.rename(columns={'code': 'ts_code',
                           'code_name': 'name',
                           'ipoDate': 'list_date',
                           'outDate': 'delist_date',
                           'type': 'stocktype',
                           'status': 'list_status',
                           }, inplace=True)
        # 转换为tushare格式
        # 转换股票代码
        df.ts_code = df.ts_code.map(lambda x: x[3:] + '.' + x[:2].upper())
        # 状态为上市
        df.loc[df.list_status == '1', ['list_status']] = 'L'
        # 状态为退市
        df.loc[df.list_status == '0', ['list_status']] = 'D'
        # 退市日期为空时替换为'0000-00-00'
        df.loc[df.delist_date == '', ['delist_date']] = '0000-00-00'

        writeSQL(df, 'stock_basic', replace=True)

    bs.logout()
