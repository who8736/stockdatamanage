#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2022/2/10 9:22
# software: PyCharm

import datetime as dt

import akshare as ak
import pandas as pd

from ..db.sqlrw import writeSQL
from ..db import engine


def downStockList():
    """ 更新股票列表
    """
    # 沪市股票列表
    stock_info_sh_dfa = ak.stock_info_sh_name_code(indicator="主板A股")
    stock_info_sh_dfb = ak.stock_info_sh_name_code(indicator="科创板")
    stock_info_sh_df = pd.concat([stock_info_sh_dfa, stock_info_sh_dfb])
    stock_info_sh_df.rename(columns={'证券代码': 'code',
                                     '证券简称': 'name',
                                     '上市日期': 'list_date',
                                     }, inplace=True)
    df = stock_info_sh_df[['code', 'name', 'list_date']]

    # 深市股票列表
    stock_info_sz_df = ak.stock_info_sz_name_code(indicator="A股列表")
    stock_info_sz_df.rename(columns={'A股代码': 'code',
                                     'A股简称': 'name',
                                     'A股上市日期': 'list_date',
                                     }, inplace=True)
    stock_info_sz_df = stock_info_sz_df[['code', 'name', 'list_date']]
    df = pd.concat([df, stock_info_sz_df])
    df['delist_date'] = None

    # 补充沪市终止上市日期
    stock_info_sh_delist_df = ak.stock_info_sh_delist()
    stock_info_sh_delist_df.rename(columns={'公司代码': 'code',
                                            '公司简称': 'name',
                                            '上市日期': 'list_date',
                                            '暂停上市日期': 'delist_date',
                                            }, inplace=True)
    stock_info_sh_delist_df = stock_info_sh_delist_df[[
        'code', 'name', 'list_date', 'delist_date']]
    stock_info_sh_delist_df = stock_info_sh_delist_df[stock_info_sh_delist_df.code != '-']

    # 补充深市终止上市日期
    stock_info_sz_delist_df = ak.stock_info_sz_delist(indicator="终止上市公司")
    stock_info_sz_delist_df.rename(columns={'证券代码': 'code',
                                            '证券简称': 'name',
                                            '上市日期': 'list_date',
                                            '终止上市日期': 'delist_date',
                                            }, inplace=True)
    stock_info_sz_delist_df = stock_info_sz_delist_df[[
        'code', 'name', 'list_date', 'delist_date']]
    stock_info_sz_delist_df = stock_info_sz_delist_df[stock_info_sz_delist_df.code != '-']

    df = pd.concat([df, stock_info_sh_delist_df, stock_info_sz_delist_df])
    # df = df.merge(delist_df, on='code', how='outer')

    writeSQL(df, 'stock_basic', replace=True)
    pass


def downTradeCal():
    sql = 'select max(cal_date) from trade_cal'
    lastday = engine.execute(sql).fetchone()[0]
    today = dt.date.today()
    if lastday < today:
        df = ak.tool_trade_date_hist_sina()
        df = df.loc[df.trade_date > lastday, :]
        df['exchange'] = 'SSE'
        df['is_open'] = 1
        df.rename(columns={'trade_date': 'cal_date'}, inplace=True)
        writeSQL(df, 'trade_cal')


if __name__ == '__main__':
    pass
