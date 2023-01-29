#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2022/2/10 9:22
# software: PyCharm

import datetime as dt
import logging

import akshare as ak
import pandas as pd
import numpy as np
from tenacity import retry, stop_after_attempt, RetryError
# from sqlalchemy import text

from ..db.sqlrw import writeSQL, readStockUpdate, readStockBasicUpdate
from ..db import engine, executesql
from ..util.initlog import logfun


@logfun
@retry(stop=stop_after_attempt(3))
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


@logfun
def downTradeCal():
    sql = 'select max(cal_date) from trade_cal'
    lastday = executesql(sql)

    # lastday = conn.execute(text(sql)).fetchone()[0]
    # lastday = result[0]
    today = dt.date.today()
    if lastday < today:
        df = ak.tool_trade_date_hist_sina()
        df = df.loc[df.trade_date > lastday, :]
        df['exchange'] = 'SSE'
        df['is_open'] = 1
        df.rename(columns={'trade_date': 'cal_date'}, inplace=True)
        writeSQL(df, 'trade_cal')


@logfun
def downloadDaily():
    """下载日交易历史数据
    """
    update_dates = readStockUpdate()
    for _, row in update_dates.iterrows():
        code = row.code
        _date = row.trade_date
        if _date is np.nan:
            start_date = '19800101'
        else:
            _date += dt.timedelta(days=1)
            start_date = _date.strftime('%Y%m%d')
        # print(start_date)
        # 当前时间减18小时得到需更新的日期，即0点至18点仅更新至前一日数据
        _end_date = dt.datetime.today() - dt.timedelta(hours=18)
        end_date = _end_date.strftime('%Y%m%d')
        if start_date >= end_date:
            continue

        logging.info(f'code:{code},start:{start_date},end:{end_date}')
        try:
            df = _downloadDaily(code, start_date, end_date)
        except RetryError:
            continue
        else:
            writeSQL(df, 'daily')


@retry(stop=stop_after_attempt(3))
def _downloadDaily(code, start_date, end_date):
    df = ak.stock_zh_a_hist(
        symbol=code, start_date=start_date, end_date=end_date)
    if df.empty:
        return df
    df.columns = ['trade_date', 'open', 'close', 'high',
                  'low', 'vol', 'amount', '_', 'pct_chg', 'change', '_']
    df = df[['trade_date', 'open', 'close', 'high',
             'low', 'vol', 'amount', 'pct_chg', 'change']]
    df['code'] = code
    return df


def downloadDailyBasic():
    """下载个股指标历史数据
    """
    update_dates = readStockBasicUpdate()
    for _, row in update_dates.iterrows():
        code = row.code
        start_date = row.trade_date
        if start_date is np.nan:
            start_date = dt.date(1980, 1, 1)
        else:
            start_date += dt.timedelta(days=1)

        # print(start_date)
        # 当前时间减18小时得到需更新的日期，即0点至18点仅更新至前一日数据
        end_date = dt.datetime.today() - dt.timedelta(hours=18)
        end_date = end_date.date()
        if start_date >= end_date:
            continue

        logging.info(f'code:{code},start:{start_date},end:{end_date}')
        try:
            df = _downloadDailyBasic(code, start_date, end_date)
        except RetryError:
            continue
        else:
            writeSQL(df, 'daily_basic')


@retry(stop=stop_after_attempt(3))
def _downloadDailyBasic(code, start_date, end_date):
    df = ak.stock_a_lg_indicator(symbol=code)
    if df.empty:
        return df
    # df['trade_date'] = df['trade_date'].map(lambda x: x.strftime('%Y%m%d'))
    df = df[(df.trade_date >= start_date) & (df.trade_date <= end_date)]
    # df.columns = ['trade_date', 'pe', 'pe_ttm', 'pb',
    #               'ps', 'ps_ttm', 'dv_ratio', 'dv_ttm', 'total_mv', ]
    df['code'] = code
    return df


def downloadIndexDaily():
    sql = 'SELECT code, max(trade_date) update_date FROM index_daily group by code;'
    with engine.connect() as conn:
        updatedf = pd.read_sql(text(sql), conn)
    for index, row in updatedf.iterrows():
        code = row['code']
        start_date = row['update_date'] + dt.timedelta(days=1)
        start_date = start_date.strftime('%Y%m%d')
        print(row['code'], row['update_date'])

        # 当前时间减18小时得到需更新的日期，即0点至18点仅更新至前一日数据
        end_date = dt.datetime.today() - dt.timedelta(hours=18)
        end_date = end_date.date().strftime('%Y%m%d')

        logging.info(
            f'download index daily: code:{code},start:{start_date},end:{end_date}')
        try:
            df = ak.index_zh_a_hist(
                symbol=code, period="daily", start_date=start_date, end_date=end_date)

            df.rename(columns={'日期': 'trade_date',
                               '开盘': 'open',
                               '收盘': 'close',
                               '最高': 'high',
                               '最低': 'low',
                               '成交量': 'vol',
                               '成交额': 'amount',
                               '涨跌幅': 'pct_chg',
                               '涨跌额': 'change',
                               }, inplace=True)
            df['code'] = code
            df = df[['code', 'trade_date', 'open', 'close', 'high', 'low',
                     'vol', 'amount', 'pct_chg', 'change']]
        except RetryError:
            continue
        else:
            writeSQL(df, 'index_daily')


def downloadIndexDailyIndicator():
    """下载指数每日指标，市盈率、市净率、股息率
    """
    pass
    index_dict = {'上证指数': '000001.SH',
                  '上证380': '000009.SH',
                  '上证180': '000010.SH',
                  '上证50': '000016.SH',
                  '科创50': '000688.SH',
                  '中证1000': '000852.SH',
                  '中证100': '000903.SH',
                  '中证500': '000905.SH',
                  '深证成指': '399001.SZ',
                  '创业板指': '399006.SZ',
                  '沪深300': '399300.SZ',
                  '创业板50': '399673.SZ',
                  '证券公司': '399975.SZ',
                  }

    # 当前时间减18小时得到需更新的日期，即0点至18点仅更新至前一日数据
    end_date = dt.datetime.today() - dt.timedelta(hours=18)
    end_date = end_date.date()

    # for index_name in index_dict.keys():
    for index_code in index_dict.values():
        # index_code = index_dict[index_name]
        # print(index_name, index_code)

        sql = f'select max(trade_date) from index_dailyindicator where code="{index_code}"'
        res = executesql(sql)
        # print(res)
        if res:
            start_date = res + dt.timedelta(days=1)
        else:
            start_date = dt.date(1980, 1, 1)
        logging.info(
            f'download index daily indicator: {index_code}, {start_date}, {end_date}')
        # df = pd.DataFrame({'code':[], 'trade_date': [], 'pe':[], 'pb':[], 'dv': []})
        dfpe = ak.index_value_hist_funddb(symbol=index_name, indicator="市盈率")
        dfpe = dfpe.loc[(dfpe.日期 >= start_date) & (
            dfpe.日期 <= end_date), ['日期', '市盈率']]
        dfpb = ak.index_value_hist_funddb(symbol=index_name, indicator="市净率")
        dfpb = dfpb[['日期', '市净率']]
        dfdv = ak.index_value_hist_funddb(symbol=index_name, indicator="股息率")
        dfdv = dfdv[['日期', '股息率']]
        # print(dfpe.head())
        # print(dfpb.head())
        # print(dfdv.head())

        df = dfpe.merge(dfpb, how='left', on='日期')
        df = df.merge(dfdv, how='left', on='日期')
        df.rename(columns={'日期': 'trade_date',
                           '市盈率': 'pe',
                           '市净率': 'pb',
                           '股息率': 'dv',
                           }, inplace=True)
        df['code'] = index_code
        # print(df.head())
        writeSQL(df, 'index_dailyindicator')


def downloadETFDaily():
    """下载etf每日净值
    """
    codes = [
        '510300',
    ]
    end_date = dt.date.today().strftime('%Y%m%d')

    for code in codes:
        sql = f'select max(trade_date) from etf_daily where code="{code}"'
        res = engine.execute(sql).fetchone()[0]
        # print(res)
        if res:
            start_date = res.strftime('%Y%m%d')
        else:
            start_date = '19800101'
        logging.info(
            f'download etf daily worth: {code}, {start_date}, {end_date}')

        if start_date >= end_date:
            continue
        df = ak.fund_etf_fund_info_em(fund=code, start_date=start_date)
        df.rename(columns={'净值日期': 'trade_date',
                           '单位净值': 'unit_networth',
                           '累计净值': 'cumulative_networth',
                           }, inplace=True)
        df['code'] = code
        writeSQL(df, 'etf_daily')


if __name__ == '__main__':
    pass
