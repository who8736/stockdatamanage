# -*- coding: utf-8 -*-
"""
Created on 2016年12月2日

@author: who8736

"""

import datetime as dt
# from datetime import timedelta
import logging

import pandas as pd

from ..db.sqlconn import engine
from ..db.sqlrw import (
    readStockList, readTTMProfitsForDate, writeSQL, readProfit, readClassify
)
from ..util import datatrans
from ..util.datatrans import calDate, quarterList


# import logging
# from wtforms.ext import dateutil


def getStockForClassify(code=None, date=None):
    """ 返回行业的股票清单
    date: 查询日期， 当日无行业清单时取最近一日的行业清单
    """
    if date is None:
        date = dt.datetime.today().strftime('%Y%m%d')
    sql = f'select ts_code, classify_code from classify_member where true '
    if code:
        sql += f' and classify_code="{code}" '
    sql += (f' and date=(select max(date) from classify_member where '
            f'date<="{date}")')
    df = pd.read_sql(sql, engine)

    stockname = readStockList()
    df = df.merge(stockname, on='ts_code', how='left')
    return df
    # result = engine.execute(sql).fetchall()
    # if result:
    #     return [row[0] for row in result]


def readClassifyCodeForStock(ts_code, date=None):
    """ 当查询指定股票的4级行业的代码
    """
    sql = f'select classify_code from classify_member where ts_code="{ts_code}" '
    if date is None:
        sql += ' and date=(select max(date) from classify_member)'
    else:
        sql += f' and date="{date}"'
    result = engine.execute(sql).fetchone()
    if result:
        return result[0]


# def getHYStock():
#     """ 查询已进行行业分类的股票列表
#     """
#     sql = 'select ts_code from hangyestock;'
#     result = engine.execute(sql)
#     hystock = result.fetchall()
#     hystock = [i[0] for i in hystock]
#     return hystock


def getClassifyProfitCnt(code, quarter):
    """ 查询行业在指定季度中已发布财报的股票数量
    """
    sql = (f'select count(1) from classify_profits where code="{code}" and '
           f'date="{quarter}"')
    result = engine.execute(sql)
    return result.fetchone()[0]


# def getHYList(level=4):
#     """ 查询指定级别的所有行业代码
#     """
#     sql = 'select hyid from hangyename where hylevel=%(level)s;' % locals()
#     #     print sql
#     result = engine.execute(sql).fetchall()
#     return [i[0] for i in result]


def getSubHY(hyID, subLevel):
    """ 查询指定行业包含的下级行业代码
    """
    level = len(hyID) // 2
    sql = ('select code from classify '
           'where level%(level)sid="%(hyID)s" and '
           'level="%(subLevel)s";') % locals()
    #     print sql
    result = engine.execute(sql)
    result = result.fetchall()
    #     print 'getSubHY:', result
    if result:
        return [i[0] for i in result]


def readClassifyName(code=None):
    """取行业代码和名称
    """
    assert isinstance(code, (str, pd.DataFrame)), 'code应为str或DataFrame'
    sql = f'select code, name from classify;'
    df = pd.read_sql(sql, engine)
    if df.empty:
        return None
    if isinstance(code, str):
        return df.loc[df.code == code, 'name'].values[0]
    df = df[df.code.isin(code)]
    return df


def getHYStockCount(code):
    """ 返回4级行业下的股票数量
    """
    sql = f'select count(1) from classify_member where classify_code="{code}";'
    result = engine.execute(sql).fetchone()
    if result:
        return result[0]


# def getHYProfitsIncRate(hyID, _date):
#     sql = (f'select inc from classify_profits '
#            f'where code="{hyID}" and end_date="{_date}";')
#     # print(sql)
#     result = engine.execute(sql).fetchone()
#     if result is None:
#         return None
#     else:
#         return result[0]
#
#
# def getHYProfitsIncRates(hyID):
#     curYear = datatrans.getCurYear()
#     lastYearQuarter1 = f'{curYear - 3}1231'
#     lastYearQuarter2 = f'{curYear - 2}1231'
#     lastYearQuarter3 = f'{curYear - 1}1231'
#     hyIncRate1 = getHYProfitsIncRate(hyID, lastYearQuarter1)
#     hyIncRate2 = getHYProfitsIncRate(hyID, lastYearQuarter2)
#     hyIncRate3 = getHYProfitsIncRate(hyID, lastYearQuarter3)
#     return hyIncRate1, hyIncRate2, hyIncRate3


def calClassifyStaticTTMProfit(end_date, replace=False):
    """ 计算指定行业的静态TTM利润
    公司净利润为归属于母公司股东的净利润，在整年度内分 3 个时点进行统一更新，以得
    到过去 4 个季度的净利润，具体规则为：
    在 5 月 1 日之前采用第 N-1 年前 3 季度财务数据与第 N-2 年第 4 季度财务数据之和；
    5 月 1 日(含)至 9 月 1 日之前采用当年第 1 季度财务数据与第 N-1 年第 2、3、4 季度财务数据之和；
    9 月 1 日(含)至 11 月 1 日之前采用当年中期报告财务数据与第 N-1 年第 3、4 季度财务数据之和；
    11 月 1 日(含)至次年 5 月 1 日之前采用本年度前 3 季度财务数据与上年度第 4季度财务数据之和。

    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    """
    logging.debug(
        f'calClassifyStaticTTMProfit date: {end_date}')
    df = calClassifyStaticTTMProfitLow(end_date, replace=replace)
    calClassifyStaticTTMProfitHigh(df, end_date, replace=replace)


# noinspection DuplicatedCode
def calClassifyStaticTTMProfitHigh(classify, end_date, replace=False):
    """ 计算第1、2、3级行业的TTM利润
    """
    for level in range(1, 4):
        df = classify[
            ['code', 'profits', 'profits_com', 'lastprofits', ]].copy()
        df.loc[:, 'code'] = df.code.str[:level * 2]
        result = df.groupby('code').sum()
        result['inc'] = result.profits_com / result.lastprofits
        result.loc[result.lastprofits > 0, 'inc'] = (
                result.inc * 100 - 100).round(2)
        result.loc[result.lastprofits < 0, 'inc'] = (
                100 - result.inc * 100).round(2)
        result.loc[result.lastprofits == 0, 'inc'] = 100
        result['end_date'] = dt.datetime.strptime(end_date, '%Y%m%d')
        # print(result)
        writeSQL(result, 'classify_profits', replace=replace)


# noinspection DuplicatedCode
def calClassifyStaticTTMProfitLow(end_date, replace=False):
    """ 计算第4级行业的TTM利润
    报告期后上市的股票才将该报告期利润计入行业利润
    """
    _caldate, _lastcaldate = calDate(end_date)
    # 当期行业清单
    stocks = getStockForClassify(date=_caldate)
    assert isinstance(stocks, pd.DataFrame)

    # 股票TTM利润, 本年与上年
    curProfits = readTTMProfitsForDate(end_date)
    stocks = stocks.merge(curProfits[['ts_code', 'ttmprofits']],
                          how='left', on='ts_code')
    stocks.rename(columns={'classify_code': 'code', 'ttmprofits': 'profits'},
                  inplace=True)
    stocks.dropna(inplace=True)

    lastEndDate = f'{int(end_date[:4]) - 1}{end_date[4:]}'
    lastProfits = readTTMProfitsForDate(lastEndDate)
    lastProfits.rename(columns={'ttmprofits': 'lastprofits'}, inplace=True)
    stocks = stocks.merge(lastProfits[['ts_code', 'lastprofits']],
                          how='left', on='ts_code')
    # print(lastProfits.head())

    # 计算本期行业利润
    classify = stocks[['code', 'profits']].groupby('code').sum()

    # 计算上年可比的行业利润与增长率
    stocksCom = stocks[['code', 'profits', 'lastprofits']].copy()
    stocksCom.dropna(inplace=True)
    stocksCom.rename(columns={'profits': 'profits_com'}, inplace=True)
    classifyCom = stocksCom.groupby('code').sum()
    classifyCom['inc'] = classifyCom.profits_com / classifyCom.lastprofits
    classifyCom.loc[classifyCom.lastprofits > 0, 'inc'] = (
            classifyCom.inc * 100 - 100).round(2)
    classifyCom.loc[classifyCom.lastprofits < 0, 'inc'] = (
            100 - classifyCom.inc * 100).round(2)
    classifyCom.loc[classifyCom.lastprofits == 0, 'inc'] = 100
    # print(classifyCom)

    classify = classify.merge(classifyCom, how='left', on='code')
    classify.reset_index(inplace=True)
    classify['end_date'] = dt.datetime.strptime(end_date, '%Y%m%d')
    # print(classify)
    writeSQL(classify, 'classify_profits', replace)
    return classify


def getHYQuarters():
    """ 取得进行行业分析比较时采用的财报季度
        如果某行业上一季度已公布财报的公司数量占该行业的公司总数的80%以上
        则以当前日期的上一季度作为分析用的日期，否则采用上上季度的日期
    """
    lastQuarter = datatrans.getLastQuarter()
    last2Quarter = datatrans.quarterSub(lastQuarter, 1)
    hylist = getHYList()
    hyQuarters = {}
    for code in hylist:
        hyStockCount = getHYStockCount(code)
        hyLirunCount = getClassifyProfitCnt(code, lastQuarter)
        if hyStockCount == 0:
            continue
        # print(hyID, float(hyLirunCount) / hyStockCount)
        if hyLirunCount / hyStockCount > 0.8:
            hyQuarters[code] = lastQuarter
        else:
            hyQuarters[code] = last2Quarter
    return hyQuarters


def readClassifyPE(date=None, code=None):
    """ 计算行业在指定日期的市盈率
    :param date:
    :param code: None or list

    :return:
    """
    if date is None:
        condition = f'(select max(date) from classify_pe)'
    else:
        condition = f'"{date}"'
    sql = f'select code, pe from classify_pe where date={condition}'
    df = pd.read_sql(sql, engine)
    if code is not None:
        df = df[df.code.isin(code)]
    if not df.empty:
        return df


def calClassifyPE(date):
    """ 计算所有行业在指定日期的市盈率
    """
    # if date is None:
    #     date = dt.datetime.today() - timedelta(days=1)
    #     date = date.strftime('%Y%m%d')
    logging.debug(f'update hangyepe: {date}')
    sql = f'''replace into classify_pe(code, date, pe)
            SELECT classify_code, '{date}',
                    ROUND(SUM(b.total_mv) / SUM(b.profits_ttm), 2) pe_ttm
            FROM
                (SELECT c.ts_code, c.classify_code
                FROM classify_member c, (SELECT ts_code, MAX(date) mdate
                                          FROM classify_member
                                          WHERE date <= '{date}'
                                          GROUP BY ts_code) groupc
                WHERE c.ts_code = groupc.ts_code AND c.date = groupc.mdate) a
                LEFT JOIN
                (SELECT ts_code, trade_date, pe_ttm, total_mv,
                        ROUND(total_mv / pe_ttm, 2) AS profits_ttm
                FROM daily_basic
                WHERE trade_date = '{date}' AND pe_ttm > 0) b ON a.ts_code = b.ts_code
            GROUP BY classify_code;
'''
    # print(sql)
    try:
        engine.execute(sql)
    except Exception as e:
        logging.error(f'failed to read hangyepe for date:{date}:', e)


def resetClassifyProfits(startQuarter='20200331', endQuarter='20201231'):
    """
    重算指定日期所有行业TTM利润
    :return:
    """
    dates = datatrans.quarterList(startQuarter, endQuarter)
    for date in dates:
        calAllHYTTMProfits(date)


def calClassifyProfitsIncCompare(startDate='20170930', endDate='20200930'):
    """计算N年内所有行业利润增长水平
        采用可比利润计算，按计算时的行业分类，取当前和N年前行业股票,
        剔除期间新增股票后, 计算利润增长水平与复利
    """
    # 历史利润
    df = readProfit(startDate, endDate)

    # 增加行业代码
    sql = f'''select ts_code, classify_code from classify_member 
                where date=(select max(date) from classify_member);'''
    classifymember = pd.read_sql(sql, engine)
    df = df.merge(classifymember, on='ts_code')

    # 分类汇总
    dfgroup = df.groupby('classify_code').sum()
    dfgroup.reset_index(inplace=True)

    # 总体利润增长率
    baseField = f'profit{startDate}'
    curField = f'fprofit{endDate}'
    dfgroup[inc] = dfgroup[curField] / dfgroup[baseField]
    dfgroup.loc[dfgroup[baseField] > 0, 'inc'] = (
            dfgroup[incField] * 100 - 100).round(2)
    dfgroup.loc[dfgroup[baseField] < 0, 'inc'] = (
            100 - dfgroup[incField] * 100).round(2)

    # 利润增长的稳定性
    dates = quarterList(startDate, endDate)
    for i in range(1, len(dates)):
        print(dates[i - 1], dates[i])
        lastField = f'profit{dates[i - 1]}'
        curField = f'profit{dates[i]}'
        incField = f'inc{dates[i]}'
        dfgroup[incField] = dfgroup[curField] / dfgroup[lastField]
        dfgroup.loc[dfgroup[lastField] > 0, incField] = (
                dfgroup[incField] * 100 - 100).round(2)
        dfgroup.loc[dfgroup[lastField] < 0, incField] = (
                100 - dfgroup[incField] * 100).round(2)

    # 输出结果
    classify = readClassify()
    dfgroup = dfgroup.merge(classify, how='left', left_on='classify_code', right_on='code')
    df.to_excel('classifyproifitinc.xlsx')
    dfgroup.to_excel('classifyprofitgroup.xlsx')
    print(df)
