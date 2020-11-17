# -*- coding: utf-8 -*-
"""
Created on 2016年12月2日

@author: who8736

"""

import datetime as dt
# from datetime import timedelta
import logging

import pandas as pd

from . import sqlrw
from . import datatrans
from .datatrans import lastYearDate, calDate
from .sqlconn import engine
from .sqlrw import readStockList, readTTMProfitsForDate, writeSQL


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
    return pd.read_sql(sql, engine)
    # result = engine.execute(sql).fetchall()
    # if result:
    #     return [row[0] for row in result]


def readClassify(ts_code):
    """ 当查询指定股票的4级行业的代码
    """
    sql = f'select classify_code from classify_member where ts_code="{ts_code}";'
    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        return result[0]


# def getHYStock():
#     """ 查询已进行行业分类的股票列表
#     """
#     sql = 'select ts_code from hangyestock;'
#     result = engine.execute(sql)
#     hystock = result.fetchall()
#     hystock = [i[0] for i in hystock]
#     return hystock


def getHYLirunCount(hyID, quarter):
    """ 查询行业在指定季度中已发布财报的股票数量
    """
    sql = ('select count(1) from classify_profits where hyid="%(hyID)s" and '
           'date="%(quarter)s"') % locals()
    result = engine.execute(sql)
    return result.fetchone()[0]


def getHYList(level=4):
    """ 查询指定级别的所有行业代码
    """
    sql = 'select hyid from hangyename where hylevel=%(level)s;' % locals()
    #     print sql
    result = engine.execute(sql).fetchall()
    return [i[0] for i in result]


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


def getClassifyName(code):
    sql = f'select name from classify where code="{code}";'
    result = engine.execute(sql).fetchone()
    if result:
        return result[0]


def getHYStockCount(code):
    """ 返回4级行业下的股票数量
    """
    sql = f'select count(1) from classify_member where classify_code="{code}";'
    result = engine.execute(sql).fetchone()
    if result:
        return result[0]


def getHYProfitsIncRate(hyID, _date):
    sql = (f'select profitsIncRate from classify_profits '
           f'where code="{hyID}" and end_date="{_date}";')
    print(sql)
    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        return result[0]


def getHYProfitsIncRates(hyID):
    curYear = datatrans.getCurYear()
    lastYearQuarter1 = f'{curYear - 3}1231'
    lastYearQuarter2 = f'{curYear - 2}1231'
    lastYearQuarter3 = f'{curYear - 1}1231'
    hyIncRate1 = getHYProfitsIncRate(hyID, lastYearQuarter1)
    hyIncRate2 = getHYProfitsIncRate(hyID, lastYearQuarter2)
    hyIncRate3 = getHYProfitsIncRate(hyID, lastYearQuarter3)
    return hyIncRate1, hyIncRate2, hyIncRate3


def getStockProfitsIncRate(ts_code, _date):
    sql = ('select incrate from ttmprofits '
           f'where ts_code="{ts_code}" and end_date="{_date}";')
    result = engine.execute(sql).fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def getStockProfitsIncRates(ts_code):
    curYear = datatrans.getCurYear()
    lastYearQuarter1 = f'{curYear - 3}1231'
    lastYearQuarter2 = f'{curYear - 2}1231'
    lastYearQuarter3 = f'{curYear - 1}1231'
    incRate1 = getStockProfitsIncRate(ts_code, lastYearQuarter1)
    incRate2 = getStockProfitsIncRate(ts_code, lastYearQuarter2)
    incRate3 = getStockProfitsIncRate(ts_code, lastYearQuarter3)
    # print(incRate1, incRate2, incRate3)
    return incRate1, incRate2, incRate3


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
    for hyID in hylist:
        hyStockCount = getHYStockCount(hyID)
        hyLirunCount = getHYLirunCount(hyID, lastQuarter)
        if hyStockCount == 0:
            continue
        # print(hyID, float(hyLirunCount) / hyStockCount)
        if float(hyLirunCount) / hyStockCount > 0.8:
            hyQuarters[hyID] = lastQuarter
        else:
            hyQuarters[hyID] = last2Quarter
    return hyQuarters


def getHYPE(hyID, date, reset=False):
    """ 计算行业在指定日期的市盈率
    :param hyID:
    :param date:
    :param reset: 为True时用于重算某行业的PE

    :return:
    """
    sql = f'select hype from hangyepe where hyid="{hyID}" and date="{date}"'
    result = engine.execute(sql).fetchone()
    if not reset and result is not None:
        return result[0]

    ts_codes = getStockForClassify(hyID)
    valueSum = 0
    profitSum = 0
    for ts_code in ts_codes:
        sql = (f'select date, totalmarketvalue, ttmprofits '
               f'from klinestock where ts_code="{ts_code}" and date<="{date}"'
               f'order by `date` desc limit 1;')
        result = engine.execute(sql).fetchone()
        if result is not None:
            #            value, profit = result.fetchone()
            # result = result.first()
            value = result[1]
            profit = result[2]
            # ttmpe = result[3]
            if profit is None or profit < 0 or value is None:
                continue

            #            print ts_code, result[0], result[1], result[2], result[3]
            valueSum += value
            profitSum += profit
    if profitSum != 0:
        pe = round(valueSum / profitSum, 2)
        #        print 'htHYPE', date, valueSum, profitSum, pe
        return pe


def calClassifyPE(date):
    """ 计算所有行业在指定日期的市盈率
    """
    # if date is None:
    #     date = dt.datetime.today() - timedelta(days=1)
    #     date = date.strftime('%Y%m%d')
    logging.debug(f'update hangyepe: {date}')
    sql = (f'replace into classify_pe(code, date, pe)'
           f' select classify_code, "{date}",'
           f' round(sum(b.total_mv) / sum(b.profits_ttm), 2) pe_ttm '
           f' from classify_member a left join'
           f' (select ts_code, trade_date, pe_ttm, total_mv,'
           f' round(total_mv / pe_ttm, 2) as profits_ttm'
           f' from daily_basic where trade_date="{date}" and pe_ttm > 0) b'
           f' on a.ts_code = b.ts_code group by classify_code;')
    # print(sql)
    try:
        engine.execute(sql)
    except Exception as e:
        logging.error(f'failed to read hangyepe for date:{date}:', e)


def getClassifyPE(date=None, replace=False):
    """ 读取所有行业在指定日期的市盈率
    """
    sql = (f'select code as classify_code, pe classify_pe'
           ' from classify_pe where date=')
    if date is None:
        sql += '(select max(date) from classify_pe)'
    else:
        sql += f'"{date}"'
    df = pd.read_sql(sql, engine)
    return df


def resetHYTTMLirun(startQuarter=20191, endQuarter=20191):
    """
    重算指定日期所有行业TTM利润
    :return:
    """
    dates = datatrans.quarterList(startQuarter, endQuarter)
    for date in dates:
        calAllHYTTMProfits(date)


def test1():
    """ 查询一组股票所处的行业分别有多少公司
    """
    ts_codes = ['002508', '600261', '002285', '000488',
                '002573', '300072', '000910']
    for ts_code in ts_codes:
        stockName = sqlrw.getStockName(ts_code)
        hyID = readClassify(ts_code)
        hyName = getClassifyName(hyID)
        hyCount = getHYStockCount(hyID)
        print(ts_code, stockName, hyID, hyName, hyCount)


def test2():
    hyList = getHYList()
    hyPEs = {}
    for hyID in hyList:
        pe = getHYPE(hyID, '20171027')
        hyPEs[hyID] = pe
        print(hyID, pe)


# def getHYIDName(ts_code):
#     hyID = getHYIDForStock(ts_code)
#     hyName = getHYName(hyID)
#     print ts_code, hyID, hyName


if __name__ == '__main__':
    #    tests()
    #     hylist = getHYList()
    #     hyCount = {}
    #    hyquarters = getHYQuarters()
    #    test1()
    #    test2()
    #    hypedf = getHYsPE()
    #    getHYPE('01010801', '20171027')

    #     calAllHYTTMLirun(20154)
    hyID = '000101'
    calClassifyStaticTTMProfit(hyID, 20184)
    #     calHYTTMLirun(hyID, 20162)
    #     stockList = ['000732', '', '', '', '', '', '', ]
