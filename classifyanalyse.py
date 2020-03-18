# -*- coding: utf-8 -*-
"""
Created on 2016年12月2日

@author: who8736

"""

import datetime as dt
# from datetime import timedelta
import logging

import pandas as pd
from pandas.core.frame import DataFrame

import sqlrw
import datatrans
from sqlconn import engine


# import logging
# from wtforms.ext import dateutil


def getStockListForHY(hyID):
    """ 返回指定行业的所有股票代码列表
    """
    levelNum = len(hyID) / 2
    #     levels = ['level1', 'level2', 'level3', 'level4']
    #     level = levels[levelNum - 1]
    sql = 'select ts_code from hangyestock where hyid="%(hyID)s";' % locals()
    result = engine.execute(sql)
    #     stockList = result.fetchall()
    stockList = [i[0] for i in result.fetchall()]
    #     print len(stockList), stockList
    return stockList


def getClassify(ts_code):
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
    result = engine.execute(sql)
    return [i[0] for i in result.fetchall()]


def getSubHY(hyID, subLevel):
    """ 查询指定行业包含的下级行业代码
    """
    level = len(hyID) // 2
    sql = ('select hyid from hangyename '
           'where hylevel%(level)sid="%(hyID)s" and '
           'hylevel="%(subLevel)s";') % locals()
    #     print sql
    result = engine.execute(sql)
    result = result.fetchall()
    #     print 'getSubHY:', result
    if result is None:
        return None
    else:
        return [i[0] for i in result]


def getHYName(hyID):
    print('getHYName(hyID):hyID: ', hyID)
    sql = ('select hyname from hangyename where hyid="%(hyID)s";'
           % locals())
    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        hyName = result[0]
        return hyName


def getHYStockCount(hyID):
    """ 返回4级行业下的股票数量
    """
    sql = ('select count(1) from hangyestock where hyid="%(hyID)s";'
           % locals())
    result = engine.execute(sql).fetchone()
    if result is not None:
        return result[0]


def getHYProfitsIncRate(hyID, quarter):
    sql = ('select profitsIncRate from classify_profits '
           'where hyid="%(hyID)s" and date="%(quarter)s";'
           % locals())
    print(sql)
    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        return result[0]


def getHYProfitsIncRates(hyID):
    curYear = datatrans.getCurYear()
    lastYearQuarter1 = (curYear - 3) * 10 + 4
    lastYearQuarter2 = (curYear - 2) * 10 + 4
    lastYearQuarter3 = (curYear - 1) * 10 + 4
    hyIncRate1 = getHYProfitsIncRate(hyID, lastYearQuarter1)
    hyIncRate2 = getHYProfitsIncRate(hyID, lastYearQuarter2)
    hyIncRate3 = getHYProfitsIncRate(hyID, lastYearQuarter3)
    return hyIncRate1, hyIncRate2, hyIncRate3


def getStockProfitsIncRate(ts_code, quarter):
    sql = ('select incrate from ttmprofits '
           'where ts_code="%(ts_code)s" and date="%(quarter)s";'
           % locals())
    result = engine.execute(sql).fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def getStockProfitsIncRates(ts_code):
    curYear = datatrans.getCurYear()
    lastYearQuarter1 = (curYear - 3) * 10 + 4
    lastYearQuarter2 = (curYear - 2) * 10 + 4
    lastYearQuarter3 = (curYear - 1) * 10 + 4
    incRate1 = getStockProfitsIncRate(ts_code, lastYearQuarter1)
    incRate2 = getStockProfitsIncRate(ts_code, lastYearQuarter2)
    incRate3 = getStockProfitsIncRate(ts_code, lastYearQuarter3)
    print(incRate1, incRate2, incRate3)
    return incRate1, incRate2, incRate3


def calHYTTMProfits(hyID, date):
    """ 计算指定行业的TTMLirun
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    """
    print("calHYTTMProfits hyID: %s, date: %d" % (hyID, date))
    if len(hyID) == 8:
        return calHYTTMProfitsLowLevel(hyID, date)
    else:
        return calHYTTMProfitsHighLevel(hyID, date)


def calHYTTMProfitsHighLevel(hyID, date):
    """ 计算第1、2、3级行业的TTM利润
    """
    level = len(hyID) // 2
    subHyIDList = getSubHY(hyID, level + 1)
    if subHyIDList is None:
        return None
    profitsCur = 0
    profitsLast = 0
    for subHyID in subHyIDList:
        sql = ('select profits, profitsLast from classify_profits '
               'where hyid="%(subHyID)s" and date=%(date)s;') % locals()
        #         print sql
        result = engine.execute(sql).fetchone()
        #         print 'result:', result
        if result is None or result[0] is None:
            continue
        profitsCur += result[0]
        if result[1] is not None:
            profitsLast += result[1]

    #     LastDate = date - 10
    #     sql = ('select profits from classify_profits '
    #            'where hyid="%(subHyID)s" and date="%(LastDate)s";') % locals()
    # #     print sql
    #     result = engine.execute(sql).fetchone()
    # #     print 'result 145:', result
    #     if result is None:
    #         profitsLast = None
    # #         profitsIncRate = None
    #     else:
    #         profitsLast = result[0]

    if profitsLast == 0:
        sql = (('replace into classify_profits(hyid, date, profits) '
                'values("%(hyID)s", "%(date)s", %(profitsCur)s);') % locals())
    else:
        profitsInc = profitsCur - profitsLast
        profitsIncRate = round(profitsInc / abs(profitsLast) * 100, 2)
        sql = (('replace into classify_profits'
                '(hyid, date, profits, profitsLast, '
                'profitsInc, profitsIncRate) '
                'values("%(hyID)s", "%(date)s", '
                '%(profitsCur)s, %(profitsLast)s, '
                '%(profitsInc)s, %(profitsIncRate)s);') % locals())
    #     print sql
    result = engine.execute(sql)


#     print 'result 158:', result
#     if result is None:
#         return False
#     else:
#         return result[0]


def calHYTTMProfitsLowLevel(code, date):
    """ 计算第4级行业的TTM利润
    """
    stockList = getStockListForHY(code)
    curProfits = sqlrw.readTTMProfitsForDate(date)
    lastProfits = sqlrw.readTTMProfitsForDate(date - 10)
    lastProfits = lastProfits[lastProfits['ts_code'].isin(stockList)]
    if lastProfits.empty or curProfits.empty:
        return False
    curProfits = curProfits[curProfits['ts_code'].isin(lastProfits['ts_code'])]
    profitsCur = sum(curProfits['ttmprofits'])
    profitsLast = sum(lastProfits['ttmprofits'])

    profitsInc = profitsCur - profitsLast
    #     print 'allTTMLirunCur', allTTMLirunCur
    #     print 'allTTMLirunLast', allTTMLirunLast
    #     print 'profitsCur', profitsCur
    #     print 'profitsLast', profitsLast
    profitsIncRate = round(profitsInc / abs(profitsLast) * 100, 2)
    #     print profitsInc, profitsIncRate
    #     return [profitsInc, profitsIncRate]
    sql = ('replace into classify_profits'
           '(code, date, profits, profitsLast, profitsInc, profitsIncRate) '
           f'values("{code}", "{date}", '
           f'"{profitsCur}", "{profitsLast}", '
           f'"{profitsInc}", "{profitsIncRate}");')
    engine.execute(sql)
    return True


def calAllHYTTMProfits(date):
    """ 计算各级行业TTM利润，依次计算第4、3、2、1级
    """
    for level in range(4, 0, -1):
        sql = 'select code from hangyename where hylevel=%(level)s;' % locals()
        result = engine.execute(sql)
        hyIDList = result.fetchall()
        hyIDList = [i[0] for i in hyIDList]
        print(hyIDList)
        for hyID in hyIDList:
            print(hyID)
            calHYTTMProfits(hyID, date)


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
        print(hyID, float(hyLirunCount) / hyStockCount)
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

    ts_codes = getStockListForHY(hyID)
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
    # TODO: 重写sql, 可用daily_basic中的总市值与ttmpe计算ttmprofits
    logging.debug(f'update hangyepe: {date}')
    sql = (f'replace into classify_pe(code, date, pe)'
           f' select classify_code, "{date}",'
           f' round(sum(b.total_mv) / sum(b.profits_ttm), 2) pe_ttm '
           f' from classify_member a left join'
           f' (select ts_code, trade_date, pe_ttm, total_mv,'
           f' round(total_mv / pe_ttm, 2) as profits_ttm'
           f'from daily_basic where trade_date = "{date}" and pe_ttm > 0) b'
           f'on a.ts_code = b.ts_code group by classify_code;')
    try:
        engine.execute(sql)
    except Exception as e:
        logging.error(f'failed to read hangyepe for date:{date}:', e)


def getClassifyPE(date=None, replace=False):
    """ 读取所有行业在指定日期的市盈率
    """
    sql = f'select code, date, pe from classify_pe where date='
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
    dates = datatrans.QuarterList(startQuarter, endQuarter)
    for date in dates:
        calAllHYTTMProfits(date)


def test1():
    """ 查询一组股票所处的行业分别有多少公司
    """
    ts_codes = ['002508', '600261', '002285', '000488',
                '002573', '300072', '000910']
    for ts_code in ts_codes:
        stockName = sqlrw.getStockName(ts_code)
        hyID = getClassify(ts_code)
        hyName = getHYName(hyID)
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
    #    test()
    #     hylist = getHYList()
    #     hyCount = {}
    #    hyquarters = getHYQuarters()
    #    test1()
    #    test2()
    #    hypedf = getHYsPE()
    #    getHYPE('01010801', '20171027')

    #     calAllHYTTMLirun(20154)
    hyID = '000101'
    calHYTTMProfits(hyID, 20184)
    #     calHYTTMLirun(hyID, 20162)
    #     stockList = ['000732', '', '', '', '', '', '', ]
