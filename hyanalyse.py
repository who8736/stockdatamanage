# -*- coding: utf-8 -*-
'''
Created on 2016年12月2日

@author: who8736
'''
import sqlrw
import datatrans
import logging
from wtforms.ext import dateutil


def getStockListForHY(hyID):
    levelNum = len(hyID) / 2
#     levels = ['level1', 'level2', 'level3', 'level4']
#     level = levels[levelNum - 1]
    sql = 'select stockid from hangyestock where hyid="%(hyID)s";' % locals()
    result = sqlrw.engine.execute(sql)
#     stockList = result.fetchall()
    stockList = [i[0] for i in result.fetchall()]
#     print len(stockList), stockList
    return stockList


def getHYIDForStock(stockID):
    """ 当查询指定股票的4级行业的代码
    """
    sql = ('select hyid from hangyestock where stockid="%(stockID)s";'
           % locals())
    result = sqlrw.engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        hyID = result[0]
        return hyID


def getHYIDForHY(hyID, subLevel):
    """ 查询指定行业包含的下级行业代码
    """
    level = len(hyID) / 2
    sql = ('select hyid from hangyename '
           'where hylevel%(level)sid="%(hyID)s" and hylevel="%(subLevel)s";') % locals()
#     print sql
    result = sqlrw.engine.execute(sql)
    result = result.fetchall()
#     print 'getHYIDForHY:', result
    if result is None:
        return None
    else:
        return [i[0] for i in result]


def getHYName(hyID):
    print 'getHYName(hyID):hyID: ', hyID
    sql = ('select hyname from hangyename where hyid="%(hyID)s";'
           % locals())
    result = sqlrw.engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        hyName = result[0]
        return hyName


def getHYProfitsIncRate(hyID, quarter):
    sql = ('select profitsIncRate from hyprofits '
           'where hyid="%(hyID)s" and date="%(quarter)s";'
           % locals())
    print sql
    result = sqlrw.engine.execute(sql).fetchone()
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
    return (hyIncRate1, hyIncRate2, hyIncRate3)


def getStockProfitsIncRate(stockID, quarter):
    sql = ('select incrate from ttmlirun '
           'where stockid="%(stockID)s" and date="%(quarter)s";'
           % locals())
    result = sqlrw.engine.execute(sql).fetchone()
    if result is not None:
        return result[0]
    else:
        return None


def getStockProfitsIncRates(stockID):
    curYear = datatrans.getCurYear()
    lastYearQuarter1 = (curYear - 3) * 10 + 4
    lastYearQuarter2 = (curYear - 2) * 10 + 4
    lastYearQuarter3 = (curYear - 1) * 10 + 4
    incRate1 = getStockProfitsIncRate(stockID, lastYearQuarter1)
    incRate2 = getStockProfitsIncRate(stockID, lastYearQuarter2)
    incRate3 = getStockProfitsIncRate(stockID, lastYearQuarter3)
    print (incRate1, incRate2, incRate3)
    return (incRate1, incRate2, incRate3)


def calHYTTMLirun(hyID, date):
    """ 计算指定行业的TTMLirun
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    """
    if len(hyID) == 8:
        return calHYTTMLirunLowLevel(hyID, date)
    else:
        return calHYTTMLirunHighLevel(hyID, date)


def calHYTTMLirunHighLevel(hyID, date):
    """ 计算第1、2、3级行业的TTM利润
    """
    level = len(hyID) / 2
    subHyIDList = getHYIDForHY(hyID, level + 1)
    if subHyIDList is None:
        return None
    profitsCur = 0
    for subHyID in subHyIDList:
        sql = ('select profits from hyprofits '
               'where hyid="%(subHyID)s" and date=%(date)s;') % locals()
#         print sql
        result = sqlrw.engine.execute(sql).fetchone()
#         print 'result:', result
        if result is None or result[0] is None:
            continue
        profitsCur += result[0]

    LastDate = date - 10
    sql = ('select profits from hyprofits '
           'where hyid="%(subHyID)s" and date="%(LastDate)s";') % locals()
#     print sql
    result = sqlrw.engine.execute(sql).fetchone()
#     print 'result 145:', result
    if result is None:
        profitsLast = None
#         profitsIncRate = None
    else:
        profitsLast = result[0]

    if profitsLast is None or profitsLast == 0:
        sql = (('replace into hyprofits(hyid, date, profits) '
                'values("%(hyID)s", "%(date)s", %(profitsCur)s);') % locals())
    else:
        profitsInc = profitsCur - profitsLast
        profitsIncRate = round(profitsInc / profitsLast, 2)
        sql = (('replace into hyprofits'
                '(hyid, date, profits, profitsInc, profitsIncRate) '
                'values("%(hyID)s", "%(date)s", %(profitsCur)s, '
                '%(profitsInc)s, %(profitsIncRate)s);') % locals())
#     print sql
    result = sqlrw.engine.execute(sql)
#     print 'result 158:', result
#     if result is None:
#         return False
#     else:
#         return result[0]


def calHYTTMLirunLowLevel(hyID, date):
    """ 计算第4级行业的TTM利润
    """
    stockList = getStockListForHY(hyID)
    allTTMLirunCur = sqlrw.readTTMLirunForDate(date)
    allTTMLirunLast = sqlrw.readTTMLirunForDate(date - 10)
    allTTMLirunLast = allTTMLirunLast[
        allTTMLirunLast['stockid'].isin(stockList)]
    if allTTMLirunLast.empty or allTTMLirunCur.empty:
        return False
    allTTMLirunCur = allTTMLirunCur[
        allTTMLirunCur['stockid'].isin(allTTMLirunLast['stockid'])]
    profitsCur = sum(allTTMLirunCur['ttmprofits'])
    profitsLast = sum(allTTMLirunLast['ttmprofits'])

    profitsInc = profitsCur - profitsLast
#     print 'allTTMLirunCur', allTTMLirunCur
#     print 'allTTMLirunLast', allTTMLirunLast
#     print 'profitsCur', profitsCur
#     print 'profitsLast', profitsLast
    profitsIncRate = round(profitsInc / abs(profitsLast) * 100, 2)
#     print profitsInc, profitsIncRate
#     return [profitsInc, profitsIncRate]
    sql = (('replace into hyprofits'
            '(hyid, date, profits, profitsInc, profitsIncRate) '
            'values("%(hyID)s", "%(date)s", "%(profitsCur)s", '
            '"%(profitsInc)s", "%(profitsIncRate)s");') % locals())
#     print sql
    sqlrw.engine.execute(sql)
    return True


def calAllHYTTMLirun(date):
    """ 计算各级行业TTM利润，依次计算第4、3、2、1级
    """
    for level in range(4, 0, -1):
        sql = 'select hyid from hangyename where hylevel=%(level)s;' % locals()
        result = sqlrw.engine.execute(sql)
        hyIDList = result.fetchall()
        hyIDList = [i[0] for i in hyIDList]
        print hyIDList
        for hyID in hyIDList:
            print hyID
            calHYTTMLirun(hyID, date)


def test():
    dates = datatrans.dateList(20171, 20172)
    for date in dates:
        calAllHYTTMLirun(date)


# def getHYIDName(stockID):
#     hyID = getHYIDForStock(stockID)
#     hyName = getHYName(hyID)
#     print stockID, hyID, hyName

if __name__ == '__main__':
    test()
#     calAllHYTTMLirun(20154)
#     hyID = '000101'
#     calHYTTMLirun(hyID, 20154)
    #     calHYTTMLirun(hyID, 20162)
#     stockList = ['000732', '', '', '', '', '', '', ]
