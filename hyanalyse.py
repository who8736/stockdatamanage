# -*- coding: utf-8 -*-
'''
Created on 2016年12月2日

@author: who8736
'''
import sqlrw
import datatrans
import logging


def getStockListForHY(hyID):
    levelNum = len(hyID) / 2
    levels = ['level1', 'level2', 'level3', 'level4']
    level = levels[levelNum - 1]
    sql = 'select stockid from hangye where %(level)s="%(hyID)s";' % locals()
    result = sqlrw.engine.execute(sql)
#     stockList = result.fetchall()
    stockList = [i[0] for i in result.fetchall()]
#     print len(stockList), stockList
    return stockList


def getHYID(stockID, level):
    if level < 1 or level > 4:
        logging.error('error HY level: %s', level)
        return None
    sql = ('select level%(level)s from hangye where stockid="%(stockID)s";'
           % locals())
    result = sqlrw.engine.execute(sql)
    return result.fetchone()[0]


def getHYName(hyID):
    print 'getHYName(hyID):hyID: ', hyID
    sql = ('select levelname from hangyename where levelid="%(hyID)s";'
           % locals())
    result = sqlrw.engine.execute(sql)
    hyName = result.fetchone()[0]
    return hyName


def getHYProfitsIncRate(hyID, quarter):
    sql = ('select profitsIncRate from hyprofits '
           'where hyid="%(hyID)s" and date="%(quarter)s";'
           % locals())
    result = sqlrw.engine.execute(sql)
    return result.fetchone()[0]


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
    print 'allTTMLirunCur', allTTMLirunCur
    print 'allTTMLirunLast', allTTMLirunLast
    print 'profitsCur', profitsCur
    print 'profitsLast', profitsLast
    profitsIncRate = round(profitsInc / abs(profitsLast) * 100, 2)
    print profitsInc, profitsIncRate
#     return [profitsInc, profitsIncRate]
    sql = (('replace into hyprofits'
            '(hyid, date, profitsInc, profitsIncRate) '
            'values("%(hyID)s", "%(date)s", "%(profitsInc)s", '
            '"%(profitsIncRate)s");') % locals())
    sqlrw.engine.execute(sql)
    return True


def calAllHYTTMLirun(date):
    sql = 'select levelid from hangyename;'
    result = sqlrw.engine.execute(sql)
    hyIDList = result.fetchall()
    hyIDList = [i[0] for i in hyIDList]
    print hyIDList
    for hyID in hyIDList:
        print hyID
        calHYTTMLirun(hyID, date)


def test():
    dates = datatrans.dateList(20111, 20154)
    for date in dates:
        calAllHYTTMLirun(date)

if __name__ == '__main__':
    #     test()
    #     calAllHYTTMLirunForDate(20154)
    hyID = '02030301'
    calHYTTMLirun(hyID, 20164)
#     calHYTTMLirun(hyID, 20162)
