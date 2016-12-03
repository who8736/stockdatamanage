# -*- coding: utf-8 -*-
'''
Created on 2016年12月2日

@author: who8736
'''
import sqlrw
import datatrans


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
    profitsIncRate = profitsInc / abs(profitsLast) * 100
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
