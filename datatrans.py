# -*- coding: utf-8 -*-
'''
Created on 2016年5月6日

@author: who8736
'''
import datetime as dt
import logging

import pandas as pd
from pandas.core.frame import DataFrame
from lxml import etree


def quarterSub(quarterDate, subNum):
    """
    # 从quarterDate中减去subNum个季度
    quarterDate: YYYYQ格式， 4位表示年，1表示季度

    """
    return quarterAdd(quarterDate, -subNum)


def quarterAdd(quarterDate, addNum):
    """
    # 从quarterDate中减去subNum个季度
    quarterDate: YYYYQ格式， 4位表示年，1表示季度

    """
    quarters = int(quarterDate / 10) * 4 + (quarterDate % 10)
    quarters = quarters + addNum
    year = (quarters - 1) / 4
#     print year
    quarterDate = year * 10 + (quarters - year * 4)
    return quarterDate


def dateList(startDate, endDate):
    dates = []
    while startDate <= endDate:
        dates.append(startDate)
        startDate += 1
        if startDate % 10 > 4:
            startDate = (int(startDate / 10) + 1) * 10 + 1
    return dates


def transDateToQuarter(date):
    return date.year * 10 + int((date.month + 2) / 3)


def getLastQuarter():
    curQuarter = transDateToQuarter(dt.datetime.today())
    return quarterSub(curQuarter, 1)


def getCurYear():
    return dt.datetime.today().year


def gubenDataToDf(stockID, guben):
    gubenTree = etree.HTML(guben)
    gubenData = gubenTree.xpath('''//html//body//div//div
                                //div//div//table//tr//td
                                //table//tr//td//table//tr//td''')
    date = [gubenData[i][0].text for i in range(0, len(gubenData), 2)]
    date = [dt.datetime.strptime(d, '%Y-%m-%d') for d in date]
#     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
#     print totalshares
#     t = [i[:-2] for i in totalshares]
#     print t
    try:
        totalshares = [float(i[:-2]) * 10000 for i in totalshares]
    except ValueError, e:
        logging.error('stockID:%s, %s', stockID, e)
#     print totalshares
    gubenDf = DataFrame({'stockid': stockID,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf


def gubenDfToList(df):
    timea = dt.datetime.now()
    gubenList = []
    for date, row in df.iterrows():
        stockid = row['stockid']
        date = row['date']
        totalshares = row['totalshares']

        guben = {'stockid': stockid,
                 'date': date,
                 'totalshares': totalshares
                 }
        gubenList.append(guben)
    timeb = dt.datetime.now()
    logging.debug('klineDfToList took %s' % (timeb - timea))
    return gubenList


def transLirunDf(df, year, quarter):
    date = [year * 10 + quarter for unusedi in range(df['code'].count())]
    stockid = df['code']
    profits = df['net_profits']
    if quarter == 4:
        year += 1
    reportdate = df['report_date'].apply(lambda x: str(year) + '-' + x)
    rd = []
    for d in reportdate:
        if d[-5:] == '02-29':
            d = d[:-5] + '02-28'
        dd = dt.datetime.strptime(d, '%Y-%m-%d')
        rd.append(dd)
    data = {'stockid': stockid,
            'date': date,
            'profits': profits * 10000,
            'reportdate': rd}
    df = pd.DataFrame(data)
#     print 'transLirunDf, len(df):%s' % len(df)
    df = df.drop_duplicates()
    return df


def transGuzhiDataToDict(guzhi):
    guzhiTree = etree.HTML(guzhi)
    xpathStr = '//html//body//div//tr'
    guzhiData = guzhiTree.xpath(xpathStr)
    guzhiDict = {}
    try:
        stockID = guzhiData[2][1].text.strip()  # 取得股票代码
    except IndexError:
        return None  # 无数据
    peg = guzhiData[2][3].text.strip()
    next1YearPE = guzhiData[2][6].text.strip()
    next2YearPE = guzhiData[2][7].text.strip()
    next3YearPE = guzhiData[2][8].text.strip()
    if stockID != '--':
        guzhiDict['stockid'] = stockID  # 取得股票代码
    if peg != '--':
        guzhiDict['peg'] = float(peg.replace(',', ''))
    if next1YearPE != '--':
        guzhiDict['next1YearPE'] = float(next1YearPE.replace(',', ''))
    if next2YearPE != '--':
        guzhiDict['next2YearPE'] = float(next2YearPE.replace(',', ''))
    if next3YearPE != '--':
        guzhiDict['next3YearPE'] = float(next3YearPE.replace(',', ''))
    return guzhiDict


def transDfToList(df):
    outList = []
    for index, row in df.iterrows():
        tmpDict = row.to_dict()
        tmpDict[df.index.name] = index
        outList.append(tmpDict)
    return outList


def transQuarterToDate(date):
    year = date / 10
    month = (date % 10) * 3
    days = {3: 31, 6: 30, 9: 30, 12: 31}
    day = days[month]
    return '%(year)d-%(month)02d-%(day)d' % locals()

if __name__ == '__main__':
    quarterDate = 20161
    subNum = 6

    q = quarterSub(quarterDate, subNum - 1)
    print q
    print dateList(q, quarterDate)
    print len(dateList(q, quarterDate))
