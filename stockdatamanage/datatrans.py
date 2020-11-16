# -*- coding: utf-8 -*-
"""
Created on 2016年5月6日

@author: who8736
"""
import datetime as dt
# from datetime import datetime, timedelta, date
import logging

import pandas as pd
from pandas.core.frame import DataFrame
from lxml import etree
# from initlog import initlog


def quarterSub(_quarterDate, subNum):
    """
    # 从quarterDate中减去subNum个季度
    quarterDate: YYYYQ格式， 4位表示年，1表示季度

    """
    return quarterAdd(_quarterDate, -subNum)


def quarterAdd(_quarterDate, addNum):
    """
    # 从quarterDate中加上subNum个季度
    quarterDate: YYYYQ格式， 4位表示年，1表示季度

    """
    quarters = int(_quarterDate / 10) * 4 + (_quarterDate % 10)
    quarters = quarters + addNum
    year = (quarters - 1) // 4
#     print year
    _quarterDate = year * 10 + (quarters - year * 4)
    return _quarterDate


def QuarterList(startDate, endDate):
    """ 生成从startDate到endDate的季度列表
    """
    _dateList = []
    while startDate <= endDate:
        _dateList.append(startDate)
        startDate += 1
        if startDate % 10 > 4:
            startDate = (int(startDate / 10) + 1) * 10 + 1
    return _dateList


def dateStrList(startDate, endDate, formatStr='%Y%m%d'):
    """生成从startDate到endDate的日期列表，日期样式为"20160101"
    :param startDate: str, 'YYYYmmdd'
    :param endDate: str, 'YYYYmmdd'
    :param formatStr:
    :return:
    """
    if isinstance(startDate, dt.date):
        startDate = startDate.strftime(formatStr)
    if isinstance(endDate, dt.date):
        endDate = endDate.strftime(formatStr)
    sql = 'select cal_date from trade_cal where is_open=1 and exchange="SSE"'
    result = engine.execute(sql).fetchall()
    if result:
        return [row[0] for row in result]


def dateList(startDate, endDate):
    dateList = []
    curDate = startDate
    while curDate <= endDate:
        dateList.append(curDate)
        curDate = curDate + dt.timedelta(days=1)
    return dateList


# def parse_ymd(s):
#     """ 日期字符串转换为datetime
#     """
#     year_s, mon_s, day_s = s.split('-')
#     return datetime(int(year_s), int(mon_s), int(day_s))


def transDateToQuarter(_date):
    """ 将datetime.datetime类型的日期转换为季度格式
    # 2015年4月7日，返回20152
    """
    return _date.year * 10 + int((_date.month + 2) / 3)


def getLastQuarter():
    """ 返回当前日期的上一个季度
    # 如今天是2015年4月7日，返回20151
    """
    curQuarter = transDateToQuarter(dt.datetime.today())
    return quarterSub(curQuarter, 1)


def getCurYear():
    return dt.datetime.today().year


def gubenDataToDfSina(ts_code, guben):
    """
    新浪的股本数据转换为DataFrame格式
    :param ts_code: 股本代码， 600000
    :param guben: 下载股本数据, html格式
    :return: 股本数据， DataFrame格式
    """
    gubenTree = etree.HTML(guben)
    gubenData = gubenTree.xpath('''//html//body//div//div
                                //div//div//table//tr//td
                                //table//tr//td//table//tr//td''')
    date = [gubenData[i][0].text for i in range(0, len(gubenData), 2)]
    date = [datetime.strptime(d, '%Y-%m-%d') for d in date]
#     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
#     print totalshares
#     t = [i[:-2] for i in totalshares]
#     print t
    danwei = {'万股':10000, '亿股':100000000}
    try:
        totalshares = [float(i[:-2]) * danwei[i[-2:]] for i in totalshares]
    except ValueError as e:
        logging.error('ts_code:%s, %s', ts_code, e)
#     print totalshares
    gubenDf = DataFrame({'ts_code': ts_code,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf


def gubenDataToDfEastymoney(ts_code, guben):
    """
    东方财富的股本数据转换为DataFrame格式
    :param ts_code: 股本代码， 600000
    :param guben: 下载股本数据, html格式
    :return: 股本数据， DataFrame格式
    """
    gubenTree = etree.HTML(guben)
    gubenData = gubenTree.xpath('''//html//body//div//div
                                //div//div//table//tr//td
                                //table//tr//td//table//tr//td''')
    date = [gubenData[i][0].text for i in range(0, len(gubenData), 2)]
    date = [datetime.strptime(d, '%Y-%m-%d') for d in date]
    #     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
    #     print totalshares
    #     t = [i[:-2] for i in totalshares]
    #     print t
    try:
        totalshares = [float(i[:-2]) * 10000 for i in totalshares]
    except ValueError as e:
        logging.error('ts_code:%s, %s', ts_code, e)
    #     print totalshares
    gubenDf = DataFrame({'ts_code': ts_code,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf


def gubenDfToList(df):
    timea = dt.datetime.now()
    gubenList = []
    for date, row in df.iterrows():
        ts_code = row['ts_code']
        date = row['date']
        totalshares = row['totalshares']

        guben = {'ts_code': ts_code,
                 'date': date,
                 'totalshares': totalshares
                 }
        gubenList.append(guben)
    timeb = dt.datetime.now()
    logging.debug('klineDfToList took %s' % (timeb - timea))
    return gubenList


def transLirunDf(df, year, quarter):
    date = [year * 10 + quarter for _ in range(df['code'].count())]
    ts_code = df['code']
    profits = df['net_profits']
    if quarter == 4:
        year += 1
    reportdate = df['report_date'].apply(lambda x: str(year) + '-' + x)
    rd = []
    for d in reportdate:
        if d[-5:] == '02-29':
            d = d[:-5] + '02-28'
        dd = datetime.strptime(d, '%Y-%m-%d')
        rd.append(dd)
    data = {'ts_code': ts_code,
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
        ts_code = guzhiData[2][1].text.strip()  # 取得股票代码
    except IndexError:
        return None  # 无数据
    peg = guzhiData[2][3].text.strip()
    next1YearPE = guzhiData[2][6].text.strip()
    next2YearPE = guzhiData[2][7].text.strip()
    next3YearPE = guzhiData[2][8].text.strip()
    if ts_code != '--':
        guzhiDict['ts_code'] = ts_code  # 取得股票代码
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
        if df.index.name is not None:
            tmpDict[df.index.name] = index
        outList.append(tmpDict)
    return outList


def transQuarterToDate(_date):
    year = _date // 10
    month = (_date % 10) * 3
    days = {3: 31, 6: 30, 9: 30, 12: 31}
    day = days[month]
    # print(year, month, day)
    return f'{year}{month:02}{day:02}'


def lastQarterDate(_date):
    if isinstance(_date, str):
        _date = dt.datetime.strptime(_date, '%Y%m%d')
    quarterFirstDay = dt.date(_date.year, (_date.month - 1) // 3 * 3 + 1, 1)
    return quarterFirstDay - dt.timedelta(days=1)


def lastYearDate(_date):
    return str(int(_date[:4]) - 1) + _date[4:]


def lastYearEndDate(_date):
    return str(int(_date[:4]) - 1) + '1231'


def transTushareDateToQuarter(date):
    year = int(date[:4])
    qdic = {'03': 1, '06': 2, '09': 3, '12': 4}
    quarter = qdic[date[4:6]]
    return year * 10 + quarter


