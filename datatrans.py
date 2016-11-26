# -*- coding: utf-8 -*-
'''
Created on 2016年5月6日

@author: who8736
'''


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
    print year
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
