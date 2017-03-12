# -*- coding: utf-8 -*-
'''
Created on 2017年2月10日

@author: who8736
'''

# import datetime
import time
from io import BytesIO

import matplotlib.pyplot as plt
from matplotlib.finance import candlestick_ohlc
import matplotlib.gridspec as gs
from matplotlib.dates import DateFormatter, MonthLocator

from sqlrw import engine
from datatrans import dateStrList


def scatter(startDate, endDate):
    dateList = dateStrList(startDate, endDate)
    for date in dateList:
        print date
        sql = ('select pe, lirunincrease from pelirunincrease '
               'where date="%(date)s";' % locals())
        result = engine.execute(sql)
        peList = []
        incrateList = []
        for pe, lirunincrease in result.fetchall():
            if pe is not None and lirunincrease is not None:
                peList.append(pe)
                incrateList.append(lirunincrease)
        if not peList:
            continue
        plt.scatter(incrateList, peList)
        plt.axes().set_xlim((-200, 200))
        plt.axes().set_ylim((-200, 200))
        filename = './data/plot/%(date)s.png' % locals()
        plt.savefig(filename)
        plt.clf()
#         plt.show()


def plotKline(stockID):

    #     ax2 = fig.add_subplot(2, 1, 2)
    sql = ('select date, open, high, low, close, ttmpe '
           'from kline%(stockID)s '
           'order by date desc limit 1000;' % locals())
    result = engine.execute(sql)
    stockDatas = result.fetchall()
    klineDatas = []
    dates = []
    peDatas = []
#     klineDatas = result.fetchall()
    for date, _open, high, low, close, ttmpe in stockDatas:
        klineDatas.append([time.mktime(date.timetuple()),
                           _open, high, low, close])
        dates.append(time.mktime(date.timetuple()))
        peDatas.append(ttmpe)

    print peDatas
    gs1 = gs.GridSpec(3, 1)
    gs1.update(hspace=0)
    fig = plt.figure()
    ax1 = fig.add_subplot(gs1[0:2, :])
    candlestick_ohlc(ax1, klineDatas)
    ax1.set_title(stockID)
    ax2 = fig.add_subplot(gs1[2:3, :])
    ax2.plot(dates, peDatas)
#     ax2.xaxis.set_major_locator(MonthLocator())
#     ax2.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

#     ax1.subplots_adjust(hspace=None)
#     fig.subplots_adjust(hspace=0)
#     plt.show()
    imgData = BytesIO()
    fig.savefig(imgData, format='png')
#     imgData.seek(0)

    return imgData
#     datetime.date
#     datetime.timestamp()
#     datetime.

if __name__ == '__main__':
    startDate = '2017-01-01'
    endDate = '2017-03-31'
#     k = dateStrList(startDate, endDate)
#     print k
#     scatter(startDate, endDate)
    plotKline('600519')
