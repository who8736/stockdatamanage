# -*- coding: utf-8 -*-
'''
Created on 2017年2月10日

@author: who8736
'''

# import datetime
import time
from io import BytesIO

import matplotlib

matplotlib.use('Agg')  # @UndefinedVariable

import matplotlib.pyplot as plt  # @IgnorePep8
# from matplotlib.finance import candlestick_ohlc  # @IgnorePep8
from mpl_finance import candlestick_ohlc  # @IgnorePep8
import matplotlib.gridspec as gs  # @IgnorePep8
from matplotlib.dates import DateFormatter, MonthLocator  # @IgnorePep8
from matplotlib.ticker import FixedLocator  # @IgnorePep8
import tushare  # @IgnorePep8

from sqlrw import engine  # @IgnorePep8
from datatrans import dateStrList  # @IgnorePep8


def scatter(startDate, endDate):
    dateList = dateStrList(startDate, endDate)
    for date in dateList:
        print(date)
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


def plotKlineOld(stockID):
    #     return plotKline(stockID)
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

    print(dates)
    print(peDatas)
    gs1 = gs.GridSpec(3, 1)
    gs1.update(hspace=0)
    fig = plt.figure()
    ax1 = fig.add_subplot(gs1[0:2, :])
    candlestick_ohlc(ax1, klineDatas)
    ax1.set_title(stockID)
    ax2 = fig.add_subplot(gs1[2:3, :])
    ax2.plot(dates, peDatas)
    ax2.xaxis.set_major_locator(MonthLocator())
    ax2.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
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


def plotKline(stockID):
    """ 绘制K线与TTMPE图
    """
    sql = ('select date, open, high, low, close, ttmpe '
           'from kline%(stockID)s '
           'order by date desc limit 1000;' % locals())
    result = engine.execute(sql).fetchall()
    stockDatas = [i for i in reversed(result)]
    klineDatas = []
    dates = []
    peDatas = []
    indexes = list(range(len(stockDatas)))
    for i in indexes:
        date, _open, high, low, close, ttmpe = stockDatas[i]
        klineDatas.append([i, _open, high, low, close])
        dates.append(date.strftime("%Y-%m-%d"))
        peDatas.append(ttmpe)

    gs1 = gs.GridSpec(3, 1)
    gs1.update(hspace=0)
    fig = plt.figure()
    ax1 = fig.add_subplot(gs1[0:2, :])
    candlestick_ohlc(ax1, klineDatas)
    ax1.set_title(stockID)
    plt.grid(True)
    ax2 = fig.add_subplot(gs1[2:3, :])
    ax2.plot(indexes, peDatas)
    ax1.set_xlim((0, len(stockDatas)))
    ax2.set_xlim((0, len(stockDatas)))
    tickerIndex, tickerLabels = getMonthIndex(dates)
    locator = FixedLocator(tickerIndex)
    ax1.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_locator(locator)
    ax2.set_xticklabels(tickerLabels)
#     for label in ax2.get_xticklabels():
#         label.set_rotation(45)
    plt.grid(True)
    plt.legend()
    plt.show()
    imgData = BytesIO()
    fig.savefig(imgData, format='png')
    return imgData


def getMonthIndex(dates):
    month = ''
    monthIndex = []
    monthstr = []
    for i in range(len(dates)):
        date = dates[i]
        if month != date[:4]:
            month = date[:4]
            monthIndex.append(i)
            monthstr.append(month)
    return monthIndex, monthstr


def test():
    df = tushare.get_k_data('600000')
    df = df[-200:]
    ax = plt.subplot(111)
    print(df.head())
    ax.plot(df.index, df.close)
    monthIndex = getMonthIndex(df.date)
    tickerIndex = df.index[monthIndex]
    tickerLabels = df.date[monthIndex].str[:7]
    locator = FixedLocator(tickerIndex)
    ax.xaxis.set_major_locator(locator)
    ax.set_xticklabels(tickerLabels)
    for label in ax.get_xticklabels():
        label.set_rotation(45)
    plt.grid(True)
#     plt.savefig('testplot.png')
    plt.legend()
    plt.show()


if __name__ == '__main__':
    startDate = '2017-01-01'
    endDate = '2017-03-31'
#     k = dateStrList(startDate, endDate)
#     print k
#     scatter(startDate, endDate)
    plotKline('002100')
#     test()
