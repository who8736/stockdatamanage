# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 10:22:14 2019

@author: ff
"""
from datetime import datetime

import pandas as pd
import baostock as bs
import matplotlib.pyplot as plt
import numpy as np

import sqlrw


def histTTMPETrend(stockID, type='stock', histLen=1000, startDate=None, endDate=None):
    """
    返回某股票PE水平
    histLen为参照时间长度，如当前TTMPE与前1000个交易日的TTMPE比较
    取值0-100
    为0时表示当前TTMPE处于历史最低位
    为100时表示当前TTMPE处于历史最高位

    :param stockID:
    :param type: 'stock'股票， 'index'指数
    :param histLen:
    :param startDate:
    :param endDate:
    :return:
    """

    if startDate is None:
        startDate = datetime.strptime('20100101', '%Y%m%d').date()
    if endDate is None:
        endDate = datetime.today().date()

    #    绘图， 调整双坐标系
    # stockID = '000651'
    ttmpe = sqlrw.readTTMPE(stockID, startDate, type=type)
    #    ttmpe = ttmpe[-15:]
    ttmpe = ttmpe.set_index('date')
    ttmpe['perate'] = 0
    #    print(ttmpe)

    #    histLen = 10
    for cur in range(histLen, len(ttmpe)):
        begin = cur - histLen
        tmp = ttmpe[begin:cur]
        tmp = tmp[tmp.ttmpe <= ttmpe.iloc[cur, 0]]
        #        print('-----------------')
        #        print(tmp)
        #        print('cur ttmpe: %d' % ttmpe.iloc[cur, 0])
        #        print(begin, cur, len(tmp))
        ttmpe.iloc[cur, 1] = len(tmp) / histLen * 100

    histLen = 1000
    ttmpe['perate1'] = 0
    for cur in range(histLen, len(ttmpe)):
        begin = cur - histLen
        tmp = ttmpe[begin:cur]
        tmp = tmp[tmp.ttmpe <= ttmpe.iloc[cur, 0]]
        #        print('-----------------')
        #        print(tmp)
        #        print('cur ttmpe: %d' % ttmpe.iloc[cur, 0])
        #        print(begin, cur, len(tmp))
        ttmpe.iloc[cur, 2] = len(tmp) / histLen * 100
    return ttmpe


def downHFQ(stockID):
    lg = bs.login()
    if lg.error_code != '0':
        return None
    if stockID[0] == "6":
        stockID = "sh." + stockID
    else:
        stockID = "sz." + stockID

    rs = bs.query_history_k_data_plus(stockID,
                                      "date,close",
                                      frequency="d", adjustflag="3")
    reList = []
    while rs.next():
        reList.append(rs.get_row_data())
    result = pd.DataFrame(reList, columns=rs.fields)
    result = result.set_index('date')
    bs.logout()
    return result


def plotPERate():
    """
    绘制股票历史PE及PE水平
    histLen: 当前PE在过去histLen天内的水平，0代表历史最低，100代表历史最高
    :return:
    """
    stockID = '002087'
    histLen = 200
    ttmpe = histTTMPETrend(stockID, histLen)

    fig = plt.figure()
    ax1 = fig.add_subplot(111)
    ax1.plot(ttmpe.index, ttmpe.ttmpe, 'b')
    ax1.set_ylabel('PE')
    ax2 = ax1.twinx()
    ax2.plot(ttmpe.index, ttmpe.perate, 'y')
    ax2.plot(ttmpe.index, ttmpe.perate1, 'c')
    ax1.set_ylabel('历史PE水平')
    plt.show()


if __name__ == '__main__':
    # stockID = '000651'
    # histLen = 200
    # ttmpe = histTTMPETrend(stockID, histLen)
    # print(ttmpe)

    plotPERate()

    # histClose = downHFQ(stockID)
