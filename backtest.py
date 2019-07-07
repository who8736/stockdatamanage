# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 10:22:14 2019

@author: ff
"""
from datetime import datetime
import re
import os

import pandas as pd
import baostock as bs
import matplotlib.pyplot as plt
import numpy as np

import sqlrw


def histTTMPETrend(stockID, type='stock', histLen=1000, startDate=None,
                   endDate=None):
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
    ttmpe = sqlrw.readTTMPE(stockID, startDate, datatype=type)
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


# 专用于测试正则表达式替换语法，测试完后删除
###################################################################
from sqlrw import engine


def readCurrentTTMPE(stockID):
    sql = (f'select ttmpe from klinestock where stockid="{stockID}" and date=('
           f'select max(`date`) from klinestock where stockid="{stockID}")')

    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        return result[0]


def readTTMLirunForDate(date):
    """从TTMLirun表读取某季度股票TTM利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式TTM利润
    """
    sql = (f'select * from ttmlirun where '
           f'`date` = "{date}"')
    df = pd.read_sql(sql, engine)
    return df


def readLirunForDate(date):
    """从Lirun表读取一期股票利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式利润
    """
    sql = (f'select * from lirun where '
           f'`date`="{date}"')
    df = pd.read_sql(sql, engine)
    return df


def tihuan():
    rootPath = os.path.abspath('.')
    for parentPath, dirName, filenames in os.walk(rootPath):
        for filename in filenames:
            if (os.path.splitext(filename)[-1] == '.py'
                    and filename != 'backtest.py'):
                name = os.path.join(parentPath, filename)
                print(name)
                _tihuan(name)


def _tihuan(filename):
    # filename = 'bokehtest.py'
    outstr = []
    with open(filename, 'r', encoding='utf-8') as f:
        instr = f.readlines()
        for i in instr:
            tmp = re.sub('%\((\w*)\)s', r'{\1}', i)
            tmp = re.sub(' % locals\(\)', '', tmp)
            tmp = re.sub('(\'.*{.*\')', r'f\1', tmp)
            outstr.append(tmp)
            print(tmp)
    with open(filename, 'w', encoding='utf-8') as f:
        f.writelines(outstr)


###################################################################
# 测试段结束


if __name__ == '__main__':
    # stockID = '000651'
    # histLen = 200
    # ttmpe = histTTMPETrend(stockID, histLen)
    # print(ttmpe)

    # plotPERate()

    tihuan()

    # histClose = downHFQ(stockID)
