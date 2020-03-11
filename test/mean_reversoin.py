"""本文件用于时间序列分析的测试
两种常见的用于判断时间序列是否平稳
第一种： ADF Test
使用statsmodels模块中的adfuller

第二种：
"""

import matplotlib.dates as mdates
import matplotlib.pyplot as plt  # @IgnorePep8
import numpy as np
import pandas as pd
from matplotlib.dates import DateFormatter
from matplotlib.dates import YearLocator  # @IgnorePep8
from statsmodels.tsa.stattools import adfuller

from sqlconn import engine
from sqlrw import readStockListFromSQL


def adfTestPE(stockID, startDate, endDate):
    sql = (f'select ttmpe from klinestock '
           f'where stockid="{stockID}" '
           f'and date>="{startDate}" and date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    resulta = adfuller(df['ttmpe'])
    df1 = np.diff(df['ttmpe'])
    resultb = adfuller(df1)
    print('resulta:\n', resulta)
    print('resultb:\n', resultb)


def adfTestProfits(stockID, startDate, endDate):
    sql = (f'select ttmprofits from ttmlirun '
           f'where stockid="{stockID}" '
           f'and date>="{startDate}" and date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    resulta = adfuller(df['ttmprofits'])
    df1 = np.diff(df['ttmprofits'])
    resultb = adfuller(df1)
    print('resulta:\n', resulta)
    print('resultb:\n', resultb)
    flag = (resultb[0] < resultb[4]['1%']
            and resultb[0] < resultb[4]['5%']
            and resultb[0] < resultb[4]['10%'])
    return resultb[0], resultb[1], flag


def adfTestProfits1(stockID, startDate, endDate):
    """

    :param stockID:
    :param startDate:
    :param endDate:
    :return:

    adfuller返回值
    adf （float）
    测试统计
    pvalue （float）
    MacKinnon基于MacKinnon的近似p值（1994年，2010年）
    usedlag （int）
    使用的滞后数量
    nobs（ int）
    用于ADF回归的观察数和临界值的计算
    critical values（dict）
    测试统计数据的临界值为1％，5％和10％。基于MacKinnon（2010）
    icbest（float）
    如果autolag不是None，则最大化信息标准。
    resstore （ResultStore，可选）
    一个虚拟类，其结果作为属性附加
    """
    sql = (f'select incrate from ttmlirun '
           f'where stockid="{stockID}" '
           f'and date>="{startDate}" and date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    resulta = adfuller(df['incrate'])
    df1 = np.diff(df['incrate'])
    resultb = adfuller(df1)
    print('resulta:\n', resulta)
    print('resultb:\n', resultb)
    # flag = (resultb[0] < resultb[4]['1%']
    #         and resultb[0] < resultb[4]['5%']
    #         and resultb[0] < resultb[4]['10%'])
    # return (round(resultb[0], 2), round(resultb[1], 2), flag)
    return resultb


def adfTestAll():
    """对所有股票2009年1季度至2020年1季度TTM利润增长率进行ADF检测"""
    stockID = '000651'
    startDate = '20091'
    endDate = '20201'
    adfTestProfits(stockID, startDate, endDate)

    stockList = readStockListFromSQL()
    print(stockList)
    stockIDs = []
    stockNames = []
    adfs = []
    pvalues = []
    flags = []
    cvalue1s = []
    cvalue5s = []
    cvalue10s = []
    # for stockID, name in stockList[:10]:
    for stockID, name in stockList:
        print('正在处理:', stockID)
        try:
            result = adfTestProfits1(stockID, startDate, endDate)
        except Exception as e:
            print(stockID, e)
        else:
            flag = (result[0] < result[4]['1%'] and
                    result[0] < result[4]['5%'] and
                    result[0] < result[4]['10%'] and
                    result[1] < 0.05)
            stockIDs.append(stockID)
            stockNames.append(name)
            adfs.append(result[0])
            pvalues.append(round(result[1], 4))
            flags.append(flag)
            cvalue1s.append(round(result[4]['1%'], 4))
            cvalue5s.append(round(result[4]['5%'], 4))
            cvalue10s.append(round(result[4]['10%'], 4))

    df = pd.DataFrame({'stockid': stockIDs,
                       'name': stockNames,
                       'adf': adfs,
                       'pvalue': pvalues,
                       'flag': flags,
                       'cvalue1': cvalue1s,
                       'cvalue5': cvalue5s,
                       'cvalue10': cvalue10s
                       })
    df.to_excel('test.xlsx')
    print(df)


def findPairs(stockIDa, stockIDb, startDate='20090101', endDate='20191231'):
    """两支股票的TTMPE是否存在协整关系
    时间区间为20090101至20191231
    1.两只股票TTMPE的折线图
    2.两只股票TTMPE的散点图
    3.两只股票的线性回归
    4.根据线性回归结果计算残差，返回残差的ADF检验结果
    """
    sql = (f'select date, ttmpe from klinestock where stockid="{stockIDa}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    dfa.rename(columns={'ttmpe': 'ttmpea'}, inplace=True)
    dfa.set_index('date', inplace=True)
    print(dfa)

    sql = (f'select date, ttmpe from klinestock where stockid="{stockIDb}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfb = pd.read_sql(sql, engine)
    dfb.rename(columns={'ttmpe': 'ttmpeb'}, inplace=True)
    dfb.set_index('date', inplace=True)
    print(dfb)

    df = pd.merge(dfa, dfb, left_index=True, right_index=True)
    # df.set_index('date', inplace=True)
    print(df)

    # 绘制拆线图
    fig1 = plt.figure()
    ax1 = fig1.add_subplot()
    ax1.plot(dfa.index, dfa.ttmpea, color='blue')
    ax1.plot(dfb.index, dfb.ttmpeb, color='yellow')
    # 设置X轴的刻度间隔
    # 可选:YearLocator,年刻度; MonthLocator,月刻度; DayLocator,日刻度
    ax1.xaxis.set_major_locator(YearLocator())
    # 设置X轴主刻度的显示格式
    ax1.xaxis.set_major_formatter(DateFormatter('%Y'))
    # 设置鼠标悬停时，在左下显示的日期格式
    ax1.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    # 自动调整X轴标签角度
    fig1.autofmt_xdate()

    # 绘制散点图
    fig2 = plt.figure()
    ax2 = fig2.add_subplot()
    ax2.scatter(df.ttmpea, df.ttmpeb)
    # fig2.autofmt_xdate()

    plt.grid(True)
    plt.show()


if __name__ == '__main__':
    pass
    findPairs('000651', '000002', startDate='20140101')
