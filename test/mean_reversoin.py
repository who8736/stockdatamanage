"""本文件用于时间序列分析的测试
两种常见的用于判断时间序列是否平稳
第一种： ADF Test
使用statsmodels模块中的adfuller

第二种：
"""

import matplotlib.dates as mdates
import matplotlib.pyplot as plt  # @IgnorePep8
import matplotlib.gridspec as gridspec
from matplotlib.dates import DateFormatter
from matplotlib.dates import YearLocator  # @IgnorePep8
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
import tushare as ts

from sqlconn import engine
from sqlrw import readStockListFromSQL


def adfTestPE(stockID, startDate, endDate, plotFlag=False):
    sql = (f'select ttmpe from klinestock '
           f'where stockid="{stockID}" '
           f'and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    resulta = adfuller(dfa['ttmpe'])
    dfb = np.diff(dfa['ttmpe'])
    resultb = adfuller(dfb)
    print('stock:', stockID)
    print('resulta:\n', resulta)
    print('resultb:\n', resultb)
    print('mean of dfb:', np.mean(dfb))
    if plotFlag:
        plotDf(stockID, dfa, dfb)

    return resulta


def plotDf(stockID, dfa, dfb):
    """
    暂时用于绘制用于PE的ADF检测的两个图，其中dfa为TTMPE，dfb为TTMPE的一阶差分
    :param dfa:
    :param dfb:
    :return:
    """
    fig = plt.figure(figsize=(10, 5))
    gs = gridspec.GridSpec(1, 2)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[0, 1])
    # 绘制拆线图
    ax1.plot(dfa.ttmpe, color='blue', label=stockID)
    ax1.legend()
    ax1.set_title(stockID, fontsize=12)
    ax2.plot(dfb, color='blue', label=stockID)
    ax2.legend()
    ax2.set_title(stockID, fontsize=12)
    # 设置X轴的刻度间隔
    # 可选:YearLocator,年刻度; MonthLocator,月刻度; DayLocator,日刻度
    # ax1.xaxis.set_major_locator(YearLocator())
    # 设置X轴主刻度的显示格式
    # ax1.xaxis.set_major_formatter(DateFormatter('%Y'))
    # 设置鼠标悬停时，在左下显示的日期格式
    # ax1.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    # 自动调整X轴标签角度
    # fig.autofmt_xdate()

    plt.grid(True)
    plt.show()


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


def adfTestAllProfits():
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


def adfTestAllPE(stockList, startDate, endDate, plotFlag):
    """
    
    :param stockList: 
    :param startDate: 
    :param endDate: 
    :return: 
    """
    stockIDs = []
    adfs = []
    pvalues = []
    flags = []
    cvalue1s = []
    cvalue5s = []
    cvalue10s = []
    # for stockID, name in stockList[:10]:
    for stockID in stockList:
        print('正在处理:', stockID)
        try:
            result = adfTestPE(stockID, startDate, endDate, plotFlag=plotFlag)
        except Exception as e:
            print(stockID, e)
        else:
            flag = (result[0] < result[4]['1%'] and
                    result[0] < result[4]['5%'] and
                    result[0] < result[4]['10%'] and
                    result[1] < 0.05)
            stockIDs.append(stockID)
            adfs.append(result[0])
            pvalues.append(round(result[1], 4))
            cvalue1s.append(round(result[4]['1%'], 4))
            cvalue5s.append(round(result[4]['5%'], 4))
            cvalue10s.append(round(result[4]['10%'], 4))
            flags.append(flag)
    df = pd.DataFrame({'stockid': stockIDs,
                       'adf': adfs,
                       'pvalue': pvalues,
                       'cvalue1': cvalue1s,
                       'cvalue5': cvalue5s,
                       'cvalue10': cvalue10s,
                       'flag': flags
                       })
    nameDf = pd.DataFrame(readStockListFromSQL(), columns=['id', 'name'])
    df = pd.merge(df, nameDf, left_on='stockid', right_on='id')
    df.to_excel('adf_pe.xlsx')
    # print(df)

if __name__ == '__main__':
    pass
    # stockList = ['000651', '000333', '000002']
    startDate = '20140101'
    endDate = '20191231'

    # pro = ts.pro_api()
    # indexDf = pro.index_weight(index_code='399300.SZ', start_date='20200301')
    # stockList = indexDf.con_code.str[:6].to_list()
    # adfTestAllPE(stockList, startDate, endDate, plotFlag=False)

    adfTestPE('000651', startDate, endDate, plotFlag=True)
