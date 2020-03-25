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
from sqlrw import readStockList
from sqlrw import getStockName
from test.linear_regression import linearPlot


def adfTestPE(ts_code, startDate, endDate, plotFlag=False):
    sql = (f'select ttmpe from klinestock '
           f'where ts_code="{ts_code}" '
           f'and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    resulta = adfuller(dfa['ttmpe'])
    dfb = np.diff(dfa['ttmpe'])
    resultb = adfuller(dfb)
    print('stock:', ts_code)
    print('resulta:\n', resulta)
    print('resultb:\n', resultb)
    print('mean of dfb:', np.mean(dfb))
    if plotFlag:
        plotDf(ts_code, dfa, dfb)

    return resulta


def plotDf(ts_code, dfa, dfb):
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
    ax1.plot(dfa.ttmpe, color='blue', label=ts_code)
    ax1.legend()
    ax1.set_title(ts_code, fontsize=12)
    ax2.plot(dfb, color='blue', label=ts_code)
    ax2.legend()
    ax2.set_title(ts_code, fontsize=12)
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


def adfTestProfits(ts_code, startDate, endDate):
    sql = (f'select ttmprofits from ttmlirun '
           f'where ts_code="{ts_code}" '
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


def adfTestProfitsInc(data):
    """
    分析归属母公司股东的净利润-扣除非经常损益同比增长率(%)历年变化情况

    :param ts_code:
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
    mean = np.mean(data)
    result = adfuller(data)
    diffdata = np.diff(data)
    diffmean = np.mean(diffdata)
    diffresult = adfuller(diffdata)
    # print('resulta:\n', resulta)
    # print('resultb:\n', resultb)
    flag = (result[0] < result[4]['1%']
            and result[0] < result[4]['5%']
            and result[0] < result[4]['10%']
            and result[1] < 0.05)
    diffflag = (diffresult[0] < diffresult[4]['1%']
                and diffresult[0] < diffresult[4]['5%']
                and diffresult[0] < diffresult[4]['10%']
                and diffresult[1] < 0.05)
    # return (round(resultb[0], 2), round(resultb[1], 2), flag)
    resultdict = {'mean': mean,
                  'flag': flag,
                  'adf': result[0],
                  'pvalue': result[1],
                  'cvalue1': result[4]['1%'],
                  'cvalue5': result[4]['5%'],
                  'cvalue10': result[4]['10%'],
                  'diffmean': diffmean,
                  'diffflag': diffflag,
                  'diffadf': diffresult[0],
                  'diffpvalue': diffresult[1],
                  'diffcvalue1': diffresult[4]['1%'],
                  'diffcvalue5': diffresult[4]['5%'],
                  'diffcvalue10': diffresult[4]['10%'],
                  }
    return resultdict


def adfTestAllProfitsInc():
    """对所有股票2009年1季度至2020年1季度TTM利润增长率进行ADF检测"""
    # ts_code = '000651'
    startDate = '20090331'
    endDate = '20200331'
    # adfTestProfits(ts_code, startDate, endDate)

    stocks = readStockList()
    # stocks = stocks[:20]
    cnt = len(stocks)
    cur = 1
    # print(stocks)

    resultList = []
    # for ts_code, name in stockList[:10]:
    for ts_code in stocks.ts_code:
        print(f'{cur}/{cnt}: {ts_code}')
        cur += 1
        sql = (f'select dt_netprofit_yoy from fina_indicator'
               f' where ts_code="{ts_code}"'
               f' and end_date>="{startDate}" and end_date<="{endDate}"')
        data = engine.execute(sql).fetchall()
        if len(data) < 10:
            continue
        data = [i[0] for i in data]
        try:
            result = adfTestProfitsInc(data)
            plotProfitInc(ts_code, data)
        except Exception as e:
            print(ts_code, e)
            result = None
        if result is not None:
            result['ts_code'] = ts_code
            resultList.append(result)

    df = pd.DataFrame(resultList)
    stocks = pd.merge(stocks, df, left_on='ts_code', right_on='ts_code')
    stocks.to_excel('adf_profit_inc.xlsx')
    # print(df)


def plotProfitInc(ts_code, data):
    # sql = select
    # data = getProfitsInc(ts_code, startDate, endDate)
    filename = f'profit_inc_{ts_code[:6]}.png'
    name = getStockName(ts_code)
    title = f'{ts_code} {name}'
    # linearPlot(data, plot=True, title=title, filename=filename)
    linearPlot(data, plot=False, title=title, filename=filename)


def adfTestAllPE(stockList, startDate, endDate, plotFlag):
    """
    
    :param stockList: 
    :param startDate: 
    :param endDate: 
    :return: 
    """
    ts_codes = []
    adfs = []
    pvalues = []
    flags = []
    cvalue1s = []
    cvalue5s = []
    cvalue10s = []
    # for ts_code, name in stockList[:10]:
    for ts_code in stockList:
        print('正在处理:', ts_code)
        try:
            result = adfTestPE(ts_code, startDate, endDate, plotFlag=plotFlag)
        except Exception as e:
            print(ts_code, e)
        else:
            flag = (result[0] < result[4]['1%'] and
                    result[0] < result[4]['5%'] and
                    result[0] < result[4]['10%'] and
                    result[1] < 0.05)
            ts_codes.append(ts_code)
            adfs.append(result[0])
            pvalues.append(round(result[1], 4))
            cvalue1s.append(round(result[4]['1%'], 4))
            cvalue5s.append(round(result[4]['5%'], 4))
            cvalue10s.append(round(result[4]['10%'], 4))
            flags.append(flag)
    df = pd.DataFrame({'ts_code': ts_codes,
                       'adf': adfs,
                       'pvalue': pvalues,
                       'cvalue1': cvalue1s,
                       'cvalue5': cvalue5s,
                       'cvalue10': cvalue10s,
                       'flag': flags
                       })
    nameDf = pd.DataFrame(readStockListFromSQL(), columns=['id', 'name'])
    df = pd.merge(df, nameDf, left_on='ts_code', right_on='id')
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

    # adfTestPE('000651', startDate, endDate, plotFlag=True)
    adfTestAllProfitsInc()
