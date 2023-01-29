"""本文件用于时间序列分析的测试
两种常见的用于判断时间序列是否平稳
第一种： ADF Test
使用statsmodels模块中的adfuller

第二种：
"""

import datetime as dt
import logging
import os

import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt  # @IgnorePep8
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller

from stockdatamanage.db.sqlconn import engine
from stockdatamanage.db.sqlrw import readStockList
from stockdatamanage.util.bokeh_plot import plotProfitInc
from stockdatamanage.config import DATAPATH


def adfTestPE(ts_code, startDate, endDate, plotFlag=False):
    sql = (f'select pe_ttm from daily_basic '
           f'where ts_code="{ts_code}" '
           f'and trade_date>="{startDate}" and trade_date<="{endDate}"')
    with engine.connect() as conn:
        dfa = pd.read_sql(text(sql), conn)
    dfa.dropna(inplace=True)
    resulta = adfuller(dfa['pe_ttm'])
    dfb = np.diff(dfa['pe_ttm'])
    resultb = adfuller(dfb)
    print('stock:', ts_code)
    print('resulta:\n', resulta)
    print('resultb:\n', resultb)
    print('mean of dfb:', np.mean(dfb))
    if plotFlag:
        plotDf(ts_code, dfa, dfb)

    return resulta


# noinspection PyUnusedLocal
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
    ax1.plot(dfa.pe_ttm, color='blue', label=ts_code)
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
    plt.savefig(os.path.join(
        DATAPATH, 'linear_img/pe', f'ADFTest{ts_code}.png'))
    # plt.show()


def adfTestProfits(ts_code, startDate, endDate):
    sql = (f'select ttmprofits from ttmlirun '
           f'where ts_code="{ts_code}" '
           f'and date>="{startDate}" and date<="{endDate}"')
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn)
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
    std = np.std(data)
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
                  'std': std,
                  'sharp': round(mean / std, 2),
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


def adfTestAllProfitsInc(startDate=None, endDate=None):
    """对所有股票TTM利润增长率进行ADF检测"""
    # ts_code = '000651'
    # adfTestProfits(ts_code, startDate, endDate)
    if endDate is None:
        endDate = dt.date.today().strftime('%Y%m%d')
    if startDate is None:
        startDate = f'{int(endDate[:4]) - 3}0331'

    stocks = readStockList()
    stocks = stocks[:6]
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
            logging.warning(f'{ts_code}: {e}')
        else:
            result['ts_code'] = ts_code
            resultList.append(result)

    df = pd.DataFrame(resultList)
    stocks = pd.merge(stocks, df, left_on='ts_code', right_on='ts_code')
    stocks.to_excel(os.path.join(DATAPATH, 'profits_inc_adf.xlsx'))
    # print(df)


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
    nameDf = readStockList()
    # nameDf = pd.DataFrame(readStockListFromSQL(), columns=['id', 'name'])
    df = pd.merge(df, nameDf, left_on='ts_code', right_on='ts_code')
    df.to_excel(os.path.join(DATAPATH, 'adf_pe.xlsx'))
    # print(df)


if __name__ == '__main__':
    pass
    # stockList = ['000651', '000333', '000002']
    startDate = '20180101'
    endDate = '20210423'

    # pro = ts.pro_api()
    # indexDf = pro.index_weight(index_code='399300.SZ', start_date='20200301')
    # stockList = indexDf.con_code.str[:6].to_list()
    stocks = readStockList()
    # stocks = stocks[:6]
    adfTestAllPE(stocks['ts_code'], startDate, endDate, plotFlag=True)

    # adfTestPE('000651', startDate, endDate, plotFlag=True)
    adfTestAllProfitsInc()
