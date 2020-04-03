# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 15:27:38 2019

@author: ff
"""

# import pandas as pd
# from pandas import DataFrame
from urllib.request import urlopen
# from lxml import etree
# from datetime import datetime
# import baostock as bs
# import tushare as ts
# from bokeh.plotting import figure
from matplotlib.widgets import Cursor

# from download import getreq
from xml import etree

from download import *
from sqlrw import *
from bokeh.plotting import show, output_file
# from sqlalchemy.ext.declarative import declarative_base

from datamanage import *
from sqlrw import readStockList
# from sqlconn import Session
# from misc import urlGubenEastmoney
from misc import *
# from initlog import initlog
from datatrans import *
from dataanalyse import *
from classifyanalyse import *
import bokehtest
from plot import *

matplotlib.use('Qt5Agg')  # @UndefinedVariable

INDEXNAME = {'000001.SH': '上证综指',
             # '000005.SH': '上证商业类指数',
             # '000006.SH': '上证房地产指数',
             '000016.SH': '上证50',
             # '000300.SH': '沪深300',
             # '000905.SH': '中证500',
             '399001.SZ': '深证成指',
             # '399005.SZ': '中小板指',
             '399006.SZ': '创业板指',
             # '399016.SZ': '深证创新',
             '399300.SZ': '沪深300',
             '399905.SZ': '中证500',
             }


def analyIndex(code1='000001.SH', code2='000016.SH', startDate='20170101',
               plot=False):
    """
    分析指数历史走势，从2007年开始，划分多个阶段，比较每个阶段各指数的强弱
    000001.SH	上证综指
    000005.SH	上证商业类指数
    000006.SH	上证房地产指数
    000016.SH	上证50
    000300.SH	沪深300
    000905.SH	中证500
    399001.SZ	深证成指
    399005.SZ	中小板指
    399006.SZ	创业板指
    399016.SZ	深证创新
    399300.SZ	沪深300
    399905.SZ	中证500

    :return:
    """
    # 000001.SH 上证综指 20071016最高点位6092.06
    pass
    sql = (f'select trade_date, close close_sh from index_daily'
           f' where ts_code="{code1}" and trade_date >= "{startDate}"')
    df1 = pd.read_sql(sql, engine)
    if df1.empty:
        return None
    # print(df1)
    max1 = df1[df1.close_sh == df1.close_sh.max()]
    df1right = df1[df1.trade_date > max1.trade_date.values[0]]
    min1 = df1right[df1right.close_sh==df1right.close_sh.min()]

    sql = (f'select trade_date, close close_sz from index_daily'
           f' where ts_code="{code2}" and trade_date >= "{startDate}"')
    df2 = pd.read_sql(sql, engine)
    if df2.empty:
        return None
    max2 = df2[df2.close_sz == df2.close_sz.max()]

    df1['line1'] = df1.close_sh / df1.close_sh[0]
    df2['line2'] = df2.close_sz / df2.close_sz[0]
    df = pd.merge(df1, df2, left_on='trade_date', right_on='trade_date')
    dfcolumn = ['trade_date', 'line1', 'line2']
    df = df[dfcolumn]
    # df.plot()
    # plt.show()
    fig = plt.figure()
    ax = plt.subplot()
    label1 = INDEXNAME[code1]
    label2 = INDEXNAME[code2]

    if plot:
        line1 = ax.plot(df.index, df.line1, label=label1, color='blue')
        line2 = ax.plot(df.index, df.line2, label=label2, color='red')
        dates = [date.strftime('%Y%m%d') for date in df.trade_date]
        tickerIndex, tickerLabels = getMonthIndex(dates, type='year')
        locator = FixedLocator(tickerIndex)
        ax.xaxis.set_major_locator(locator)
        ax.set_xticklabels(tickerLabels)
        for label in ax.get_xticklabels():
            label.set_rotation(45)
        font1 = {'family': 'simsun', 'weight': 'normal', 'size': 12, }
        plt.legend(prop=font1)
        cursor = Cursor(ax, useblit=True, color='black', linewidth=1)
        # plt.savefig(f'data/incate-{code1}-{code2}.png')
        plt.grid()
        plt.show()

    return df.line2.values[-1]


def downGubenFromEastmoney():
    """ 从东方财富下载总股本变动数据
    url: 
    """
    pass
    ts_code = '600000.SH'
    # startDate = '2019-04-01'
    bs.login()
    # from misc import usrlGubenEastmoney
    # urlGubenEastmoney('600000')
    gubenURL = urlGubenEastmoney(ts_code)
    # req = getreq(gubenURL, includeHeader=True)
    req = getreq(gubenURL)
    guben = urlopen(req).read()

    gubenTree = etree.HTML(guben)
    # //*[@id="lngbbd_Table"]/tbody/tr[1]/th[3]
    # gubenData = gubenTree.xpath('//tr')
    gubenData = gubenTree.xpath('''//html//body//div//div
                                //div//div//table//tr//td
                                //table//tr//td//table//tr//td''')
    date = [gubenData[i][0].text for i in range(0, len(gubenData), 2)]
    date = [datetime.strptime(d, '%Y%m%d') for d in date]
    #     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
    #     print totalshares
    #     t = [i[:-2] for i in totalshares]
    #     print t
    try:
        totalshares = [float(i[:-2]) * 10000 for i in totalshares]
    except ValueError as e:
        # logging.error('ts_code:%s, %s', ts_code, e)
        print('ts_code:%s, %s', ts_code, e)
    #     print totalshares
    gubenDf = DataFrame({'ts_code': ts_code,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf


def urlGuben(ts_code):
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/ts_code'
            '/%s/stocktype/TotalStock.phtml' % ts_code)


def profits_inc_linear_adf():
    df_adf = pd.read_excel('data/profits_inc_adf.xlsx')
    df_adf.set_index('ts_code', inplace=True)
    df_adf = df_adf[['name', 'mean', 'std', 'sharp', 'flag']]

    df_linear = pd.read_excel('data/profits_inc_linear.xlsx')
    df_linear.set_index('ts_code', inplace=True)
    df_linear = df_linear[['intercept', 'coef', 'r2']]

    df_pe = readLastTTMPEs(df_adf.index)
    df_pe.set_index('ts_code', inplace=True)
    # df = pd.merge(df_adf, df_linear, how='left',
    #               left_on='ts_code', right_on='ts_code')
    df = pd.merge(df_adf, df_linear, left_index=True, right_index=True)
    # df = pd.merge(df, df_pe, how='left',
    #               left_on='ts_code', right_on='ts_code')
    df = pd.merge(df, df_pe, left_index=True, right_index=True)

    df.to_excel('data/profits_inc_adf_linear.xlsx')


def del_downLiutongGubenFromBaostock():
    """ 从baostock下载每日K线数据，并根据成交量与换手率计算流通总股本
    """
    code = 'sz.000651'
    startDate = '2019-03-01'
    endDate = '2019-04-15'
    fields = "date,code,close,volume,turn,peTTM,tradestatus"

    lg = bs.login()
    print('baostock login code: ', lg.error_code)
    rs = bs.query_history_k_data_plus(code, fields, startDate, endDate)
    dataList = []
    while rs.next():
        dataList.append(rs.get_row_data())
    result = pd.DataFrame(dataList, columns=rs.fields)
    print(result)
    #    lg.logout()
    bs.logout()
    return result


# def checkGuben(date='2019-04-19'):
#     """ 以下方法用于从tushare.pro下载日频信息中的股本数据
#         与数据库保存的股本数据比较，某股票的总股本存在差异时说明股本有变动
#         返回需更新的股票列表
#     """
#     pro = ts.pro_api()
#     tradeDate = '%s%s%s' % (date[:4], date[5:7], date[8:])
#     dfFromTushare = pro.daily_basic(ts_code='', trade_date=tradeDate,
#                                     fields='ts_code,total_share')
#     dfFromTushare['ts_code'] = dfFromTushare['ts_code'].str[:6]
#
#     sql = """ select a.ts_code, a.date, a.totalshares from guben as a,
#             (SELECT ts_code, max(date) as maxdate FROM stockdata.guben
#             group by ts_code) as b
#             where a.ts_code=b.ts_code and a.date = b.maxdate;
#             """
#     dfFromSQL = pd.read_sql(sql, con=engine)
#     df = pd.merge(dfFromTushare, dfFromSQL, how='left', on='ts_code')
#     df.loc[0:, 'cha'] = df.apply(
#         lambda x: abs(x['total_share'] * 10000 - x['totalshares']) / (
#                 x['total_share'] * 10000), axis=1)
#
#     chaRate = 0.0001
#     dfUpdate = df[df.cha >= chaRate]
#     print(dfUpdate)
#     for ts_code in dfUpdate['ts_code']:
#         sql = ('select max(date) from guben where ts_code="%s" limit 1;'
#                % ts_code)
#         dateA = _getLastUpdate(sql)
#         setGubenLastUpdate(ts_code, dateA)
#
#     # 对于需更新股本的股票，逐个更新股本并修改更新日期
#     # 对于无需更新股本的股票，将其更新日期修改为上一交易日
#     dfFinished = df[df.cha < chaRate]
#     for ts_code in dfFinished['ts_code']:
#         setGubenLastUpdate(ts_code, date)
#     # print(df3)
#     return dfUpdate


def downGubenTest():
    """ 仅做测试用，下载单个股本数据，验证股本下载函数是否正确"""
    # ts_codes = ["300539"]
    ts_codes = readStockList().ts_code.to_list()
    for ts_code in ts_codes:
        downGuben(ts_code, replace=True)
        time.sleep(1)


# def del_resetKlineExtData():
#     """
#
#     :return:
#     """
#     stockList = sqlrw.readStockList()
#     print(type(stockList))
#     print(stockList)
#     for ts_code in stockList:
#         pass
# updateKlineEXTData(ts_code, '2016-01-01')

# for()


def resetTTMLirun():
    """
    重算TTM利润
    :return:
    """
    startQuarter = 20174
    endQuarter = 20191
    dates = datatrans.QuarterList(startQuarter, endQuarter)
    for date in dates:
        logging.debug('updateLirun: %s', date)
        calAllTTMLirun(date, incrementUpdate=False)
        calAllHYTTMProfits(date)


def del_resetLirun():
    """
    下载所有股票的利润数据更新到数据库， 主要用于修复库内历史数据缺失的情况
    :return:
    """
    startDate = '2018-01-01'
    fields = 'ts_code,ann_date,end_date,total_profit,n_income,n_income_attr_p'
    pro = ts.pro_api()

    # stockList = readStockListFromSQL()
    stockList = [['600306', 'aaa']]
    for ts_code, stockName in stockList:
        print(ts_code, stockName)
        # ts_code = '002087'
        ts_code = tsCode(ts_code)
        df = pro.income(ts_code=ts_code, start_date=startDate, fields=fields)
        df['date'] = df['end_date'].apply(transTushareDateToQuarter)
        df['ts_code'] = df['ts_code'].apply(lambda x: x[:6])
        df['reportdate'] = df['ann_date'].apply(
            lambda x: '%s-%s-%s' % (x[:4], x[4:6], x[6:]))
        df.rename(columns={'n_income_attr_p': 'profits'}, inplace=True)
        df1 = df[['ts_code', 'date', 'profits', 'reportdate']]
        if not df1.empty:
            writeSQL(df1, 'lirun')

        # tushare每分钟最多访问接口80次
        time.sleep(0.4)


def testBokeh():
    """bokeh测试用"""
    # b = BokehPlot('000651')
    # p = b.plot()
    # output_file("kline.html", title="kline plot test")
    output_file('vbar.html')
    p = figure(plot_width=400, plot_height=400)
    p.vbar(x=[1, 2, 3], width=0.5, bottom=[1, 2, 3],
           top=[1.2, 2, 3.1], color="firebrick")
    show(p)  # open a browser


def del_gatherKline():
    stockList = readStockList()
    for ts_code in stockList:
        # ts_code = '000002'
        sql = f"""insert ignore stockdata.kline(`ts_code`, `date`, `open`, 
                    `high`, `close`, `low`, `volume`, `totalmarketvalue`, 
                    `ttmprofits`, `ttmpe`) 
                select '{ts_code}', s.`date`, s.`open`, s.`high`, s.`close`, 
                    s.`low`, s.`volume`, s.`totalmarketvalue`, s.`ttmprofits`, 
                    s.`ttmpe` from klinestock where ts_code='{ts_code}' as s;
              """
        print('process ts_code: ', ts_code)
        # print(sql)
        engine.execute(sql)
        # if ts_code > '000020':
        #     break


def testBokehtest():
    """
    测试bokehtest中的功能
    :return:
    """
    mybokeh = bokehtest.BokehPlotStock('000651', 1000)
    myplot = mybokeh.plot()
    output_file("kline.html", title="kline plot test")
    show(myplot)  # open a browser


def testDownIndexWeightRepair():
    """
    下载指数成份和权重, 用于修复历史数据，对每个指数从库中日期最早日期向前修复
    000001.SH	上证综指
    000005.SH	上证商业类指数
    000006.SH	上证房地产指数
    000016.SH	上证50
    000300.SH	沪深300
    000905.SH	中证500
    399001.SZ	深证成指
    399005.SZ	中小板指
    399006.SZ	创业板指
    399016.SZ	深证创新
    399300.SZ	沪深300
    399905.SZ	中证500

    :return:
    """
    pro = ts.pro_api()
    codeList = ['000001.SH',
                '000005.SH',
                '000006.SH',
                '000016.SH',
                '399001.SZ',
                '399005.SZ',
                '399006.SZ',
                '399016.SZ',
                '399300.SZ',
                '399905.SZ', ]

    times = []
    cur = 0
    perTimes = 60
    downLimit = 70
    for code in codeList:
        initDate = dt.date(2001, 1, 1)
        while initDate < dt.datetime.today().date():
            nowtime = dt.datetime.now()
            # if (perTimes > 0 and downLimit > 0 and cur >= downLimit
            #         and (nowtime < times[cur - downLimit] + dt.timedelta(
            #             seconds=perTimes))):
            if (perTimes > 0 and 0 < downLimit <= cur
                    and (nowtime < times[cur - downLimit] + dt.timedelta(
                        seconds=perTimes))):
                _timedelta = nowtime - times[cur - downLimit]
                sleeptime = perTimes - _timedelta.seconds
                print(f'******暂停{sleeptime}秒******')
                time.sleep(sleeptime)

            startDate = initDate.strftime('%Y%m%d')
            initDate += dt.timedelta(days=30)
            endDate = initDate.strftime('%Y%m%d')
            initDate += dt.timedelta(days=1)
            print(f'下载{code},日期{startDate}-{endDate}')
            df = pro.index_weight(index_code=code,
                                  start_date=startDate, end_date=endDate)
            writeSQL(df, 'index_weight')

            nowtime = dt.datetime.now()
            times.append(nowtime)
            cur += 1


def __testDownload():
    """测试专用函数:数据下载
    """
    pass
    # 下载行业分类
    # downHYFile()
    # filename = 'csi20200317.xls'
    # writeHYToSQL(filename)
    # writeHYNameToSQL(filename)

    # 下载k线
    # startDate = datetime.strptime('2018-01-27', '%Y-%m-%d')
    # endDate = datetime.strptime('2018-03-29', '%Y-%m-%d')
    # for tradeDate in dateList(startDate, endDate):
    #     downKline(tradeDate)

    # 股本
    # -------------------------------
    # 下载指定股票股本信息
    # date = '2019-04-19'
    # gubenUpdateDf = checkGuben(date)
    # for ts_code in gubenUpdateDf['ts_code']:
    #     downGuben(ts_code)
    #     setGubenLastUpdate(ts_code, date)
    #     time.sleep(1)  # tushare.pro每分钟最多访问接口200次

    # 指数
    # -------------------------------
    # 下载指数成份股列表
    # for year in range(2009, 2020):
    #     ID = '000010'
    #     startStr = '%s0101' % year
    #     endStr = '%s1231' % year
    #     startDate = datetime.strptime(startStr, '%Y%m%d').date()
    #     endDate = datetime.strptime(endStr, '%Y%m%d').date()
    #     downChengfen(ID, startDate, endDate)

    # 下载指数K线数据
    # startDate = datetime.strptime('20100101', '%Y%m%d')
    # downIndex('000010.SH', startDate=startDate)

    # # 下载每日指标
    # updateDailybasic()
    # ts_code = '000651'
    # startDate = '20090829'
    # endDate = '20171231'
    # tradeDate = '20191231'
    # dates = dateStrList(startDate, endDate)
    # for d in dates:
    #     # df = downDailyBasic(ts_code=ts_code,
    #     #                     startDate=startDate,
    #     #                     endDate=endDate)
    #     # df = downDailyBasic(tradeDate=tradeDate)
    #     df = downDailyBasic(tradeDate=d)
    #     print(df)
    #     # df.to_excel('test.xlsx')
    #     writeSQL(df, 'dailybasic')

    # 下载股权质押统计数据
    # IDs = readts_codesFromSQL()
    # IDs = IDs[:10]
    # times = []
    # cnt = len(IDs)
    # for i in range(cnt):
    #     nowtime = datetime.now()
    #     if i >= 50 and (nowtime < times[i - 50] + timedelta(seconds=60)):
    #         _timedelta = nowtime - times[i - 50]
    #         sleeptime = 60 - _timedelta.seconds
    #         print(f'******暂停{sleeptime}秒******')
    #         time.sleep(sleeptime)
    #         nowtime = datetime.now()
    #     times.append(nowtime)
    #     print(f'第{i}个，时间：{nowtime}')
    #     ts_code = IDs[i]
    #     print(ts_code)
    #     flag = True
    #     df = None
    #     while flag:
    #         try:
    #             df = downPledgeStat(ts_code)
    #             flag = False
    #         except Exception as e:
    #             print(e)
    #             time.sleep(10)
    #     # print(df)
    #     time.sleep(1)
    #     if df is not None:
    #         writeSQL(df, 'pledgestat')

    # 下载利润表
    # ts_code = '000651'
    # startDate = '20160101'
    # endDate = '20200202'
    # downIncome(ts_code, startDate=startDate, endDate=endDate)
    # downIncome(ts_code)

    # 下载指数基本信息
    # downIndexBasic()

    # 下载指数每日指标
    # 000001.SH	上证综指
    # 000005.SH	上证商业类指数
    # 000006.SH	上证房地产指数
    # 000016.SH	上证50
    # 000300.SH	沪深300
    # 000905.SH	中证500
    # 399001.SZ	深证成指
    # 399005.SZ	中小板指
    # 399006.SZ	创业板指
    # 399016.SZ	深证创新
    # 399300.SZ	沪深300
    # 399905.SZ	中证500
    # downIndexDailyBasic('000001.SH')
    # testDownIndexDailyBasic()

    # 下载指数日K线
    # downIndexDaily()

    # 下载指数成分和权重
    # testDownIndexWeightRepair()
    # downIndexWeight()


def __testUpdate():
    """测试专用函数:数据下载
    """
    pass
    # 自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
    # 更新交易日历
    # updateTradeCal()

    # 更新股票列表
    # downStockList()

    # 更新股票日交易数据
    # updateDaily()

    # 更新每日指标
    # updateDailybasic()

    # 更新非季报表格
    # 财务披露表
    # 质押表（另外单独更新）
    # 业绩预告
    # 业绩快报
    # 分红送股

    # 更新股票季报数据
    # 资产负债表
    # 利润表
    # 现金流量表
    # 财务指标表
    # updateQuarterData()

    # 更新行业列表
    # downHYList()

    # 更新股票估值
    # updateGuzhiData()

    # 更新股票评分
    # updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    # updateAllMarketPE()


# 更新股票市值与PE
# stockList = sqlrw.readts_codesFromSQL()
# for ts_code in stockList:
#     print(ts_code)
# ts_code = '600306'
# ts_codes = ["002953", "002955", "300770", "300771",
#             "300772", "300773", "300775", "300776", "300777",
#             "300778", "300779", "300780", "300781", "600989",
#             "603267", "603327", "603697", "603967", "603982", ]
# for ts_code in ts_codes:
#     sqlrw.updateKlineEXTData(ts_code,
#                              datetime.strptime('2016-01-26', '%Y-%m-%d'))

# tushare.pro下载日交易数据
# updateKline()

# 更新全部股票数据
# startUpdate()

# 更新股本数据
# updateGubenSingleThread()
# downGuben('603970', replace=True)
# downGubenTest()

# 更新股票日交易数据
# threadNum = 10
# stockList = sqlrw.readts_codesFromSQL()
# print(stockList)
# updateKlineEXTData(stockList, threadNum)

# 计算上证180指数PE
# startDate = datetime.strptime('20190617', '%Y%m%d').date()
# calPEHistory('000010', startDate)
# calPEHistory('000010.SH', startDate=None)

# 计算行业利润增长率
# hyID = '030201'
# date = 20184
# calHYTTMLirun('03020101', date)
# calHYTTMLirun('03020102', date)
# calHYTTMLirun('03020103', date)
# calHYTTMLirun('03020104', date)
# calHYTTMLirun(hyID, date)

# 计算行业PE
# hyID = '03020101'
# date = '20200102'
# pe = getHYPE(hyID, date)
# pe = getHYsPE(date)
# print('行业PE：', pe)

# 更新指数数据及PE
# updateIndex()

# 更新全市PE
# datamanage.updateAllMarketPE()

# 更新历史评分
# startDate = '20191220'
# endDate = '20191231'
# formatStr = '%Y%m%d'
# cf = configparser.ConfigParser()
# cf.read('stockdata.conf')
# if cf.has_option('main', 'token'):
#     token = cf.get('main', 'token')
# else:
#     token = ''
# ts.set_token(token)
# print(token)
# pro = ts.pro_api()
# df = pro.trade_cal(exchange='', start_date='20200101', end_date='20201231')
# dateList = df['cal_date'].loc[df.is_open==1].tolist()
# for date in dateList:
#     print('计算评分：', date)
#     calpfnew(date, False)
# calpfnew('20200228', True)


def __testRepair():
    """测试专用函数:数据修复
    """
    pass
    # 修复股票日K线
    # downDailyRepair()

    # resetKlineExtData()

    # 重算TTMlirun
    # dates = datatrans.QuarterList(20061, 20191)
    # for date in dates:
    #     print('cal ttmlirun: %d' % date)
    #     # calAllTTMLirun(date)
    #     calAllHYTTMLirun(date)
    # calAllTTMLirun(20102)

    # 重新下载lirun数据
    # resetLirun()

    # 重算TTMLirun
    # resetTTMLirun()

    # 重算指定日期所有行业TTM利润
    # resetHYTTMLirun(startQuarter=19901, endQuarter=20191)

    # 重建表
    # createHangyePE()
    # createValuation()


def __testValuation():
    """测试专用函数:股票评分
    """
    pass
    ##############################################
    # 股票评分
    ##############################################
    # 估值筛选
    # dataanalyse.testShaixuan()

    # 计算评分
    # calpf()

    # 更新估值数据
    # testChigu()
    # testShaixuan()

    # 更新股票评分
    # calpf()

    # 更新股票估值
    # calGuzhi()

    # 测试新评分函数
    # stockspf = calpfnew('20171228', replace=False)
    # stockspf = calpfnew('20171227', replace=True)
    # print('=' * 20)
    # print(stockspf)


def __testPlot():
    """测试专用函数:绘图
    """
    pass

    p = PlotProfitsInc(ts_code='000651.SZ', startDate='20150331',
                       endDate='20191231')
    # bokeh绘图
    # testBokeh()

    # 指数PE绘图
    # plotIndexPE()
    # testPlotKline('600519')
    # bokehtest.test()
    # plotImg = BokehPlotPE()
    # fig = plotImg.plot()
    # plotKlineStock('600519', days=1000)

    # 测试bokehtest模块中的功能
    # testBokehtest()


def __testMisc():
    """测试专用函数:杂项测试
    """
    pass
    code1 = '000001.SH'
    code2 = '399300.SZ'
    code3 = '399905.SZ'
    result = analyIndex(code2, code2=code3, plot=True)
    # results = []
    # for code2 in INDEXNAME.keys():
    #     result = analyIndex(code1, code2)
    #     results.append(dict(code=code2, name=INDEXNAME[code2], inc=result))
    # df = pd.DataFrame(results)
    # df.to_excel('data/指数涨幅比较.xlsx')
    # print(df)

    # profits_inc_linear_adf()
    # matplotlib.use('Qt5Agg')  # @UndefinedVariable
    # sql = 'select trade_date, pe from index_pe where ts_code="000010.SH"'
    # df = pd.read_sql(sql, engine)
    # plotPE(df)
    # calPEHistory('000010.SH', '20200317')
    # calClassifyPE('20200310')
    # testChigu()
    # testShaixuan()
    ##############################################
    # 杂项测试
    ##############################################
    # sqlrw中的readStockListFromSQL读取指定日期股票
    # testReadStockListFromSQL()

    # sqlrw中的readLastTTMPE读取指定日期TTMPE
    # testReadLastTTMPE()

    # 读取近几个季度的TTM利润
    # date = '20200102'
    # stocks = readStockListDf(date)
    # sectionNum = 6  # 取6个季度
    # incDf = sqlrw.readLastTTMLirun(stocks.ts_code.tolist(), sectionNum, date)
    # incDf = sqlrw.readLastTTMLirunForts_code('000651', 6, '20181231')

    # print(incDf)
    # print(incDf.loc[incDf.ts_code=='000651'])

    # 测试sqlrw.writeSQL函数的replace功能
    # testWriteSQL()

    # 测试datamanage中的updatePf
    # datamanage.updatePf()

    # 建表测试
    # createTable()

    # 使用tushare下载器
    # tableList = ['balancesheet', 'cashflow', 'forecast',
    #              'express', 'dividend', 'fina_indicator',
    #              'disclosure_date']
    # for tablename in tableList:
    #     downloader(tablename)

    # 发送邮件
    # datestr = '20200303'
    # from pushdata import push
    # push(f'评分{datestr}', f'valuations{datestr}.xlsx')


if __name__ == "__main__":
    """
    本文件用于测试各模块功能
    """
    initlog()

    # __testDownload()
    __testMisc()
    # __testPlot()
    # __testRepair()
    # __testUpdate()
    # __testValuation()

    print('程序正常退出')
