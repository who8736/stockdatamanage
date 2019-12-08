# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 15:27:38 2019

@author: ff
"""

import time
# import pandas as pd
# from pandas import DataFrame
from urllib.request import urlopen
# from lxml import etree
# from datetime import datetime
# import baostock as bs
# import tushare as ts
from tushare import get_report_data

# from download import getreq
from xml import etree

from download import *
from sqlrw import *
from bokeh.plotting import show, output_file

import datamanage
import dataanalyse
import bokehtest
import download
import migration
from datamanage import updateKline
from datamanage import updateKlineEXTData
from datamanage import startUpdate
from datamanage import updateGubenSingleThread
from sqlrw import _getLastUpdate
from sqlrw import readStockIDsFromSQL
from sqlconn import engine, Session
from initsql import dropKlineTable
from dataanalyse import testChigu, testShaixuan
from dataanalyse import calPEHistory, calAllPEHistory
# from misc import urlGubenEastmoney
from misc import *
# from initlog import initlog
from datatrans import *
from hyanalyse import *
from plot import BokehPlot, plotKlineStock
from download import downKline, _downGubenSina
from bokehtest import plotIndexPE, testPlotKline
from bokehtest import BokehPlotPE


# import dataanalyse
from valuation import calpf


def downGubenFromEastmoney():
    """ 从东方财富下载总股本变动数据
    url: 
    """
    pass
    stockID = '600000'
    # startDate = '2019-04-01'
    bs.login()
    # from misc import usrlGubenEastmoney
    # urlGubenEastmoney('600000')
    gubenURL = urlGubenEastmoney(stockID)
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
    date = [datetime.strptime(d, '%Y-%m-%d') for d in date]
    #     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
    #     print totalshares
    #     t = [i[:-2] for i in totalshares]
    #     print t
    try:
        totalshares = [float(i[:-2]) * 10000 for i in totalshares]
    except ValueError as e:
        # logging.error('stockID:%s, %s', stockID, e)
        print('stockID:%s, %s', stockID, e)
    #     print totalshares
    gubenDf = DataFrame({'stockid': stockID,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf


def urlGuben(stockID):
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/stockid'
            '/%s/stocktype/TotalStock.phtml' % stockID)


def downLiutongGubenFromBaostock():
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
#     dfFromTushare['stockid'] = dfFromTushare['ts_code'].str[:6]
#
#     sql = """ select a.stockid, a.date, a.totalshares from guben as a,
#             (SELECT stockid, max(date) as maxdate FROM stockdata.guben
#             group by stockid) as b
#             where a.stockid=b.stockid and a.date = b.maxdate;
#             """
#     dfFromSQL = pd.read_sql(sql, con=engine)
#     df = pd.merge(dfFromTushare, dfFromSQL, how='left', on='stockid')
#     df.loc[0:, 'cha'] = df.apply(
#         lambda x: abs(x['total_share'] * 10000 - x['totalshares']) / (
#                 x['total_share'] * 10000), axis=1)
#
#     chaRate = 0.0001
#     dfUpdate = df[df.cha >= chaRate]
#     print(dfUpdate)
#     for stockID in dfUpdate['stockid']:
#         sql = ('select max(date) from guben where stockid="%s" limit 1;'
#                % stockID)
#         dateA = _getLastUpdate(sql)
#         setGubenLastUpdate(stockID, dateA)
#
#     # 对于需更新股本的股票，逐个更新股本并修改更新日期
#     # 对于无需更新股本的股票，将其更新日期修改为上一交易日
#     dfFinished = df[df.cha < chaRate]
#     for stockID in dfFinished['stockid']:
#         setGubenLastUpdate(stockID, date)
#     # print(df3)
#     return dfUpdate


def downGubenTest():
    """ 仅做测试用，下载单个股本数据，验证股本下载函数是否正确"""
    # stockIDs = ["300539"]
    stockIDs = readStockIDsFromSQL()
    for stockID in stockIDs:
        downGuben(stockID, replace=True)
        time.sleep(1)


def resetKlineExtData():
    """

    :return:
    """
    stockList = sqlrw.readStockIDsFromSQL()
    print(type(stockList))
    print(stockList)
    for stockID in stockList:
        updateKlineEXTData(stockID, '2016-01-01')

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
        calAllHYTTMLirun(date)


def resetLirun():
    """
    下载所有股票的利润数据更新到数据库， 主要用于修复库内历史数据缺失的情况
    :return:
    """
    startDate = '2018-01-01'
    fields = 'ts_code,ann_date,end_date,total_profit,n_income,n_income_attr_p'
    pro = ts.pro_api()

    # stockList = readStockListFromSQL()
    stockList = [['600306', 'aaa']]
    for stockID, stockName in stockList:
        print(stockID, stockName)
        # stockID = '002087'
        stockID = tsCode(stockID)
        df = pro.income(ts_code=stockID, start_date=startDate, fields=fields)
        df['date'] = df['end_date'].apply(transTushareDateToQuarter)
        df['stockid'] = df['ts_code'].apply(lambda x: x[:6])
        df['reportdate'] = df['ann_date'].apply(lambda x: '%s-%s-%s' % (x[:4], x[4:6], x[6:]))
        df.rename(columns={'n_income_attr_p': 'profits'}, inplace=True)
        df1 = df[['stockid', 'date', 'profits', 'reportdate']]
        if not df1.empty:
            writeSQL(df1, 'lirun')

        # tushare每分钟最多访问接口80次
        time.sleep(0.4)


def testBokeh():
    b = BokehPlot('000651')
    p = b.plot()
    output_file("kline.html", title="kline plot test")
    show(p)  # open a browser


def gatherKline():
    stockList = readStockIDsFromSQL()
    for stockID in stockList:
        # stockID = '000002'
        sql = ("insert ignore stockdata.kline(`stockid`, `date`, `open`, "
               "                              `high`, `close`, `low`, "
               "                              `volume`, `totalmarketvalue`, "
               "                              `ttmprofits`, `ttmpe`) "
               "select '%s', s.`date`, s.`open`, s.`high`, s.`close`, s.`low`, "
               "       s.`volume`, s.`totalmarketvalue`, s.`ttmprofits`, "
               "       s.`ttmpe` from klinestock where stockid='%s' as s;\n") % (stockID, stockID)
        print('process stockID: ', stockID)
        # print(sql)
        engine.execute(sql)
        # if stockID > '000020':
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


if __name__ == "__main__":
    """
    本文件用于测试各模块功能
    """
    initlog()

    ##############################################
    # 数据下载
    ##############################################
    # 下载k线
    # startDate = datetime.strptime('2018-01-27', '%Y-%m-%d')
    # endDate = datetime.strptime('2018-03-29', '%Y-%m-%d')
    # for tradeDate in dateList(startDate, endDate):
    #     downKline(tradeDate)

    # 股本
    #-------------------------------
    # 下载指定股票股本信息
    # date = '2019-04-19'
    # gubenUpdateDf = checkGuben(date)
    # for stockID in gubenUpdateDf['stockid']:
    #     downGuben(stockID)
    #     setGubenLastUpdate(stockID, date)
    #     time.sleep(1)  # tushare.pro每分钟最多访问接口200次

    # 指数
    #-------------------------------
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

    ##############################################
    # 数据更新
    ##############################################

    # 更新股票市值与PE
    # stockList = sqlrw.readStockIDsFromSQL()
    # for stockID in stockList:
    #     print(stockID)
    # stockID = '600306'
    # stockIDs = ["002953", "002955", "300770", "300771",
    #             "300772", "300773", "300775", "300776", "300777",
    #             "300778", "300779", "300780", "300781", "600989",
    #             "603267", "603327", "603697", "603967", "603982", ]
    # for stockID in stockIDs:
    #     sqlrw.updateKlineEXTData(stockID,
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
    # stockList = sqlrw.readStockIDsFromSQL()
    # print(stockList)
    # updateKlineEXTData(stockList, threadNum)

    # 计算上证180指数PE
    # startDate = datetime.strptime('20190617', '%Y%m%d').date()
    # calPEHistory('000010', startDate)

    # 计算行业利润增长率
    # hyID = '030201'
    # date = 20184
    # calHYTTMLirun('03020101', date)
    # calHYTTMLirun('03020102', date)
    # calHYTTMLirun('03020103', date)
    # calHYTTMLirun('03020104', date)
    # calHYTTMLirun(hyID, date)

    # 更新指数数据及PE
    # ID = '000010.SH'
    # startDate = datetime.strptime('20190701', '%Y%m%d').date()
    # dataanalyse.calPEHistory(ID[:6], startDate)

    # datamanage.updateIndex()

    # 更新全市PE
    # datamanage.updateAllMarketPE()

    ##############################################
    # 数据修复
    ##############################################

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

    ##############################################
    # 绘图
    ##############################################
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

    ##############################################
    # 导入导出
    ##############################################
    a = datetime.now()
    print(a)
    # migration.export()
    migration.importData()
    b = datetime.now()
    print(b)
    print('time cost:', b - a)

    print('程序正常退出')
