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

# from download import getreq
from xml import etree

from download import *
from sqlrw import *
from bokeh.plotting import show, output_file

from datamanage import updateKline
from datamanage import updateKlineEXTData
from datamanage import startUpdate
from datamanage import updateGubenSingleThread
from sqlrw import _getLastUpdate
from sqlrw import readStockIDsFromSQL
from sqlconn import engine
from initsql import dropKlineTable
from dataanalyse import testChigu, testShaixuan
# from misc import urlGubenEastmoney
from misc import *
# from initlog import initlog
from datatrans import *
from hyanalyse import *
from plot import BokehPlot
from download import downKline


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


def downGubenShuju(stockID='300445', date='2019-04-19'):
    print('start update guben: %s' % stockID)
    print('guben date: ', date)
    updateDate = gubenUpdateDate(stockID)
    # print(type(updateDate))
    # print(updateDate.strftime('%Y%m%d'))
    startDate = updateDate.strftime('%Y%m%d')
    pro = ts.pro_api()
    code = tsCode(stockID)
    # print(code)
    df = pro.daily_basic(ts_code=code, start_date=startDate,
                         fields='trade_date,total_share')
    # print(df)
    gubenDate = []
    gubenValue = []
    for idx in reversed(df.index):
        if idx > 0 and df.total_share[idx] != df.total_share[idx - 1]:
            # print(type(idx), idx, df.trade_date[idx], df.total_share[idx])
            # print(type(idx - 1), idx, df.trade_date[idx - 1], df.total_share[idx - 1])
            gubenDate.append(datetime.strptime(df.trade_date[idx - 1],
                                               '%Y%m%d'))
            gubenValue.append(df.total_share[idx - 1] * 10000)
    resultDf = pd.DataFrame({'stockid': stockID,
                             'date': gubenDate,
                             'totalshares': gubenValue})
    print(resultDf)
    writeGubenToSQL(stockID, resultDf)
    return resultDf


def downGubenTest():
    """ 仅做测试用，下载单个股本数据，验证股本下载函数是否正确"""
    downGuben('300445')


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


def resetLirun():
    """
    下载所有股票的利润数据更新到数据库， 主要用于修复库内历史数据缺失的情况
    :return:
    """
    startDate = '1990-01-01'
    fields = 'ts_code,ann_date,end_date,total_profit,n_income,n_income_attr_p'
    pro = ts.pro_api()

    stockList = readStockListFromSQL()
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
               "       s.`ttmpe` from kline where stockid='%s' as s;\n") % (stockID, stockID)
        print('process stockID: ', stockID)
        # print(sql)
        engine.execute(sql)
        # if stockID > '000020':
        #     break


def calAllPEHistory(startDate=None, endDate=None):
    startDate = datetime.strptime('2019-04-19', '%Y-%m-%d')
    endDate = datetime.strptime('2019-06-07', '%Y-%m-%d')
    stockList = readStockIDsFromSQL()
    # print(stockList)
    for curDate in dateList(startDate, endDate):
        print('cal date: ', curDate)
        _calAllPEHistory(curDate)


def _calAllPEHistory(curDate):
    sql = ('select stockid, totalmarketvalue, ttmprofits '
           'from stockdata.kline '
           'where date="%(curDate)s" and ttmprofits>0;'
           % locals())
    result = engine.execute(sql).fetchall()
    if result:
        print(result)
        for i in result:
            print(i[0], i[1]/1, i[2]/1)
        data = list(zip(*result))
        value = sum(data[1])
        ttmprofits = sum(data[2])
        pe = round(value / ttmprofits, 2)
        print(value)
        print(ttmprofits)
        print(pe)
        sql = ('insert ignore pehistory(name, date, pe) '
               'values("all", "%(curDate)s", %(pe)s);' % locals())
        print(sql)
        engine.execute(sql)
        # resultzip = zip(result)
        # for i in resultzip:
        #     print(i)


if __name__ == "__main__":
    initlog()
    """072497"""
    pass
    # df = downLiutongGubenFromBaostock()

    # downGubenTest()
    # df['stockid'].apply(downGuben)

    # 检查股本信息，找出需要更新的股票
    # df = checkGuben()
    # print(df)

    # 下载指定股票股本信息
    # date = '2019-04-19'
    # gubenUpdateDf = checkGuben(date)
    # for stockID in gubenUpdateDf['stockid']:
    #     downGubenShuju(stockID)
    #     setGubenLastUpdate(stockID, date)
    #     time.sleep(1)  # tushare.pro每分钟最多访问接口200次
    # downGubenShuju('000157')

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

    # 估值筛选
    # dataanalyse.testShaixuan()

    # 计算评分
    # calpf()

    # bokeh绘图
    # testBokeh()

    # kline分表汇总
    # gatherKline()

    # tushare.pro下载日交易数据
    # updateKline()

    # 更新全部股票数据
    # startUpdate()

    # 更新股本数据
    # updateGubenSingleThread()

    # 更新股票日交易数据
    # threadNum = 10
    # stockList = sqlrw.readStockIDsFromSQL()
    # print(stockList)
    # updateKlineEXTData(stockList, threadNum)

    # 计算全市场PE历史
    # calAllPEHistory()

    # 更新估值数据
    # testChigu()
    # testShaixuan()

    # 更新股票评分
    # calpf()

    # 删除名称中包含股票代码的日K线表
    # dropKlineTable()

    # 下载k线
    # downKline(datetime.strptime('2015-10-07', '%Y-%m-%d'))

    # 更新股票市值与PE
    stockList = sqlrw.readStockIDsFromSQL()
    for stockID in stockList:
        print(stockID)
        sqlrw.updateKlineEXTData(stockID,
                                 datetime.strptime('2010-01-01', '%Y-%m-%d'))

    print('程序正常退出')
