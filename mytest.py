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
import datetime as dt
# import baostock as bs
# import tushare as ts
from bokeh.plotting import figure

# from download import getreq
from xml import etree

from download import *
from sqlrw import *
from bokeh.plotting import show, output_file
from sqlalchemy.ext.declarative import declarative_base

from datamanage import updateKlineEXTData, updateDailybasic
from datamanage import updateIndex
from sqlrw import readStockIDsFromSQL
from sqlconn import Session
# from misc import urlGubenEastmoney
from misc import *
# from initlog import initlog
from datatrans import *
from hyanalyse import *
import bokehtest
from valuation import calpfnew
from initsql import createTable


# import dataanalyse


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
        df['reportdate'] = df['ann_date'].apply(
            lambda x: '%s-%s-%s' % (x[:4], x[4:6], x[6:]))
        df.rename(columns={'n_income_attr_p': 'profits'}, inplace=True)
        df1 = df[['stockid', 'date', 'profits', 'reportdate']]
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
               "       s.`ttmpe` from klinestock where stockid='%s' as s;\n") % (
                  stockID, stockID)
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


def testReadStockListFromSQL():
    stocksA = readStockListFromSQL()
    print(stocksA)
    stocksALen = len(stocksA)
    print('A股票总数:', len(stocksA))
    stocksB = readStockListFromSQL(20191201)
    print(stocksB)
    stocksBLen = len(stocksB)
    print('B股票总数:', len(stocksB))
    print('相差：', stocksALen - stocksBLen)
    print('相差的股票：', [i for i in stocksA if i not in stocksB])


def testReadLastTTMPE():
    date = '20191202'
    stocks = readStockListDf(int(date))
    result = readLastTTMPEs(stocks.stockid.tolist(), date)
    print(result)


def testWriteSQL():
    data = [{'username': 'a', 'email': 'aa@bb.cc', 'password': 'aaa_modify1'},
            {'username': 'b', 'email': 'bb@bb.cc', 'password': 'bbb_modify2'}, ]
    print(data)
    df = pd.DataFrame(data)
    print(df)
    # return
    # datafield = ['username', 'email', 'password']
    session = Session()
    metadata = MetaData(bind=engine)
    Base = declarative_base()

    class TableTest(Base):
        # __table__ = Table('user', Base.metadata, autoload=True)
        __table__ = Table('user', Base.metadata,
                          autoload=True, autoload_with=engine)

    for index, row in df.iterrows():
        d = {key: getattr(row, key) for key in row.keys()}
        table = TableTest(**d)
        session.merge(table)

    # for d in data:
    #     u1 = TableTest(**d)
    #     session.merge(u1)
    session.commit()


def testDownIndexDailyBasic():
    """
    下载指数每日指标
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
    for code in codeList:
        # sql = (f'select max(trade_date) from index_dailybasic'
        #        f' where ts_code="{code}"')
        # result = engine.execute(sql).fetchone()[0]
        # startDate = None
        # if isinstance(result, type(datetime.date)):
        #     result = result + timedelta(days=1)
        #     startDate = result.strftime('YYYYmmdd')
        startDate = '20040101'
        endDate = '20080101'
        df = pro.index_dailybasic(ts_code=code,
                                  start_date=startDate, end_date=endDate)
        writeSQL(df, 'index_dailybasic')


def testDownIndexDaily():
    """
    下载指数每日指标
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
    for code in codeList:
        sql = (f'select max(trade_date) from index_daily'
               f' where ts_code="{code}"')
        result = engine.execute(sql).fetchone()[0]
        startDate = None
        endDate = None
        if isinstance(result, dt.date):
            result = result + timedelta(days=1)
            startDate = result.strftime('YYYYmmdd')
        # startDate = '20040101'
        # endDate = '20080101'
        df = pro.index_daily(ts_code=code,
                             start_date=startDate, end_date=endDate)
        writeSQL(df, 'index_daily')


def testDownIndexWeight():
    """
    下载指数成份和权重
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
    for code in codeList:
        sql = (f'select max(trade_date) from index_weight'
               f' where index_code="{code}"')
        result = engine.execute(sql).fetchone()[0]
        startDate = None
        endDate = None
        if isinstance(result, dt.date):
            result = result + timedelta(days=1)
            startDate = result.strftime('YYYYmmdd')
        # startDate = '20040101'
        # endDate = '20080101'
        df = pro.index_weight(index_code=code,
                              start_date=startDate, end_date=endDate)
        writeSQL(df, 'index_weight')


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
            nowtime = datetime.now()
            if (perTimes > 0 and downLimit > 0 and cur >= downLimit
                    and (nowtime < times[cur - downLimit] + timedelta(
                        seconds=perTimes))):
                _timedelta = nowtime - times[cur - downLimit]
                sleeptime = perTimes - _timedelta.seconds
                print(f'******暂停{sleeptime}秒******')
                time.sleep(sleeptime)

            startDate = initDate.strftime('%Y%m%d')
            initDate += timedelta(days=30)
            endDate = initDate.strftime('%Y%m%d')
            initDate += timedelta(days=1)
            print(f'下载{code},日期{startDate}-{endDate}')
            df = pro.index_weight(index_code=code,
                                  start_date=startDate, end_date=endDate)
            writeSQL(df, 'index_weight')

            nowtime = datetime.now()
            times.append(nowtime)
            cur += 1


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
    # -------------------------------
    # 下载指定股票股本信息
    # date = '2019-04-19'
    # gubenUpdateDf = checkGuben(date)
    # for stockID in gubenUpdateDf['stockid']:
    #     downGuben(stockID)
    #     setGubenLastUpdate(stockID, date)
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
    # stockID = '000651'
    # startDate = '20090829'
    # endDate = '20171231'
    # tradeDate = '20191231'
    # dates = dateStrList(startDate, endDate)
    # for d in dates:
    #     # df = downDailyBasic(stockID=stockID,
    #     #                     startDate=startDate,
    #     #                     endDate=endDate)
    #     # df = downDailyBasic(tradeDate=tradeDate)
    #     df = downDailyBasic(tradeDate=d)
    #     print(df)
    #     # df.to_excel('test.xlsx')
    #     writeSQL(df, 'dailybasic')

    # 下载股权质押统计数据
    # IDs = readStockIDsFromSQL()
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
    #     stockID = IDs[i]
    #     print(stockID)
    #     flag = True
    #     df = None
    #     while flag:
    #         try:
    #             df = downPledgeStat(stockID)
    #             flag = False
    #         except Exception as e:
    #             print(e)
    #             time.sleep(10)
    #     # print(df)
    #     time.sleep(1)
    #     if df is not None:
    #         writeSQL(df, 'pledgestat')

    # 下载利润表
    # stockID = '000651'
    # startDate = '20160101'
    # endDate = '20200202'
    # downIncome(stockID, startDate=startDate, endDate=endDate)
    # downIncome(stockID)

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
    # testDownIndexDaily()

    # 下载指数成分和权重
    testDownIndexWeightRepair()

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

    # 重建表
    # createHangyePE()
    # createValuation()

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
    # incDf = sqlrw.readLastTTMLirun(stocks.stockid.tolist(), sectionNum, date)
    # incDf = sqlrw.readLastTTMLirunForStockID('000651', 6, '20181231')

    # print(incDf)
    # print(incDf.loc[incDf.stockid=='000651'])

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

    print('程序正常退出')
