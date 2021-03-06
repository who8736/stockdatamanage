# -*- coding: utf-8 -*-
"""
Created on 2016年1月10日

@author: who8736
"""

# import sys  # python的系统调用模块
# import os
import datetime
import logging
import time
import configparser
import datetime as dt
# from datetime import datetime, timedelta
# import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool
from functools import wraps
from dateutil.relativedelta import relativedelta

# import tushare
import numpy as np
import pandas as pd
import baostock as bs
import tushare as ts

# import datatrans
import analyse.report
from datatrans import lastQarterDate, dateStrList
import classifyanalyse
import analyse
import sqlrw
import valuation
from sqlconn import engine
# from sqlrw import checkGuben, setGubenLastUpdate
# from sqlrw import getStockKlineUpdateDate
from sqlrw import (getIndexPEUpdateDate, getAllMarketPEUpdateDate,
                   readStockList, engine, readCal)
# import download
from download import (downGuzhi, downStockList,
                      downDaily, downDailyBasic, downTradeCal,
                      downIndexDaily, downIndexDailyBasic,
                      downIndexBasic, downIndexWeight,
                      DownloaderQuarter, downHYList, downAdjFactor)
# from download import downHYList
from initlog import initlog
from datatrans import dateList
import config
from check import checkQuarterData


def logfun(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        #         def _func(*args):
        #         print "hello, %s" % func.__name__
        logging.info('===========start %s===========', func.__name__)
        startTime = dt.datetime.now()
        func(*args, **kwargs)
        endTime = dt.datetime.now()
        logging.info('===========end %s===========', func.__name__)
        logging.info('%s cost time: %s ',
                     func.__name__, endTime - startTime)

    #         return _func
    return wrapper


@logfun
def startUpdate():
    """自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
    """
    # 更新交易日历
    updateTradeCal()

    # 更新股票列表
    updateStockList()

    # 更新股票日交易数据
    updateDaily()

    # 更新每日指标
    updateDailybasic()

    # 更新复权因子
    updateAdjFacotr()

    # 更新非季报表格
    # 财务披露表（另外单独更新）
    # 质押表（另外单独更新）
    # 业绩预告（另外单独更新）
    # 业绩快报（另外单独更新）
    # 分红送股（另外单独更新）

    # 更新股票季报数据
    # 资产负债表
    # 利润表
    # 现金流量表
    # 财务指标表
    updateQuarterData()

    # 更新行业列表
    updateHYList()

    # 更新股票估值
    # updateGuzhiData()

    # 更新股票评分
    updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    updateAllMarketPE()


@logfun
def updateAllMarketPE():
    """
    更新全市场PE
    :return:
    """
    startDate = getAllMarketPEUpdateDate()
    analyse.report.calAllPEHistory(startDate)


@logfun
def updateIndex():
    """
    更新指数数据及PE
    :return:
    """
    downIndexBasic()
    downIndexDaily()
    downIndexDailyBasic()
    downIndexWeight()

    ID = '000010.SH'
    # startDate = getIndexKlineUpdateDate() + dt.timedelta(days=1)
    # startDate = getIndexPEUpdateDate()
    startDate = getIndexPEUpdateDate() + dt.timedelta(days=1)
    analyse.report.calPEHistory(ID, startDate)


@logfun
def updateHYList():
    downHYList()


@logfun
def updateQuarterData():
    """更新股票季报数据

    :return:
    """
    stocks = readStockList()

    # 每支股票最后的报告期和公告日期
    sql = (f'select ts_code, max(end_date) end_date, max(f_ann_date) ann_date'
           f' from income group by ts_code')
    df = pd.read_sql(sql, engine)
    stocks = pd.merge(stocks, df, how='left',
                      left_on='ts_code', right_on='ts_code')
    end_date = lastQarterDate(dt.datetime.today().date())
    stocks = stocks[(stocks.end_date.isnull()) | (stocks.end_date < end_date)]

    # 每支股票最后更新季报日期与每日指标中的最后日期计算的销售收入是否存在差异
    # 如有差异则说明需更新季报
    df = checkQuarterData()
    stocks = pd.merge(stocks, df, how='left',
                      left_on='ts_code', right_on='ts_code')
    stocks.set_index('ts_code', inplace=True)

    resultList = []
    for ts_code in stocks.index:
        # print(ts_code)
        # result, div = checkQuarterData(ts_code)
        # resultList.append(dict(ts_code=ts_code, result=result, div=div))
        if np.isnan(stocks.loc[ts_code, 'cha']):
            datestr = ''
        elif stocks.loc[ts_code, 'cha'] < 0.001:
            continue
        else:
            ann_date = stocks.loc[ts_code, 'ann_date']
            ann_date += relativedelta(days=1)
            datestr = ann_date.strftime('%Y%m%d')

        # if result == 0:
        #     continue
        # elif result == 1:
        #     # 更新该股票全部财务数据
        #     datestr = ''
        # else:
        #     ann_date = stocks[stocks.ts_code == ts_code].ann_date.values[0]
        #     ann_date += relativedelta(days=1)
        #     datestr = ann_date.strftime('%Y%m%d')
        downloader = DownloaderQuarter(ts_code=ts_code, startDate=datestr)
        downloader.run()

    # df = pd.DataFrame(resultList)
    # df.to_excel('data/mvpettm.xlsx')


# @logfun
# def updateKlineEXTData(stockList, threadNum):
#     pool = ThreadPool(processes=threadNum)
#     pool.map(sqlrw.updateKlineEXTData, stockList)
#     pool.close()
#     pool.join()


# @logfun
# def updateGuben(stockList, threadNum):
#     """ 更新股本多线程版， 因新浪限制， 暂时无用
#     """
#     pool = ThreadPool(processes=threadNum)
#     pool.map(downGuben, stockList)
#     pool.close()
#     pool.join()


@logfun
def del_updateGubenSingleThread():
    """ 更新股本单线程版
    """
    # stockList = sqlrw.readGubenUpdateList()
    # for ts_code in stockList:
    #     downGuben(ts_code)
    #     time.sleep(5)
    # 以上代码为原股本下载代码

    endTime = dt.datetime.now()
    endTime = endTime + dt.timedelta(days=-1)
    # 选择要提前的天数
    startTime = endTime + dt.timedelta(days=-10)
    # 格式化处理
    startDate = startTime.strftime('%Y%m%d')
    endDate = endTime.strftime('%Y%m%d')

    pro = ts.pro_api()
    df = pro.trade_cal(exchange='SSE', start_date=startDate,
                       end_date=endDate)
    date = df[df.is_open == 1].cal_date.max()
    # gubenUpdateDf = checkGuben(date)
    # for ts_code in gubenUpdateDf['ts_code']:
    #     downGuben(ts_code)
    #     setGubenLastUpdate(ts_code, date)
    #     time.sleep(2)


@logfun
def updatePf():
    """ 重算评分
    """
    # valuation.calpfnew()
    sql = 'select max(date) from valuation'
    result = engine.execute(sql).fetchone()
    if result is None:
        startDate = '20090101'
    else:
        startDate = result[0] + dt.timedelta(days=1)
        startDate = startDate.strftime('%Y%m%d')
    endDate = dt.datetime.today() - dt.timedelta(days=1)
    endDate = endDate.strftime('%Y%m%d')
    pro = ts.pro_api()
    df = pro.trade_cal(exchange='', start_date=startDate, end_date=endDate)
    dateList = df['cal_date'].loc[df.is_open == 1].tolist()
    # print(type(dateList))
    # print(dateList)
    for date in dateList:
        print('计算评分：', date)
        valuation.calpfnew(date)


#
# def updateDataTest(stockList):
#     stockList = stockList[:10]


@logfun
def updateGuzhi(stockList, threadNum):
    """ 因东方财富修改估值文件下载功能， 暂不能用
    """
    pool = ThreadPool(processes=threadNum)
    #     pool.map(sqlrw.downGuzhiToFile, stockList)
    pool.map(downGuzhi, stockList)
    pool.close()
    pool.join()


@logfun
def del_updateKlineBaseData(stockList, threadNum):
    """ 启动多线程更新K线历史数据主函数
    """
    pool = ThreadPool(processes=threadNum)
    # pool.map(downKline, stockList)
    pool.close()
    pool.join()


@logfun
def del_updateKline():
    """ 更新日交易数据
    """
    pass
    # startDate = getStockKlineUpdateDate() + timedelta(days=1)
    # endDate = dt.datetime.today().date() - timedelta(days=1)
    # for tradeDate in dateList(startDate, endDate):
    #     downKline(tradeDate)


# @logfun
# def updateMainTable(stockList, threadNum):
#     """ 更新主表数据多线程版， # 因新浪反爬虫策略，改用单线程
#     """
#     pool = ThreadPool(processes=threadNum)
#     pool.map(downMainTable, stockList)
#     pool.close()
#     pool.join()


# @logfun
# def updateMainTableSingleThread(stockList):
#     """ 更新主表数据单线程版， 因主表数据暂时无用
#     """
#     for ts_code in stockList:
#         downMainTable(ts_code)
#         time.sleep(1)


@logfun
def updateHYData(date):
    classifyanalyse.calAllHYTTMProfits(date)


@logfun
def updateGuzhiData():
    analyse.report.testChigu()
    analyse.report.testShaixuan()


def readStockListFromFile(filename):
    stockFile = open(filename, 'r')
    stockList = [i[:6] for i in stockFile.readlines()]
    print(stockList)
    return stockList


@logfun
def updateDaily():
    """更新日K线
    """
    downDaily()


@logfun
def updateDailybasic():
    """更新每日指标
    """
    sql = 'select max(trade_date) from daily_basic'
    lastdate = engine.execute(sql).fetchone()[0]
    lastdate += dt.timedelta(days=1)
    startDate = lastdate.strftime('%Y%m%d')
    endDate = dt.datetime.today().date() - dt.timedelta(days=1)
    endDate = endDate.strftime('%Y%m%d')
    dates = dateStrList(startDate, endDate)
    for d in dates:
        downDailyBasic(tradeDate=d)


@logfun
def updateTradeCal():
    """更新交易日历
    """
    sql = 'select year(max(cal_date)) from trade_cal'
    lastYear = engine.execute(sql).fetchone()[0]
    if lastYear is None:
        downTradeCal('1990')
    elif lastYear < dt.datetime.today().year:
        downTradeCal(str(int(lastYear) + 1))


@logfun
def updateStockList():
    downStockList()


@logfun
def updateAdjFacotr():
    """
    下载复权因子
    前复权 = 当日收盘价 × 当日复权因子 / 最新复权因子	qfq
    后复权 = 当日收盘价 × 当日复权因子	hfq
    :return:
    """
    sql = 'select max(trade_date) from adj_factor'
    result = engine.execute(sql).fetchone()
    if result is None or result[0] is None:
        startDate = '20090101'
    else:
        startDate = result[0].strftime('%Y%m%d')
    endDate = dt.datetime.today().strftime('%Y%m%d')
    dateList = readCal(startDate=startDate, endDate=endDate)
    for d in dateList:
        downAdjFactor(d)


if __name__ == '__main__':
    initlog()

    #     logging.debug('This is debug message')
    #     logging.info('This is info message')
    #     logging.warning('This is warning message')

    # 使用baostock数据源时需先做登录操作
    lg = bs.login()
    if lg.error_code != '0':
        logging.error('login baostock failed')

    # 加载tushare.pro用户的token,需事先在sql.conf文件中设置
    cf = config.Config()
    ts.set_token(cf.tushareToken)
    print('设置访问tushare的token:', cf.tushareToken)

    #     updateDataTest()

    startUpdate()

    # updateLirun()
    # updatePf()

#     stockList = sqlrw.readts_codesFromSQL()
#     updateGubenSingleThread()
#     updateMainTableSingleThread(stockList)
#     stockList = stockList[:10]
#     threadNum = 20
#     updateGuzhi(stockList, threadNum)


#     ts_code = '000005'
#     sqlrw.downMainTable(ts_code)
