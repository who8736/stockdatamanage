# -*- coding: utf-8 -*-
"""
Created on 2016年1月10日

@author: who8736
"""

# import sys  # python的系统调用模块
# import os
import logging
import time
import configparser
from datetime import datetime, timedelta
# import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool
from functools import wraps
import datetime as dt
import baostock as bs
import tushare as ts

import datatrans
import hyanalyse
import dataanalyse
import sqlrw
import valuation
from sqlrw import checkGuben, setGubenLastUpdate
from sqlrw import getStockKlineUpdateDate, getIndexKlineUpdateDate
from sqlrw import getAllMarketPEUpdateDate
import download
from download import downGuben, downGuzhi, downKline
from download import downMainTable, downloadLirun, downStockList
from download import downIndex
# from download import downHYList
from initlog import initlog
from datatrans import dateList


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

    # 更新股票列表
    downStockList()
#     stockList = stockList[:10]

    # 更新行业列表
    downHYList()

    # 更新股票利润数据
    updateLirun()

    # 更新股本
    # 因新浪反爬虫策略，更新股本数据改用单线程
    #     updateGuben(stockList, threadNum)
    updateGubenSingleThread()

    # 更新股票日交易数据
    threadNum = 10
    stockList = sqlrw.readStockIDsFromSQL()
    # updateKlineBaseData(stockList, threadNum)
    updateKline()
    updateKlineEXTData(stockList, threadNum)

    # 因新浪反爬虫策略，更新股本数据改用单线程, 20170903
    # 主表数据暂时没用，停止更新， 20170904
#     updateMainTableSingleThread(stockList, threadNum)
#     updateMainTable(stockList, threadNum)

    # 更新股票估值
    updateGhuzhiData()

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
    dataanalyse.calAllPEHistory(startDate)


@logfun
def updateIndex():
    """
    更新指数数据及PE
    :return:
    """
    ID = '000010.SH'
    startDate = getIndexKlineUpdateDate() + dt.timedelta(days=1)
    download.downChengfen(ID, startDate)
    downIndex(ID, startDate)
    dataanalyse.calPEHistory(ID[:6], startDate)


@logfun
def downHYList():
    download.downHYList()


@logfun
def updateLirun():
    startQuarter = sqlrw.getLirunUpdateStartQuarter()
    endQuarter = sqlrw.getLirunUpdateEndQuarter()

    dates = datatrans.QuarterList(startQuarter, endQuarter)
    for date in dates:
        #         print date
        logging.debug('updateLirun: %s', date)
        try:
            df = downloadLirun(date)
        except ValueError:
            continue
        if df is None:
            continue
        # 读取已存储的利润数据，从下载数据中删除该部分，对未存储的利润写入数据库
        lirunCur = sqlrw.readLirunForDate(date)
        df = df[~df.stockid.isin(lirunCur.stockid)]
        df = df[df.profits.notnull()]
#         print df

        # 对未存储的利润写入数据库，并重新计算TTM利润
        if not df.empty:
            sqlrw.writeLirun(df)
            sqlrw.calAllTTMLirun(date)
            updateHYData(date)


@logfun
def updateKlineEXTData(stockList, threadNum):
    pool = ThreadPool(processes=threadNum)
    pool.map(sqlrw.updateKlineEXTData, stockList)
    pool.close()
    pool.join()


@logfun
def updateGuben(stockList, threadNum):
    """ 更新股本多线程版， 因新浪限制， 暂时无用
    """
    pool = ThreadPool(processes=threadNum)
    pool.map(downGuben, stockList)
    pool.close()
    pool.join()


@logfun
def updateGubenSingleThread():
    """ 更新股本单线程版
    """
    # stockList = sqlrw.readGubenUpdateList()
    # for stockID in stockList:
    #     downGuben(stockID)
    #     time.sleep(5)
    # 以上代码为原股本下载代码

    endTime = datetime.now()
    endTime = endTime + timedelta(days=-1)
    # 选择要提前的天数
    startTime = endTime + timedelta(days=-10)
    # 格式化处理
    startDate = startTime.strftime('%Y%m%d')
    endDate = endTime.strftime('%Y%m%d')

    pro = ts.pro_api()
    df = pro.trade_cal(exchange='SSE', start_date=startDate,
                       end_date=endDate)
    date = df[df.is_open == 1].cal_date.max()
    gubenUpdateDf = checkGuben(date)
    for stockID in gubenUpdateDf['stockid']:
        downGuben(stockID)
        setGubenLastUpdate(stockID, date)
        time.sleep(2)


@logfun
def updatePf():
    """ 重算评分
    """
    valuation.calpf()

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
def updateKlineBaseData(stockList, threadNum):
    """ 启动多线程更新K线历史数据主函数
    """
    pool = ThreadPool(processes=threadNum)
    pool.map(downKline, stockList)
    pool.close()
    pool.join()


@logfun
def updateKline():
    """ 更新日交易数据
    """
    startDate = getStockKlineUpdateDate() + timedelta(days=1)
    endDate = datetime.today().date() - timedelta(days=1)
    for tradeDate in dateList(startDate, endDate):
        downKline(tradeDate)


@logfun
def updateMainTable(stockList, threadNum):
    """ 更新主表数据多线程版， # 因新浪反爬虫策略，改用单线程
    """
    pool = ThreadPool(processes=threadNum)
    pool.map(downMainTable, stockList)
    pool.close()
    pool.join()


@logfun
def updateMainTableSingleThread(stockList):
    """ 更新主表数据单线程版， 因主表数据暂时无用
    """
    for stockID in stockList:
        downMainTable(stockID)
        time.sleep(1)


@logfun
def updateHYData(date):
    hyanalyse.calAllHYTTMLirun(date)


@logfun
def updateGhuzhiData():
    dataanalyse.testChigu()
    dataanalyse.testShaixuan()


def readStockListFromFile(filename):
    stockFile = open(filename, 'r')
    stockList = [i[:6] for i in stockFile.readlines()]
    print(stockList)
    return stockList


# def readTestStockList():
#     filename = '.\\data\\teststock.txt'
#     return readStockListFromFile(filename)


# def readChiguStock():
#     filename = '.\\data\\chigustockid.txt'
#     readStockListFromFile(filename)

#
# def readYouzhiStock():
#     filename = '.\\data\\youzhiid.txt'
#     readStockListFromFile(filename)


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
    cf = configparser.ConfigParser()
    cf.read('sql.conf')
    if cf.has_option('main', 'token'):
        token = cf.get('main', 'token')
    else:
        token = ''
    ts.set_token(token)
    print(token)

#     updateDataTest()

    startUpdate()

    # updateLirun()
    # updatePf()

#     stockList = sqlrw.readStockIDsFromSQL()
#     updateGubenSingleThread()
#     updateMainTableSingleThread(stockList)
#     stockList = stockList[:10]
#     threadNum = 20
#     updateGuzhi(stockList, threadNum)


#     stockID = '000005'
#     sqlrw.downMainTable(stockID)
