# -*- coding: utf-8 -*-
'''
Created on 2016年1月10日

@author: who8736
'''

# import sys  # python的系统调用模块
import os
import logging
import time
# import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool
from functools import wraps
import datetime as dt

import datatrans
import hyanalyse
import dataanalyse
import sqlrw
from download import downGuben, downGuzhi, downKline
from download import downMainTable, downloadLirun, downStockList


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
    # 更新股票列表与行业列表
    downStockList()
    stockList = sqlrw.readStockIDsFromSQL()
#     stockList = stockList[:10]

    # 更新股票交易与估值数据
    threadNum = 20
    updateKlineBaseData(stockList, threadNum)
    updateLirun()

    # 因新浪反爬虫策略，更新股本数据改用单线程
#     updateGuben(stockList, threadNum)
    updateGubenSingleThread()

    updateKlineEXTData(stockList, threadNum)
#     updateGuzhi(stockList, threadNum)   # 待删除

    # 因新浪反爬虫策略，更新股本数据改用单线程, 20170903
    # 主表数据暂时没用，停止更新， 20170904
#     updateMainTableSingleThread(stockList, threadNum)
#     updateMainTable(stockList, threadNum)
    updateGhuzhiData()

#     logging.info('--------全部更新完成--------')


@logfun
def updateLirun():
    startQuarter = sqlrw.getLirunUpdateStartQuarter()
    endQuarter = sqlrw.getLirunUpdateEndQuarter()

    dates = datatrans.dateList(startQuarter, endQuarter)
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
    stockList = sqlrw.readGubenUpdateList()
    for stockID in stockList:
        downGuben(stockID)
        time.sleep(5)

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
def updateHYData():
    hyanalyse.calAllHYTTMLirun(20173)


@logfun
def updateGhuzhiData():
    dataanalyse.testChigu()
    dataanalyse.testShaixuan()


def readStockListFromFile(filename):
    stockFile = open(filename, 'r')
    stockList = [i[:6] for i in stockFile.readlines()]
    print stockList
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
    logfilename = os.path.join(os.path.abspath(os.curdir), 'datamanage.log')
    print os.path.abspath(os.curdir)
    print logfilename
    formatStr = ('%(asctime)s %(filename)s[line:%(lineno)d] '
                 '%(levelname)s %(message)s')
    logging.basicConfig(level=logging.DEBUG,
                        format=formatStr,
                        #                     datefmt = '%Y-%m-%d %H:%M:%S',
                        filename=logfilename,
                        filemode='a')

    ##########################################################################
    # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，
    # 并将其添加到当前的日志处理对象#
    console = logging.StreamHandler()
#     console.setLevel(logging.INFO)
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] '
                                  '%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    ##########################################################################

#     logging.debug('This is debug message')
#     logging.info('This is info message')
#     logging.warning('This is warning message')

#     updateDataTest()

    startUpdate()

#     stockList = sqlrw.readStockIDsFromSQL()
#     updateGubenSingleThread()
#     updateMainTableSingleThread(stockList)
#     stockList = stockList[:10]
#     threadNum = 20
#     updateGuzhi(stockList, threadNum)


#     stockID = '000005'
#     sqlrw.downMainTable(stockID)
