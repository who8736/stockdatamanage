# -*- coding: utf-8 -*-
'''
Created on 2016年1月10日

@author: who8736
'''

# import sys  # python的系统调用模块
import os
import logging
# import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool
from functools import wraps
import sqlrw
import datetime as dt

# import datatrans
import hyanalyse
import dataanalyse


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
    stockList = sqlrw.readStockIDsFromSQL()
#     stockList = stockList[:10]
    threadNum = 20
    updateKlineBaseData(stockList, threadNum)
    updateLirun()
    updateGuben(stockList, threadNum)
    updateKlineEXTData(stockList, threadNum)
    updateGuzhi(stockList, threadNum)
    updateMainTable(stockList, threadNum)
    updateGhuzhiData()

#     logging.info('--------全部更新完成--------')


@logfun
def updateLirun():
    sqlrw.updateLirun()


@logfun
def updateKlineEXTData(stockList, threadNum):
    pool = ThreadPool(processes=threadNum)
    pool.map(sqlrw.updateKlineEXTData, stockList)
    pool.close()
    pool.join()


@logfun
def updateGuben(stockList, threadNum):
    pool = ThreadPool(processes=threadNum)
    pool.map(sqlrw.downGubenToSQL, stockList)
    pool.close()
    pool.join()

#
# def updateDataTest(stockList):
#     stockList = stockList[:10]


@logfun
def updateGuzhi(stockList, threadNum):
    pool = ThreadPool(processes=threadNum)
#     pool.map(sqlrw.downGuzhiToFile, stockList)
    pool.map(sqlrw.downGuzhiToSQL, stockList)
    pool.close()
    pool.join()


@logfun
def updateKlineBaseData(stockList, threadNum):
    """ 启动多线程更新K线历史数据主函数
    """
    pool = ThreadPool(processes=threadNum)
    pool.map(sqlrw.downKlineToSQL, stockList)
    pool.close()
    pool.join()


@logfun
def updateMainTable(stockList, threadNum):
    pool = ThreadPool(processes=threadNum)
    pool.map(sqlrw.downloadMainTable, stockList)
    pool.close()
    pool.join()


@logfun
def updateHYData():
    hyanalyse.calAllHYTTMLirun(20164)


@logfun
def updateGhuzhiData():
    dataanalyse.testChigu()
    dataanalyse.testShaixuan()


def readStockListFromFile(filename):
    stockFile = open(filename, 'r')
    stockList = [i[:6] for i in stockFile.readlines()]
    print stockList
    return stockList


def readTestStockList():
    filename = '.\\data\\teststock.txt'
    return readStockListFromFile(filename)


def readChiguStock():
    filename = '.\\data\\chigustockid.txt'
    readStockListFromFile(filename)


def readYouzhiStock():
    filename = '.\\data\\youzhiid.txt'
    readStockListFromFile(filename)


if __name__ == '__main__':
    logfilename = os.path.join(os.path.abspath(os.curdir), 'datamanage.log')
    print os.path.abspath(os.curdir)
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

#     stockID = '000005'
#     sqlrw.downloadMainTable(stockID)
