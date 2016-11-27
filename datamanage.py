# -*- coding: utf-8 -*-
'''
Created on 2016年1月10日

@author: who8736
'''

# import sys  # python的系统调用模块
import os
import logging
import ConfigParser
from multiprocessing.dummy import Pool as ThreadPool

import sqlrw
import datetime as dt

# import datatrans


class DataManage():

    def __init__(self, parent=None):
        # 更新股票列表
        #         sqlrw.updateStockList()
        self.stockList = sqlrw.readStockIDsFromSQL()
        self.stockNum = len(self.stockList)
        self.klineList = []
        self.updateFailList = []

        # 初始化数据更新页控件
        self.loadConf()

    def startUpdate(self):
        """自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
        """
        self.updateKlineBaseData()
        self.updateLirun()
        self.updateGuben()
        self.updateKlineEXTData()
        self.updateGuzhi()
        self.updateMainTable()

        logging.info('--------全部更新完成--------')

    def updateLirun(self):
        logging.info('===========start updateLirun===========')
        print 'sbDownloadThreadNum:%d' % self.downlodThreadNum
        print 'stockList have %d item.' % self.stockNum
        startTime = dt.datetime.now()

        sqlrw.updateLirun()
#         startQuarter = sqlrw.getLirunUpdateStartQuarter()
#         endQuarter = sqlrw.getLirunUpdateEndQuarter()
#
#         dates = datatrans.dateList(startQuarter, endQuarter)
#         for date in dates:
#             print date
#             df = sqlrw.downloadLirun(date)
#             if df is None:
#                 continue
#             print len(df)
#             # 读取已存储的利润数据，从下载数据中删除该部分，对未存储的利润写入数据库
#             lirunCur = sqlrw.readLirunForDate(date)
#             df = df[~df.stockid.isin(lirunCur.stockid)]
#             df = df[df.profits.notnull()]
#             print df
#
#             # 对未存储的利润写入数据库，并重新计算TTM利润
#             if not df.empty:
#                 sqlrw.writeLirun(df)
#                 sqlrw.calAllTTMLirun(date)

        endTime = dt.datetime.now()
        logging.info('===========end updateLirun===========')
        logging.info('updateLirun cost time: %s ',
                     endTime - startTime)

    def updateKlineEXTData(self):
        logging.info('===========start updateKlineEXTData===========')
        startTime = dt.datetime.now()

        pool = ThreadPool(processes=self.downlodThreadNum)
        pool.map(sqlrw.updateKlineEXTData, self.stockList)
        pool.close()
        pool.join()

        endTime = dt.datetime.now()
        logging.info('===========end updateKlineEXTData===========')
        logging.info('updateKlineBaseData cost time: %s ',
                     endTime - startTime)

#     def updateKlineMarketValue(self):
#         for i in self.stockList:
#             stockID = i[0]
#             logging.info('start updateKlineMarketValue %s', stockID)
#             sqlrw.updateKlineMarketValue(stockID)

#     def updateKlineTTMLirun(self):
#         """
#         a更新Kline表TTM利润
#         """
#         for i in self.stockList:
#             stockID = i[0]
#             logging.info('start updateKlineTTMLirun %s', stockID)
#             sqlrw.updateKlineTTMLirun(stockID)

    def updateGuben(self):
        logging.info('===========start updateGuben===========')
        startTime = dt.datetime.now()

        pool = ThreadPool(processes=self.downlodThreadNum)
        pool.map(sqlrw.downGubenToSQL, self.stockList)
        pool.close()
        pool.join()

        endTime = dt.datetime.now()
        logging.info('===========end updateGuben===========')
        logging.info('updateGuben cost time: %s ',
                     endTime - startTime)

    def loadConf(self):
        self.downlodThreadNum = 16
        self.writeThreadNum = 16
        self.flagIncreamentUpdate = 1
        self.maxRetry = 3
        if not os.path.isfile('datamanage.conf'):
            self.saveConf()
            return

        try:
            cf = ConfigParser.ConfigParser()
            cf.read('datamanage.conf')
            if cf.has_option('main', 'downlodThreadNum'):
                self.downlodThreadNum = cf.getint(
                    'main', 'downlodThreadNum')
            if cf.has_option('main', 'writeThreadNum'):
                self.writeThreadNum = cf.getint('main', 'writeThreadNum')
            if cf.has_option('main', 'startDate'):
                self.startDate = cf.get('main', 'startDate')
            if cf.has_option('main', 'endDate'):
                self.endDate = cf.get('main', 'endDate')
            if cf.has_option('main', 'flagIncreamentUpdate'):
                self.flagIncreamentUpdate = cf.getint(
                    'main', 'flagIncreamentUpdate')
            if cf.has_option('main', 'maxRetry'):
                self.maxRetry = cf.getint('main', 'maxRetry')
            if cf.has_option('main', 'startQuarter'):
                self.startQuarter = cf.getint('main', 'startQuarter')
            if cf.has_option('main', 'endQuarter'):
                self.endQuarter = cf.getint('main', 'endQuarter')
        except Exception, e:
            print e
            logging.error('read conf file error.')

    def saveConf(self):
        cf = ConfigParser.ConfigParser()
        # add section / set option & key
        cf.add_section('main')
        cf.set('main', 'downlodThreadNum', self.downlodThreadNum)
        cf.set('main', 'writeThreadNum', self.writeThreadNum)
        cf.set('main', 'startDate', self.startDate)
        cf.set('main', 'endDate', self.endDate)
        cf.set('main', 'flagIncreamentUpdate', self.flagIncreamentUpdate)
        cf.set('main', 'maxRetry', self.maxRetry)
        cf.set('main', 'startQuarter', self.startQuarter)
        cf.set('main', 'endQuarter', self.endQuarter)

        # write to file
        cf.write(open('datamanage.conf', 'w+'))

    def updateDataTest(self):
        self.stockList = self.stockList[:10]
        self.stockNum = len(self.stockList)
#         self.updateKlineBaseData()

    def updateGuzhi(self):
        logging.info('===========start updateGuzhi===========')
        startTime = dt.datetime.now()

        pool = ThreadPool(processes=self.downlodThreadNum)
        pool.map(sqlrw.downGuzhiToFile, self.stockList)
        pool.close()
        pool.join()

        endTime = dt.datetime.now()
        logging.info('===========end updateGuzhi===========')
        logging.info('updateGuzhi cost time: %s ',
                     endTime - startTime)

    def updateKlineBaseData(self):
        """ 启动多线程更新K线历史数据主函数
        """
        logging.info('===========start updateKlineBaseData===========')
        print 'sbDownloadThreadNum:%d' % self.downlodThreadNum
        print 'stockList have %d item.' % self.stockNum
        startTime = dt.datetime.now()

        pool = ThreadPool(processes=self.downlodThreadNum)
        pool.map(sqlrw.downKlineToSQL, self.stockList)
        pool.close()
        pool.join()

        endTime = dt.datetime.now()
        logging.info('===========end updateKlineBaseData===========')
        logging.info('updateKlineBaseData cost time: %s ',
                     endTime - startTime)

    def updateMainTable(self):
        logging.info('===========start updateMainTable===========')
        startTime = dt.datetime.now()

        pool = ThreadPool(processes=self.downlodThreadNum)
        pool.map(sqlrw.downloadMainTable, self.stockList)
        pool.close()
        pool.join()

        endTime = dt.datetime.now()
        logging.info('===========end updateMainTable===========')
        logging.info('updateMainTable cost time: %s ',
                     endTime - startTime)

    def readStockListFromFile(self, filename):
        stockFile = open(filename, 'r')
        stockList = [i[:6] for i in stockFile.readlines()]
        print stockList
        self.stockList = stockList
        self.stockNum = len(stockList)

    def readTestStockList(self):
        filename = '.\\data\\teststock.txt'
        return self.readStockListFromFile(filename)

    def readChiguStock(self):
        filename = '.\\data\\chigustockid.txt'
        self.readStockListFromFile(filename)

    def readYouzhiStock(self):
        filename = '.\\data\\youzhiid.txt'
        self.readStockListFromFile(filename)


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

    datamanage = DataManage()
#     datamanage.updateDataTest()
#     datamanage.readTestStockList()
    datamanage.startUpdate()

#     stockID = '000005'
#     sqlrw.downloadMainTable(stockID)

#     datamanage.updateMainTable()
