# -*- coding: utf-8 -*-
'''
Created on 2016年1月10日

@author: who8736
'''

# import sys  # python的系统调用模块
import os
# import time
from time import sleep
import logging
import ConfigParser
# import socket
# import urllib
# 多线程类
# from threading import Thread, RLock
from multiprocessing.dummy import Pool as ThreadPool
# import urllib2
# 任务队列，方便各子线程共用
# from Queue import Queue

import sqlrw
import datetime as dt
# from pandas.core.frame import DataFrame

import datatrans


class DataManage():

    def __init__(self, parent=None):
        # 更新股票列表
        #         sqlrw.updateStockList()
        self.stockList = sqlrw.readStockIDsFromSQL()
        self.stockNum = len(self.stockList)
        self.klineList = []
#         self.threadDownloadEndCount = 0
#         self.threadWriteEndCount = 0
#         self.startDownloadTime = dt.datetime.now()
#         self.endDownloadTime = dt.datetime.now()
#         self.downloadTimes = dt.datetime.now()
        self.updateFailList = []

        # 初始化数据更新页控件
        self.loadConf()

        # 初始化线程队列
#         self.downloadQueue = Queue()
#         self.writeQueue = Queue()
#         self.flagDownloadFinal = False
        # 创建一个可重入线程锁，保证线程安全性
#         self.lock = RLock()

        # 重试次数
        self.retryMax = 3

    def startUpdate(self):
        """自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
        """
#         pass
#         self.updateKlineBaseData()
#         self.updateLirun()
        self.updateGuben()
#         self.updateKlineEXTData()
#         self.updateGuzhi()
#         self.updateMainTableA()

        logging.info('--------全部更新完成--------')

    def updateLirun(self):
        #         self.threadRun(sqlrw.fullUpdateLirun())
        logging.info('===========start updateLirun===========')
        print 'sbDownloadThreadNum:%d' % self.downlodThreadNum
        print 'stockList have %d item.' % self.stockNum
        startTime = dt.datetime.now()

        startQuarter = sqlrw.getLirunUpdateStartQuarter()
        endQuarter = sqlrw.getLirunUpdateEndQuarter()

        dates = datatrans.dateList(startQuarter, endQuarter)
        for date in dates:
            print date
            df = sqlrw.downloadLirun(date)
            if df is None:
                continue
            print len(df)
            # 读取已存储的利润数据，从下载数据中删除该部分，对未存储的利润写入数据库
            lirunCur = sqlrw.readLirunForDate(date)
            df = df[~df.stockid.isin(lirunCur.stockid)]
            df = df[df.profits.notnull()]
            print df

            #                 sqlrw.writeLirun(df)
            # 对未存储的利润写入数据库，并重新计算TTM利润
            if not df.empty:
                sqlrw.writeLirun(df)
                sqlrw.calAllTTMLirun(date)
#         self.flagUpdateLirun = True

        endTime = dt.datetime.now()
        logging.info('===========end updateLirun===========')
        logging.info('updateLirun cost time: %s ',
                     endTime - startTime)

#     def threadRun(self, func, args=None):
#         thread = Thread(target=func)
#         thread.setDaemon(True)
#         thread.start()

    def updateKlineEXTData(self):
        logging.info('===========start updateKlineEXTData===========')
        startTime = dt.datetime.now()

        pool = ThreadPool(processes=self.downlodThreadNum)
        pool.map(sqlrw.updateKlineEXTData, self.stockList)
        pool.close()
        pool.join()

#         count = 0
#         for stockID in self.stockList:
#             count += 1
# #             stockID = i[0]
#             logging.info('stockID is %s, %d/%d',
#                          stockID,
#                          count,
#                          self.stockNum)
#             if self.flagIncreamentUpdate == 1:
#                 sqlrw.updateKlineEXTData(stockID)
#             else:
#                 sqlrw.updateKlineEXTData(stockID, '1990-01-01')
        endTime = dt.datetime.now()
        logging.info('===========end updateKlineEXTData===========')
        logging.info('updateKlineBaseData cost time: %s ',
                     endTime - startTime)

    def updateKlineMarketValue(self):
        for i in self.stockList:
            stockID = i[0]
            logging.info('start updateKlineMarketValue %s', stockID)
            sqlrw.updateKlineMarketValue(stockID)

    def updateKlineTTMLirun(self):
        """
        a更新Kline表TTM利润
        """
        for i in self.stockList:
            stockID = i[0]
            logging.info('start updateKlineTTMLirun %s', stockID)
            sqlrw.updateKlineTTMLirun(stockID)

#     def updateGuben(self):
#         logging.info('===========start updateGuben===========')
#         self.flagDownloadFinal = False
# # #         self.updateFinalNum = 0
# #         print 'downloadThreadNum:%d' % self.downlodThreadNum
# #         # print self.codeList
# #         # print self.stockList
#         print 'stockList have %d item.' % self.stockNum
#         self.startDownloadTime = dt.datetime.now()
#         self.setDownloadGubenQueue()
#
#         # 调用线程函数启动线程
# #         self.downlodThreadNum = self.sbDownloadThreadNum.value()
#         self.startDownloadWorkers(self.downlodThreadNum)
#
#         self.startWriteTime = dt.datetime.now()
#         self.startWriteWorkers(self.writeThreadNum)
#         while self.threadDownloadEndCount < self.downlodThreadNum:
#             sleep(1)
#         self.flagDownloadFinal = True
#         while self.threadWriteEndCount < self.writeThreadNum:
#             sleep(1)
#         logging.info('===========end updateGuben===========')

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
#                     self.startDate = '2012-01-01'
#                     self.endDate = '2012-01-01'
        self.flagIncreamentUpdate = 1
        self.maxRetry = 3
#                     self.startQuarter = 19901
#                     self.endQuarter = 20163
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
        #         self.loadConf()
        #         if not os.path.isfile('stockanalyse.conf'):
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

#     def initStockList(self):
#         del self.stockList[:]
#         self.stockList = sqlrw.readStockIDsFromSQL()
#         print 'self.stockList:', self.stockList
#         self.stockNum = len(self.stockList)

    def updateDataTest(self):
        self.stockList = self.stockList[:10]
        self.stockNum = len(self.stockList)
#         self.updateKlineBaseData()

#     def updateGuzhi(self):
#         logging.info('===========start updateGuzhi===========')
#
#         print 'update guzhi'
#         print 'downloadThreadNum:%d' % self.downlodThreadNum
#         # print self.codeList
#         # print self.stockList
#         print 'stockList have %d item.' % self.stockNum
#         self.startDownloadTime = dt.datetime.now()
#         self.setDownloadGuzhiQueue()
#
#         # 调用线程函数启动线程
#         self.startDownloadWorkers(self.downlodThreadNum)
#         while self.threadDownloadEndCount < self.downlodThreadNum:
#             sleep(1)
#         logging.info('===========end updateGuzhi===========')

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

#     def setDownloadGuzhiQueue(self):
#         for i in range(self.stockNum):
#             self.downloadQueue.put([sqlrw.downGuzhiToFile,
#                                     self.stockList[i][0],
#                                     ])

#     def setDownloadKlineQueue(self):
#         # 初始化K线更新状态表
#         #         self.initUpdateTable()
#         # 初始化股票列表
#         #         self.initStockList()
#         # 将待更新的每支股票分别压入队列
#         self.updateFailList = []
#         for i in range(self.stockNum):
#             self.downloadQueue.put([self.downloadKlineWorker,
#                                     [i,
#                                      self.stockList[i][0],
#                                      ]
#                                     ])

#     def setDownloadGubenQueue(self):
#         # 初始化K线更新状态表
#         #         self.initUpdateTable()
#         # 初始化股票列表
#         #         self.initStockList()
#         # 将待更新的每支股票分别压入队列
#         self.updateFailList = []
#         for i in range(self.stockNum):
#             self.downloadQueue.put([self.downloadGuben,
#                                     [i, self.stockList[i][0]
#                                      ]
#                                     ])

#     def setDownloadMainTableQueue(self):
#         # 初始化K线更新状态表
#         #         self.initUpdateTable()
#         # 初始化股票列表
#         #         self.initStockList()
#         # 将待更新的每支股票分别压入队列
#         #        startDate = self.dateEditStart.date().toString('yyyy-MM-dd')
#         #        endDate = self.dateEditEnd.date().toString('yyyy-MM-dd')
#
#         self.threadDownloadEndCount = 0
#         for i in range(self.stockNum):
#             self.downloadQueue.put([self.downloadMainTable,
#                                     [i,
#                                      self.stockList[i]
#                                      ]
#                                     ])
#         self.updateFailList = []

#     def updateKlineBaseData(self):
#         """ 启动多线程更新K线历史数据主函数
#         """
#         logging.info('===========start updateKlineBaseData===========')
#         print 'sbDownloadThreadNum:%d' % self.downlodThreadNum
#         # print self.codeList
#         # print self.stockList
#         print 'stockList have %d item.' % self.stockNum
#         self.startDownloadTime = dt.datetime.now()
#         self.setDownloadKlineQueue()
#
#         # 调用线程函数启动线程
# #         self.downlodThreadNum = self.sbDownloadThreadNum.value()
#         self.startDownloadWorkers(self.downlodThreadNum)
#
#         self.startWriteTime = dt.datetime.now()
#         self.startWriteWorkers(self.writeThreadNum)
#         while self.threadDownloadEndCount < self.downlodThreadNum:
#             sleep(1)
#         self.flagDownloadFinal = True
#         while self.threadWriteEndCount < self.writeThreadNum:
#             sleep(1)
#         logging.info('===========end updateKlineBaseData===========')

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

#     def updateMainTable(self):
#         logging.info('===========start updateMainTable===========')
#
#         logging.info('sbDownloadThreadNum:%d', self.downlodThreadNum)
#         logging.info('stockList have %d item.', self.stockNum)
#         self.startDownloadTime = dt.datetime.now()
#         self.setDownloadMainTableQueue()
#
#         # 调用线程函数启动线程
# #         self.downlodThreadNum = self.sbDownloadThreadNum.value()
#         self.startDownloadWorkers(self.downlodThreadNum)
#         while self.threadDownloadEndCount < self.downlodThreadNum:
#             sleep(1)
#         logging.info('===========end updateMainTable===========')

    def updateMainTableA(self):
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


#     def downloadKlineWorker(self, myArgs):
#         index, stockID = myArgs
#
# #         logging.info('start downloadKlineWorker %s, startDate=%s',
# #                      stockID, startDate)
#         tryCount = 0
#         klineState = False
# #         df = DataFrame({'emptydf': []})
#         while True:
#             tryCount += 1
#             df = sqlrw.downloadKline(stockID)
#
#             if df is not None:
#                 # print u"下载数据成功：%s" % stockID
#                 # logging.info('end getHist %s' % stockID)
#                 klineState = True
#                 break
#             else:
#                 logging.info(
#                     '[%s]:fail to download data, retry...', stockID)
#             if tryCount >= self.retryMax:
#                 logging.error('download %s fail!!!', stockID)
#                 self.updateFailList.append(stockID)
#                 break
#
#         self.lock.acquire()
#         self.writeQueue.put(
#             [self.writeKline, [index, klineState, stockID, df]])
#         self.lock.release()
#         logging.info('final getHist %s', stockID)
#         return klineState

#     def downloadGuben(self, myArgs):
#         index, stockID = myArgs
#
#         tryCount = 0
#         while True:
#             tryCount += 1
#             df = sqlrw.gubenURLToDf(stockID)
#
#             if isinstance(df, DataFrame):
#                 #                 print u'下载数据成功：%s' % stockID
#                 logging.info('end getHist %s' % stockID)
#                 gubenState = True
#                 break
#             if tryCount >= self.retryMax:
#                 logging.info('stop download %s data!!!', stockID)
#                 self.updateFailList.append(stockID)
#                 gubenState = False
#                 break
#             else:
#                 logging.info('fail to download %s data, retry...', stockID)
#
# #         args = [index, gubenState, stockID, df]
#         self.lock.acquire()
#         self.writeQueue.put([self.writeGuben,
#                              [index, stockID, gubenState, df]])
#         self.lock.release()
# #         logging.info('end getHist %s' % stockID)
#         return gubenState

#     def downloadMainTable(self, stockID):
#         #     def downloadMainTable(self, myArgs):
#         #         index, stockItem = myArgs
#         #         print index, stockItem
#         #         stockID = stockItem[0]
#         # print stockID
#
#         #         logging.info('start getHist %s' % stockID)
#         tryCount = 0
#         downloadState = False
#         while True:
#             tryCount += 1
#             if tryCount > self.retryMax:
#                 logging.info(u'stop download %s dada!!!', stockID)
#                 self.updateFailList.append(stockID)
#                 break
#             result = sqlrw.downloadMainTable(stockID)
#             if result:
#                 logging.info(u'下载三个主表成功：%s', stockID)
#                 downloadState = True
#                 break
#             else:
#                 logging.info(u'fail to download %s dada, retry...', stockID)
#
# #         logging.info('end getHist %s' % stockID)
# #         self.sinUpdateFinal.emit(index, downloadState)
#         return downloadState

#     def writeKline(self, myArgs):
#         unusedindex, klineState, stockID, df = myArgs
#         tableName = sqlrw.tablenameKline(stockID)
# #         logging.info('writeKline index: %s, klineState: %s',
# #                      index, klineState)
#
# #         sqlrw.quickWriteSQLB(stockID, df)
#         if (df is not None) and (not df.empty):
#             klineState = sqlrw.writeSQL(df, tableName)
#         return klineState
#         self.sinUpdateFinal.emit(index, klineState)

#     def writeGuben(self, myArgs):
#         index, stockID, gubenState, df = myArgs
#         tablename = 'guben'
#         lastUpdate = sqlrw.getGubenLastUpdateDate(stockID)
#         print 'aaaaaaaaaaa'
#         print ('index, stockID, gubenState, df',
#                 index, stockID, gubenState, df)
#         print '%s: %s' % (stockID, lastUpdate)
#         print df
#         df = df[df.date > lastUpdate]
#         print 'bbbbbbbbbbbbb'
#         print df
#         if not df.empty:
#             try:
#                 gubenState = sqlrw.writeSQL(df, tablename)
#             except Exception, e:
#                 logging.error('%s:%s' % (Exception, e))
#                 gubenState = False

#     def startDownloadWorkers(self, threadNum):
#         # 维护一个线程池，并启动各个线程
#         pool = []
#         self.threadDownloadEndCount = 0
#         for unusedi in range(threadNum):
#             # 线程池中加入一个线程
#             pool.append(Thread(target=self.downloadWorker))
#             # 确保主进程结束后子线程也退出
#             pool[-1].setDaemon(True)
#             pool[-1].start()

#     def startWriteWorkers(self, threadNum):
#         # 维护一个线程池，并启动各个线程
#         pool = []
#         self.threadWriteEndCount = 0
#         for unusedi in range(threadNum):
#             # 线程池中加入一个线程
#             pool.append(Thread(target=self.writeWorker))
#             # 确保主进程结束后子线程也退出
#             pool[-1].setDaemon(True)
#             pool[-1].start()

#     def writeWorker(self):
#         print 'start writeWorker'
#         # 从网址队列中取出一个网址，并获得它的序号
#         while not (self.writeQueue.empty() and self.flagDownloadFinal):
#             # 获得线程锁，防止其它线程同时对队列进行操作
#             #             print u'取写队列'
#             self.lock.acquire()
#             if self.writeQueue.empty():
#                 self.lock.release()
#                 time.sleep(1)
#                 continue
#             func, myArgs = self.writeQueue.get()
#             # 释放锁，让其它线程可以从队列中读取网址
#             self.lock.release()
#             func(myArgs)
#             self.writeQueue.task_done()
# #         print u'写线程结束'
# #         self.sinWriteFinal.emit()  # 线程结束，抛出信号
#         self.lock.acquire()
#         self.threadWriteEndCount += 1
#         self.lock.release()

#     def downloadWorker(self):
#         # 从网址队列中取出一个网址，并获得它的序号
#         while not self.downloadQueue.empty():
#             #             print u'取下载队列'
#             # 获得线程锁，防止其它线程同时对队列进行操作
#             self.lock.acquire()
#             func, myArgs = self.downloadQueue.get()
#             # 释放锁，让其它线程可以从队列中读取网址
#             self.lock.release()
#             func(myArgs)
#             self.downloadQueue.task_done()
# #         self.sinDownloadFinal.emit()  # 线程结束，抛出信号
#         self.lock.acquire()
#         self.threadDownloadEndCount += 1
#         self.lock.release()
# #         self.threadDownloadEndCount += 1
#         print 'threadDownloadEndCount', self.threadDownloadEndCount

    def readStockListFromFile(self, filename):
        stockFile = open(filename, 'r')
        stockList = [i[:6] for i in stockFile.readlines()]
#             stockName = sqlrw.readStockName(stockID)
#             stockList.append([stockID, stockName])
        print stockList

        self.stockList = stockList
        self.stockNum = len(stockList)
#         self.initUpdateTable()
#         self.initGuzhiTable()
#         self.initHistPETable()

    def readTestStockList(self):
        filename = '.\\data\\teststock.txt'
        return self.readStockListFromFile(filename)

    def readChiguStock(self):
        filename = '.\\data\\chigustockid.txt'
        self.readStockListFromFile(filename)

    def readYouzhiStock(self):
        filename = '.\\data\\youzhiid.txt'
        self.readStockListFromFile(filename)


# def downloadData(url, timeout=69, maxRetry=3):
#     try:
#         print u'开始下载数据。。。'
#         socket.setdefaulttimeout(timeout)
#         sock = urllib.urlopen(url)
#         data = sock.read()
#     except IOError, e:
#         print u'数据下载失败（%s）： %s' % (e, url)
#         return False
#     else:
#         sock.close()
#     return data

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
