# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 21:11:51 2016

@author: who8736
"""

import os
# import sys
import logging
# import urllib
import urllib2
import socket
import datetime as dt
import ConfigParser

from lxml import etree
import tushare as ts  # @UnresolvedImport
# import mysql.connector
import pandas as pd  # @UnusedImport
from pandas.compat import StringIO
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import DATE, DECIMAL, String
# from sqlalchemy import FLOAT, Integer
# from sqlalchemy import Sequence, Index
from sqlalchemy.orm import sessionmaker, scoped_session
# from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy
# from _sqlite3 import Row
from pandas.core.frame import DataFrame
from tushare.stock import cons as ct

import datatrans
# import eastmoneydata
# import yahoodata
# from sqlconn import engine, Session
# from datatrans import transQuarterToDate, dateList
# from datatrans import quarterAdd


class SQLConn():

    def __init__(self, parent=None):
        self.loadSQLConf()
        user = self.user
        password = self.password
        ip = self.ip
        connectStr = (u'mysql://%(user)s:%(password)s@%(ip)s'
                      u'/stockdata?charset=utf8' % locals())
        self.engine = create_engine(connectStr,
                                    strategy=u'threadlocal', echo=False)
        self.Session = scoped_session(
            sessionmaker(bind=self.engine, autoflush=False))

    def loadSQLConf(self):
        if not os.path.isfile('sql.conf'):
            self.user = u'root'
            self.password = u'password'
            self.ip = '127.0.0.1'
            self.saveConf()
            return

        try:
            cf = ConfigParser.ConfigParser()
            cf.read('sql.conf')
            if cf.has_option('main', 'user'):
                self.user = cf.get('main', 'user')
            if cf.has_option('main', 'password'):
                self.password = cf.get('main', 'password')
            if cf.has_option('main', 'ip'):
                self.ip = cf.get('main', 'ip')
        except Exception, e:
            print e
            logging.error('read conf file error.')

    def saveConf(self):
        #         self.loadConf()
        #         if not os.path.isfile('stockanalyse.conf'):
        cf = ConfigParser.ConfigParser()
        # add section / set option & key
        cf.add_section('main')
        cf.set('main', 'user', self.user)
        cf.set('main', 'password', self.password)
        cf.set('main', 'ip', self.ip)

        # write to file
        cf.write(open('sql.conf', 'w+'))

sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session


# def getHist(stockID='600004', start='1979-01-01', end=None):
#     """抓取股票历史K线
#     stockID： 股票代码
#     start: 开始日期
#     end: 结束日期
#     """
#     if end is None:
#         end = dt.datetime.today().strftime('%Y-%m-%d')
# #     print start
# #     print end
#     return ts.get_hist_data(stockID, start, end)


# def fullUpdateLirun():
#     stockList = readStockID()
#     lirunList = []
#     for i in stockList:
#         stockID = i[0]
#         lirun = lirunFileToList(stockID)
#         if lirun:
#             lirunList.append(lirun)
#     tableName = 'lirun'
#     return writeSQL(lirunList, tableName)

def downGubenToSQL(stockID, retry=20, timeout=10):
    """下载单个股票股本数据写入数据库"""
    gubenURL = urlGuben(stockID)

    socket.setdefaulttimeout(timeout)
    headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; Windows NT 6.1; '
                              'en-US;rv:1.9.1.6) Gecko/20091201 '
                              'Firefox/3.5.6')}
    req = urllib2.Request(gubenURL, headers=headers)
#     downloadStat = False
    gubenDf = pd.DataFrame()
    for _ in range(retry):
        try:
            guben = urllib2.urlopen(req).read()
        except IOError, e:
            logging.warning('[%s]:download %s guben data, retry...',
                            e, stockID)
        else:
            gubenDf = gubenDataToDf(stockID, guben)
            tablename = 'guben'
            lastUpdate = getGubenLastUpdateDate(stockID)
        #     print '%s: %s' % (stockID, lastUpdate)
            gubenDf = gubenDf[gubenDf.date > lastUpdate]
            if not gubenDf.empty:
                writeSQL(gubenDf, tablename)
            return
    logging.error('fail download %s guben data.', stockID)


def downKlineToSQL(stockID, startDate=None, endDate=None, retry_count=20):
    """下载单个股票K线历史写入数据库"""
    logging.debug('download kline: %s', stockID)
    if startDate is None:  # startDate为空时取股票最后更新日期
        startDate = getKlineLastUpdateDate(stockID)
#         print stockID, startDate
        startDate = startDate.strftime('%Y-%m-%d')
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
#     retryCount = 0
    for cur_retry in range(1, retry_count + 1):
        try:
            df = ts.get_hist_data(stockID, startDate, endDate, retry_count=1)
        except IOError:
            logging.warning('fail download %s Kline data %d times, retry....',
                            stockID, cur_retry)
#             logging.warning(e)
#             return
        else:
            if (df is None) or df.empty:
                return
            tableName = tablenameKline(stockID)
            if not existTable(tableName):
                createKlineTable(stockID)
            writeSQL(df, tableName)
            return
    logging.error('fail download %s Kline data!', stockID)


def lirunFileToList(stockID, date):
    fileName = filenameLirun(stockID)
    lirunFile = open(fileName, 'r')
    lirunData = lirunFile.readlines()

    dateList = lirunData[0].split()
    logging.debug(repr(dateList))

    try:
        index = dateList.index(date)
        logging.debug(repr(index))
    except ValueError:
        return []

    profitsList = lirunData[42].split()
    if profitsList[0].decode('gbk') != u'归属于母公司所有者的净利润':
        logging.error('lirunFileToList read %s error', stockID)
        return []

    return {'stockid': stockID,
            'date': date,
            'profits': profitsList[index],
            'reportdate': dateList[index]
            }


def tablenameKline(stockID):
    return 'kline%s' % stockID


def createGubenTable():
    pass  # TODO: 创建总股本表


def createKlineTable(stockID):
    #     engine = getEngine()
    tableName = 'kline%s' % stockID
    sql = 'create table %s like klinesample' % tableName
    return engine.execute(sql)
#     engine.close()


def createStockList():
    """code,代码
    name,名称
    industry,所属行业
    area,地区
    pe,市盈率
    outstanding,流通股本
    totals,总股本(万)
    totalAssets,总资产(万)
    liquidAssets,流动资产
    fixedAssets,固定资产
    reserved,公积金
    reservedPerShare,每股公积金
    eps,每股收益
    bvps,每股净资
    pb,市净率
    timeToMarket,上市日期"""
#     engine = getEngine()
    metadata = MetaData()
    unusedTable = Table('stocklist', metadata,
                        Column('code', String(6), primary_key=True),
                        Column('name', String(10)),
                        Column('industry', String(10)),
                        Column('area', String(10)),
                        Column('pe', DECIMAL(precision=10, scale=3)),
                        Column('outstanding', DECIMAL(precision=20, scale=3)),
                        Column('totals', DECIMAL(precision=20, scale=3)),
                        Column('totalAssets', DECIMAL(precision=20, scale=3)),
                        Column('liquidAssets', DECIMAL(precision=20, scale=3)),
                        Column('fixedAssets', DECIMAL(precision=20, scale=3)),
                        Column('reserved', DECIMAL(precision=20, scale=3)),
                        Column(
                            'reservedPerShare',
                            DECIMAL(precision=10, scale=3)),
                        Column('esp', DECIMAL(precision=10, scale=3)),
                        Column('bvps', DECIMAL(precision=10, scale=3)),
                        Column('pb', DECIMAL(precision=10, scale=3)),
                        Column('timeToMarket', DATE)
                        )
    metadata.create_all(engine)
#     engine.close()


def getStockBasicsFromCSV():
    """
        获取沪深上市公司基本情况
    Return
    --------
    DataFrame
               code,代码
               name,名称
               industry,细分行业
               area,地区
               pe,市盈率
               outstanding,流通股本
               totals,总股本(万)
               totalAssets,总资产(万)
               liquidAssets,流动资产
               fixedAssets,固定资产
               reserved,公积金
               reservedPerShare,每股公积金
               eps,每股收益
               bvps,每股净资
               pb,市净率
               timeToMarket,上市日期
    """
    csvFile = open('all.csv')
#     df = pd.read_csv(csvFile.read(), dtype={'code': 'object'})
    text = csvFile.read()
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
#     df = pd.read_csv(csvFile, dtype={'code': 'object'})
    df = df.set_index('code')
    return df


def get_stock_basics():
    """
        获取沪深上市公司基本情况
    Return
    --------
    DataFrame
               code,代码
               name,名称
               industry,细分行业
               area,地区
               pe,市盈率
               outstanding,流通股本
               totals,总股本(万)
               totalAssets,总资产(万)
               liquidAssets,流动资产
               fixedAssets,固定资产
               reserved,公积金
               reservedPerShare,每股公积金
               eps,每股收益
               bvps,每股净资
               pb,市净率
               timeToMarket,上市日期
    """
    url = ct.ALL_STOCK_BASICS_FILE
#     print url
    req = getreq(url)
#     headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; '
#                               'Windows NT 6.1;'
#                               'en-US;rv:1.9.1.6) Gecko/20091201 '
#                               'Firefox/3.5.6')}
#     req = urllib2.Request(url, headers=headers)
#     proxy = urllib2.ProxyHandler({'http': '127.0.0.1:8087'})
#     opener = urllib2.build_opener(proxy)
#     urllib2.install_opener(opener)
    text = urllib2.urlopen(req, timeout=30).read()
#     text = urllib2.urlopen(req, timeout=20).read()
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
    df = df.set_index('code')
    return df


def updateStockList(retry=10):
    sl = pd.DataFrame()
    for _ in range(retry):
        try:
            #             sl = ts.get_stock_basics().fillna(value=0)
            sl = getStockBasicsFromCSV().fillna(value=0)
        except socket.timeout:
            logging.warning('updateStockList timeout!!!')
        else:
            logging.debug('updateStockList ok')
            break
    if sl.empty:
        logging.error('updateStockList fail!!!')
        return False
    sl.index.name = 'stockid'
#     print sl
#    engine = getEngine()
#     if not append:
#         clearStockList()
    clearStockList()
#    print sl.head()
#    writeSQL(sl, 'stocklist')
    sl.to_sql(u'stocklist',
              engine,
              if_exists=u'append')
#             dtype = {'date': sqlalchemy.types.DATE})
#    engine.close()


def dropTable(tableName):
    #     engine = getEngine()
    engine.execute('DROP TABLE %s' % tableName)
#     engine.close()


def getLowPEStockList(maxPE=40):
    """选取指定范围PE的股票
    maxPE: 最大PE
    """
#     engine = getEngine()
    sql = 'select stockid, pe from stocklist where pe > 0 and pe <= %s' % maxPE
    df = pd.read_sql(sql, engine)
#     engine.close()
    return df


def getAllTableName(tableName):
    #     engine = getEngine()
    result = engine.execute('show tables like %s', tableName)
    return result.fetchall()


def clearStockList():
    #     engine = getEngine()
    engine.execute('TRUNCATE TABLE stocklist')
#     engine.close()


def getKlineLastUpdateDate(stockID):
    tablename = tablenameKline(stockID)
    if not existTable(tablename):
        createKlineTable(stockID)
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')
#     engine = getEngine()
    sql = 'select max(date) from %s' % tablename
    return getLastUpdate(sql)


def getGubenLastUpdateDate(stockID):
    sql = 'select max(date) from guben where stockid="%s"' % stockID
    return getLastUpdate(sql)


def readStockListFromSQL():
    #     engine = getEngine()
    result = engine.execute('select stockid, name from stocklist')
#     engine.close()
#     result = engine.execute('select * from stocklisttest')
    stockIDList = []
    for i in result:
        stockIDList.append([i[0], i[1]])
        pass
#     print result.rowcount

#     print stockIDList
    return stockIDList


def readStockIDsFromSQL():
    result = engine.execute('select stockid from stocklist')
    return [i[0] for i in result.fetchall()]


def readStockListFromFile(filename):
    """ 从文件读取股票代码列表, 文件格式每行为一个股票代码
    Parameters
    --------
    filename: str 股票代码文件名  e.g: '.\test.txt'

    Return
    --------
    stockIDList: list 股票代码  e.g: ['600519', '000333']
    """

    stockFile = open(filename, 'rb')
    stockIDList = [i[:6] for i in stockFile.readlines()]
    stockFile.close()
    return stockIDList


def readStockListDf():
    stockList = readStockListFromSQL()
    stockIDs = []
    stockNames = []
    for i in stockList:
        stockIDs.append(i[0])
#         name = i[1].encode('utf-8').decode('utf-8')
        name = i[1].encode('gbk')
#         print repr(name)
        stockNames.append(name)
#         print repr(i[1])
    stockListDf = DataFrame({'stockid': stockIDs,
                             'name': stockNames})
    return stockListDf


# def readStockID(stockList):
#     return [i[0] for i in stockList]


# def getAllHist():
#     stockIDList = readStockID()
#     print type(stockIDList)
#     print stockIDList
#     count = len(stockIDList)
#     curNum = 1
#     for i in stockIDList:
#         print 'get stock data %d of %d, stockID is %s' % (curNum, count, i)
# #        df = getHist(i[0])
# #         writeSQL(i, df)
#         curNum += 1
#
#     print 'data update done.'


def updateKlineMarketValue(stockID, startDate='1990-01-01', endDate=None):
    gubenDf = readGuben(stockID, startDate)
    klineTablename = 'kline%s' % stockID
    gubenCount = gubenDf['date'].count()

#     engine = getEngine()
    for i in range(gubenCount):
        startDate = gubenDf['date'][i]
        try:
            endDate = gubenDf['date'][i + 1]
        except KeyError, e:
            endDate = None
#             print e
        totalShares = gubenDf['totalshares'][i]

        sql = ('update %(klineTablename)s '
               'set totalmarketvalue = close * %(totalShares)s'
               ' where date >= "%(startDate)s"' % locals())
        if endDate:
            sql += ' and date < "%s"' % endDate
        engine.execute(sql)
#     engine.close()


def updateKlineTTMLirun(stockID, startDate='1990-01-01', endDate='2099-12-31'):
    """
    a更新Kline表TTM利润
    """
    TTMLirunDf = readTTMLirunForStockID(stockID, startDate)
#     print TTMLirunDf
    klineTablename = 'kline%s' % stockID
    TTMLirunCount = TTMLirunDf['date'].count()

#     engine = getEngine()
    for i in range(TTMLirunCount):
        startDate = TTMLirunDf['reportdate'][i]
        try:
            endDate = TTMLirunDf['reportdate'][i + 1]
        except KeyError:
            endDate = None
#             print e
        TTMLirun = TTMLirunDf['ttmprofits'][i]

        sql = 'update %s set ttmprofits = %s' % (klineTablename, TTMLirun)
        sql += ' where date >= "%s"' % startDate
        if endDate:
            sql += ' and date < "%s"' % endDate
        engine.execute(sql)
#     engine.close()


def gubenDfToList(df):
    timea = dt.datetime.now()
    gubenList = []
    for date, row in df.iterrows():
        #         print date, row
        #         date = row['date']
        stockid = row['stockid']
        date = row['date']
        totalshares = row['totalshares']

        guben = {'stockid': stockid,
                 'date': date,
                 'totalshares': totalshares
                 }
        gubenList.append(guben)
    timeb = dt.datetime.now()
    logging.debug('klineDfToList took %s' % (timeb - timea))
    return gubenList


def writeLirun(df):
    tablename = 'lirun'
    return writeSQL(df, tablename)


def writeGuben(stockID, df):
    logging.debug('start writeGuben %s' % stockID)
    timea = dt.datetime.now()
#     print df
    gubenList = gubenDfToList(df)
#     print klineList
    if not gubenList:
        return False

#     engine = getEngine()

    tableName = 'guben'
    if not existTable(tableName):
        createGubenTable(stockID)

#     Session = scoped_session(sessionmaker(bind = engine, autoflush = False))
    session = Session()
    metadata = MetaData(bind=engine)
    mytable = Table(tableName, metadata, autoload=True)

    session.execute(mytable.insert().prefix_with('IGNORE'), gubenList)
    session.commit()
    session.close()
#     engine.close()

#     logging.info('end quickWriteSQLB %s' % stockID)
    timeb = dt.datetime.now()
    logging.debug('writeGuben stockID %s took %s' %
                  (stockID, (timeb - timea)))
    return True


def writeSQL(data, tableName, insertType='IGNORE'):
    """insetType: IGNORE 忽略重复主键；

    """
#     timea = dt.datetime.now()
    logging.debug('start writeSQL %s' % tableName)

#     engine = getEngine()

    if not existTable(tableName):
        logging.error('not exist %s' % tableName)
        return False

    if isinstance(data, DataFrame):
        if data.empty:
            # logging.info('write a empty DataFrame, return caller')
            return True
        data = transDfToList(data)

    if not data:
        return True

#     print 'write data:\n%s' % data
#     Session = scoped_session(sessionmaker(bind = engine, autoflush = False))
    session = Session()
    metadata = MetaData(bind=engine)
    mytable = Table(tableName, metadata, autoload=True)

#     print 'cccccccccc'
#     print type(data)
#     print data
    session.execute(mytable.insert().prefix_with(insertType), data)
    session.commit()
    session.close()
#     engine.close()

#     timeb = dt.datetime.now()
#     logging.info('writeSQL %s took %s', tableName, (timeb - timea))
    return True


def writeStockIDListToFile(stockIDList, filename):
    stockFile = open(filename, 'wb')
    stockFile.write('\n'.join(stockIDList))
    stockFile.close()


# def quickWriteSQL(stockID, df):
#     logging.info('start quickWriteSQL %s' % stockID)
#     Base = declarative_base()
#
#     class kline(Base):
#         __tablename__ = 'kline%s' % stockID
#         date = Column(DATE, primary_key=True)  # 日期
#         open = Column(DECIMAL(precision=10, scale=2))  # 开盘价
#         high = Column(DECIMAL(precision=10, scale=2))  # 最高价
#         close = Column(DECIMAL(precision=10, scale=2))  # 收盘价
#         low = Column(DECIMAL(precision=10, scale=2))  # 最低价
#         volume = Column(DECIMAL(precision=20, scale=2))  # 成交量
#         price_change = Column(DECIMAL(precision=10, scale=2))  # 价格变动
#         p_change = Column(DECIMAL(precision=10, scale=2))  # 涨跌幅
#         ma5 = Column(DECIMAL(precision=10, scale=3))  # 5日均价
#         ma10 = Column(DECIMAL(precision=10, scale=3))  # 10日均价
#         ma20 = Column(DECIMAL(precision=10, scale=3))  # 20日均价
#         v_ma5 = Column(DECIMAL(precision=20, scale=3))  # 5日均量
#         v_ma10 = Column(DECIMAL(precision=20, scale=3))  # 10日均量
#         v_ma20 = Column(DECIMAL(precision=20, scale=3))  # 20日均量
#         totals = Column(DECIMAL(precision=20, scale=3))  # 总股本
#         total_shares = Column(DECIMAL(precision=20, scale=3))  # 总市值
#         ttm_profit = Column(DECIMAL(precision=20, scale=3))  # TTM利润
#         ttm_pe = Column(DECIMAL(precision=10, scale=3))  # TTM市盈率
#         turnover = Column(DECIMAL(precision=10, scale=3))  # 换手率[注#指数无此项]
#
#         def __init__(self, date, open_, high, close, low, volume,
#                      price_change, p_change, ma5, ma10, ma20,
#                      v_ma5, v_ma10, v_ma20, turnover):
#             self.date = date
#             self.open = open_
#             self.high = high
#             self.close = close
#             self.low = low
#             self.volume = volume
#             self.price_change = price_change
#             self.p_change = p_change
#             self.ma5 = ma5
#             self.ma10 = ma10
#             self.ma20 = ma20
#             self.v_ma5 = v_ma5
#             self.v_ma10 = v_ma10
#             self.v_ma20 = v_ma20
#             self.turnover = turnover
#
#         def __repr__(self):
#             return '<kline (%s, %f, %f>' % (self.date, self.open, self.high)
#
#     timea = dt.datetime.now()
# #     engine = getEngine()
#     Base.metadata.create_all(engine)
# #    tableName = 'kline%s' % stockID
# #    Session = scoped_session(sessionmaker(bind = engine, autoflush = False))
#     session = Session()
#     try:
#         for date, row in df.iterrows():
#             query = session.query(kline)
#             a = query.get(date)
#             if a is None:  # 以主键获取，等效于上句
#                 (open_, high, close, low, volume,
#                  price_change, p_change, ma5, ma10, ma20,
#                  v_ma5, v_ma10, v_ma20, turnover) = row
#                 k = kline(date, open_, high, close, low, volume,
#                           price_change, p_change, ma5, ma10, ma20,
#                           v_ma5, v_ma10, v_ma20, turnover)
#                 session.add(k)
#         session.flush()
#         session.commit()
#         session.close()
#         print 'commit final'
#     except:
#         logging.error(sys.exc_info()[0])
#         raise
#     logging.info('end quickWriteSQL %s', stockID)
#     timeb = dt.datetime.now()
#     print timeb - timea
#     engine.close()

#
# def quickWriteSQLA(stockID, df):
#     #     return
#     logging.info('**start quickWriteSQLA %s' % stockID)
#     Base = declarative_base()
#
#     class kline(Base):
#         __tablename__ = 'kline%s' % stockID
#         date = Column(DATE, primary_key=True)  # 日期
#         open = Column(DECIMAL(precision=10, scale=2))  # 开盘价
#         high = Column(DECIMAL(precision=10, scale=2))  # 最高价
#         close = Column(DECIMAL(precision=10, scale=2))  # 收盘价
#         low = Column(DECIMAL(precision=10, scale=2))  # 最低价
#         volume = Column(DECIMAL(precision=20, scale=2))  # 成交量
# #         price_change = Column(DECIMAL(precision = 10, scale = 2))  # 价格变动
# #         p_change = Column(DECIMAL(precision = 10, scale = 2))  # 涨跌幅
# #         ma5 = Column(DECIMAL(precision = 10, scale = 3))  # 5日均价
# #         ma10 = Column(DECIMAL(precision = 10, scale = 3))  # 10日均价
# #         ma20 = Column(DECIMAL(precision = 10, scale = 3))  # 20日均价
# #         v_ma5 = Column(DECIMAL(precision = 20, scale = 3))  # 5日均量
# #         v_ma10 = Column(DECIMAL(precision = 20, scale = 3))  # 10日均量
# #         v_ma20 = Column(DECIMAL(precision = 20, scale = 3))  # 20日均量
# #         totals = Column(DECIMAL(precision = 20, scale = 3))  # 总股本
# #         total_shares = Column(DECIMAL(precision = 20, scale = 3))  # 总市值
# #         ttm_profit = Column(DECIMAL(precision = 20, scale = 3))  # TTM利润
# #         ttm_pe = Column(DECIMAL(precision = 10, scale = 3))  # TTM市盈率
#         turnover = Column(DECIMAL(precision = 10, scale = 3))  # 换手率[注#指数无此项]
#
#         def __init__(self, date, open_, high, close, low, volume):
#             self.date = date
#             self.open = open_
#             self.high = high
#             self.close = close
#             self.low = low
#             self.volume = volume
#
#         def __repr__(self):
#             return '<kline (%s, %f, %f>' % (self.date, self.open, self.high)
#
#     timea = dt.datetime.now()
# #     engine = getEngine()
#     Base.metadata.create_all(engine)
# #    tableName = 'kline%s' % stockID
# #    Session = scoped_session(sessionmaker(bind = engine, autoflush = False))
#     session = Session()
#     try:
#         for unusedIndex, row in df.iterrows():
#             #             print index, row
#             date = row['date']
#             open_ = row['open']
#             high = row['high']
#             close = row['close']
#             low = row['low']
#             volume = row['volume']
#             a = session.query(kline.date).filter(kline.date == date).all()
#             if not a:  #
#                 k = kline(date, open_, high, close, low, volume)
#                 session.add(k)
#         session.flush()
#         session.commit()
#         print 'commit final'
#     except:
#         logging.error(sys.exc_info()[0])
#         raise
#     logging.info('end quickWriteSQLA %s' % stockID)
#     timeb = dt.datetime.now()
#     print timeb - timea
# #     engine.close()


def downloadLirun(date):
    """
    # 获取业绩报表数据
    """
    return downloadLirunFromTushare(date)
#     return downloadLirunFromEastmoney(date)


def downloadLirunFromEastmoney(stockList, date):
    """
    a获取业绩报表数据,数据源为Eastmoney
    Parameters
    --------
    year:int 年度 e.g:2014
    quarter:int 季度 :1、2、3、4，只能输入这4个季度
    a说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度

    Return
    --------
    DataFrame
        code,代码
        net_profits,净利润(万元)
        report_date,发布日期
    """
    lirunList = []
    date = datatrans.transQuarterToDate(date).replace('-', '')
    for stockID in stockList:
        lirun = lirunFileToList(stockID, date)
        if lirun:
            lirunList.append(lirun)
    return DataFrame(lirunList)
#     return lirunList


def downloadLirunFromTushare(date):
    """
    a获取业绩报表数据,数据源为Tushare
    Parameters
    --------
    year:int 年度 e.g:2014
    quarter:int 季度 :1、2、3、4，只能输入这4个季度
    a说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度

    Return
    --------
    DataFrame
        code,代码
        net_profits,净利润(万元)
        report_date,发布日期
    """
    year = date / 10
    quarter = date % 10
    df = ts.get_report_data(year, quarter)
    if df is None:
        return None
    df = df.loc[:, ['code', 'net_profits', 'report_date']]
    return transLirunDf(df, year, quarter)


def getreq(url):
    #     headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; Windows NT 6.1; '
    #                               'en-US;rv:1.9.1.6) Gecko/20091201 '
    #                               'Firefox/3.5.6')}
    headers = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; WOW64; '
                              'rv:49.0) Gecko/20100101 Firefox/49.0'),
               'Accept': ('text/html,application/xhtml+xml,application/xml;'
                          'q=0.9,*/*;q=0.8'),
               'Accept-Encoding': 'gzip, deflate',
               'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
               'Connection': 'keep-alive',
               'DNT': '1',
               'Upgrade-Insecure-Requests': '1',
               }
    return urllib2.Request(url, headers=headers)


def gubenURLToDf(stockID):
    gubenURL = urlGuben(stockID)
    timeout = 6
    try:
        print u'开始下载数据。。。'
        socket.setdefaulttimeout(timeout)
#         sock = urllib.urlopen(gubenURL)
#         guben = sock.read()
        req = getreq(gubenURL)
        guben = urllib2.urlopen(req).read()
    except IOError, e:
        print e
        print u'数据下载失败： %s' % stockID
        return None
    else:
        #         sock.close()
        #     print guben
        return gubenDataToDf(stockID, guben)


def gubenFileToDf(stockID):
    filename = filenameGuben(stockID)
    try:
        gubenFile = open(filename, 'r')
        guben = gubenFile.read()
        gubenFile.close()
    except IOError, e:
        print e
        print u'读取总股本文件失败： %s' % stockID
        return False
    return gubenDataToDf(stockID, guben)


def gubenDataToDf(stockID, guben):
    gubenTree = etree.HTML(guben)
    gubenData = gubenTree.xpath('''//html//body//div//div
                                //div//div//table//tr//td
                                //table//tr//td//table//tr//td''')
    date = [gubenData[i][0].text for i in range(0, len(gubenData), 2)]
    date = [dt.datetime.strptime(d, '%Y-%m-%d') for d in date]
#     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
#     print totalshares
#     t = [i[:-2] for i in totalshares]
#     print t
    try:
        totalshares = [float(i[:-2]) * 10000 for i in totalshares]
    except ValueError, e:
        logging.error('stockID:%s, %s', stockID, e)
#     print totalshares
    gubenDf = DataFrame({'stockid': stockID,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf


# def downloadGuzhiToFiles(stockList):
#     for stockID in stockList:
#         downGuzhiToFile(stockID)


def downGuzhiToFile(stockID):
    url = urlGuzhi(stockID)
    filename = filenameGuzhi(stockID)
    logging.debug('write guzhi file: %s', filename)
    return downloadDataToFile(url, filename)


def filenameGuzhi(stockID):
    return '.\\data\\guzhi\\%s.xml' % stockID


def readGuzhiFileToDict(stockID):
    guzhiFilename = filenameGuzhi(stockID)
    guzhiFile = open(guzhiFilename)
    guzhiData = guzhiFile.read()
    guzhiFile.close()
    return transGuzhiDataToDict(guzhiData)


def transGuzhiDataToDict(guzhi):
    guzhiTree = etree.HTML(guzhi)
    xpathStr = '//html//body//div//tr'
    guzhiData = guzhiTree.xpath(xpathStr)
    guzhiDict = {}
    try:
        stockID = guzhiData[2][1].text.strip()  # 取得股票代码
    except IndexError:
        return None  # 无数据
    peg = guzhiData[2][3].text.strip()
    next1YearPE = guzhiData[2][6].text.strip()
    next2YearPE = guzhiData[2][7].text.strip()
    next3YearPE = guzhiData[2][8].text.strip()
#    print '|%s|' % peg
    if stockID != '--':
        guzhiDict['stockid'] = stockID  # 取得股票代码
    if peg != '--':
        guzhiDict['peg'] = float(peg.replace(',', ''))
    if next1YearPE != '--':
        guzhiDict['next1YearPE'] = float(next1YearPE.replace(',', ''))
    if next2YearPE != '--':
        guzhiDict['next2YearPE'] = float(next2YearPE.replace(',', ''))
    if next3YearPE != '--':
        guzhiDict['next3YearPE'] = float(next3YearPE.replace(',', ''))
    return guzhiDict


def readGuzhiFilesToDf(stockList):
    guzhiList = []
    for stockID in stockList:
        logging.debug('readGuzhiFilesToDf: %s', stockID)
        guzhidata = readGuzhiFileToDict(stockID)
        if guzhidata is not None:
            guzhiList.append(guzhidata)
    return DataFrame(guzhiList)


# def readQuartersTTMLirunInc(stockList):
#     """批量读取前6个季度TTM利润增长率
#     # stockList: 股票代码列表
#     # 返回： DataFrame,
#     """
#
#     for stockID in stockList:
#         readTTMLirunForDate(date)


def downloadKline(stockID, startDate=None, endDate=None):
    if startDate is None:  # startDate为空时取股票最后更新日期
        startDate = getKlineLastUpdateDate(stockID)
        startDate = startDate.strftime('%Y-%m-%d')
#     else:
#         startDate = '1990-01-01'
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
    return downloadKlineTuShare(stockID, startDate, endDate)


def downloadKlineTuShare(stockID,
                         startDate='1990-01-01', endDate='2099-12-31'):
    try:
        histDf = ts.get_hist_data(stockID, startDate, endDate)
    except IOError:
        return None
#     logging.info('downloadKlineTuShare final:%s, %s, %s',
#                  stockID, startDate, endDate)
    return histDf


# def downloadTTMLirun(date):
#     logging.info('start downloadTTMLirun: %d', date)
#     df = readLirunList(date)
#     ttmdf = calAllTTMLirun(df, date)
#     # TODO: 补充写TTMLirun
# #     writeTTMLirun(ttmdf)
#     logging.info('end downloadTTMLirun: %d', date)
#     return True


def readLirunList(date):
    #     engine = getEngine()
    sql = 'select * from lirun where `date` >= %s and `date` <= %s' % (
        str(date - 10), str(date))
#     sql = 'select * from lirun'
    df = pd.read_sql(sql, engine)
#     print df
#     print type(df)
#     engine.close()
    return df


def readTTMLirunForStockID(stockID,
                           startDate='1990-01-01', endDate=None):
    """取指定股票一段日间的TTM利润，startDate当日无数据时，取之前最近一次数据
    Parameters
    --------
    stockID: str 股票代码  e.g: '600519'
    startDate: str 起始日期  e.g: '1990-01-01'
    endDate: str 截止日期  e.g: '1990-01-01'

    Return
    --------
    DataFrame: 返回DataFrame格式TTM利润
    """
    #     engine = getEngine()
    # 指定日期（含）前最近一次股本变动日期
    sql = (u'select max(reportdate) from ttmlirun '
           u'where stockid="%(stockID)s" '
           u'and reportdate<="%(startDate)s"' % locals())
#     print sql

    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result is None:
        #         lastUpdate = None
        TTMLirunStartDate = startDate
    else:
        #         lastUpdate = result[0]
        TTMLirunStartDate = result[0]
#     if lastUpdate is None:
#         TTMLirunStartDate = startDate
#     else:
#         TTMLirunStartDate = lastUpdate
    sql = (u'select * from ttmlirun where stockid = "%(stockID)s"'
           u' and `reportdate` >= "%(TTMLirunStartDate)s"' % locals())
    if endDate:
        sql += u' and `date` <= "%s"' % endDate
#     print sql
    df = pd.read_sql(sql, engine)
#     print df

    # 指定日期（含）前存在股本变动数据的，重设第1次变动日期为startDate，
    # 减少更新Kline表中总市值所需计算量
#     if lastUpdate is not None:
    if TTMLirunStartDate != startDate:
        df.loc[df.reportdate == TTMLirunStartDate,
               u'reportdate'] = startDate
#     print df
#     engine.close()
    return df


def readTTMLirunForDate(date):
    """从TTMLirun表读取某季度股票TTM利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式TTM利润
    """
    #     engine = getEngine()
    sql = (u'select * from ttmlirun where '
           u'`date` = "%(date)s"' % locals())
#     print sql
    df = pd.read_sql(sql, engine)
#     engine.close()
    return df


def readLirunForDate(date):
    """从Lirun表读取一期股票利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式利润
    """
    #     engine = getEngine()
    sql = (u'select * from lirun where '
           u'`date` = "%(date)s"' % locals())
#     print sql
    df = pd.read_sql(sql, engine)
#     engine.close()
    return df


def readTTMPE(stockID):
    #     engine = getEngine()
    #     sql = 'select * from ttmpe%s' % stockID
    sql = 'select date, ttmpe from kline%s' % stockID
    df = pd.read_sql(sql, engine)
#     engine.close()
    return df


def readCurrentTTMPE(stockID):
    sql = ('select ttmpe from kline%(stockID)s '
           'where kline%(stockID)s.date=('
           'select max(`date`) from kline%(stockID)s'
           ')' % locals())
    result = engine.execute(sql)
    return result.fetchone()[0]


def readCurrentTTMPEs(stockList):
    #     engine = getEngine()
    #     sql = 'select * from ttmpe%s' % stockID
    idList = []
    peList = []
    for stockID in stockList:
        idList.append(stockID)
        peList.append(readCurrentTTMPE(stockID))

#     engine.close()
    return DataFrame({'stockid': stockList, 'pe': peList})


def alterKline():
    #     engine = getEngine()
    #    stockList = readStockID()
    #     for i in stockList:
    #     print tablename
    #         sql = 'show tables like "kline%s"' % i[0]
    sql = 'show tables like %s'
#         sql = ('SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES '
#                 'WHERE TABLE_SCHEMA="stockdata" '
#                 'and table_name like "kline%"')
#     tablename = tablenameKline(stockID)
#     sql = ('ALTER TABLE `stockdata`.`kline000001`'
#            'CHANGE COLUMN `volume` `volume` DOUBLE NULL DEFAULT NULL ,'
#            'CHANGE COLUMN `totalmarketvalue` `totalmarketvalue` '
#            'DOUBLE NULL DEFAULT NULL ,'
#            'CHANGE COLUMN `ttmprofits` `ttmprofits` '
#            'DOUBLE NULL DEFAULT NULL ;' % tablename)
#     print sql

    result = engine.execute(sql, 'kline%')
    result = result.fetchall()
#     engine.close()
#     print result

    for i in result:
        #         engine = getEngine()
        tablename = i[0]
#         print tablename
        sql = 'call stockdata.alterkline(%s)'
        try:
            result = engine.execute(sql, tablename)
#             result = result.fetchall()
            print tablename
        except sqlalchemy.exc.OperationalError, e:
            print e
    return
#     engine.close()


def calAllTTMLirun(date, incrementUpdate=True):
    """计算全部股票本期TTM利润并写入TTMLirun表
    date: 格式YYYYQ， 4位年+1位季度
    # 计算公式： TTM利润 = 本期利润 + 上年第四季度利润 - 上年同期利润
    # 计算原理：TTM利润为之前连续四个季度利润之和
    # 本期利润包含今年以来产生所有利润，上年第四季度利润 减上年同期利润为上年同期后一个季度至年末利润
    # 两者相加即为TTM利润
    # 举例：2016年1季度TTM利润 = 2016年1季度利润 + 2015年4季度利润  - 2015年1季度利润
    # 数据完整的情况下等同于：
    # 2015年2季度利润 + 2015年3季度利润 + 2015年4季度利润 + 2016年1季度利润
    # 当本期为第4季度时，计算公式仍有效， 如：
    # 2016年4季度TTM利润 = 2016年4季度利润 + 2015年4季度利润  - 2015年4季度利润
    #　但为提高效率，当本期为第4季度时，TTM利润=本期利润， 直接返回利润数据
    """
    lirunCur = readLirunForDate(date)
    if (date % 10) == 4:
        TTMLirun = lirunCur
        TTMLirun.columns = [['stockid', 'date',
                             'ttmprofits', 'reportdate']]
        return writeSQL(TTMLirun, 'ttmlirun')

    if incrementUpdate:
        TTMLirunCur = readTTMLirunForDate(date)
        lirunCur = lirunCur[~lirunCur.stockid.isin(TTMLirunCur.stockid)]
#     stockList = lirunCur['stockid']
    # 上年第四季度利润, 仅取利润字段并更名为profits1
    #
    lastYearEnd = (date / 10 - 1) * 10 + 4
    lirunLastYearEnd = readLirunForDate(lastYearEnd)
    print 'lirunLastYearEnd.head():', lirunLastYearEnd.head()
    lirunLastYearEnd = lirunLastYearEnd[['stockid', 'profits']]
    lirunLastYearEnd.columns = ['stockid', 'profits1']

    # 上年同期利润, 仅取利润字段并更名为profits2
    lastYearQuarter = date - 10
    lirunLastQarter = readLirunForDate(lastYearQuarter)
    lirunLastQarter = lirunLastQarter[['stockid', 'profits']]
    lirunLastQarter.columns = ['stockid', 'profits2']

    # 整合以上三个季度利润，stockid为整合键
    TTMLirun = pd.merge(lirunCur, lirunLastYearEnd, on='stockid')
    TTMLirun = pd.merge(TTMLirun, lirunLastQarter, on='stockid')

    TTMLirun['ttmprofits'] = (TTMLirun.profits +
                              TTMLirun.profits1 - TTMLirun.profits2)
    TTMLirun = TTMLirun[['stockid', 'date', 'ttmprofits', 'reportdate']]
    print 'TTMLirun.head():', TTMLirun.head()

    # 写入ttmlirun表后，重算TTM利润增长率
    writeSQL(TTMLirun, 'ttmlirun')
    return calTTMLirunIncRate(date)


def calTTMLirunIncRate(date, incrementUpdate=True):
    """计算全部股票本期TTM利润增长率并写入TTMLirun表
    date: 格式YYYYQ， 4位年+1位季度
    # 计算公式： TTM利润增长率= (本期TTM利润  - 上年同期TTM利润) / TTM利润 * 100
    """
    TTMLirunCur = readTTMLirunForDate(date)
    if incrementUpdate:
        TTMLirunCur = TTMLirunCur[TTMLirunCur.incrate.isnull()]
    TTMLirunLastYear = readTTMLirunForDate(date - 10)
#     print TTMLirunLastYear.head()
    TTMLirunLastYear = TTMLirunLastYear[['stockid', 'ttmprofits']]
    TTMLirunLastYear.columns = ['stockid', 'ttmprofits1']
#     print TTMLirunLastYear.head()

    # 整合以上2个表，stockid为整合键
    TTMLirunCur = pd.merge(TTMLirunCur, TTMLirunLastYear, on='stockid')

    TTMLirunCur['incrate'] = (TTMLirunCur.ttmprofits /
                              TTMLirunCur.ttmprofits1 * 100 - 100)
#     print TTMLirunCur.head()
#     TTMLirunCur = TTMLirunCur[['stockid', 'date', 'ttmprofits',
#                                'reportdate', 'incrate']]
#     print TTMLirunCur.head()
#     print TTMLirunLastYear.head()
    for i in TTMLirunCur.values:
        #         print type(i)
        #         print i
        stockID = i[0]
        incRate = round(i[4], 2)
#         print date, stockID, incRate
        sql = (u'update ttmlirun '
               u'set incrate = %(incRate)s'
               u' where stockid = "%(stockID)s"'
               u'and `date` = %(date)s' % locals())
#         sql = (u'update ttmlirun '
#                u'set incrate = null'
#                u' where stockid = "%(stockID)s"' % locals())
        engine.execute(sql)
    return


def calTTMLirun(stockdf, date):
    lirun1 = stockdf[stockdf.date == date - 10]  # 上年同期利润
    lirun2 = stockdf[stockdf.date == (date / 10 - 1) * 10 + 4]  # 上年末利润
    lirun3 = stockdf[stockdf.date == date]  # 本期利润
    if lirun1.empty or lirun2.empty or lirun3.empty:
        return None
    lirun1 = lirun1.iat[0, 2]
#     print '============================'
#     print stockdf
#     print '----------------'
#     print lirun2
#     print lirun2.isnull()
#     print '----------------'
#     print lirun3
    lirun2 = lirun2.iat[0, 2]
    stockID = lirun3.iat[0, 0]
#     print stockID
    reportdate = lirun3.iat[0, 3]
    lirun3 = lirun3.iat[0, 2]
#     print stockdf
#     print lirun1
#     print lirun2
#     print lirun3
#     print (date / 10 - 1) * 10 + 4
    # TTM利润 = 本期利润＋上年末利润-上年同期利润
    lirun = lirun3 + lirun2 - lirun1
    return [stockID, date, lirun, reportdate]


# def writeTTMLirun(df):
#     logging.info('start writeTTMLirun')
#     Base = declarative_base()
#
#     class ttmlirun(Base):
#         __tablename__ = 'ttmlirun'
#         __table_args__ = (Index('my_index', "stockid", "date"),)
#         id = Column(Integer, Sequence("user_id_seq"), primary_key=True)
#         stockid = Column(String(6), index=True)  # 股票代码
#         date = Column(Integer, index=True)  # 利润归属日期
#         ttmprofits = Column(FLOAT)  # 利润
#         reportdate = Column(DATE)  # 利润公告日期
#
#         def __init__(self, stockid, date, ttmprofits, reportdate):
#             self.stockid = stockid
#             self.date = date
#             self.ttmprofits = ttmprofits
#             self.reportdate = reportdate
#
#         def __repr__(self):
#             return '<lirun (%s, %s, %d>' % (self.stockid,
#                                             self.date, self.profits)
#
#     timea = dt.datetime.now()
# #     engine = getEngine()
#     Base.metadata.create_all(engine)
# #    tableName = 'ttmlirun'
# #     Session = scoped_session(sessionmaker(bind = engine,
#                                 autoflush = False))
#     session = Session()
#     try:
#         for unusedIndex, row in df.iterrows():
#             #             print '========================='
#             #             print index
#             #             print '-------------'
#             #             print row
#             #             date, reportdate, stockID, ttmprofits = row
#             stockID = row['stockid']
#             date = row['date']
#             ttmprofits = row['ttmprofits']
#             reportdate = row['reportdate']
# #             print stockID, date, ttmprofits, reportdate
# #             query = session.query(guben)
#             a = session.query(ttmlirun).filter(ttmlirun.stockid == stockID)\
#                 .filter(ttmlirun.date == date).all()
# #             print 'fdskafjklsdjafl'
# #             print a
# #             a = query.get(date)
#             if not a:  #
#                 #
#                 k = ttmlirun(stockID, date, ttmprofits, reportdate)
#                 session.add(k)
#         session.flush()
#         session.commit()
#         session.close()
#     except:
#         logging.error(sys.exc_info()[0])
#         raise
#     logging.info('end writeTTMLirun')
#     timeb = dt.datetime.now()
# #     engine.close()
#     print timeb - timea


def updateKlineEXTData(stockID, startDate=None):
    """
    # 更新Kline表MarketValue、TTMPE等附加数据
    Parameters
    --------
    stockID: str 股票代码  e.g:600519
    startDate: str 更新起始日期  e.g: 2016-01-01

    Return
    --------
    # 无返回值
    """
    logging.debug('updateKlineEXTData: %s', stockID)
    if startDate is None:
        startDate = getTTMPELastUpdate(stockID)
        startDate = startDate.strftime('%Y-%m-%d')
    updateKlineMarketValue(stockID, startDate)
#     updateKlineTTMLirun(stockID, startDate)
#     endDate = updateKlineTTMPE(stockID, startDate)
#     setKlineTTMPELastUpdate(stockID, endDate)


def getTTMPELastUpdate(stockID):
    """TTMPE最近更新日期，
    Parameters
    --------
    stockID:str 股票代码 e.g:600519

    Return
    --------
    datetime：TTMPE最近更新日期
    """
    sql = (u'select ttmpe from lastupdate '
           u'where stockid="%(stockID)s"' % locals())
#     print sql
    return getLastUpdate(sql)


def getLirunUpdateStartQuarter():
    """根据利润表数据判断本次利润表更新的起始日期，
    Parameters
    --------


    Return
    --------
    startQuarter：YYYYQ 起始更新日期
    """
    sql = (u'select min(maxdate) from (SELECT stockid, max(date) as maxdate '
           u'FROM stockdata.lirun group by stockid) as temp;')
#     print sql
    result = engine.execute(sql)
    lastQuarter = result.first()[0]
    startQuarter = datatrans.quarterAdd(lastQuarter, 1)
    return startQuarter


def getLirunUpdateEndQuarter():
    #     today = dt.datetime.now()
    curQuarter = datatrans.transDateToQuarter(dt.datetime.now())
    return datatrans.quarterSub(curQuarter, 1)


def getLastUpdate(sql):
    """TTMPE最近更新日期，
    Parameters
    --------
    sql: str  指定查询更新日期的SQL语句
    e.g: 'select ttmpe from lastupdate where stockid="002796"'

    Return
    --------
    datetime：datetime
    """
    lastUpdateDate = engine.execute(sql).first()
#     print type(lastUpdateDate)
    if lastUpdateDate is None:
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')

    lastUpdateDate = lastUpdateDate[0]
#     print type(lastUpdateDate)
    if isinstance(lastUpdateDate, dt.date):
        return lastUpdateDate + dt.timedelta(days=1)
    else:
        logging.debug('lastUpdateDate is: ', type(lastUpdateDate))
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')
#     try:
#         lastUpdateDate = result.first()[0] + dt.timedelta(days=1)
#     except TypeError, e:
#         logging.warning('getLastUpdate: %s', sql)
#         logging.warning(e)
#         return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')
# #     lastUpdatDate = lastUpdatDate.strftime('%Y-%m-%d')
# #     logging.info('getLastUpdate date: %s', lastUpdatDate)
#     return lastUpdateDate


def setKlineTTMPELastUpdate(stockID, endDate):
    sql = ('insert into lastupdate (`stockid`, `ttmpe`) '
           'values ("%(stockID)s", "%(endDate)s") '
           'on duplicate key update `ttmpe`="%(endDate)s";' % locals())
    result = engine.execute(sql)
    return result


# def updateTTMPEA(stockID):
#     tablename = 'ttmpe%s' % stockID
#     if not existTable(tablename):
#         createTTMPETable(tablename)
#     closeDf = readClose(stockID)
# #     print closeDf.head()
#     ttmLirunDf = readTTMLirun(stockID)
#     gubenDf = readGuben(stockID)
#
# #     print ttmLirunDf
# #     print gubenDf
#     dateList = []
#     peList = []
#     engine = getEngine()
#     for unusedIndex, row in closeDf.iterrows():
#         date = row['date']
#         close = row['close']
# #         print  date, close
#         sql = 'select * from %s where `date` = "%s"' % (tablename, date)
#         result = engine.execute(sql)
#         if result.rowcount != 0:
#             continue
#         guben = readGubenFromDf(gubenDf, date)
#         if guben is None:
#             continue
# #         print date, guben
#         ttmLirun = readTTMLirunFromDf(ttmLirunDf, date)
#         if ttmLirun is None:
#             continue
#         dateList.append(date)
#         peList.append(close * guben / 10000 / ttmLirun)
# #         print date, ttmLirun
#     TTMPEDf = pd.DataFrame({'date': dateList,
#                             'ttmpe': peList})
#     writeTTMPE(stockID, TTMPEDf)
#     engine.close()


def updateKlineTTMPE(stockID, startDate='1990-01-01', endDate=None):
    """
    # 更新Kline表TTMPE
    """
    #     engine = getEngine()
    klineTablename = 'kline%s' % stockID
    sql = ('update %(klineTablename)s '
           'set ttmpe = totalmarketvalue / ttmprofits'
           ' where date >= "%(startDate)s"' % locals())
    if endDate:
        sql += ' and date < "%s"' % endDate
    sql += ' and totalmarketvalue is not null'
    sql += ' and ttmprofits is not null'
    unusedResult = engine.execute(sql)
    sql = 'select max(date) from %(klineTablename)s' % locals()
    result = engine.execute(sql)
    endDate = result.fetchone()[0]
    if endDate is not None:
        endDate = endDate.strftime('%Y-%m-%d')
    else:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
    return endDate
#     print result.fetchall()
#     engine.close()


def urlMainTable(stockID, tableType):
    url = ('http://money.finance.sina.com.cn/corp/go.php'
           '/vDOWN_%(tableType)s/displaytype/4'
           '/stockid/%(stockID)s/ctrl/all.phtml' % locals())
    return url


def filenameMainTable(stockID, tableType):
    filename = '.\\data\\%s\\%s.csv' % (tableType, stockID)
    return filename


def downloadMainTable(stockID):
    mainTableType = ['BalanceSheet', 'ProfitStatement', 'CashFlow']
    for tableType in mainTableType:
        url = urlMainTable(stockID, tableType)
        filename = filenameMainTable(stockID, tableType)
#         print url
        logging.debug('downloadMainTable %s, %s', stockID, tableType)
        result = downloadDataToFile(url, filename)
        if not result:
            logging.error('download fail: %s', url)
            continue
    return result


def downloadGuben(stockID):
    url = urlGuben(stockID)
    filename = filenameGuben(stockID)
    return downloadDataToFile(url, filename)


def urlGuben(stockID):
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/stockid'
            '/%s/stocktype/TotalStock.phtml' % stockID)


def filenameGuben(stockID):
    return '.\\data\\guben\\%s.csv' % stockID


def filenameLirun(stockID):
    filename = '.\\data\\ProfitStatement\\%s.csv' % stockID
    return filename


def urlGuzhi(stockID):
    url = 'http://f9.eastmoney.com/soft/gp72.php?code=%s' % stockID
    if stockID[0] == '6':
        url += '01'
    else:
        url += '02'
    return url


def downloadData(url, timeout=10, maxRetry=10):
    #     tryCount = 0
    for _ in range(maxRetry):
        #         tryCount += 1
        try:
            #             print u'开始下载数据。。。'
            socket.setdefaulttimeout(timeout)
    #         sock = urllib.urlopen(url)
    #         data = sock.read()
            headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; '
                                      'Windows NT 6.1;'
                                      'en-US;rv:1.9.1.6) Gecko/20091201 '
                                      'Firefox/3.5.6')}
            req = urllib2.Request(url, headers=headers)
            content = urllib2.urlopen(req).read()
        except IOError, e:
            logging.warning('[%s]fail to download data, retry url:%s',
                            e, url)
        else:
            return content
    logging.error('download data fail!!! url:%s', url)
    return False


def downloadDataToFile(url, filename, timeout=10, maxRetry=10):
    data = downloadData(url, timeout, maxRetry)
    if not data:
        return False
#     print u'开始写入数据文件'
    try:
        mainFile = open(filename, 'wb')
        mainFile.write(data)
    except IOError, e:
        print e
        print u'写文件失败： %s' % filename
        return False
    else:
        mainFile.close()
#     print u'写入文件成功：%s' % filename
    return True


# def writeTTMPE(stockID, df):
#     logging.info('start writeTTMPE %s', stockID)
#     tableName = 'ttmpe%s' % stockID
#     Base = declarative_base()
#
#     class ttmpeTable(Base):
#         __tablename__ = tableName
#         __table_args__ = (Index('my_index', 'date'),)
#         id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
#         date = Column(DATE, index=True)  # 变动日期
#         ttmpe = Column(FLOAT)  # TTM市盈率
#
#         def __init__(self, date, ttmpe):
#             self.date = date
#             self.ttmpe = ttmpe
#
#         def __repr__(self):
#             return '<ttmpe (%s, %f>' % (self.date, self.ttmpe)
#
#     timea = dt.datetime.now()
# #     engine = getEngine()
#     Base.metadata.create_all(engine)
#     Session = scoped_session(sessionmaker(bind = engine, autoflush = False))
#     session = Session()
#     try:
#         for unusedIndex, row in df.iterrows():
#             #             print '========================='
#             #             print index
#             #             print '-------------'
#             #             print row
#             date = row['date']
#             ttmpe = row['ttmpe']
# #             print date, totalshares
# #             query = session.query(guben)
#             a = session.query(ttmpeTable)
#             a = a.filter(ttmpeTable.date == date).all()
# #             print 'fdskafjklsdjafl'
# #             print a
# #             a = query.get(date)
#             if not a:  #
#                 #
#                 k = ttmpeTable(date, ttmpe)
#                 session.add(k)
#         session.flush()
#         session.commit()
#     except:
#         logging.error(sys.exc_info()[0])
#         raise
#     logging.info('end writeGuben %s' % stockID)
#     timeb = dt.datetime.now()
#     print timeb - timea
#     engine.close()


def readGubenFromDf(df, date):
    """从股本列表中读取指定日期的股本数值
    # 指定日期无股本数据时，选取之前最近的股本数据
    """
    d = df[df.date <= date]
    if d.empty:
        return None
    guben = d[d.date == d['date'].max(), 'totalshares']
#     guben = d.iat[0, 3]
    return guben


def readTTMLirunFromDf(df, date):
    """从TTM利润列表中读取指定日期的TTM利润数值
    # 指定日期无TTM利润数据时，选取之前最近的TTM利润数据
    """
    d = df[df.reportdate <= date]
    if d.empty:
        return None
    d = d[d.reportdate == d['reportdate'].max()]
    ttmLirun = d[d.date == d['date'].max(), 'ttmprofits']
#     ttmLirun = d.iat[0, 3]
    return ttmLirun


def readGuben(stockID, startDate='1990-01-01', endDate=None):
    """取指定股票一段日间的股本变动数据，startDate当日无数据时，取之前最近一次变动数据
    Parameters
    --------
    stockID: str 股票代码  e.g: '600519'
    startDate: str 起始日期  e.g: '1990-01-01'
    endDate: str 截止日期  e.g: '1990-01-01'

    Return
    --------
    DataFrame
        date: 股本变动日期
        totalshares: 变动后总股本
    """
    # engine = getEngine()

    # 指定日期（含）前最近一次股本变动日期
    sql = (u'select max(date) from guben '
           u'where stockid="%(stockID)s" '
           u'and date<="%(startDate)s"' % locals())
#     print sql
    result = engine.execute(sql)
    lastUpdate = result.fetchone()[0]

    # 指定日期（含）前无股本变动数据的，查询起始日期设定为startDate
    # 否则设定为最近一次变动日期
    if lastUpdate is None:
        #         gubenStartDate = dt.datetime.strftime(startDate, '%Y-%m-%d')
        gubenStartDate = startDate
    else:
        gubenStartDate = lastUpdate

    sql = (u'select date, totalshares from guben '
           u'where stockid = "%(stockID)s"'
           u' and `date` >= "%(gubenStartDate)s"' % locals())
    if endDate:
        sql += u' and `date` <= "%s"' % endDate
#     print sql
    df = pd.read_sql(sql, engine)
#     print df
#     df['date'][df.date == gubenstartDate] = startDate
#     df.iat[0, 0] = startDate

    # 指定日期（含）前存在股本变动数据的，重设第1次变动日期为startDate，
    # 减少更新Kline表中总市值所需计算量
    if lastUpdate is not None:
        df.loc[df.date == gubenStartDate, 'date'] = startDate
#     print df
#     engine.close()
    return df


def readClose(stockID):
    #     engine = getEngine()
    sql = 'select date, close from kline%s' % stockID
#     sql = 'select * from lirun'
    df = pd.read_sql(sql, engine)
#     engine.close()
#     print df
#     print type(df)
    return df


def existTable(tablename):
    #     engine = getEngine()
    #     print tablename
    sql = 'show tables like "%s"' % tablename
#     print sql
    result = engine.execute(sql)
#     engine.close()
    return False if result.rowcount == 0 else True


def createTTMPETable(tablename):
    #     engine = getEngine()
    sql = 'create table %s like ttmpesimple' % tablename
    result = engine.execute(sql)
#     engine.close()
    return result


def transLirunDf(df, year, quarter):
    date = [year * 10 + quarter for unusedi in range(df['code'].count())]
    stockid = df['code']
    profits = df['net_profits']
    if quarter == 4:
        year += 1
    reportdate = df['report_date'].apply(lambda x: str(year) + '-' + x)
#     print reportdate
#     reportdate = [dt.datetime.strptime(d, '%Y-%m-%d') for d in reportdate]
    rd = []
    for d in reportdate:
        #         print d
        if d[-5:] == '02-29':
            d = d[:-5] + '02-28'
        dd = dt.datetime.strptime(d, '%Y-%m-%d')
        rd.append(dd)
    data = {'stockid': stockid,
            'date': date,
            'profits': profits * 10000,
            'reportdate': rd}
    df = pd.DataFrame(data)
    print 'transLirunDf, len(df):%s' % len(df)
    df = df.drop_duplicates()
#     print 'dfa:%s' % len(dfa)
#     df = df.dropna()
    #     dateSeries = pd.Series({[date for i in range(df['code'].count())]})
    return df


def getStockIDsForClassified(classified):
    #     engine = getEngine()
    sql = ('select stockid from classified '
           'where cname = "%(classified)s"' % locals())
    result = engine.execute(sql)
#     engine.close()
#     result = engine.execute('select * from stocklisttest')
    stockIDList = [classifiedID[0] for classifiedID in result.fetchall()]
    return stockIDList


def downloadClassified():
    """下载行业分类"""
    classifiedDf = ts.get_industry_classified(standard='sw')
    classifiedDf = classifiedDf[['code', 'c_name']]
    classifiedDf.columns = ['stockid', 'cname']
    return classifiedDf


def classifiedToSQL(classifiedDf):
    tablename = 'classified'
#     classifiedList = transDfToList(classifiedDf)
    return writeSQL(classifiedDf, tablename)


def getClassifiedForStocksID(stockID):
    #     engine = getEngine()
    sql = ('select cname from classified '
           'where stockid = "%(stockID)s"' % stockID)
    result = engine.execute(sql)
#     engine.close()
    classified = result.first()[0]
    return classified


def transDfToList(df):
    #    return [v for unusedk, v in df.to_dict('records').iteritems()]
    #    return [v for v in df.to_dict('records')]
    outList = []
    for index, row in df.iterrows():
        tmpDict = row.to_dict()
        tmpDict[df.index.name] = index
        outList.append(tmpDict)
    return outList


def readStockName(stockID):
    sql = 'select name from stocklist where stockid=%s' % stockID
    result = engine.execute(sql)
    return result.first()[0]


if __name__ == '__main__':
    logfilename = os.path.join(os.path.abspath(os.curdir), 'stockanalyse.log')
    formatStr = ('%(asctime)s %(filename)s[line:%(lineno)d] '
                 '%(levelname)s %(message)s')
    logging.basicConfig(level=logging.DEBUG,
                        format=formatStr,
                        # datefmt = '%Y-%m-%d %H:%M:%S +0000',
                        filename=logfilename,
                        filemode='a')

    ##########################################################################
    # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，并将其添加到当前的日志处理对象#
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    ##########################################################################

    logging.info('===================start=====================')
    timec = dt.datetime.now()
#     timea = dt.datetime.now()
    testStockID = u'600519'
    testStockList = ['000001', '600519', '000651']
# #     engine = getEngine()
    startDate = u'2016-04-30'
    endDate = u'2016-04-29'
#      df = getHist(stockID, start, end)
#     klineDf = downloadKlineWorker(testStockID, startDate, endDate)
#     print klineDf

#
# 下载雅虎数据
#     df = getHist(stockID)
#     timea = dt.datetime.now()
#     df = yahoodata.getKline(stockID)
#     timeb = dt.datetime.now()
#     logging.info('yahoodata.getKline took %s' % (timeb - timea))
#     print df
#     quickWriteSQLB(stockID, df)

# 重定义Kline表结构
#     alterKline()

# 更新Kline表总市值
#     result = updateKlineMarketValue(testStockID, startDate)
#     print result

# 更新Kline表TTM利润
#     updateKlineTTMLirun(stockID)

# 更新TTM利润增长率
#     for date in dateList(20131, 20161):
#         print date
#         calTTMLirunIncRate(date)

# 更新Kline表TTMPE
#     updateKlineEXTData(testStockID)
#     updateKlineEXTData(testStockID, '1990-01-01')
#     result = updateKlineTTMPE(testStockID, startDate)
#     print result

# 更新利润表
#     print 'ffffffffffffffff'
#     dataList = lirunFileToList(stockID)
#     if dataList:
#         tableName = 'lirun'
#         writeSQL(dataList, tableName)
#     date = 20154
#     df = downloadLirunFromTushare(date)


# 更新单个股票股本信息
#     df = eastmoneydata.getGuben(stockID)
#     writeGuben(stockID, df)
#     quickWriteSQL(stockID, df)
#     downloadGuben(stockID)
#     gubenDf = gubenFileToDf(testStockID)
#     gubenDf = gubenURLToDf(stockID)
#     print gubenDf
#     writeGuben(stockID, gubenDf)

# 下载指定季度利润信息（tushare模块试验）
#     df = ts.get_report_data(2014, 3)
#     for idx, row in df.iterrows():
# #         print idx
# #         print '--------'
# #         print row[0]
#         if row['code'] == '600519':
#             print row['code'], row['net_profits']
#         pass

#     lirun = downloadLirun(20151)
#     print lirun

# 更新指定季度利润信息
#     year = 2009
#     quarter = 4
#     df = getLirun(year, quarter)
#     print df.head(5)
#     writeLirun(df)

# 更新TTM利润信息
#     date = 20161
#     result = downloadTTMLirun(date)
#     for date in datatrans.dateList(20143, 20153):
#         print date
#         result = calAllTTMLirun(date)
#     print result

# 更新TTM市盈率
#     updateKlineEXTData(testStockID, startDate)
#     updateKlineEXTData(testStockID)

# 更新股票列表
#     createStockList()
#     updateStockList()

#     url = 'http://218.244.146.57/static/all.csv'
#     filename = './stocklisttest.csv'
#     downloadDataToFile(url, filename)
    df = getStockBasicsFromCSV()
    print df.head(5)

# 删除所有表
#     engine = getEngine()
#     tableNames = getAllTableName(engine)
#     print tableNames
#     for i in tableNames:
#         print i
#         dropTable(engine, i[0])

# 读股本信息
#     df = readGuben(testStockID, startDate)
#     print df

# 读取TTM利润信息
#     df = readTTMLirunForStockID(testStockID, startDate)
#     print df
#     pe = readCurrentTTMPE(testStockID)
#     print pe

#     pe = readCurrentTTMPEs(testStockList)
#     print pe

# 取股票最后更新日期
#     getKlineLastUpdateDate(stockID)

# 行业分类
# standard: sina 新浪行业， sw 申万行业
#     t = downloadClassified()
#     print t

    timed = dt.datetime.now()
    logging.info('datamanage test took %s' % (timed - timec))
    logging.info('===================end=====================')
#     print timeb - timea
#     print timea
#     print timec
#     print timed
