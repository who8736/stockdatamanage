# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 21:11:51 2016

@author: who8736
"""

import os
import logging
# import urllib2
# import socket
import datetime as dt
from datetime import datetime
# import ConfigParser
# import re
# import time

# import sys


# from lxml import etree
# import lxml
# import tushare as ts  # @UnresolvedImport
import pandas as pd
from pandas.compat import StringIO
# from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import DATE, DECIMAL, String
# from sqlalchemy.orm import sessionmaker, scoped_session
# import sqlalchemy
from pandas.core.frame import DataFrame
# from tushare.stock import cons as ct
import xlrd
import tushare as ts

import datatrans
import initsql
from sqlconn import SQLConn
# from misc import filenameGuben, filenameLirun, filenameGuzhi
from misc import filenameLirun, filenameGuzhi
from initlog import initlog

# from download import downHYFile

# from bokeh.sampledata import stocks


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session


def writeHYToSQL(filename):
    """ 从文件中读取行业与股票对应关系并写入数据库
    """
    filename = os.path.join('./data', filename)
    xlsFile = xlrd.open_workbook(filename, encoding_override="cp1252")
    table = xlsFile.sheets()[4]
    stockIDList = table.col_values(0)[1:]
    hyIDList = table.col_values(8)[1:]
    hyDf = pd.DataFrame({'stockid': stockIDList, 'hyid': hyIDList})
    engine.execute('TRUNCATE TABLE hangyestock')
    writeSQL(hyDf, 'hangyestock')


def writeHYNameToSQL(filename):
    filename = os.path.join('./data', filename)
    xlsFile = xlrd.open_workbook(filename, encoding_override="cp1252")
    table = xlsFile.sheets()[0]
    hyIDList = table.col_values(0)[1:]
    hyNameList = table.col_values(1)[1:]
    hyLevelList = [len(hyID) / 2 for hyID in hyIDList]
    hyLevel1IDList = [hyID[:2] for hyID in hyIDList]
    hyLevel2IDList = [hyID[:4] for hyID in hyIDList]
    hyLevel3IDList = [hyID[:6] for hyID in hyIDList]
    hyNameDf = pd.DataFrame({'hyid': hyIDList,
                             'hyname': hyNameList,
                             'hylevel': hyLevelList,
                             'hylevel1id': hyLevel1IDList,
                             'hylevel2id': hyLevel2IDList,
                             'hylevel3id': hyLevel3IDList})
    engine.execute('TRUNCATE TABLE hangyename')
    writeSQL(hyNameDf, 'hangyename')


def writeGubenToSQL(gubenDf, replace=False):
    """单个股票股本数据写入数据库
    :type gubenDf: DataFrame
    """
    tablename = 'guben'
    # lastUpdate = gubenUpdateDate(stockID)
    # gubenDf = gubenDf[pd.Timestamp(gubenDf.date) > lastUpdate]
    # gubenDf = gubenDf[gubenDf.date > lastUpdate]
    if gubenDf.empty:
        return None
    if replace:
        for index, row in gubenDf.iterrows():
            sql = (('replace into guben'
                    '(stockid, date, totalshares) '
                    'values("%s", "%s", %s)')
                   % (row['stockid'], row['date'], row['totalshares']))
            engine.execute(sql)
    else:
        return writeSQL(gubenDf, tablename)


def checkGuben(tradeDate):
    """
        以下方法用于从tushare.pro下载日频信息中的股本数据
        与数据库保存的股本数据比较，某股票的总股本存在差异时说明股本有变动
        返回需更新的股票列表
    """
    pro = ts.pro_api()
    dfFromTushare = pro.daily_basic(ts_code='', trade_date=tradeDate,
                                    fields='ts_code,total_share')
    dfFromTushare['stockid'] = dfFromTushare['ts_code'].str[:6]

    sql = """ select a.stockid, a.date, a.totalshares from guben as a, 
            (SELECT stockid, max(date) as maxdate FROM stockdata.guben 
            group by stockid) as b 
            where a.stockid=b.stockid and a.date = b.maxdate;
            """
    dfFromSQL = pd.read_sql(sql, con=engine)
    df = pd.merge(dfFromTushare, dfFromSQL, how='left', on='stockid')
    df.loc[0:, 'cha'] = df.apply(
        lambda x: abs(x['total_share'] * 10000 - x['totalshares']) / (
                x['total_share'] * 10000), axis=1)

    chaRate = 0.0001
    dfUpdate = df[df.cha >= chaRate]

    # 对于无需更新股本的股票，将其更新日期修改为上一交易日
    dfFinished = df[df.cha < chaRate]
    date = '%s-%s-%s' % (tradeDate[:4], tradeDate[4:6], tradeDate[6:])
    for stockID in dfFinished['stockid']:
        setGubenLastUpdate(stockID, date)
    # 返回需更新股本的股票
    return dfUpdate


def writeGuzhiToSQL(stockID, data):
    """下载单个股票估值数据写入数据库"""
    guzhiDict = datatrans.transGuzhiDataToDict(data)
    if guzhiDict is None:
        return True
    # print guzhiDict
    #     guzhiDf = DataFrame(guzhiDict, index=[0])
    #     writeSQLUpdate(guzhiDict, 'guzhi')
    #     print guzhiDict
    tablename = 'guzhi'
    if 'peg' in list(guzhiDict.keys()):
        peg = guzhiDict['peg']
    else:
        peg = 'null'
    if 'next1YearPE' in list(guzhiDict.keys()):
        next1YearPE = guzhiDict['next1YearPE']
    else:
        next1YearPE = 'null'
    if 'next2YearPE' in list(guzhiDict.keys()):
        next2YearPE = guzhiDict['next2YearPE']
    else:
        next2YearPE = 'null'
    if 'next3YearPE' in list(guzhiDict.keys()):
        next3YearPE = guzhiDict['next3YearPE']
    else:
        next3YearPE = 'null'
    sql = (('replace into %(tablename)s'
            '(stockid, peg, next1YearPE, next2YearPE, next3YearPE) '
            'values("%(stockID)s", %(peg)s, '
            '%(next1YearPE)s, %(next2YearPE)s, '
            '%(next3YearPE)s);') % locals())
    return engine.execute(sql)


# def writeKline(stockID, df, insertType='IGNORE'):
#     """股票K线历史写入数据库"""
#     tableName = tablenameKline(stockID)
#     if not initsql.existTable(tableName):
#         initsql.createKlineTable(stockID)
#     return writeSQL(df, tableName, insertType)


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
    # if profitsList[0].decode('gbk') != '归属于母公司所有者的净利润':
    if profitsList[0] != '归属于母公司所有者的净利润':
        logging.error('lirunFileToList read %s error', stockID)
        return []

    return {'stockid': stockID,
            'date': date,
            'profits': profitsList[index],
            'reportdate': dateList[index]
            }


# def tablenameKline(stockID):
#     return 'kline%s' % stockID


def loadChigu():
    sql = 'select stockid from chigu;'
    result = engine.execute(sql)
    return [i[0] for i in result.fetchall()]


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
    metadata = MetaData()
    _ = Table('stocklist', metadata,
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
    text = csvFile.read()
    # text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
    df = df.set_index('code')
    return df


def dropTable(tableName):
    engine.execute('DROP TABLE %s' % tableName)


def dropNAData():
    """ 清除K线图数据中交易量为0的数据
    """
    # stockList = readStockIDsFromSQL()
    #     stockList = ['002100']
    # for stockID in stockList:
    # tablename = tablenameKline(stockID)
    # sql = 'delete from %(tablename)s where volume=0;' % locals()
    sql = 'delete from klinestock where volume=0;'
    engine.execute(sql)


def getLowPEStockList(maxPE=40):
    """选取指定范围PE的股票
    maxPE: 最大PE
    """
    sql = 'select stockid, pe from stocklist where pe > 0 and pe <= %s' % maxPE
    df = pd.read_sql(sql, engine)
    return df


def getAllTableName(tableName):
    result = engine.execute('show tables like %s', tableName)
    return result.fetchall()


def clearStockList():
    if initsql.existTable('stocklist'):
        engine.execute('TRUNCATE TABLE stocklist')


def getAllMarketPEUpdateDate():
    """
    全市场PE更新日期
    :return:
    """
    sql = 'select max(date) from pehistory where id="all";'
    return _getLastUpdate(sql)


def getStockKlineUpdateDate():
    """
    股票更新日期
    :return:
    """
    sql = 'select max(date) from klinestock;'
    return _getLastUpdate(sql)


def getIndexKlineUpdateDate():
    """
    指数更新日期
    :return:
    """
    sql = 'select max(date) from klineindex;'
    return _getLastUpdate(sql)


def __klineUpdateDate():
    """
    待删除函数, kline改为分区表后无需按股票代码查询更新日期
    :return:
    """
    pass
    # tablename = tablenameKline(stockID)
    # if not initsql.existTable(tablename):
    #     initsql.createKlineTable(stockID)
    #     return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')
    # sql = 'select max(date) from %s' % tablename
    # return _getLastUpdate(sql)


def gubenUpdateDate(stockID):
    # sql = 'select max(date) from guben where stockid="%s" limit 1;' % stockID
    sql = 'select guben from lastupdate where stockid="%s";' % stockID
    return _getLastUpdate(sql)


def getGuzhi(stockID):
    sql = 'select * from guzhiresult where stockid="%s" limit 1' % stockID
    result = engine.execute(sql)
    return result.fetchone()


def readStockListFromSQL():
    result = engine.execute('select stockid, name from stocklist '
                            'where timetomarket>0')
    stockIDList = []
    for i in result:
        stockIDList.append([i[0], i[1]])
        pass
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
        # name = i[1].encode('utf-8')
        #         print type(i[1]), i[1], name
        #         name = i[1]
        stockNames.append(i[1])
    stockListDf = DataFrame({'stockid': stockIDs,
                             'name': stockNames})
    return stockListDf


def updateKlineMarketValue(stockID, startDate=None, endDate=None):
    if startDate is None:
        return
    gubenDf = readGuben(stockID, startDate)
    klineTablename = 'klinestock'
    gubenCount = gubenDf['date'].count()

    for i in range(gubenCount):
        gubenStartDate = gubenDf['date'][i]
        try:
            gubenEndDate = gubenDf['date'][i + 1]
        except KeyError as e:
            gubenEndDate = None
        #             print e
        totalShares = gubenDf['totalshares'][i]

        sql = ('update %(klineTablename)s '
               'set totalmarketvalue = close * %(totalShares)s'
               ' where stockid="%(stockID)s" and date>="%(gubenStartDate)s"'
               % locals())
        if endDate:
            sql += ' and date < "%s"' % gubenEndDate
        engine.execute(sql)


def updateKlineTTMLirun(stockID, startDate=None):
    """
    更新Kline表TTM利润
    """
    if startDate is None:
        return
    TTMLirunDf = readTTMLirunForStockID(stockID, startDate)
    klineTablename = 'klinestock'
    TTMLirunCount = TTMLirunDf['date'].count()

    for i in range(TTMLirunCount):
        startDate = TTMLirunDf['reportdate'][i]
        try:
            endDate = TTMLirunDf['reportdate'][i + 1]
        except KeyError:
            endDate = None
        TTMLirun = TTMLirunDf['ttmprofits'][i]

        sql = ('update %(klineTablename)s set ttmprofits = %(TTMLirun)s'
               ' where stockid="%(stockID)s" and date>="%(startDate)s"'
               % locals())
        if endDate:
            sql += ' and date < "%s"' % endDate
        engine.execute(sql)


def writeLirun(df):
    tablename = 'lirun'
    return writeSQL(df, tablename)


def writeGuben(stockID, df):
    """ 确认无用后删除
    """
    logging.debug('start writeGuben %s' % stockID)
    timea = dt.datetime.now()
    gubenList = datatrans.gubenDfToList(df)
    if not gubenList:
        return False

    tableName = 'guben'
    if not initsql.existTable(tableName):
        # initsql.createGubenTable(stockID)
        initsql.createGubenTable()

    session = Session()
    metadata = MetaData(bind=engine)
    mytable = Table(tableName, metadata, autoload=True)

    session.execute(mytable.insert().prefix_with('IGNORE'), gubenList)
    session.commit()
    session.close()
    timeb = dt.datetime.now()
    logging.debug('writeGuben stockID %s took %s' %
                  (stockID, (timeb - timea)))
    return True


def writeStockList(stockList):
    clearStockList()
    stockList.to_sql('stocklist', engine, if_exists='append')


def writeSQL(dfdata, tableName, replace=False):
    """
    Dataframe格式数据写入tableName指定的表中
    replace: True，主键重复时更新数据， False, 忽略重复主键, 默认为False
    """
    logging.debug('start writeSQL %s' % tableName)

    if not initsql.existTable(tableName):
        logging.error('not exist %s' % tableName)
        return False

    if isinstance(dfdata, DataFrame):
        if dfdata.empty:
            return True
        dfdata = dfdata.where(pd.notnull(dfdata), None)
        data = datatrans.transDfToList(dfdata)
    else:
        data = dfdata

    if not data:
        return True

    try:
        session = Session()
        metadata = MetaData(bind=engine)
        mytable = Table(tableName, metadata, autoload=True)
        if replace:
            session.add(mytable)
            # session.execute(mytable.replace(), data)
            session.merge(data)
        else:
            session.execute(mytable.insert().prefix_with('IGNORE'), data)
        session.commit()
        session.close()
    except Exception as e:
        print(e)
        print('写表失败： %s' % tableName)
        return False
    return True


def writeStockIDListToFile(stockIDList, filename):
    stockFile = open(filename, 'wb')
    stockFile.write(bytes('\n').join(stockIDList))
    stockFile.close()


def read180StockIDsFromSQL():
    result = engine.execute('select stockid from chengfen180')
    return [i[0] for i in result.fetchall()]


# def gubenFileToDf(stockID):
#     filename = filenameGuben(stockID)
#     try:
#         gubenFile = open(filename, 'r')
#         guben = gubenFile.read()
#         gubenFile.close()
#     except IOError as e:
#         print(e)
#         print(('读取总股本文件失败： %s' % stockID))
#         return False
#     return datatrans.gubenDataToDf(stockID, guben)


def readGuzhiFileToDict(stockID):
    """
    读取估值文件
    :rtype: dict
    :type stockID: string
    :return dict
    """
    guzhiFilename = filenameGuzhi(stockID)
    guzhiFile = open(guzhiFilename)
    guzhiData = guzhiFile.read()
    guzhiFile.close()
    return datatrans.transGuzhiDataToDict(guzhiData)


def readGuzhiFilesToDf(stockList):
    guzhiList = []
    for stockID in stockList:
        logging.debug('readGuzhiFilesToDf: %s', stockID)
        guzhidata = readGuzhiFileToDict(stockID)
        if guzhidata is not None:
            guzhiList.append(guzhidata)
    return DataFrame(guzhiList)


def readGuzhiSQLToDf(stockList):
    listStr = ','.join(stockList)
    sql = 'select * from guzhi where stockid in (%s);' % listStr
    #     result = engine.execute(sql)
    df = pd.read_sql(sql, engine)
    print(df)
    return df


def readValuationSammary():
    sql = ('select stockid, name, pf, pe, peg, pe200, pe1000 '
           'from valuation order by pf desc;')
    stocks = pd.read_sql(sql, engine)
    return stocks


def readValuation(stockID):
    sql = 'select * from valuation where stockid="%(stockID)s"' % locals()
    result = engine.execute(sql).fetchone()
    return result


# def downloadKline(stockID, startDate=None, endDate=None):
#     if startDate is None:  # startDate为空时取股票最后更新日期
#         startDate = getKlineUpdateDate(stockID)
#         startDate = startDate.strftime('%Y-%m-%d')
#     if endDate is None:
#         endDate = dt.datetime.today().strftime('%Y-%m-%d')
#     return downloadKlineTuShare(stockID, startDate, endDate)
#
#
# def downloadKlineTuShare(stockID,
#                          startDate='1990-01-01', endDate='2099-12-31'):
#     try:
#         histDf = ts.get_hist_data(stockID, startDate, endDate)
#     except IOError:
#         return None
#     return histDf


def readLirunList(date):
    sql = 'select * from lirun where `date` >= %s and `date` <= %s' % (
        str(date - 10), str(date))
    df = pd.read_sql(sql, engine)
    return df


def readPERate(stockID):
    """ 读取一只股票的PE历史水平，
    # 返回PE200， PE1000两个数值，分别代表该股票当前PE值在过去200、1000个交易日中的水平
    """
    sql = ('select pe200, pe1000 from guzhiresult '
           'where stockid="%(stockID)s" limit 1' % locals())
    print(sql)
    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result is None:
        return None, None
    else:
        return result


def readTTMLirunForStockID(stockID, startDate=None, endDate=None):
    """取指定股票一段日间的TTM利润，startDate当日无数据时，取之前最近一次数据
    Parameters
    --------
    stockID: str 股票代码  e.g: '600519'
    startDate: date 起始日期  e.g: '1990-01-01'
    endDate: date 截止日期  e.g: '1990-01-01'

    Return
    --------
    DataFrame: 返回DataFrame格式TTM利润
    """
    # 指定日期（含）前最近一次利润变动日期
    if startDate is None:
        return
    startDateStr = startDate.strftime('%Y-%m-%d')
    sql = ('select max(reportdate) from ttmlirun '
           'where stockid="%(stockID)s" '
           'and reportdate<="%(startDateStr)s"' % locals())
    #     print sql
    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result[0] is None:
        TTMLirunStartDate = startDateStr
    else:
        TTMLirunStartDate = result[0]
    sql = ('select * from ttmlirun where stockid = "%(stockID)s" '
           'and `reportdate` >= "%(TTMLirunStartDate)s" '
           'order by date' % locals())
    #     print sql
    if endDate is not None:
        sql += ' and `date` <= "%s"' % endDate.strftime('%Y-%m-%d')
    df = pd.read_sql(sql, engine)

    # 指定日期（含）前存在股本变动数据的，重设第1次变动日期为startDate，
    # 减少更新Kline表中总市值所需计算量
    if TTMLirunStartDate != startDateStr:
        df.loc[df.reportdate == TTMLirunStartDate, 'reportdate'] = startDate
    return df


def readLastTTMLirunForStockID(stockID, limit=1):
    """取指定股票最近几期TTM利润
    Parameters
    --------
    stockID: str 股票代码  e.g: '600519'
    limit: 取最近期数的数据

    Return
    --------
    list: 返回list格式TTM利润
    """
    sql = ('select incrate from ttmlirun '
           'where stockid="%(stockID)s" '
           'order by date desc '
           'limit %(limit)s' % locals())
    #     print sql
    result = engine.execute(sql).fetchall()
    result = [i[0] for i in reversed(result)]
    return result


#     df = pd.read_sql(sql, engine)
#     return df


def readStockKlineDf(stockID, days):
    sql = ('select date, open, high, low, close, ttmpe '
           'from klinestock where stockid="%(stockID)s" '
           'order by date desc limit %(days)s;' % locals())
    result = engine.execute(sql).fetchall()
    stockDatas = [i for i in reversed(result)]
    # klineDatas = []
    dateList = []
    openList = []
    closeList = []
    highList = []
    lowList = []
    peList = []
    indexes = list(range(len(result)))
    for i in indexes:
        date, _open, high, low, close, ttmpe = stockDatas[i]
        dateList.append(date.strftime("%Y-%m-%d"))
        # QuarterList.append(date)
        openList.append(_open)
        closeList.append(close)
        highList.append(high)
        lowList.append(low)
        peList.append(ttmpe)
    klineDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})
    return klineDf


def readLastTTMLirun(stockList, limit=1):
    """取股票列表最近几期TTM利润
    Parameters
    --------
    stockList: str 股票代码  e.g: ['600519', '002518']
    limit: 取最近期数的数据

    Return
    --------
    DataFrame: 返回DataFrame格式TTM利润
    """
    TTMLirunList = []
    for stockID in stockList:
        TTMLirun = readLastTTMLirunForStockID(stockID, limit)
        TTMLirun.insert(0, stockID)
        TTMLirunList.append(TTMLirun)

    #     print TTMLirunList
    columns = ['incrate%s' % i for i in range(limit)]
    columns.insert(0, 'stockid')
    TTMLirunDf = DataFrame(TTMLirunList, columns=columns)
    return TTMLirunDf


def readTTMLirunForDate(date):
    """从TTMLirun表读取某季度股票TTM利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式TTM利润
    """
    sql = ('select * from ttmlirun where '
           '`date` = "%(date)s"' % locals())
    df = pd.read_sql(sql, engine)
    return df


def readLirunForDate(date):
    """从Lirun表读取一期股票利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式利润
    """
    sql = ('select * from lirun where '
           '`date` = "%(date)s"' % locals())
    df = pd.read_sql(sql, engine)
    return df


def readTTMPE(stockID):
    """ 读取某支股票的全部TTMPE
    """
    sql = ('select date, ttmpe from klinestock where stockid="%(stockID)s";'
           % locals())
    df = pd.read_sql(sql, engine)
    return df


def readCurrentTTMPE(stockID):
    sql = ('select ttmpe from klinestock where stockid="%(stockID)s" and date=('
           'select max(`date`) from klinestock where stockid="%(stockID)s")'
           % locals())

    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        return result[0]


def readCurrentTTMPEs(stockList):
    #     idList = []
    peList = []
    for stockID in stockList:
        result = readCurrentTTMPE(stockID)
        if result is None:
            logging.debug('readCurrentTTMPE failed: %s', stockID)
        peList.append(result)

    return DataFrame({'stockid': stockList, 'pe': peList})


# def alterKline():
#     sql = 'show tables like %s'
#     result = engine.execute(sql, 'kline%')
#     result = result.fetchall()
#
#     for i in result:
#         tablename = i[0]
#         sql = 'call stockdata.alterkline(%s)'
#         try:
#             engine.execute(sql, tablename)
#             #             result = result.fetchall()
#             print(tablename)
#         except sqlalchemy.exc.OperationalError as e:
#             print(e)
#     return


def calAllTTMLirun(date, incrementUpdate=True):
    """计算全部股票本期TTM利润并写入TTMLirun表
    date: 格式YYYYQ， 4位年+1位季度
    incrementUpdate: True, 增量更新， False, 覆盖已有数据的更新方式
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
        TTMLirun = lirunCur.copy()
        TTMLirun.columns = ['stockid', 'date', 'ttmprofits', 'reportdate']
    #         return writeSQL(TTMLirun, 'ttmlirun')
    else:
        if incrementUpdate:
            TTMLirunCur = readTTMLirunForDate(date)
            lirunCur = lirunCur[~lirunCur.stockid.isin(TTMLirunCur.stockid)]

        # 上年第四季度利润, 仅取利润字段并更名为profits1
        lastYearEnd = (date // 10 - 1) * 10 + 4
        lirunLastYearEnd = readLirunForDate(lastYearEnd)
        print(('lirunLastYearEnd.head():', lirunLastYearEnd.head()))
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
    print('TTMLirun.head():\n', TTMLirun.head())

    # 写入ttmlirun表后，重算TTM利润增长率
    if incrementUpdate:
        writeSQL(TTMLirun, 'ttmlirun')
    else:
        replaceTTMLinrun(TTMLirun)

    return calTTMLirunIncRate(date)


def replaceTTMLinrun(df):
    """
    以替换方式更新TTM利润，用于批量修正TTM利润表错误
    :param df:
    :return:
    """
    for index, row in df.iterrows():
        # print(row['stockid'])
        stockID = row['stockid']
        _date = row['date']
        ttmprofits = row['ttmprofits']
        reportdate = row['reportdate']
        sql = ('replace into ttmlirun(stockid, date, ttmprofits, reportdate) '
               'values("%(stockID)s", %(_date)s, '
               '%(ttmprofits)s, "%(reportdate)s");' % locals())
        print(sql)
        engine.execute(sql)


def calTTMLirunIncRate(date, incrementUpdate=True):
    """计算全部股票本期TTM利润增长率并写入TTMLirun表
    date: 格式YYYYQ， 4位年+1位季度
    # 计算公式： TTM利润增长率= (本期TTM利润  - 上年同期TTM利润) / TTM利润 * 100
    """
    TTMLirunCur = readTTMLirunForDate(date)
    if incrementUpdate:
        TTMLirunCur = TTMLirunCur[TTMLirunCur.incrate.isnull()]
    TTMLirunLastYear = readTTMLirunForDate(date - 10)
    TTMLirunLastYear = TTMLirunLastYear[['stockid', 'ttmprofits']]
    TTMLirunLastYear.columns = ['stockid', 'ttmprofits1']
    TTMLirunLastYear = TTMLirunLastYear[TTMLirunLastYear.ttmprofits1 != 0]

    # 整合以上2个表，stockid为整合键
    TTMLirunCur = pd.merge(TTMLirunCur, TTMLirunLastYear, on='stockid')

    TTMLirunCur['incrate'] = ((TTMLirunCur.ttmprofits -
                               TTMLirunCur.ttmprofits1) /
                              abs(TTMLirunCur.ttmprofits1) * 100)
    for i in TTMLirunCur.values:
        stockID = i[0]
        incRate = round(i[4], 2)
        sql = ('update ttmlirun '
               'set incrate = %(incRate)s'
               ' where stockid = "%(stockID)s"'
               'and `date` = %(date)s' % locals())
        engine.execute(sql)
    return


def calTTMLirun(stockdf, date):
    lirun1 = stockdf[stockdf.date == date - 10]  # 上年同期利润
    lirun2 = stockdf[stockdf.date == (date / 10 - 1) * 10 + 4]  # 上年末利润
    lirun3 = stockdf[stockdf.date == date]  # 本期利润
    if lirun1.empty or lirun2.empty or lirun3.empty:
        return None
    lirun1 = lirun1.iat[0, 2]
    lirun2 = lirun2.iat[0, 2]
    stockID = lirun3.iat[0, 0]
    reportdate = lirun3.iat[0, 3]
    lirun3 = lirun3.iat[0, 2]
    # TTM利润 = 本期利润＋上年末利润-上年同期利润
    lirun = lirun3 + lirun2 - lirun1
    return [stockID, date, lirun, reportdate]


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
    if startDate is None:
        startDate = getTTMPELastUpdate(stockID) + dt.timedelta(days=1)
    # startDateStr = startDate.strftime('%Y-%m-%d')
    logging.debug(
        'updateKlineEXTData: %(stockID)s, date: %(startDate)s' % locals())
    try:
        updateKlineMarketValue(stockID, startDate)
        updateKlineTTMLirun(stockID, startDate)
        endDate = updateKlineTTMPE(stockID, startDate)
        setKlineTTMPELastUpdate(stockID, endDate)
    except Exception as e:
        logging.error('fail to updateKlineEXTData: %s,[%s]', stockID, e)


def getTTMPELastUpdate(stockID):
    """TTMPE最近更新日期，
    Parameters
    --------
    stockID:str 股票代码 e.g:600519

    Return
    --------
    datetime：TTMPE最近更新日期
    """
    sql = ('select ttmpe from lastupdate where stockid="%(stockID)s"'
           % locals())
    return _getLastUpdate(sql)


def getLirunUpdateStartQuarter():
    """根据利润表数据判断本次利润表更新的起始日期
    选取利润表中每支股票的财报最大日期，且为在stocklist股票基本信息表中存在的股票
    再找出财报日期中最小的数值作为本次利润表更新的起始日期
    Parameters
    --------


    Return
    --------
    startQuarter：YYYYQ 起始更新日期
    """
    sql = ('select min(maxdate) from (SELECT stockid, max(date) as maxdate '
           'FROM stockdata.lirun group by stockid '
           'having stockid in (select stockid from stocklist WHERE '
           'LEFT(name, 1) != "*")) as temp;')

    result = engine.execute(sql)
    lastQuarter = result.first()[0]
    startQuarter = datatrans.quarterAdd(lastQuarter, 1)
    return startQuarter


def getLirunUpdateEndQuarter():
    curQuarter = datatrans.transDateToQuarter(dt.datetime.now())
    return datatrans.quarterSub(curQuarter, 1)


def _getLastUpdate(sql):
    """
    Parameters
    --------
    sql: str  指定查询更新日期的SQL语句
    e.g: 'select ttmpe from lastupdate where stockid="002796"'

    Return
    --------
    datetime：datetime
    """
    result = engine.execute(sql).first()
    if result is None:
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d').date()

    lastUpdateDate = result[0]
    if lastUpdateDate is None:
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d').date()

    # lastUpdateDate = lastUpdateDate[0]
    if isinstance(lastUpdateDate, dt.date):
        return lastUpdateDate
        # return lastUpdateDate + dt.timedelta(days=1)
    else:
        logging.debug('lastUpdateDate type is: %s', type(lastUpdateDate))
        return dt.datetime.strptime(lastUpdateDate, '%Y-%m-%d').date()


def writeChigu(stockList):
    engine.execute('TRUNCATE TABLE chigu')
    for stockID in stockList:
        sql = ('insert into chigu (`stockid`) '
               'values ("%s");') % stockID
        engine.execute(sql)


def savePELirunIncrease(startDate='2007-01-01', endDate=None):
    stockList = readStockListFromSQL()
    for stockID, stockName_ in stockList:
        #     sql = (u'insert ignore into pelirunincrease(stockid, date, pe) '
        #            u'select "%(stockID)s", date, ttmpe '
        #            u'from klinestock%(stockID)s '
        #            u'where `date`>="%(startDate)s";') % locals()
        #
        #     engine.execute(sql)

        TTMLirunDf = readTTMLirunForStockID(stockID, startDate)
        TTMLirunDf = TTMLirunDf.dropna().reset_index(drop=True)
        klineTablename = 'klinestock'
        TTMLirunCount = len(TTMLirunDf)
        for i in range(TTMLirunCount):
            incrate = TTMLirunDf['incrate'].iloc[i]
            startDate_ = TTMLirunDf['reportdate'].iloc[i]
            try:
                endDate_ = TTMLirunDf['reportdate'].iloc[i + 1]
            except IndexError:
                endDate_ = None

            sql = ('update pelirunincrease '
                   'set lirunincrease = %(incrate)s'
                   ' where stockid="%(stockID)s"'
                   ' and date>="%(startDate_)s"') % locals()
            if endDate_ is not None:
                sql += (' and date<"%(endDate_)s"' % locals())
            engine.execute(sql)


#         break


def setKlineTTMPELastUpdate(stockID, endDate):
    sql = ('insert into lastupdate (`stockid`, `ttmpe`) '
           'values ("%(stockID)s", "%(endDate)s") '
           'on duplicate key update `ttmpe`="%(endDate)s";' % locals())
    result = engine.execute(sql)
    return result


def setGubenLastUpdate(stockID, endDate=None):
    sql = ('insert into lastupdate (`stockid`, `guben`) '
           'values ("%(stockID)s", "%(endDate)s") '
           'on duplicate key update `guben`="%(endDate)s";' % locals())
    result = engine.execute(sql)
    return result


def updateKlineTTMPE(stockID, startDate, endDate=None):
    """
    # 更新Kline表TTMPE
    """
    #     engine = getEngine()
    if startDate is None:
        return

    startDateStr = startDate.strftime('%Y-%m-%d')
    klineTablename = 'klinestock'
    sql = ('update %(klineTablename)s '
           'set ttmpe = round(totalmarketvalue / ttmprofits, 2)'
           ' where stockid="%(stockID)s" and date>="%(startDateStr)s"'
           % locals())
    if endDate:
        sql += ' and date < "%s"' % endDate
    sql += ' and totalmarketvalue is not null'
    sql += ' and ttmprofits is not null'
    unusedResult = engine.execute(sql)
    sql = 'select max(date) from klinestock where stockid="%(stockID)s"' % locals()
    result = engine.execute(sql)
    endDate = result.fetchone()[0]
    if endDate is not None:
        endDate = endDate.strftime('%Y-%m-%d')
    else:
        endDate = (dt.datetime.today() - dt.timedelta(days=1))
        endDate = endDate.strftime('%Y-%m-%d')
    return endDate


# def updateLirun():
#     startQuarter = getLirunUpdateStartQuarter()
#     endQuarter = getLirunUpdateEndQuarter()
#
#     dates = datatrans.QuarterList(startQuarter, endQuarter)
#     for date in dates:
#         #         print date
#         logging.debug('updateLirun: %s', date)
#         try:
#             df = downloadLirun(date)
#         except ValueError:
#             continue
#         if df is None:
#             continue
# #         print len(df)
#         # 读取已存储的利润数据，从下载数据中删除该部分，对未存储的利润写入数据库
#         lirunCur = readLirunForDate(date)
#         df = df[~df.stockid.isin(lirunCur.stockid)]
#         df = df[df.profits.notnull()]
# #         print df
#
#         # 对未存储的利润写入数据库，并重新计算TTM利润
#         if not df.empty:
#             writeLirun(df)
#             calAllTTMLirun(date)


# 疑似无用函数，待删除
# def readGubenFromDf(df, date):
#     """从股本列表中读取指定日期的股本数值
#     # 指定日期无股本数据时，选取之前最近的股本数据
#     """
#     d = df[df.date <= date]
#     if d.empty:
#         return None
#     guben = d[d.date == d['date'].max(), 'totalshares']
# #     guben = d.iat[0, 3]
#     return guben


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


def readGuben(stockID, startDate=None, endDate=None):
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
    # 指定日期（含）前最近一次股本变动日期
    if startDate is None:
        return
    startDateStr = startDate.strftime('%Y-%m-%d')
    sql = ('select max(date) from guben '
           'where stockid="%(stockID)s" '
           'and date<="%(startDateStr)s"' % locals())
    result = engine.execute(sql)
    lastUpdate = result.fetchone()[0]

    # 指定日期（含）前无股本变动数据的，查询起始日期设定为startDate
    # 否则设定为最近一次变动日期
    if lastUpdate is None:
        gubenStartDate = startDateStr
    else:
        gubenStartDate = lastUpdate.strftime('%Y-%m-%d')

    sql = ('select date, totalshares from guben '
           'where stockid = "%(stockID)s"'
           ' and `date`>="%(gubenStartDate)s"' % locals())
    if endDate:
        sql += ' and `date` <= "%s"' % endDate.strftime('%Y-%m-%d')
    df = pd.read_sql(sql, engine)

    # 指定日期（含）前存在股本变动数据的，重设第1次变动日期为startDate，
    # 减少更新Kline表中总市值所需计算量
    if lastUpdate is not None:
        gbdate = datetime.strptime(gubenStartDate, '%Y-%m-%d').date()
        df.loc[df['date'] == gbdate, 'date'] = startDateStr
    return df


def readGubenUpdateList():
    """ 比较股票已存股本数据与最新股数据，不相同时则表示需要更新的股票
    """
    sql = 'select stockid, totals as totalsnew from stocklist;'
    dfNew = pd.read_sql(sql, engine)

    #     dfNew = ts.get_stock_basics()
    #     dfNew = dfNew.reset_index()
    #     dfNew = dfNew[['code', 'totals']]
    #     dfNew.columns = ['stockid', 'totalsnew']
    sql = ('SELECT stockid, totalshares as totalsold '
           'FROM (select * from stockdata.guben order by date desc) '
           'as tablea group by stockid;')
    dfOld = pd.read_sql(sql, engine)
    dfOld.totalsold = dfOld.totalsold / 100000000
    dfOld = dfOld.round(2)
    updateList = pd.merge(dfNew, dfOld, on='stockid', how='left')
    updateList.fillna({'totalsold': 0}, inplace=True)
    updateList = updateList[abs(
        updateList.totalsnew - updateList.totalsold) > 0.1]
    return updateList.stockid


def readClose(stockID):
    sql = ('select date, close from klinestock where stockid="%(stockID)s";'
           % locals())
    df = pd.read_sql(sql, engine)
    return df


def readCurrentClose(stockID):
    sql = ('select close from klinestock where stockid="%(stockID)s" '
           'and date=('
           'select max(`date`) from klinestock where stockid="%(stockID)s"'
           ')' % locals())
    result = engine.execute(sql)
    return result.fetchone()[0]


def readCurrentPEG(stockID):
    sql = 'select peg from guzhiresult where stockid="%s" limit 1' % stockID
    logging.info(sql)
    result = engine.execute(sql)
    try:
        result = result.fetchone()[0]
    except TypeError:
        return None
    else:
        return result


def getStockIDsForClassified(classified):
    sql = ('select stockid from classified '
           'where cname = "%(classified)s"' % locals())
    result = engine.execute(sql)
    stockIDList = [classifiedID[0] for classifiedID in result.fetchall()]
    return stockIDList


def classifiedToSQL(classifiedDf):
    """ 旧版写行业分类到数据库， 计划删除本函数
    """
    tablename = 'classified'
    return writeSQL(classifiedDf, tablename)


def getChiguList():
    #     sql = ('select chigu.stockid, stocklist.name from chigu, stocklist '
    #            'where chigu.stockid=stocklist.stockid')
    sql = 'select stockid from chigu'
    result = engine.execute(sql)
    return [stockid[0] for stockid in result.fetchall()]


def getGuzhiList():
    #     sql = ('select guzhiresult.stockid, stocklist.name '
    #            'from guzhiresult, stocklist '
    #            'where guzhiresult.stockid=stocklist.stockid')
    sql = 'select stockid from youzhiguzhi'
    result = engine.execute(sql)
    return [stockid[0] for stockid in result.fetchall()]


#     return result.fetchall()


def getYouzhiList():
    #     sql = ('select youzhiguzhi.stockid, stocklist.name '
    #            'from youzhiguzhi, stocklist '
    #            'where youzhiguzhi.stockid=stocklist.stockid')
    #     sql = 'select stockid from guzhiresult'
    sql = 'select stockid from youzhiguzhi'
    result = engine.execute(sql)
    return [stockid[0] for stockid in result.fetchall()]


def getClassifiedForStocksID(stockID):
    sql = ('select cname from classified '
           'where stockid = "%(stockID)s"' % stockID)
    result = engine.execute(sql)
    classified = result.first()[0]
    return classified


def getStockName(stockID):
    sql = 'select name from stocklist where stockid="%s";' % stockID
    result = engine.execute(sql).first()
    if result is not None:
        return result[0]
    else:
        return None


def readStockKlineDf(stockID, days):
    """
    读取股票K线数据
    :param stockID: str, 6位股票代码
    :param days: int, 读取的天数
    :return: Dataframe
    klineDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})
    """
    sql = ('select date, open, high, low, close, ttmpe '
           'from klinestock where stockid="%(stockID)s" '
           'order by date desc limit %(days)s;' % locals())
    return _readKlineDf(sql)


def readIndexKlineDf(indexID, days):
    """
    读取指数K线数据
    :param indexID: str, 9位指数代码
    :param days: int, 读取的天数
    :return: Dataframe
    indexDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})
    """
    sql = ('select date, open, high, low, close, ttmpe '
           'from klineindex '
           'where id="%(indexID)s" '
           'order by date desc limit %(days)s;' % locals())
    return _readKlineDf(sql)


def _readKlineDf(sql):
    result = engine.execute(sql).fetchall()
    stockDatas = [i for i in reversed(result)]
    # klineDatas = []
    dateList = []
    openList = []
    closeList = []
    highList = []
    lowList = []
    peList = []
    indexes = list(range(len(result)))
    for i in indexes:
        date, _open, high, low, close, ttmpe = stockDatas[i]
        dateList.append(date.strftime("%Y-%m-%d"))
        # QuarterList.append(date)
        openList.append(_open)
        closeList.append(close)
        highList.append(high)
        lowList.append(low)
        peList.append(ttmpe)
    klineDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})
    return klineDf


if __name__ == '__main__':
    initlog()
    pass
    #    hylist = getHYList()
    #     print(readCurrentTTMPE('002508'))

    # 测试updateKlineEXTData
    stockID = '000651'
    startDate = '2016-01-01'
    updateKlineEXTData(stockID, startDate)
