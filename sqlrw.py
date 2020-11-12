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
# from pandas.compat import StringIO
from io import StringIO
# from sqlalchemy import create_engine
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import DATE, DECIMAL, String
from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker, scoped_session
# import sqlalchemy
from pandas.core.frame import DataFrame
# from tushare.stock import cons as ct
import xlrd
import tushare as ts

import datatrans
from datatrans import lastYearDate, lastYearEndDate
import initsql
from sqlconn import SQLConn
# from misc import filenameGuben, filenameLirun, filenameGuzhi
# from misc import filenameLirun, filenameGuzhi
from initlog import initlog

# from download import downHYFile

# from bokeh.sampledata import stocks


sqlconn = SQLConn()
engine = sqlconn.engine
Session = sqlconn.Session


def writeClassifyMemberToSQL(_date, filename):
    """ 从文件中读取行业与股票对应关系并写入数据库
    """
    try:
        xlsFile = xlrd.open_workbook(filename, encoding_override="cp1252")
        table = xlsFile.sheets()[4]
        tsCodes = table.col_values(0)[1:]
        tsCodes = [i + ('.SH' if i[0] == '6' else '.SZ') for i in tsCodes]
        classifyCodes = table.col_values(8)[1:]
        classifyDf = pd.DataFrame({'ts_code': tsCodes,
                             'classify_code': classifyCodes,
                             'date': _date})
        # engine.execute('TRUNCATE TABLE classify_member')
        writeSQL(classifyDf, 'classify_member')
    except (AssertionError, xlrd.biffh.XLRDError) as e:
        pass
        logging.warning(f'writeClassifyMemberToSQL {_date}, file:{filename}, error:{e}')



def writeClassifyNameToSQL(filename):
    try:
        xlsFile = xlrd.open_workbook(filename, encoding_override="cp1252")
        table = xlsFile.sheets()[0]
        classifyCodes = table.col_values(0)[1:]
        classifyNames = table.col_values(1)[1:]
    except (AssertionError, xlrd.biffh.XLRDError) as e:
        pass
        logging.warning(f'writeClassifyNameToSQL file:{filename}, error:{e}')
    else:
        classifyLevel = [len(hyID) / 2 for hyID in classifyCodes]
        classifylv1 = [hyID[:2] for hyID in classifyCodes]
        classifylv2 = [hyID[:4] for hyID in classifyCodes]
        classifylv3 = [hyID[:6] for hyID in classifyCodes]
        classifyDf = pd.DataFrame({'code': classifyCodes,
                                 'name': classifyNames,
                                 'level': classifyLevel,
                                 'level1id': classifylv1,
                                 'level2id': classifylv2,
                                 'level3id': classifylv3})
        engine.execute('TRUNCATE TABLE classify')
        writeSQL(classifyDf, 'classify')


def writeGubenToSQL(gubenDf, replace=False):
    """单个股票股本数据写入数据库
    :param replace:
    :type gubenDf: DataFrame
    """
    tablename = 'guben'
    # lastUpdate = gubenUpdateDate(ts_code)
    # gubenDf = gubenDf[pd.Timestamp(gubenDf.date) > lastUpdate]
    # gubenDf = gubenDf[gubenDf.date > lastUpdate]
    if gubenDf.empty:
        return None
    if replace:
        for index, row in gubenDf.iterrows():
            sql = (('replace into guben'
                    '(ts_code, date, totalshares) '
                    'values("%s", "%s", %s)')
                   % (row['ts_code'], row['date'], row['totalshares']))
            engine.execute(sql)
    else:
        return writeSQL(gubenDf, tablename)


def writeGuzhiToSQL(ts_code, data):
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
            '(ts_code, peg, next1YearPE, next2YearPE, next3YearPE) '
            'values("%(ts_code)s", %(peg)s, '
            '%(next1YearPE)s, %(next2YearPE)s, '
            '%(next3YearPE)s);') % locals())
    return engine.execute(sql)


# def writeKline(ts_code, df, insertType='IGNORE'):
#     """股票K线历史写入数据库"""
#     tableName = tablenameKline(ts_code)
#     if not initsql.existTable(tableName):
#         initsql.createKlineTable(ts_code)
#     return writeSQL(df, tableName, insertType)


def lirunFileToList(ts_code, date):
    fileName = filenameLirun(ts_code)
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
        logging.error('lirunFileToList read %s error', ts_code)
        return []

    return {'ts_code': ts_code,
            'date': date,
            'profits': profitsList[index],
            'reportdate': dateList[index]
            }


# def tablenameKline(ts_code):
#     return 'kline%s' % ts_code


def loadChigu():
    sql = 'select ts_code from chigu;'
    result = engine.execute(sql)
    return [i[0] for i in result.fetchall()]


def dropTable(tableName):
    engine.execute('DROP TABLE %s' % tableName)


def dropNAData():
    """ 清除K线图数据中交易量为0的数据
    """
    # stockList = readts_codesFromSQL()
    #     stockList = ['002100']
    # for ts_code in stockList:
    # tablename = tablenameKline(ts_code)
    # sql = 'delete from %(tablename)s where volume=0;' % locals()
    sql = 'delete from klinestock where volume=0;'
    engine.execute(sql)


def getLowPEStockList(maxPE=40):
    """选取指定范围PE的股票
    maxPE: 最大PE
    """
    sql = 'select ts_code, pe from stocklist where pe > 0 and pe <= %s' % maxPE
    df = pd.read_sql(sql, engine)
    return df


def getAllMarketPEUpdateDate():
    """
    全市场PE更新日期
    :return:
    """
    sql = 'select max(trade_date) from index_pe where ts_code="all";'
    return _getLastUpdate(sql)


def getIndexPEUpdateDate():
    """
    指数PE更新日期,当前仅有上证180指数的PE值需手动计算
    :return:
    """
    sql = f'select max(trade_date) from index_pe where ts_code="000010.SH";'
    return _getLastUpdate(sql)


def getGuzhi(ts_code):
    sql = f'select * from guzhiresult where ts_code="{ts_code}" limit 1'
    result = engine.execute(sql)
    return result.fetchone()


def readStockList():
    sql = 'select ts_code, name from stock_basic'
    df = pd.read_sql(sql, engine)
    return df


def writeSQL(data: pd.DataFrame, tableName: str, replace=False):
    """
    Dataframe格式数据写入tableName指定的表中
    replace: True，主键重复时更新数据， False, 忽略重复主键, 默认为False
    """
    if data.empty:
        return True
    logging.debug('start writeSQL %s' % tableName)
    if not initsql.existTable(tableName):
        logging.error('not exist %s' % tableName)
        return False
    data = data.where(pd.notnull(data), None)
    data = datatrans.transDfToList(data)

    Base = declarative_base()

    class MyTable(Base):
        __table__ = Table(f'{tableName}', Base.metadata,
                          autoload=True, autoload_with=engine)

    try:
        session = Session()
        metadata = MetaData(bind=engine)
        if replace:
            for d in data:
                # for index, row in data.iterrows():
                #     tmpDict = row.to_dict()
                #     tmpDict[data.index.name] = index
                # d = {key: getattr(row, key) for key in row.keys()}
                # for key in row.keys():
                #     print('key type:', type(key))
                #     print('value type:', type(getattr(row, key)))
                #     print('value:', getattr(row, key))
                # table = MyTable(**tmpDict)
                table = MyTable(**d)
                session.merge(table)
                session.commit()
        else:
            mytable = Table(tableName, metadata, autoload=True)
            session.execute(mytable.insert().prefix_with('IGNORE'), data)
        session.commit()
        session.close()
    except Exception as e:
        print(e)
        print('写表失败： %s' % tableName)
        return False
    return True


def writets_codeListToFile(ts_codeList, filename):
    stockFile = open(filename, 'wb')
    stockFile.write(bytes('\n').join(ts_codeList))
    stockFile.close()


def readGuzhiFileToDict(ts_code):
    """
    读取估值文件
    :rtype: dict
    :type ts_code: string
    :return dict
    """
    guzhiFilename = filenameGuzhi(ts_code)
    guzhiFile = open(guzhiFilename)
    guzhiData = guzhiFile.read()
    guzhiFile.close()
    return datatrans.transGuzhiDataToDict(guzhiData)


def readGuzhiFilesToDf(stockList):
    guzhiList = []
    for ts_code in stockList:
        logging.debug('readGuzhiFilesToDf: %s', ts_code)
        guzhidata = readGuzhiFileToDict(ts_code)
        if guzhidata is not None:
            guzhiList.append(guzhidata)
    return DataFrame(guzhiList)


def readGuzhiSQLToDf(stockList):
    listStr = ','.join(stockList)
    sql = 'select * from guzhi where ts_code in (%s);' % listStr
    #     result = engine.execute(sql)
    df = pd.read_sql(sql, engine)
    print(df)
    return df


def readValuationSammary():
    """读取股票评分信息"""
    # 基本信息
    sql = ('select ts_code, name, date, pf, pe, peg, pe200, pe1000 '
           'from valuation where date = (select max(date) from valuation) '
           'order by pf desc;')
    stocks = pd.read_sql(sql, engine)

    # 行业名称
    sql = ('select a.ts_code, a.name, c.name as classify_name'
           ' from stock_basic a, classify_member b, classify c'
           ' where a.ts_code=b.ts_code and b.classify_code=c.code'
           ' order by ts_code;')
    hyname = pd.read_sql(sql, engine)
    stocks = pd.merge(stocks, hyname, how='left')

    # 财务指标
    sql = ('select a.ts_code, a.end_date as fina_date,'
           ' a.grossprofit_margin, a.roe'
           ' from fina_indicator a,'
           ' (select ts_code, max(end_date) as fina_date '
           ' from fina_indicator group by ts_code) b'
           ' where a.ts_code = b.ts_code and a.end_date = b.fina_date;')
    finastat = pd.read_sql(sql, engine)
    finastat['grossprofit_margin'] = finastat.grossprofit_margin.round(2)
    finastat['roe'] = finastat.roe.round(2)
    finastat = finastat[['ts_code', 'fina_date', 'grossprofit_margin', 'roe']]
    stocks = pd.merge(stocks, finastat, how='left')

    # 每日指标
    # sql = ('select a.ts_code, a.dv_ttm,'
    #        ' from daily_basic a,'
    #        ' (select ts_code, max(date) as daily_date'
    #        ' from daily_basic group by ts_code) b'
    #        ' where a.ts_code=b.ts_code and a.date = b.daily_date')
    sql = """select a.ts_code, a.dv_ttm from daily_basic a,
            (select ts_code, max(trade_date) as daily_date 
            from daily_basic group by ts_code) b
            where a.ts_code = b.ts_code and a.trade_date = b.daily_date;"""

    daily = pd.read_sql(sql, engine)
    stocks = pd.merge(stocks, daily, how='left')

    # 排序
    stocks.sort_values(by=['pf', 'pe'], ascending=(False, True), inplace=True)
    return stocks


def readValuation(ts_code):
    sql = (f'select * from valuation where ts_code="{ts_code}"'
           'order by date desc limit 1')
    result = engine.execute(sql).fetchone()
    return result


# def downloadKline(ts_code, startDate=None, endDate=None):
#     if startDate is None:  # startDate为空时取股票最后更新日期
#         startDate = getKlineUpdateDate(ts_code)
#         startDate = startDate.strftime('%Y-%m-%d')
#     if endDate is None:
#         endDate = dt.datetime.today().strftime('%Y-%m-%d')
#     return downloadKlineTuShare(ts_code, startDate, endDate)
#
#
# def downloadKlineTuShare(ts_code,
#                          startDate='1990-01-01', endDate='2099-12-31'):
#     try:
#         histDf = ts.get_hist_data(ts_code, startDate, endDate)
#     except IOError:
#         return None
#     return histDf


def readLirunList(date):
    sql = 'select * from lirun where `date` >= %s and `date` <= %s' % (
        str(date - 10), str(date))
    df = pd.read_sql(sql, engine)
    return df


def readPERate(ts_code):
    """ 读取一只股票的PE历史水平，
    # 返回PE200， PE1000两个数值，分别代表该股票当前PE值在过去200、1000个交易日中的水平
    """
    sql = (f'select round(pe200, 2) pe200, round(pe1000, 2) pe1000'
           f' from guzhiresult where ts_code="{ts_code}" limit 1')
    print(sql)
    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result is None:
        return None, None
    else:
        return result


def readTTMProfits(ts_code: str, startDate=None, endDate=None):
    """取指定股票一段日间的TTM利润，startDate当日无数据时，取之前最近一次数据
    Parameters
    --------
    ts_code: str 股票代码  e.g: '600519'
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
           'where ts_code="%(ts_code)s" '
           'and reportdate<="%(startDateStr)s"' % locals())
    #     print sql
    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result[0] is None:
        TTMLirunStartDate = startDateStr
    else:
        TTMLirunStartDate = result[0]
    sql = ('select * from ttmlirun where ts_code = "%(ts_code)s" '
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


def readLastTTMProfit(ts_code, limit=1, date=None):
    """取指定股票最近几期TTM利润
    Parameters
    --------
    ts_code: str 股票代码  e.g: '600519.SH'
    date: str 查询截止日期， e.g: '20191231'
    limit: 取最近期数的数据

    Return
    --------
    list: 返回list格式TTM利润
    """
    sql = f'select incrate from ttmprofits where ts_code="{ts_code}" '
    if date is not None:
        sql += f' and ann_date<="{date}"'
    sql += f' order by end_date desc limit {limit}'
    #     print sql
    result = engine.execute(sql).fetchall()
    result = [i[0] for i in reversed(result)]
    return result


def readLastTTMProfits(stockList, limit=1, date=None):
    """取股票列表最近几期TTM利润
    Parameters
    --------
    :param stockList: str 股票代码  e.g: ['600519.SH', '002518.SZ']
    :param limit: 取最近期数的数据
    :param date:

    Return
    --------
    DataFrame: 返回DataFrame格式TTM利润
    """
    TTMLirunList = []
    for ts_code in stockList:
        TTMLirun = readLastTTMProfit(ts_code, limit, date)
        TTMLirun.insert(0, ts_code)
        TTMLirunList.append(TTMLirun)

    #     print TTMLirunList
    columns = ['incrate%s' % i for i in range(limit)]
    columns.insert(0, 'ts_code')
    TTMLirunDf = DataFrame(TTMLirunList, columns=columns)
    return TTMLirunDf


def readTTMProfitsForDate(end_date):
    """从TTMLirun表读取某季度股票TTM利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式TTM利润
    """
    sql = (f'select ts_code, ttmprofits, incrate from ttmprofits '
           f'where `end_date`="{end_date}"')
    df = pd.read_sql(sql, engine)
    return df


def readProfitsForDate(end_date):
    """从income表读取一期股票利润
    date: 格式YYYYMMDD
    return: 返回DataFrame格式利润
    """
    sql = ('select ts_code, end_date, ann_date, n_income_attr_p as profits '
           f'from income where `end_date`="{end_date}"')
    df = pd.read_sql(sql, engine)
    return df


def readTTMPE(ts_code):
    """ 读取某支股票的全部TTMPE
    """
    sql = ('select date, ttmpe from klinestock where ts_code="%(ts_code)s";'
           % locals())
    df = pd.read_sql(sql, engine)
    return df


def readLastTTMPE(ts_code, date=None):
    """读取指定股票指定日期的TTMPE，默认为最后一天的TTMPE

    :param ts_code: str
        股票代码， 如'600013'
    :param date: str
        指定日期， 格式'YYYYmmdd'
    :return:
    """
    sql = (f'select pe_ttm from daily_basic where ts_code="{ts_code}" '
           f'and trade_date=(select max(`trade_date`) from daily_basic where '
           f'ts_code="{ts_code}"')
    if date is None:
        sql += ')'
    else:
        sql += f' and trade_date<={date})'

    result = engine.execute(sql).fetchone()
    if result is None:
        return None
    else:
        return result[0]


def readCal(startDate=None, endDate=None, exchange='SSE', is_open=1):
    sql = (f'select cal_date trade_date from trade_cal'
           f' where exchange="{exchange}"')
    if startDate is not None:
        sql += f' and cal_date>="{startDate}"'
    if endDate is not None:
        sql += f' and cal_date<="{endDate}"'
    sql += f' and is_open={is_open}'
    result = engine.execute(sql).fetchall()
    return [d[0].strftime('%Y%m%d') for d in result]


def readLastTTMPEs(stockList, trade_date=None):
    """
    读取stockList中股票指定日期的TTMPE, 默认取最后一天的TTMPE
    :param stockList: list
        股票列表
    :param trade_date: str
        'YYYYmmdd'格式的日期
    :return:
    """
    sql = 'select ts_code, pe_ttm pe from daily_basic where trade_date='
    if trade_date is None:
        sql += '(select max(trade_date) from daily_basic)'
    else:
        sql += f'"{trade_date}"'

    result = engine.execute(sql).fetchall()
    if not result:
        logging.warning(f'缺少{trade_date}每日指标')
        return None
    df = pd.read_sql(sql, engine)
    # ts_codes, ttmpes = zip(*result)
    # df = pd.DataFrame({'ts_code': ts_codes, 'pe': ttmpes})
    df = df.loc[df['ts_code'].isin(stockList)]
    df = df.dropna()
    return df


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


def calAllTTMLirun(end_date, replace=False):
    """计算全部股票本期TTM利润并写入TTMLirun表
    date: 格式YYYYMMDD
    replace: True, 覆盖已有数据， False, 增量更新
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
    TTMProfits = readProfitsForDate(end_date)
    if not replace:
        existProfits = readTTMProfitsForDate(end_date)
        TTMProfits = TTMProfits[~TTMProfits.ts_code.isin(existProfits.ts_code)]
    if end_date[4:] == '1231':
        TTMProfits.rename(columns={'profits':'ttmprofits'}, inplace=True)
        print(TTMProfits)
        print('-' * 80)
    else:
        # 上年第四季度利润, 仅取利润字段并更名为profits1
        lastYearEnd = lastYearEndDate(end_date)
        profitsLastYearEnd = readProfitsForDate(lastYearEnd)
        # print(('profitsLastYearEnd.head():', profitsLastYearEnd.head()))
        profitsLastYearEnd = profitsLastYearEnd[['ts_code', 'profits']]
        profitsLastYearEnd.columns = ['ts_code', 'profits1']

        # 上年同期利润, 仅取利润字段并更名为profits2
        lastYear = lastYearDate(end_date)
        profitsSamePeriodLastYear = readProfitsForDate(lastYear)
        profitsSamePeriodLastYear = profitsSamePeriodLastYear[['ts_code', 'profits']]
        profitsSamePeriodLastYear.columns = ['ts_code', 'profits2']

        # 整合以上三个季度利润，ts_code为整合键
        TTMProfits = pd.merge(TTMProfits, profitsLastYearEnd, on='ts_code')
        TTMProfits = pd.merge(TTMProfits, profitsSamePeriodLastYear, on='ts_code')

        TTMProfits['ttmprofits'] = (TTMProfits.profits +
                                  TTMProfits.profits1 - TTMProfits.profits2)
        TTMProfits = TTMProfits[['ts_code', 'end_date', 'ttmprofits', 'ann_date']]
    TTMProfits.dropna(inplace=True)
    # print('TTMLirun.head():\n', TTMProfits.head())

    # 写入ttmlirun表后，重算TTM利润增长率
    # writeSQL(TTMProfits, 'ttmprofits', replace)
    if replace:
        replaceTTMProfits(TTMProfits)
    else:
        writeSQL(TTMProfits, 'ttmprofits')

    return calTTMProfitsIncRate(end_date)


def replaceTTMProfits(df):
    """
    以替换方式更新TTM利润，用于批量修正TTM利润表错误
    :param df:
    :return:
    """
    print(df)
    for index, row in df.iterrows():
        # print(row['ts_code'])
        ts_code = row['ts_code']
        end_date = row['end_date']
        ttmprofits = row['ttmprofits']
        ann_date = row['ann_date']
        sql = ('replace into ttmprofits'
               '(ts_code, end_date, ttmprofits, ann_date) '
               f'values("{ts_code}", "{end_date}", '
               f'{ttmprofits}, "{ann_date}");')
        print(sql)
        engine.execute(sql)


def calTTMProfitsIncRate(end_date, replace=True):
    """计算全部股票本期TTM利润增长率并写入TTMLirun表
    date: 格式YYYYMMDD
    # 计算公式： TTM利润增长率= (本期TTM利润  - 上年同期TTM利润) / TTM利润 * 100
    """
    TTMProfits = readTTMProfitsForDate(end_date)
    if not replace:
        TTMProfits = TTMProfits[TTMProfits.incrate.isnull()]
    samePeriodlastYear = lastYearDate(end_date)
    TTMProfitsLastYear = readTTMProfitsForDate(samePeriodlastYear)
    TTMProfitsLastYear = TTMProfitsLastYear[['ts_code', 'ttmprofits']]
    TTMProfitsLastYear.columns = ['ts_code', 'ttmprofits1']
    TTMProfitsLastYear = TTMProfitsLastYear[TTMProfitsLastYear.ttmprofits1 != 0]

    # 整合以上2个表，ts_code为整合键
    TTMProfits = pd.merge(TTMProfits, TTMProfitsLastYear, on='ts_code')

    TTMProfits['incrate'] = ((TTMProfits.ttmprofits -
                               TTMProfits.ttmprofits1) /
                              abs(TTMProfits.ttmprofits1) * 100)
    for _, row in TTMProfits.iterrows():
        # print(row)
        ts_code = row['ts_code']
        incRate = round(row['incrate'], 2)
        sql = (f'update ttmprofits set incrate = {incRate}'
               f' where ts_code="{ts_code}" and `end_date`="{end_date}"')
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
    ts_code = lirun3.iat[0, 0]
    reportdate = lirun3.iat[0, 3]
    lirun3 = lirun3.iat[0, 2]
    # TTM利润 = 本期利润＋上年末利润-上年同期利润
    lirun = lirun3 + lirun2 - lirun1
    return [ts_code, date, lirun, reportdate]


def getLirunUpdateEndQuarter():
    curQuarter = datatrans.transDateToQuarter(dt.datetime.now())
    return datatrans.quarterSub(curQuarter, 1)


def _getLastUpdate(sql):
    """
    Parameters
    --------
    sql: str  指定查询更新日期的SQL语句
    e.g: 'select ttmpe from lastupdate where ts_code="002796"'

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
    for ts_code in stockList:
        sql = ('insert into chigu (`ts_code`) '
               'values ("%s");') % ts_code
        engine.execute(sql)


# def savePELirunIncrease(startDate='2007-01-01', endDate=None):
#     stockList = readStockListFromSQL()
#     for ts_code, stockName_ in stockList:
#         #     sql = (u'insert ignore into pelirunincrease(ts_code, date, pe) '
#         #            u'select "%(ts_code)s", date, ttmpe '
#         #            u'from klinestock%(ts_code)s '
#         #            u'where `date`>="%(startDate)s";') % locals()
#         #
#         #     engine.execute(sql)
#
#         TTMLirunDf = readTTMProfits(ts_code, startDate)
#         TTMLirunDf = TTMLirunDf.dropna().reset_index(drop=True)
#         klineTablename = 'klinestock'
#         TTMLirunCount = len(TTMLirunDf)
#         for i in range(TTMLirunCount):
#             incrate = TTMLirunDf['incrate'].iloc[i]
#             startDate_ = TTMLirunDf['reportdate'].iloc[i]
#             try:
#                 endDate_ = TTMLirunDf['reportdate'].iloc[i + 1]
#             except IndexError:
#                 endDate_ = None
#
#             sql = ('update pelirunincrease '
#                    'set lirunincrease = %(incrate)s'
#                    ' where ts_code="%(ts_code)s"'
#                    ' and date>="%(startDate_)s"') % locals()
#             if endDate_ is not None:
#                 sql += (' and date<"%(endDate_)s"' % locals())
#             engine.execute(sql)
#
#
# #         break


# def setKlineTTMPELastUpdate(ts_code, endDate):
#     sql = ('insert into lastupdate (`ts_code`, `ttmpe`) '
#            'values ("%(ts_code)s", "%(endDate)s") '
#            'on duplicate key update `ttmpe`="%(endDate)s";' % locals())
#     result = engine.execute(sql)
#     return result


def setGubenLastUpdate(ts_code, endDate=None):
    sql = ('insert into lastupdate (`ts_code`, `guben`) '
           'values ("%(ts_code)s", "%(endDate)s") '
           'on duplicate key update `guben`="%(endDate)s";' % locals())
    result = engine.execute(sql)
    return result


def del_updateKlineTTMPE(ts_code, startDate, endDate=None):
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
           ' where ts_code="%(ts_code)s" and date>="%(startDateStr)s"'
           % locals())
    if endDate:
        sql += ' and date < "%s"' % endDate
    sql += ' and totalmarketvalue is not null'
    sql += ' and ttmprofits is not null'
    unusedResult = engine.execute(sql)
    sql = f'select max(date) from klinestock where ts_code="{ts_code}"'
    result = engine.execute(sql)
    endDate = result.fetchone()[0]
    if endDate is not None:
        endDate = endDate.strftime('%Y-%m-%d')
    else:
        endDate = (dt.datetime.today() - dt.timedelta(days=1))
        endDate = endDate.strftime('%Y-%m-%d')
    return endDate


def readGuben(ts_code, startDate=None, endDate=None):
    """取指定股票一段日间的股本变动数据，startDate当日无数据时，取之前最近一次变动数据
    Parameters
    --------
    ts_code: str 股票代码  e.g: '600519'
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
           'where ts_code="%(ts_code)s" '
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
           'where ts_code = "%(ts_code)s"'
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
    sql = 'select ts_code, totals as totalsnew from stocklist;'
    dfNew = pd.read_sql(sql, engine)

    #     dfNew = ts.get_stock_basics()
    #     dfNew = dfNew.reset_index()
    #     dfNew = dfNew[['code', 'totals']]
    #     dfNew.columns = ['ts_code', 'totalsnew']
    sql = ('SELECT ts_code, totalshares as totalsold '
           'FROM (select * from stockdata.guben order by date desc) '
           'as tablea group by ts_code;')
    dfOld = pd.read_sql(sql, engine)
    dfOld.totalsold = dfOld.totalsold / 100000000
    dfOld = dfOld.round(2)
    updateList = pd.merge(dfNew, dfOld, on='ts_code', how='left')
    updateList.fillna({'totalsold': 0}, inplace=True)
    updateList = updateList[abs(
        updateList.totalsnew - updateList.totalsold) > 0.1]
    return updateList.ts_code


def readClose(ts_code):
    sql = ('select date, close from klinestock where ts_code="%(ts_code)s";'
           % locals())
    df = pd.read_sql(sql, engine)
    return df


def readCurrentClose(ts_code):
    sql = (f'select close from daily where ts_code="{ts_code}" '
           f' and trade_date=(select max(`trade_date`)'
           f' from daily where ts_code="{ts_code}")')
    result = engine.execute(sql)
    return result.fetchone()[0]


def readCurrentPEG(ts_code):
    sql = 'select peg from guzhiresult where ts_code="%s" limit 1' % ts_code
    logging.info(sql)
    result = engine.execute(sql)
    try:
        result = result.fetchone()[0]
    except TypeError:
        return None
    else:
        return result


def getts_codesForClassified(classified):
    sql = ('select ts_code from classified '
           'where cname = "%(classified)s"' % locals())
    result = engine.execute(sql)
    ts_codeList = [classifiedID[0] for classifiedID in result.fetchall()]
    return ts_codeList


def classifiedToSQL(classifiedDf):
    """ 旧版写行业分类到数据库， 计划删除本函数
    """
    tablename = 'classified'
    return writeSQL(classifiedDf, tablename)


def getChiguList():
    #     sql = ('select chigu.ts_code, stocklist.name from chigu, stocklist '
    #            'where chigu.ts_code=stocklist.ts_code')
    sql = 'select ts_code from chigu'
    result = engine.execute(sql)
    return [ts_code[0] for ts_code in result.fetchall()]


def getGuzhiList():
    #     sql = ('select guzhiresult.ts_code, stocklist.name '
    #            'from guzhiresult, stocklist '
    #            'where guzhiresult.ts_code=stocklist.ts_code')
    sql = 'select ts_code from youzhiguzhi'
    result = engine.execute(sql)
    return [ts_code[0] for ts_code in result.fetchall()]


#     return result.fetchall()


def getYouzhiList():
    #     sql = ('select youzhiguzhi.ts_code, stocklist.name '
    #            'from youzhiguzhi, stocklist '
    #            'where youzhiguzhi.ts_code=stocklist.ts_code')
    #     sql = 'select ts_code from guzhiresult'
    sql = 'select ts_code from youzhiguzhi'
    result = engine.execute(sql)
    return [ts_code[0] for ts_code in result.fetchall()]


def getClassifiedForStocksID(ts_code):
    sql = ('select cname from classified '
           'where ts_code = "%(ts_code)s"' % ts_code)
    result = engine.execute(sql)
    classified = result.first()[0]
    return classified


def getStockName(ts_code):
    sql = f'select name from stock_basic where ts_code="{ts_code}";'
    result = engine.execute(sql).first()
    if result is not None:
        return result[0]
    else:
        return None


def readStockKline(ts_code, startDate=None, endDate=None, days=0):
    """
    读取股票K线数据
    :param endDate:
    :param startDate:
    :param ts_code: str, 6位股票代码
    :param days: int, 读取的天数
    :return: Dataframe
    klineDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})
    """
    sql = (f'select a.trade_date, a.open, a.high, a.low, a.close, b.pe_ttm'
           f' from daily a, daily_basic b where a.ts_code="{ts_code}" ')
    if startDate is not None:
        sql += f' and a.trade_date>="{startDate}"'
    if endDate is not None:
        sql += f' and a.trade_date<="{endDate}"'
    sql += f' and a.ts_code=b.ts_code and a.trade_date=b.trade_date'
    if days != 0:
        sql += f' order by a.trade_date desc limit {days}'
    return _readKline(sql)


def readProfitsIncAdf():
    stocks = pd.read_excel('data/profits_inc_adf_linear.xlsx')
    stocks = stocks.round(2)
    # stocks = stocks[(stocks['mean'] >= 10) &
    #                 (stocks.sharp >= 0.8) &
    #                 (stocks.pe_ttm <= 30)]
    stocks = stocks[(stocks['r2'] >= 0.3) & (stocks['coef'] > 0)]
    stocks.sort_values('sharp', ascending=False, inplace=True)
    return stocks


def readIndexKline(index_code, days):
    """
    读取指数K线数据
    :param index_code: str, 9位指数代码
    :param days: int, 读取的天数
    :return: Dataframe
    indexDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})
    """
    peTable = 'index_pe' if index_code == '000010.SH' else 'index_dailybasic'
    sql = (f'select a.trade_date, a.open, a.high, a.low, a.close, b.pe_ttm '
           f' from index_daily a, {peTable} b '
           f' where a.ts_code="{index_code}" and a.ts_code=b.ts_code '
           f' and a.trade_date=b.trade_date'
           f' order by trade_date desc limit {days};')
    df = _readKline(sql)
    # df = pd.read_sql(sql, engine)
    # df.rename(columns={'pe_ttm': 'pe'}, inplace=True)
    # df['date'] = df.trade_date.apply(lambda x: x.strftime('%Y%m%d'))
    # df.sort_values(by='date', inplace=True)
    # df.set_index(keys='date', inplace=True)
    # df.reset_index(inplace=True)
    # df.sort_values(by='trade_date', inplace=True)
    return df


def _readKline(sql):
    df = pd.read_sql(sql, engine)
    df.rename(columns={'trade_date': 'date', 'pe_ttm': 'pe'}, inplace=True)
    df.date = pd.to_datetime(df.date)
    df = df.set_index('date')
    df = df.sort_index()
    df = df.reset_index()
    return df
    # result = engine.execute(sql).fetchall()
    # stockDatas = [i for i in reversed(result)]
    # # klineDatas = []
    # dateList = []
    # openList = []
    # closeList = []
    # highList = []
    # lowList = []
    # peList = []
    # indexes = list(range(len(result)))
    # for i in indexes:
    #     date, _open, high, low, close, ttmpe = stockDatas[i]
    #     dateList.append(date.strftime("%Y-%m-%d"))
    #     # QuarterList.append(date)
    #     openList.append(_open)
    #     closeList.append(close)
    #     highList.append(high)
    #     lowList.append(low)
    #     peList.append(ttmpe)
    # klineDf = pd.DataFrame({'date': dateList,
    #                         'open': openList,
    #                         'close': closeList,
    #                         'high': highList,
    #                         'low': lowList,
    #                         'pe': peList})
    # return klineDf


if __name__ == '__main__':
    initlog()
    pass
    #    hylist = getHYList()
    #     print(readCurrentTTMPE('002508'))

    # 测试updateKlineEXTData
    ts_code = '000651'
    startDate = '2016-01-01'
    # updateKlineEXTData(ts_code, startDate)


def readTableFields(table):
    sql = (f'select column_name from information_schema.columns'
           f' where table_name="{table}"')
    result = engine.execute(sql).fetchall()
    return ','.join([s[0] for s in result])