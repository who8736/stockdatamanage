# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 21:11:51 2016

@author: who8736
"""

import datetime as dt
import logging

import pandas as pd
import xlrd
from pandas.core.frame import DataFrame
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base

from . import initsql
from .sqlconn import Session, engine
from ..util.datatrans import quarterList
from ..util.misc import dayDelta


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
        logging.warning(
            f'writeClassifyMemberToSQL {_date}, file:{filename}, error:{e}')


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


# def writeGuzhiToSQL(ts_code, data):
#     """下载单个股票估值数据写入数据库"""
#     guzhiDict = transGuzhiDataToDict(data)
#     if guzhiDict is None:
#         return True
#     # print guzhiDict
#     #     guzhiDf = DataFrame(guzhiDict, index=[0])
#     #     writeSQLUpdate(guzhiDict, 'guzhi')
#     #     print guzhiDict
#     tablename = 'guzhi'
#     if 'peg' in list(guzhiDict.keys()):
#         peg = guzhiDict['peg']
#     else:
#         peg = 'null'
#     if 'next1YearPE' in list(guzhiDict.keys()):
#         next1YearPE = guzhiDict['next1YearPE']
#     else:
#         next1YearPE = 'null'
#     if 'next2YearPE' in list(guzhiDict.keys()):
#         next2YearPE = guzhiDict['next2YearPE']
#     else:
#         next2YearPE = 'null'
#     if 'next3YearPE' in list(guzhiDict.keys()):
#         next3YearPE = guzhiDict['next3YearPE']
#     else:
#         next3YearPE = 'null'
#     # noinspection SqlResolve
#     sql = (f'replace into {tablename}'
#            '(ts_code, peg, next1YearPE, next2YearPE, next3YearPE) '
#            'values("%(ts_code)s", %(peg)s, '
#            '%(next1YearPE)s, %(next2YearPE)s, '
#            '%(next3YearPE)s);')
#     return engine.execute(sql)


# def writeKline(ts_code, df, insertType='IGNORE'):
#     """股票K线历史写入数据库"""
#     tableName = tablenameKline(ts_code)
#     if not initsql.existTable(tableName):
#         initsql.createKlineTable(ts_code)
#     return writeSQL(df, tableName, insertType)


# def lirunFileToList(ts_code, date):
#     fileName = filenameLirun(ts_code)
#     lirunFile = open(fileName, 'r')
#     lirunData = lirunFile.readlines()
#
#     dateList = lirunData[0].split()
#     logging.debug(repr(dateList))
#
#     try:
#         index = dateList.index(date)
#         logging.debug(repr(index))
#     except ValueError:
#         return []
#
#     profitsList = lirunData[42].split()
#     # if profitsList[0].decode('gbk') != '归属于母公司所有者的净利润':
#     if profitsList[0] != '归属于母公司所有者的净利润':
#         logging.error('lirunFileToList read %s error', ts_code)
#         return []
#
#     return {'ts_code': ts_code,
#             'date': date,
#             'profits': profitsList[index],
#             'reportdate': dateList[index]
#             }


# def tablenameKline(ts_code):
#     return 'kline%s' % ts_code


def dropTable(tableName):
    engine.execute('DROP TABLE %s' % tableName)


# def dropNAData():
#     """ 清除K线图数据中交易量为0的数据
#     """
#     # stockList = readts_codesFromSQL()
#     #     stockList = ['002100']
#     # for ts_code in stockList:
#     # tablename = tablenameKline(ts_code)
#     # sql = 'delete from %(tablename)s where volume=0;' % locals()
#     sql = 'delete from klinestock where volume=0;'
#     engine.execute(sql)


def readLowPEStock(maxPE=40):
    """选取指定范围PE的股票
    maxPE: 最大PE
    """
    sql = f'select ts_code, pe from stocklist where pe>0 and pe<={maxPE}'
    df = pd.read_sql(sql, engine)
    return df


def readGuzhi(ts_code):
    # noinspection SqlResolve
    sql = f'select * from guzhiresult where "{ts_code}"=ts_code limit 1'
    df = pd.read_sql(sql, engine)
    if not df.empty:
        return df.iloc[0].todict()


def readStockList(list_date=None):
    sql = 'select ts_code, name from stock_basic'
    if list_date:
        sql += f' where list_date>="{list_date}"'
    df = pd.read_sql(sql, engine)
    return df


def writeSQL(df: pd.DataFrame, tableName: str, replace=False):
    """
    Dataframe格式数据写入tableName指定的表中
    replace: True，主键重复时更新数据， False, 忽略重复主键, 默认为False
    """
    if df.empty:
        return True
    logging.debug('start writeSQL %s' % tableName)
    if not initsql.existTable(tableName):
        logging.error('not exist %s' % tableName)
        return False
    df = df.where(pd.notnull(df), None)
    # df = transDfToList(df)

    Base = declarative_base()

    class MyTable(Base):
        __table__ = Table(f'{tableName}', Base.metadata,
                          autoload=True, autoload_with=engine)

    try:
        session = Session()
        metadata = MetaData(bind=engine)
        if replace:
            for _index, row in df.iterrows():
                # noinspection PyArgumentList
                table = MyTable(**(row.to_dict()))
                session.merge(table)
                session.commit()
        else:
            mytable = Table(tableName, metadata, autoload=True)
            session.execute(mytable.insert().prefix_with('IGNORE'),
                            df.to_dict(orient='records'))
        session.commit()
        session.close()
    except Exception as e:
        # print(e)
        # print('写表失败： %s' % tableName)
        logging.error(f'写表失败[{tableName}]: {e}')
        return False
    return True


def readGuzhiSQLToDf(stockList):
    listStr = ','.join(stockList)
    sql = 'select * from guzhi where ts_code in (%s);' % listStr
    #     result = engine.execute(sql)
    df = pd.read_sql(sql, engine)
    print(df)
    return df


def readValuationSammary(date=None):
    """读取股票评分信息"""
    # 基本信息
    sql = ('select ts_code, name, date, pf, pe, peg, pe200, pe1000 '
           'from valuation where date = (select max(date) from valuation) '
           'order by pf desc;')
    stocks = pd.read_sql(sql, engine)

    # 行业名称
    sql = ('select a.ts_code, a.name, c.name as classify_name'
           ' from stock_basic a, classify_member b, classify c'
           ' where a.ts_code=b.ts_code ')
    if date is None:
        sql += f' and b.date=(select max(date) from classify_member) '
    else:
        sql += f' and b.date="{date}" '
    sql += (' and b.classify_code=c.code'
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
    stocks = stocks.merge(finastat, how='inner')

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
    """读取评估报告
    """
    sql = (f'select * from valuation where ts_code="{ts_code}" '
           ' order by date desc limit 1')
    df = pd.read_sql(sql, engine)
    if not df.empty:
        return df.iloc[0].to_dict()


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


# def readLirunList(date):
#     sql = 'select * from lirun where `date` >= %s and `date` <= %s' % (
#         str(date - 10), str(date))
#     df = pd.read_sql(sql, engine)
#     return df


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


def readTTMProfitsForStock(ts_code: str, startDate=None, endDate=None):
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


# TODO: 废弃本函数， 用readProfitInc代替
def del_readLastTTMProfit(ts_code, limit=1, date=None):
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


# TODO: 废弃本函数， 用readProfitInc代替
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
        TTMLirun = readProfitInc(ts_code, limit, date)
        TTMLirun.insert(0, ts_code)
        TTMLirunList.append(TTMLirun)

    #     print TTMLirunList
    columns = [f'incrate{i}' for i in range(limit)]
    columns.insert(0, 'ts_code')
    TTMLirunDf = DataFrame(TTMLirunList, columns=columns)
    return TTMLirunDf


def readTTMProfitsForDate(end_date):
    """从TTMLirun表读取某季度股票TTM利润
    end_date: str, YYYYMMDD, 报告期
    return: 返回DataFrame格式TTM利润
    """
    sql = (f'select ts_code, ttmprofits, inc from ttmprofits '
           f'where `end_date`="{end_date}"')
    df = pd.read_sql(sql, engine)
    return df


# def readProfitsForDate(end_date):
#     """从income表读取一期股票利润
#     date: 格式YYYYMMDD
#     return: 返回DataFrame格式利润
#     """
#     sql = ('select ts_code, end_date, ann_date, n_income_attr_p as profits '
#            f'from income where `end_date`="{end_date}"')
#     df = pd.read_sql(sql, engine)
#     return df


def readTTMPE(ts_code):
    """ 读取某支股票的全部TTMPE
    """
    sql = ('select date, ttmpe from klinestock where ts_code="%(ts_code)s";'
           % locals())
    df = pd.read_sql(sql, engine)
    return df


# def readLastTTMPE(ts_code, date=None):
#     """读取指定股票指定日期的TTMPE，默认为最后一天的TTMPE
#
#     :param ts_code: str
#         股票代码， 如'600013'
#     :param date: str
#         指定日期， 格式'YYYYmmdd'
#     :return:
#     """
#     sql = (f'select pe_ttm from daily_basic where ts_code="{ts_code}" '
#            f'and trade_date=(select max(`trade_date`) from daily_basic where '
#            f'ts_code="{ts_code}"')
#     if date is None:
#         sql += ')'
#     else:
#         sql += f' and trade_date<={date})'
#
#     result = engine.execute(sql).fetchone()
#     if result is None:
#         return None
#     else:
#         return result[0]


def readCal(startDate=None, endDate=None, exchange='SSE', is_open=1):
    """读取一段时间内的市场交易日期
    :rtype: list, eg. ['20201101', '20201102']
    """
    sql = (f'select cal_date trade_date from trade_cal'
           f' where exchange="{exchange}"')
    if startDate is not None:
        sql += f' and cal_date>="{startDate}"'
    if endDate is not None:
        sql += f' and cal_date<="{endDate}"'
    sql += f' and is_open={is_open}'
    result = engine.execute(sql).fetchall()
    if result:
        return [d[0].strftime('%Y%m%d') for d in result]
    else:
        return []


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


def readLastTTMPEs(stocks, trade_date=None):
    """
    读取stockList中股票指定日期的TTMPE, 默认取最后一天的TTMPE
    :param stocks: Dataframe 股票清单
    :param trade_date: str
        'YYYYmmdd'格式的日期
    :return:
    """
    if trade_date is None:
        condition = '(select max(trade_date) from daily_basic)'
    else:
        condition = f'"{trade_date}"'
    sql = (f'select ts_code, pe_ttm pe from daily_basic '
           f'where trade_date={condition}')

    df = pd.read_sql(sql, engine)
    if df.empty:
        logging.warning(f'缺少{trade_date}每日指标')
        return None

    # ts_codes, ttmpes = zip(*result)
    # df = pd.DataFrame({'ts_code': ts_codes, 'pe': ttmpes})
    df = df.loc[df.ts_code.isin(stocks.ts_code)]
    df = df.dropna()
    return df


def readUpdate(dataName, offsetdays=0):
    """
    获取数据的更新日期
    Parameters
    --------
    :param dataName: str 更新的数据的类型

    :return
    --------
    str: YYYYMMDD, 数据更新日期
    """
    sql = f'select date from update_date where dataname="{dataName}"'
    result = engine.execute(sql).fetchone()
    if result:
        _date = result[0]
        return dayDelta(_date, days=offsetdays)
    else:
        return '20100101'


def writeHold(stockList):
    """将持股清单写入数据库"""
    engine.execute('TRUNCATE TABLE chigu')
    for ts_code in stockList:
        sql = ('insert into chigu (`ts_code`) '
               'values ("%s");') % ts_code
        engine.execute(sql)


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


def readChigu():
    #     sql = ('select chigu.ts_code, stocklist.name from chigu, stocklist '
    #            'where chigu.ts_code=stocklist.ts_code')
    sql = 'select ts_code from chigu'
    return pd.read_sql(sql, engine)


def readClassify():
    """
    """
    sql = 'select code, name from classify'
    df = pd.read_sql(sql, engine)
    return df


def readClassifyName(code):
    sql = f'select name from classify where code="{code}"'
    result = engine.execute(sql).fetchone()
    if result:
        return result[0]

def readClassifyProfit(date, lv=None):
    """

    Parameters
    ----------
    date : str, YYYYMMDD
    lv : int
    """
    if not isinstance(lv, int) or (lv > 4 or lv < 1):
        lv = None
    sql = f'''select a.code, b.name, a.profits, a.profits_com, 
                        a.lastprofits, a.inc
                from classify_profits a, classify b
                where end_date="{date}" and a.code=b.code'''
    if lv is not None:
        sql += f' and length(a.code)={lv * 2}'
    return pd.read_sql(sql, engine)


def readClassifyProfitInc(codes, startDate, endDate):
    """
    读一段时期的行业利润增长率
    :rtype: DataFrame
    :param codes: list 行业代码
    :param startDate:
    :param endDate:
    """
    assert isinstance(codes, list), 'codes 应为list类型'
    incdf = None
    for code in codes:
        df = _readClassifyProfitInc(code, startDate, endDate)
        df.rename(columns={'inc': code}, inplace=True)
        if incdf is None:
            incdf = df.copy()
        else:
            incdf = incdf.merge(df, on='end_date', how='outer')
    return incdf


def _readClassifyProfitInc(code, startDate, endDate):
    sql = f'''select end_date, inc from classify_profits
                where code="{code}" and
                    end_date>="{startDate}" and end_date<="{endDate}"
            '''
    return pd.read_sql(sql, engine)

# def getGuzhiList():
#     #     sql = ('select guzhiresult.ts_code, stocklist.name '
#     #            'from guzhiresult, stocklist '
#     #            'where guzhiresult.ts_code=stocklist.ts_code')
#     sql = 'select ts_code from youzhiguzhi'
#     result = engine.execute(sql)
#     return [ts_code[0] for ts_code in result.fetchall()]


#     return result.fetchall()


# def getYouzhiList():
#     sql = 'select ts_code from youzhiguzhi'
#     result = engine.execute(sql)
#     return [ts_code[0] for ts_code in result.fetchall()]


def readStockName(ts_code):
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


def setUpdate(dataName, _date=None):
    """

    :param dataName:
    :param _date: str YYYYmmdd
    """
    # 更新最后更新日期
    if _date is None:
        _date = dt.datetime.today().strftime('%Y%m%d')
    # sql = f'update update_date set `date`="{_date}" where dataname="{dataName}"'
    sql = f'replace into update_date(`dataname`, `date`) values("{dataName}", "{_date}")'
    engine.execute(sql)


def readTableFields(table):
    sql = (f'select column_name from information_schema.columns'
           f' where table_name="{table}"')
    result = engine.execute(sql).fetchall()
    return ','.join([s[0] for s in result])


def readProfitInc(startDate, endDate=None, ptype='stock',
                  code=None, reportType='quarter'):
    """ 股票TTM利润增长率,按季或按年返回数个报告期增长率
    :param ptype: str, stock股票, classify行业
    :param reportType: str, quarter季报， year年报
    :param code:
    :param startDate:
    :param endDate:
    :return:
    """
    if ptype == 'stock':
        table = 'ttmprofits'
        field = 'ts_code'
    else:
        table = 'classify_profits'
        field = 'code'
    if endDate is None:
        endDate = startDate
    df = None
    dates = quarterList(startDate, endDate, reportType=reportType)
    import copy
    for index, date in enumerate(dates):
        sql = (f'select {field}, inc from {table} '
               f'where end_date="{date}";')
        _df = pd.read_sql(sql, engine)
        if isinstance(code, str):
            _df = _df[_df[f'{field}'] == code]
        elif isinstance(code, list):
            _df = _df[_df[f'{field}'].isin(code)]

        if _df.inc.isna().all():
            continue

        _df.rename(columns={'inc': f'inc{date}'}, inplace=True)
        if df is None:
            df = copy.deepcopy(_df)
        else:
            df = df.merge(_df, on=f'{field}', how='left')

    return df


def readProfit(startDate, endDate=None, ptype='stock',
                  code=None, reportType='quarter'):
    """ 股票TTM利润增长率,按季或按年返回数个报告期增长率
    :param ptype: str, stock股票, classify行业
    :param reportType: str, quarter季报， year年报
    :param code:
    :param startDate:
    :param endDate:
    :return:
    """
    if ptype == 'stock':
        table = 'ttmprofits'
        codefield = 'ts_code'
        profitfield = 'ttmprofits'
    else:
        table = 'classify_profits'
        codefield = 'code'
        profitfield = 'profits'
    if endDate is None:
        endDate = startDate
    df = None
    dates = quarterList(startDate, endDate, reportType=reportType)
    import copy
    for index, date in enumerate(dates):
        sql = (f'select {codefield}, {profitfield} from {table} '
               f'where end_date="{date}";')
        _df = pd.read_sql(sql, engine)
        _df.rename(columns={f'{profitfield}': f'profit{date}'}, inplace=True)
        if isinstance(code, str):
            _df = _df[_df[f'{codefield}'] == code]
        elif isinstance(code, list):
            _df = _df[_df[f'{codefield}'].isin(code)]


        if df is None:
            df = copy.deepcopy(_df)
        else:
            df = df.merge(_df, on=f'{codefield}')

    return df
