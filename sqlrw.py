# -*- coding: utf-8 -*-
"""
Created on Thu Mar 10 21:11:51 2016

@author: who8736
"""

import os
import logging
import urllib2
import socket
import datetime as dt
import ConfigParser
import re
import time
# import sys


from lxml import etree
import lxml
import tushare as ts  # @UnresolvedImport
import pandas as pd
from pandas.compat import StringIO
from sqlalchemy import create_engine, MetaData, Table, Column
from sqlalchemy import DATE, DECIMAL, String
from sqlalchemy.orm import sessionmaker, scoped_session
import sqlalchemy
from pandas.core.frame import DataFrame
from tushare.stock import cons as ct

import datatrans
import initsql
# from bokeh.sampledata import stocks


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


def downHYToSQL(retryMax=3):
    """ 下载行业与股票对应关系并写入数据库
    """
    url = u'http://www.csindex.com.cn/sseportal/ps/zhs/hqjt/csi/ZzhyflWz.xls'
    for _ in range(retryMax):
        try:
            df = pd.read_excel(url, 0,
                               parse_cols=[0, 13],
                               names=['stockid', 'hyid'],
                               converters={0: str, 1: str})
        except TypeError:
            logging.warning('down hangye data fail, retry...')
            continue
        if not df.empty:
            for index, row in df.iterrows():
                stockID = row[0]
                hyID = row[1]
                sql = (('replace into hangyestock (stockid, hyid) '
                        'values("%(stockID)s", "%(hyID)s")') % locals())
                engine.execute(sql)
            return


def downHYNameToSQL(retryMax=3):
    url = u'http://www.csindex.com.cn/sseportal/ps/zhs/hqjt/csi/ZzhyflWz.xls'
    for _ in range(retryMax):
        try:
            df = pd.read_excel(url,
                               0,
                               parse_cols=[4, 5, 7, 8, 10, 11, 13, 14],
                               converters={0: unicode, 1: unicode,
                                           2: unicode, 3: unicode,
                                           4: unicode, 5: unicode,
                                           6: unicode, 7: unicode})
        except TypeError:
            logging.warning('down hangye data fail, retry...')
            continue
        else:
            if not df.empty:
                return writeHYNameToSQL(df)
    logging.error('down hangye data fail.')
    return False


def writeHYNameToSQL(df):
    dflist = []
    for i in range(4):
        _df = df.iloc[:, i * 2:(i + 1) * 2]
        _df = _df.drop_duplicates()
#         print _df.head()
        _df.columns = ['hyid', 'hyname']
#         print _df.head()
        dflist.append(_df)
    hdDf = pd.concat(dflist)
    hdDf['hylevel'] = hdDf.hyid.str.len()
    hdDf['hylevel'] = hdDf.hylevel / 2

    hdDf['hylevel1id'] = hdDf.hyid.str[:2]
    hdDf['hylevel2id'] = hdDf.hyid.str[:4]
    hdDf['hylevel3id'] = hdDf.hyid.str[:6]

    print hdDf
    engine.execute('TRUNCATE TABLE hangyename')
    writeSQL(hdDf, 'hangyename')
    return


def downGubenToSQL(stockID, retry=3, timeout=10):
    """下载单个股票股本数据写入数据库"""
    logging.debug('downGubenToSQL: %s', stockID)
    socket.setdefaulttimeout(timeout)
    gubenURL = urlGuben(stockID)
    req = getreq(gubenURL)
#     downloadStat = False
    gubenDf = pd.DataFrame()
    proxy_handler = urllib2.ProxyHandler({"http": 'http://127.0.0.1:8087'})
    opener = urllib2.build_opener(proxy_handler)
    urllib2.install_opener(opener)
    for _ in range(retry):
        try:
            guben = urllib2.urlopen(req).read()
        except IOError, e:
            logging.warning('[%s]:download %s guben data, retry...',
                            e, stockID)
#             print type(e)
            errorString = '%s' % e
            if errorString == 'HTTP Error 456: ':
                print 'sleep 60 seconds...'
                time.sleep(60)
        else:
            gubenDf = datatrans.gubenDataToDf(stockID, guben)
            tablename = 'guben'
            lastUpdate = getGubenLastUpdateDate(stockID)
            gubenDf = gubenDf[gubenDf.date > lastUpdate]
            if not gubenDf.empty:
                writeSQL(gubenDf, tablename)
            return
    logging.error('fail download %s guben data.', stockID)


def downGuzhiToSQL(stockID, retry=20, timeout=10):
    """下载单个股票估值数据写入数据库"""
    logging.debug('downGuzhiToSQL: %s', stockID)
    url = urlGuzhi(stockID)
    data = downloadData(url)
    if data is None:
        logging.error('down %s guzhi data fail.', stockID)
        return False

    # 保存至文件
    filename = filenameGuzhi(stockID)
    try:
        mainFile = open(filename, 'wb')
        mainFile.write(data)
    except IOError, e:
        logging.error('[%s]写文件失败： %s', e, filename)
#         return False
    finally:
        mainFile.close()

    # 写入数据库
    guzhiDict = datatrans.transGuzhiDataToDict(data)
    if guzhiDict is None:
        return True
    # print guzhiDict
#     guzhiDf = DataFrame(guzhiDict, index=[0])
#     writeSQLUpdate(guzhiDict, 'guzhi')
#     print guzhiDict
    tablename = 'guzhi'
    if 'peg' in guzhiDict.keys():
        peg = guzhiDict['peg']
    else:
        peg = 'null'
    if 'next1YearPE' in guzhiDict.keys():
        next1YearPE = guzhiDict['next1YearPE']
    else:
        next1YearPE = 'null'
    if 'next2YearPE' in guzhiDict.keys():
        next2YearPE = guzhiDict['next2YearPE']
    else:
        next2YearPE = 'null'
    if 'next3YearPE' in guzhiDict.keys():
        next3YearPE = guzhiDict['next3YearPE']
    else:
        next3YearPE = 'null'
    sql = (('replace into %(tablename)s'
            '(stockid, peg, next1YearPE, next2YearPE, next3YearPE) '
            'values("%(stockID)s", %(peg)s, '
            '%(next1YearPE)s, %(next2YearPE)s, '
            '%(next3YearPE)s);') % locals())
    return engine.execute(sql)


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
        else:
            if (df is None) or df.empty:
                return
            tableName = tablenameKline(stockID)
            if not initsql.existTable(tableName):
                initsql.createKlineTable(stockID)
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
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
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
    req = getreq(url)
#     proxy = urllib2.ProxyHandler({'http': '127.0.0.1:8087'})
#     opener = urllib2.build_opener(proxy)
#     urllib2.install_opener(opener)
    text = urllib2.urlopen(req, timeout=30).read()
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
    df = df.set_index('code')
    return df


def updateStockList(retry=10):
    sl = pd.DataFrame()
    for _ in range(retry):
        try:
            sl = ts.get_stock_basics().fillna(value=0)
#             sl = getStockBasicsFromCSV().fillna(value=0)
        except socket.timeout:
            logging.warning('updateStockList timeout!!!')
        else:
            logging.debug('updateStockList ok')
            break
    if sl.empty:
        logging.error('updateStockList fail!!!')
        return False
    sl.index.name = 'stockid'
    clearStockList()
    sl.to_sql(u'stocklist',
              engine,
              if_exists=u'append')


def dropTable(tableName):
    engine.execute('DROP TABLE %s' % tableName)


def dropNAData():
    """ 清除K线图数据中交易量为0的数据
    """
    stockList = readStockIDsFromSQL()
#     stockList = ['002100']
    for stockID in stockList:
        tablename = tablenameKline(stockID)
        sql = 'delete from %(tablename)s where volume=0;' % locals()
        engine.execute(sql)


def getLowPEStockList(maxPE=40):
    """选取指定范围PE的股票
    maxPE: 最大PE
    """
    sql = 'select stockid, pe from stocklist where pe > 0 and pe <= %s' % maxPE
    df = pd.read_sql(sql, engine)
    return df


def get_report_data(year, quarter):
    """
        获取业绩报表数据
    Parameters
    --------
    year:int 年度 e.g:2014
    quarter:int 季度 :1、2、3、4，只能输入这4个季度
       说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度

    Return
    --------
    DataFrame
        code,代码
        name,名称
        eps,每股收益
        eps_yoy,每股收益同比(%)
        bvps,每股净资产
        roe,净资产收益率(%)
        epcf,每股现金流量(元)
        net_profits,净利润(万元)
        profits_yoy,净利润同比(%)
        distrib,分配方案
        report_date,发布日期
    """
    if ct._check_input(year, quarter) is True:
        ct._write_head()
        df = _get_report_data(year, quarter, 1, pd.DataFrame())
        if df is not None:
            #             df = df.drop_duplicates('code')
            df['code'] = df['code'].map(lambda x: str(x).zfill(6))
        return df


def _get_report_data(year, quarter, pageNo, dataArr,
                     retry_count=10, timeout=20):
    ct._write_console()
    for _ in range(retry_count):
        url = ct.REPORT_URL % (ct.P_TYPE['http'], ct.DOMAINS['vsf'],
                               ct.PAGES['fd'], year, quarter,
                               pageNo, ct.PAGE_NUM[1])
#         url = ('http://vip.stock.finance.sina.com.cn/q/go.php/'
#                'vFinanceAnalyze/kind/mainindex/index.phtml?'
#                's_i=&s_a=&s_c=&reportdate=%s&quarter=%s&p=1&num=60' %
#                (year, quarter))
#         request = getreq(url)
        request = urllib2.Request(url)
#         print
#         print url
        try:
            text = urllib2.urlopen(request, timeout=timeout).read()
#             print repr(text)
#             print text
        except IOError, e:
            logging.warning('[%s] fail to down, retry: %s', e, url)
        else:
            text = text.decode('GBK')
            text = text.replace('--', '')
            html = lxml.html.parse(StringIO(text))  # @UndefinedVariable
            res = html.xpath("//table[@class=\"list_table\"]/tr")
            if ct.PY3:
                sarr = [etree.tostring(node).decode('utf-8') for node in res]
            else:
                sarr = [etree.tostring(node) for node in res]
            sarr = ''.join(sarr)
            sarr = '<table>%s</table>' % sarr
#             print sarr
            df = pd.read_html(sarr)[0]
            df = df.drop(11, axis=1)
            df.columns = ct.REPORT_COLS
            dataArr = dataArr.append(df, ignore_index=True)
            xpathStr = '//div[@class=\"pages\"]/a[last()]/@onclick'
            nextPage = html.xpath(xpathStr)
            if len(nextPage) > 0:
                pageNo = re.findall(r'\d+', nextPage[0])[0]
                return _get_report_data(year, quarter, pageNo, dataArr)
            else:
                return dataArr


def getAllTableName(tableName):
    result = engine.execute('show tables like %s', tableName)
    return result.fetchall()


def clearStockList():
    engine.execute('TRUNCATE TABLE stocklist')


def getKlineLastUpdateDate(stockID):
    tablename = tablenameKline(stockID)
    if not initsql.existTable(tablename):
        initsql.createKlineTable(stockID)
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')
    sql = 'select max(date) from %s' % tablename
    return getLastUpdate(sql)


def getGubenLastUpdateDate(stockID):
    sql = 'select max(date) from guben where stockid="%s" limit 1;' % stockID
    return getLastUpdate(sql)


def getGuzhi(stockID):
    sql = 'select * from guzhiresult where stockid="%s" limit 1' % stockID
    result = engine.execute(sql)
    return result.fetchone()


def readStockListFromSQL():
    result = engine.execute('select stockid, name from stocklist')
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
        name = i[1].encode('utf-8')
#         print type(i[1]), i[1], name
#         name = i[1]
        stockNames.append(name)
    stockListDf = DataFrame({'stockid': stockIDs,
                             'name': stockNames})
    return stockListDf


def updateKlineMarketValue(stockID, startDate='1990-01-01', endDate=None):
    gubenDf = readGuben(stockID, startDate)
    klineTablename = 'kline%s' % stockID
    gubenCount = gubenDf['date'].count()

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


def updateKlineTTMLirun(stockID, startDate='1990-01-01', endDate='2099-12-31'):
    """
    a更新Kline表TTM利润
    """
    TTMLirunDf = readTTMLirunForStockID(stockID, startDate)
    klineTablename = 'kline%s' % stockID
    TTMLirunCount = TTMLirunDf['date'].count()

    for i in range(TTMLirunCount):
        startDate = TTMLirunDf['reportdate'][i]
        try:
            endDate = TTMLirunDf['reportdate'][i + 1]
        except KeyError:
            endDate = None
        TTMLirun = TTMLirunDf['ttmprofits'][i]

        sql = 'update %s set ttmprofits = %s' % (klineTablename, TTMLirun)
        sql += ' where date >= "%s"' % startDate
        if endDate:
            sql += ' and date < "%s"' % endDate
        engine.execute(sql)


def writeLirun(df):
    tablename = 'lirun'
    return writeSQL(df, tablename)


def writeGuben(stockID, df):
    logging.debug('start writeGuben %s' % stockID)
    timea = dt.datetime.now()
    gubenList = datatrans.gubenDfToList(df)
    if not gubenList:
        return False

    tableName = 'guben'
    if not initsql.existTable(tableName):
        initsql.createGubenTable(stockID)

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


def writeSQL(data, tableName, insertType='IGNORE'):
    """insetType: IGNORE 忽略重复主键；

    """
    logging.debug('start writeSQL %s' % tableName)

    if not initsql.existTable(tableName):
        logging.error('not exist %s' % tableName)
        return False

    if isinstance(data, DataFrame):
        if data.empty:
            return True
        data = data.where(pd.notnull(data), None)
        data = datatrans.transDfToList(data)

    if not data:
        return True

    session = Session()
    metadata = MetaData(bind=engine)
    mytable = Table(tableName, metadata, autoload=True)
    session.execute(mytable.insert().prefix_with(insertType), data)
    session.commit()
    session.close()
    return True


# def writeSQLUpdate(data, tableName):
#     """insetType: IGNORE 忽略重复主键；
#
#     """
#     logging.debug('start writeSQL %s' % tableName)
#
#     if not initsql.existTable(tableName):
#         logging.error('not exist %s' % tableName)
#         return False
#
#     if isinstance(data, DataFrame):
#         if data.empty:
#             return True
# #         data = datatrans.transDfToList(data)
#
#     if data is None:
#         return True
#
#     session = Session()
#     metadata = MetaData(bind=engine)
#     mytable = Table(tableName, metadata, autoload=True)
#     mytable(data)
#     session.add(mytable)
# #     session.execute(mytable.replace(), data)
#     # session.merge(data)
#     session.commit()
#     session.close()
#     return True


def writeStockIDListToFile(stockIDList, filename):
    stockFile = open(filename, 'wb')
    stockFile.write('\n'.join(stockIDList))
    stockFile.close()


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

    # 缺点： 数据中无准确的报表发布日期
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
#     df = ts.get_report_data(year, quarter)
    # 因tushare的利润下载函数不支持重试和指定超时值，使用下面的改进版本
    df = get_report_data(year, quarter)
    if df is None:
        return None
    df = df.loc[:, ['code', 'net_profits', 'report_date']]
    return datatrans.transLirunDf(df, year, quarter)


def getreq(url, includeHeader=False):
    if includeHeader:
        # headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; Windows NT 6.1; '
        #                           'en-US;rv:1.9.1.6) Gecko/20091201 '
        #                           'Firefox/3.5.6')}
        headers = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; WOW64; '
                                  'rv:49.0) Gecko/20100101 Firefox/49.0'),
                   'Accept': ('text/html,application/xhtml+xml,'
                              'application/xml;q=0.9,*/*;q=0.8'),
                   'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                   'Connection': 'keep-alive',
                   'DNT': '1',
                   'Upgrade-Insecure-Requests': '1',
                   }
        return urllib2.Request(url, headers=headers)
    else:
        return urllib2.Request(url)


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
        return datatrans.gubenDataToDf(stockID, guben)


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
    return datatrans.gubenDataToDf(stockID, guben)


def downGuzhiToFile(stockID):
    url = urlGuzhi(stockID)
    filename = filenameGuzhi(stockID)
    logging.debug('write guzhi file: %s', filename)
    return downloadDataToFile(url, filename)


def filenameGuzhi(stockID):
    return './data/guzhi/%s.xml' % stockID


def readGuzhiFileToDict(stockID):
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
    print df
    return df


# def downloadKline(stockID, startDate=None, endDate=None):
#     if startDate is None:  # startDate为空时取股票最后更新日期
#         startDate = getKlineLastUpdateDate(stockID)
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
    sql = (u'select pe200, pe1000 from guzhiresult '
           u'where stockid="%(stockID)s" limit 1' % locals())
    print sql
    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result is None:
        return (None, None)
    else:
        return result


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
    # 指定日期（含）前最近一次利润变动日期
    sql = (u'select max(reportdate) from ttmlirun '
           u'where stockid="%(stockID)s" '
           u'and reportdate<="%(startDate)s"' % locals())
    print sql
    # 指定日期（含）前无TTM利润数据的，查询起始日期设定为startDate
    # 否则设定为最近一次数据日期
    result = engine.execute(sql).fetchone()
    if result[0] is not None:
        startDate = result[0]
    sql = (u'select * from ttmlirun where stockid = "%(stockID)s"'
           u' and `reportdate` >= "%(startDate)s"' % locals())
    print sql
    if endDate is not None:
        sql += u' and `date` <= "%s"' % endDate
    df = pd.read_sql(sql, engine)

    # 指定日期（含）前存在股本变动数据的，重设第1次变动日期为startDate，
    # 减少更新Kline表中总市值所需计算量
#     if TTMLirunStartDate != startDate:
#         df.loc[df.reportdate == TTMLirunStartDate,
#                u'reportdate'] = startDate
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
    sql = (u'select incrate from ttmlirun '
           u'where stockid="%(stockID)s" '
           u'order by date desc '
           u'limit %(limit)s' % locals())
#     print sql
    result = engine.execute(sql).fetchall()
    result = [i[0] for i in reversed(result)]
    return result

#     df = pd.read_sql(sql, engine)
#     return df


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
    sql = (u'select * from ttmlirun where '
           u'`date` = "%(date)s"' % locals())
    df = pd.read_sql(sql, engine)
    return df


def readLirunForDate(date):
    """从Lirun表读取一期股票利润
    date: 格式YYYYQ, 4位年+1位季度，利润所属日期
    return: 返回DataFrame格式利润
    """
    sql = (u'select * from lirun where '
           u'`date` = "%(date)s"' % locals())
    df = pd.read_sql(sql, engine)
    return df


def readTTMPE(stockID):
    sql = 'select date, ttmpe from kline%s' % stockID
    df = pd.read_sql(sql, engine)
    return df


def readCurrentTTMPE(stockID):
    sql = ('select ttmpe from kline%(stockID)s '
           'where kline%(stockID)s.date=('
           'select max(`date`) from kline%(stockID)s'
           ')' % locals())

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


def alterKline():
    sql = 'show tables like %s'
    result = engine.execute(sql, 'kline%')
    result = result.fetchall()

    for i in result:
        tablename = i[0]
        sql = 'call stockdata.alterkline(%s)'
        try:
            result = engine.execute(sql, tablename)
#             result = result.fetchall()
            print tablename
        except sqlalchemy.exc.OperationalError, e:
            print e
    return


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
#         return writeSQL(TTMLirun, 'ttmlirun')
    else:
        if incrementUpdate:
            TTMLirunCur = readTTMLirunForDate(date)
            lirunCur = lirunCur[~lirunCur.stockid.isin(TTMLirunCur.stockid)]

        # 上年第四季度利润, 仅取利润字段并更名为profits1
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
    TTMLirunLastYear = TTMLirunLastYear[['stockid', 'ttmprofits']]
    TTMLirunLastYear.columns = ['stockid', 'ttmprofits1']

    # 整合以上2个表，stockid为整合键
    TTMLirunCur = pd.merge(TTMLirunCur, TTMLirunLastYear, on='stockid')

    TTMLirunCur['incrate'] = ((TTMLirunCur.ttmprofits -
                               TTMLirunCur.ttmprofits1) /
                              abs(TTMLirunCur.ttmprofits1) * 100)
    for i in TTMLirunCur.values:
        stockID = i[0]
        incRate = round(i[4], 2)
        sql = (u'update ttmlirun '
               u'set incrate = %(incRate)s'
               u' where stockid = "%(stockID)s"'
               u'and `date` = %(date)s' % locals())
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
    logging.debug('updateKlineEXTData: %s', stockID)
    if startDate is None:
        startDate = getTTMPELastUpdate(stockID)
        startDate = startDate.strftime('%Y-%m-%d')
    updateKlineMarketValue(stockID, startDate)
    updateKlineTTMLirun(stockID, startDate)
    endDate = updateKlineTTMPE(stockID, startDate)
    setKlineTTMPELastUpdate(stockID, endDate)


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
    result = engine.execute(sql)
    lastQuarter = result.first()[0]
    startQuarter = datatrans.quarterAdd(lastQuarter, 1)
    return startQuarter


def getLirunUpdateEndQuarter():
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
    if lastUpdateDate is None:
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')

    lastUpdateDate = lastUpdateDate[0]
    if isinstance(lastUpdateDate, dt.date):
        return lastUpdateDate + dt.timedelta(days=1)
    else:
        logging.debug('lastUpdateDate is: %s', type(lastUpdateDate))
        return dt.datetime.strptime('1990-01-01', '%Y-%m-%d')


def saveChigu(stockList):
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
        #            u'from kline%(stockID)s '
        #            u'where `date`>="%(startDate)s";') % locals()
        #
        #     engine.execute(sql)

        TTMLirunDf = readTTMLirunForStockID(stockID, startDate)
        TTMLirunDf = TTMLirunDf.dropna().reset_index(drop=True)
        klineTablename = 'kline%s' % stockID
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


def updateKlineTTMPE(stockID, startDate='1990-01-01', endDate=None):
    """
    # 更新Kline表TTMPE
    """
    #     engine = getEngine()
    klineTablename = 'kline%s' % stockID
    sql = ('update %(klineTablename)s '
           'set ttmpe = round(totalmarketvalue / ttmprofits, 2)'
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


def updateLirun():
    startQuarter = getLirunUpdateStartQuarter()
    endQuarter = getLirunUpdateEndQuarter()

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
#         print len(df)
        # 读取已存储的利润数据，从下载数据中删除该部分，对未存储的利润写入数据库
        lirunCur = readLirunForDate(date)
        df = df[~df.stockid.isin(lirunCur.stockid)]
        df = df[df.profits.notnull()]
#         print df

        # 对未存储的利润写入数据库，并重新计算TTM利润
        if not df.empty:
            writeLirun(df)
            calAllTTMLirun(date)


def urlMainTable(stockID, tableType):
    url = ('http://money.finance.sina.com.cn/corp/go.php'
           '/vDOWN_%(tableType)s/displaytype/4'
           '/stockid/%(stockID)s/ctrl/all.phtml' % locals())
    return url


def filenameMainTable(stockID, tableType):
    filename = './data/%s/%s.csv' % (tableType, stockID)
    return filename


def downloadMainTable(stockID):
    mainTableType = ['BalanceSheet', 'ProfitStatement', 'CashFlow']
    for tableType in mainTableType:
        url = urlMainTable(stockID, tableType)
        filename = filenameMainTable(stockID, tableType)
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
    return './data/guben/%s.csv' % stockID


def filenameLirun(stockID):
    filename = './data/ProfitStatement/%s.csv' % stockID
    return filename


def urlGuzhi(stockID):
    '''
    估值数据文件下载地址
    '''
    url = 'http://f9.eastmoney.com/soft/gp72.php?code=%s' % stockID
    if stockID[0] == '6':
        url += '01'
    else:
        url += '02'
    return url


def downloadData(url, timeout=10, retry_count=10):
    for _ in range(retry_count):
        try:
            socket.setdefaulttimeout(timeout)
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
    return None


def downloadDataToFile(url, filename, timeout=10, retry_count=10):
    data = downloadData(url, timeout, retry_count)
    if not data:
        return False
    try:
        mainFile = open(filename, 'wb')
        mainFile.write(data)
    except IOError, e:
        logging.error('[%s]写文件失败： %s', e, filename)
        return False
    else:
        mainFile.close()
    return True


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
    # 指定日期（含）前最近一次股本变动日期
    sql = (u'select max(date) from guben '
           u'where stockid="%(stockID)s" '
           u'and date<="%(startDate)s"' % locals())
    result = engine.execute(sql)
    lastUpdate = result.fetchone()[0]

    # 指定日期（含）前无股本变动数据的，查询起始日期设定为startDate
    # 否则设定为最近一次变动日期
    if lastUpdate is None:
        gubenStartDate = startDate
    else:
        gubenStartDate = lastUpdate

    sql = (u'select date, totalshares from guben '
           u'where stockid = "%(stockID)s"'
           u' and `date` >= "%(gubenStartDate)s"' % locals())
    if endDate:
        sql += u' and `date` <= "%s"' % endDate
    df = pd.read_sql(sql, engine)

    # 指定日期（含）前存在股本变动数据的，重设第1次变动日期为startDate，
    # 减少更新Kline表中总市值所需计算量
    if lastUpdate is not None:
        df.loc[df.date == gubenStartDate, 'date'] = startDate
    return df


def readClose(stockID):
    sql = 'select date, close from kline%s' % stockID
    df = pd.read_sql(sql, engine)
    return df


def readCurrentClose(stockID):
    sql = ('select close from kline%(stockID)s '
           'where kline%(stockID)s.date=('
           'select max(`date`) from kline%(stockID)s'
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


def downloadClassified():
    """ 旧版下载行业分类， 计划删除本函数
    """
    classifiedDf = ts.get_industry_classified(standard='sw')
    classifiedDf = classifiedDf[['code', 'c_name']]
    classifiedDf.columns = ['stockid', 'cname']
    return classifiedDf


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
#     return result.fetchall()


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


def test():
    stockList = getChiguList()

    stockReportList = []
    for stockID in stockList:
        stockName = getStockName(stockID)
        stockClose = readCurrentClose(stockID)
        pe = readCurrentTTMPE(stockID)
        peg = readCurrentPEG(stockID)
        stockReportList.append([stockID, stockName,
                                stockClose, pe, peg])
    print stockReportList


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
#     updateLirun()

# 更新TTM利润信息
#     date = 20161
#     result = downloadTTMLirun(date)
#     for date in datatrans.dateList(20143, 20153):
#         print date
#         result = calAllTTMLirun(date)
#     print result
#     print calAllTTMLirun(20164)

# 更新TTM市盈率
#     updateKlineEXTData(testStockID, startDate)
#     updateKlineEXTData(testStockID)
#     updateKlineEXTData('600519')

# 更新股票列表
#     createStockList()
#     updateStockList()

#     url = 'http://218.244.146.57/static/all.csv'
#     filename = './stocklisttest.csv'
#     downloadDataToFile(url, filename)
#     df = getStockBasicsFromCSV()
#     print df.head(5)

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
#     stockClose = readCurrentClose(testStockID)
#     peg = readCurrentPEG(testStockID)
#     print testStockID, stockClose, pe, peg
#     test()

#     pe = readCurrentTTMPEs(testStockList)
#     print pe

# 取股票最后更新日期
#     getKlineLastUpdateDate(stockID)

# 行业分类
# standard: sina 新浪行业， sw 申万行业
#     t = downloadClassified()
#     print t
#     downHYToSQL()
    downHYNameToSQL()


# 生成pelirunincrease表的数据
#     savePELirunIncrease()

# 取最近几期TTM利润数据
#     df = readLastTTMLirunForStockID('600519', 6)
#     print df
#     stockList = ['000001', '600000', '600519']
#     limit = 6
#     TTMLirunList = readLastTTMLirun(stockList, limit)
#     print TTMLirunList

# 清除K线图数据中交易量为0的数据
#     dropNAData()

    timed = dt.datetime.now()
    logging.info('datamanage test took %s' % (timed - timec))
    logging.info('===================end=====================')
#     print timeb - timea
#     print timea
#     print timec
#     print timed
