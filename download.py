# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 21:12:40 2017

@author: who8736

# 各类下载功能
"""

import logging
import zipfile
import socket
from urllib import request
from requests.exceptions import ConnectTimeout
import time
import re

import tushare
from lxml import etree
import lxml
import datetime as dt
# from datetime import datetime, timedelta

import tushare as ts
import pandas as pd
from pandas.core.frame import DataFrame
# from pandas.compat import StringIO
from io import StringIO
from tushare.stock import cons as ct
import baostock as bs

from misc import urlGubenSina, tsCode
# from misc import urlGubenEastmoney
from misc import urlGuzhi, urlMainTable
# from misc import filenameGuben
from misc import filenameMainTable, filenameGuzhi
from misc import longts_code, tsCode
# import datatrans
from datatrans import dateStrList, gubenDataToDfSina
# import hyanalyse
import sqlrw
from sqlrw import engine, readStockList, writeSQL
# from sqlrw import getStockKlineUpdateDate, lirunFileToList
from sqlrw import writeSQL
# from sqlrw import writeStockList
from sqlrw import writeHYToSQL, writeHYNameToSQL
# from sqlrw import gubenUpdateDate
from sqlrw import writeGubenToSQL
from initlog import initlog
from misc import tsCode


class DownloaderQuarter:
    """限时下载器

    :param tablename:
    :param stocks:
    :param perTimes:
    :param downLimit:
    :return:
    """

    tables = ['balancesheet', 'income', 'cashflow', 'fina_indicator']
    # 调用时间限制，如60秒调用80次
    perTimes = {'balancesheet': 60,
                'income': 60,
                'cashflow': 60,
                'fina_indicator': 60}
    # 一定时间内调用次数限制
    limit = {'balancesheet': 80,
             'income': 80,
             'cashflow': 80,
             'fina_indicator': 60}
    # 下载开始时间，按表格保存不同的时间
    times = {'balancesheet': [],
             'income': [],
             'cashflow': [],
             'fina_indicator': []}
    # 记录当前调用某个表的累计次数
    curcall = {'balancesheet': 0,
               'income': 0,
               'cashflow': 0,
               'fina_indicator': 0}

    def __init__(self, ts_code, period, retry=3):
        pass
        self.ts_code = ts_code
        self.period = period
        self.retry = retry

    # 每个股票一个下载器，下载第一张无数据时可跳过其他表
    # 下载限制由类静态成员记载与控制
    def run(self):
        pass
        for table in DownloaderQuarter.tables:
            result = pd.DataFrame()
            perTimes = DownloaderQuarter.perTimes[table]
            limit = DownloaderQuarter.limit[table]
            cur = DownloaderQuarter.curcall[table]
            for _ in range(self.retry):
                nowtime = dt.datetime.now()
                if (perTimes > 0 and limit > 0 and cur >= limit
                        and (nowtime < DownloaderQuarter.times[table][
                            cur - limit]
                             + dt.timedelta(seconds=perTimes))):
                    _timedelta = nowtime - DownloaderQuarter.times[table][
                        cur - limit]
                    sleeptime = DownloaderQuarter.perTimes[
                                    table] - _timedelta.seconds
                    print(f'******暂停{sleeptime}秒******')
                    time.sleep(sleeptime)
                try:
                    result = downStockQuarterData(table,
                                                  self.ts_code, self.period)
                except(socket.timeout, ConnectTimeout):
                    logging.warning(f'downloader timeout: '
                                    f'{table}-{self.ts_code}-{self.period}')
                else:
                    if result.empty:
                        return
                    else:
                        break
                finally:
                    nowtime = dt.datetime.now()
                    DownloaderQuarter.times[table].append(nowtime)
                    DownloaderQuarter.curcall[table] += 1


class DownloaderMisc:
    """限时下载器, 不定期更新

    :param tablename:
    :param stocks:
    :param perTimes:
    :param downLimit:
    :return:
    """

    def __init__(self, perTimes, limit, retry=3):
        pass
        self.perTimes = perTimes
        self.limit = limit
        self.retry = retry
        self.cur = 0
        self.times = []

    # 下载限制由类静态成员记载与控制
    def run(self, table, **kwargs):
        pass
        pro = ts.pro_api()
        fun = getattr(pro, table)
        for _ in range(self.retry):
            nowtime = dt.datetime.now()
            if (self.cur >= self.limit
                    and (nowtime < self.times[self.cur - self.limit]
                         + dt.timedelta(seconds=self.perTimes))):
                _timedelta = nowtime - self.times[self.cur - self.limit]
                sleeptime = self.perTimes - _timedelta.seconds
                print(f'******暂停{sleeptime}秒******')
                time.sleep(sleeptime)
            try:
                result = fun(**kwargs)
            except(socket.timeout, ConnectTimeout):
                logging.warning(f'downloader timeout: {table}')
            else:
                return result
            finally:
                nowtime = dt.datetime.now()
                self.times.append(nowtime)
                self.cur += 1


def _run(self, fun, **kwargs):
    result = None
    for _ in range(self.retry):
        nowtime = dt.datetime.now()
        if (self.perTimes > 0 and self.downLimit > 0
                and self.cur >= self.downLimit
                and (nowtime < self.times[self.cur - self.downLimit]
                     + dt.timedelta(seconds=self.perTimes))):
            _timedelta = nowtime - self.times[self.cur - self.downLimit]
            sleeptime = self.perTimes - _timedelta.seconds
            print(f'******暂停{sleeptime}秒******')
            time.sleep(sleeptime)
        try:
            result = fun(**kwargs)
        except socket.timeout:
            logging.warning('downloader timeout:', fun.__name__)
        else:
            break
        finally:
            nowtime = dt.datetime.now()
            self.times.append(nowtime)
            self.cur += 1

    return result


def downStockQuarterData(table, ts_code, period):
    print(f'downStockQuarterData table:{table}, ts_code:{ts_code}')
    pro = ts.pro_api()
    fun = getattr(pro, table)
    df = fun(ts_code=ts_code, period=period)
    writeSQL(df, table)
    return df


# def downGubenToSQL(ts_code, retry=3, timeout=10):
#     """下载单个股票股本数据写入数据库"""
#     logging.debug('downGubenToSQL: %s', ts_code)
#     socket.setdefaulttimeout(timeout)
#     gubenURL = urlGuben(ts_code)
#     req = getreq(gubenURL)
#
#     gubenDf = pd.DataFrame()
#
#     # 使用代理抓取数据
# #     proxy_handler = urllib2.ProxyHandler({"http": 'http://127.0.0.1:8087'})
# #     opener = urllib2.build_opener(proxy_handler)
# #     urllib2.install_opener(opener)
#
#     for _ in range(retry):
#         try:
#             guben = urllib2.urlopen(req).read()
#         except IOError, e:
#             logging.warning('[%s]:download %s guben data, retry...',
#                             e, ts_code)
# #             print type(e)
#             errorString = '%s' % e
#             if errorString == 'HTTP Error 456: ':
#                 print 'sleep 60 seconds...'
#                 time.sleep(60)
#         else:
#             gubenDf = datatrans.gubenDataToDf(ts_code, guben)
#             tablename = 'guben'
#             lastUpdate = getGubenLastUpdateDate(ts_code)
#             gubenDf = gubenDf[gubenDf.date > lastUpdate]
#             if not gubenDf.empty:
#                 writeSQL(gubenDf, tablename)
#             return
#     logging.error('fail download %s guben data.', ts_code)


def downStockList(retry=10):
    """ 更新股票列表与行业列表
    """
    pro = ts.pro_api()
    df = pro.stock_basic()
    writeSQL(df, 'stock_basic')


def downHYList():
    """
    更新行业列表数据
    读取行业表中的股票代码，与当前获取的股票列表比较，
    如果存在部分股票未列入行业表，则更新行业列表数据
    """
    sql = ('select ts_code from stock_basic'
           ' where ts_code not in (select ts_code from classify_member)'
           ' and list_status="L" or list_status="P"')
    result = sqlrw.engine.execute(sql).fetchall()
    # 股票列表中上市日期不为0，即为已上市
    # 且不在行业列表中，表示需更新行业数据
    if result:
        HYDataFilename = downHYFile()
        writeHYToSQL(HYDataFilename)
        writeHYNameToSQL(HYDataFilename)


def downHYFile(timeout=10):
    """ 从中证指数网下载行业数据文件

    下载按钮的xpath
    /html/body/div[3]/div/div/div[1]/div[1]/form/a[1]
    """
    logging.debug('downHYFile')
    socket.setdefaulttimeout(timeout)

    # 获取当前可用下载日期
    gubenURL = ('http://www.csindex.com.cn/zh-CN/downloads/'
                'industry-price-earnings-ratio')
    req = getreq(gubenURL)
    htmlresult = request.urlopen(req).read()
    myTree = etree.HTML(htmlresult)
    dateStr = myTree.xpath('''//html//body//div//div//div//div
                                //div//form//label//input//@value''')
    # dateStr = myTree.xpath('//a[@id="link1"]/@href')
    dateStr = dateStr[0]
    #     print dateStr
    #     print dateStr.split('-')
    dateStr = ''.join(dateStr.split('-'))
    #     print dateStr

    # 下载并解压行业数据文件
    HYFileUrl = 'http://47.97.204.47/syl/csi%s.zip' % dateStr
    print(HYFileUrl)
    HYZipFilename = './data/csi%s.zip' % dateStr
    HYDataFilename = 'csi%s.xls' % dateStr
    HYDataPath = './data/csi%s.xls' % dateStr
    dataToFile(HYFileUrl, HYZipFilename)

    zfile = zipfile.ZipFile(HYZipFilename, 'r')
    open(HYDataPath, 'wb').write(zfile.read(HYDataFilename))
    return HYDataFilename


def getreq(url, includeHeader=False):
    if includeHeader:
        # headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; Windows NT 6.1; '
        #                           'en-US;rv:1.9.1.6) Gecko/20091201 '
        #                           'Firefox/3.5.6')}
        headers = {'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; WOW64; '
                                  'rv:49.0) Gecko/20100101 Firefox/49.0'),
                   'Accept': ('text/html,application/xhtml+xml,'
                              'application/xml;q=0.9,*/*;q=0.8'),
                   # 'Accept-Encoding': 'gzip, deflate',
                   'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3',
                   'Connection': 'keep-alive',
                   'DNT': '1',
                   'Upgrade-Insecure-Requests': '1',
                   }
        return request.Request(url, headers=headers)
    else:
        return request.Request(url)


def downloadData(url, timeout=10, retry_count=3):
    """ 通用下载函数
    """
    #         proxy_handler = urllib2.ProxyHandler({"http": 'http://127.0.0.1:8087'
    #                                               })
    #     opener = urllib2.build_opener(proxy_handler)
    #     urllib2.install_opener(opener)
    for _ in range(retry_count):
        try:
            socket.setdefaulttimeout(timeout)
            headers = {'User-Agent': ('Mozilla/5.0 (Windows; U; '
                                      'Windows NT 6.1;'
                                      'en-US;rv:1.9.1.6) Gecko/20091201 '
                                      'Firefox/3.5.6')}
            req = request.Request(url, headers=headers)
            content = request.urlopen(req).read()
        except IOError as e:
            logging.warning('[%s]fail to download data, retry url:%s',
                            e, url)
            time.sleep(1)
        else:
            return content
    logging.error('download data fail!!! url:%s', url)
    return None


def dataToFile(url, filename, timeout=10, retry_count=3):
    data = downloadData(url, timeout, retry_count)
    if not data:
        return False
    try:
        mainFile = open(filename, 'wb')
        mainFile.write(data)
    except IOError as e:
        logging.error('[%s]写文件失败： %s', e, filename)
        return False
    else:
        mainFile.close()
    return True


def del_downloadClassified():
    """ 旧版下载行业分类， 计划删除本函数
    """
    classifiedDf = ts.get_industry_classified(standard='sw')
    classifiedDf = classifiedDf[['code', 'c_name']]
    classifiedDf.columns = ['ts_code', 'cname']
    return classifiedDf

    # def downGubenOld(ts_code, retry=3, timeout=10):
    #     """ 本函数待废除，如果新的downGuben正常
    #         下载单个股票股本数据写入数据库
    #
    #     """
    #     # //*[@id="lngbbd_Table"]/tbody/tr[1]
    #     logging.debug('downGubenToSQL: %s', ts_code)
    #     print('downGubenToSQL: %s' % ts_code)
    #     socket.setdefaulttimeout(timeout)
    #     # gubenURL = urlGubenEastmoney(ts_code)
    #     gubenURL = urlGubenSina(ts_code)
    #     req = getreq(gubenURL)
    # #     downloadStat = False
    #     gubenDf = pd.DataFrame()
    #
    #     # 使用代理抓取数据
    # #     proxy_handler = urllib2.ProxyHandler({"http": 'http://127.0.0.1:8087'})
    # #     opener = urllib2.build_opener(proxy_handler)
    # #     urllib2.install_opener(opener)
    #
    #     for _ in range(retry):
    #         try:
    #             guben = urllib.request.urlopen(req).read()
    #         except IOError as e:
    #             logging.warning('[%s]:download %s guben data, retry...',
    #                             e, ts_code)
    # #             print type(e)
    #             errorString = '%s' % e
    #             if errorString == 'HTTP Error 456: ':
    #                 print('sleep 60 seconds...')
    #                 time.sleep(60)
    #         else:
    #             gubenDf = datatrans.gubenDataToDfSina(ts_code, guben)
    #             if sqlrw.writeGubenToSQL(gubenDf):
    #                 logging.debug('download %s guben data final.', ts_code)
    #                 return
    # #             return gubenDf
    #     logging.error('fail download %s guben data.', ts_code)
    #     return None

    # def downloadGuben(ts_code):
    # """ 下载股本并保存到文件， 确认为无用函数后可删除
    # """
    # url = urlGuben(ts_code)
    # filename = filenameGuben(ts_code)
    # return dataToFile(url, filename)


def downGuzhi_del(ts_code):
    """ 确认无用后可删除
    # 下载单个股票估值数据， 保存并返回估值数据
    """
    logging.debug('downGuzhiToSQL: %s', ts_code)
    url = urlGuzhi(ts_code)
    data = downloadData(url)
    if data is None:
        logging.error('down %s guzhi data fail.', ts_code)
        return None

    # 保存至文件
    filename = filenameGuzhi(ts_code)
    mainFile = open(filename, 'wb')
    try:
        mainFile.write(data)
    except IOError as e:
        logging.error('[%s]写文件失败： %s', e, filename)
    #         return False
    finally:
        mainFile.close()
    return data


def downChengfen180():
    """
    从中证指数网下载指数成人股列表，考虑tushare仅提供月度数据，本函数暂时保留
    :return:
    """
    url = 'http://www.csindex.com.cn/uploads/file/autofile/cons/000010cons.xls'
    filename = './data/000010cons.xls'
    if dataToFile(url, filename):
        df = pd.read_excel(filename)
        df1 = pd.DataFrame({'name': 'sse180', 'ts_code': df.iloc[:, 4]})
        writeSQL(df1, 'chengfen')


def del_downChengfen(ID, startDate, endDate=None):
    """
    下载指数成份股，月度数据
    :param ID:
    :param startDate:
    :param endDate:
    :return:
    """
    if endDate is None:
        endDate = dt.datetime.today().date()
    pro = ts.pro_api()
    # ID = ID + '.SH'
    df = pro.index_weight(index_code=ID,
                          # trade_date='20190105',
                          start_date=startDate.strftime('%Y%m%d'),
                          end_date=endDate.strftime('%Y%m%d'))
    # df['index_code'] = df.index_code.str[:6]
    # df['ts_code'] = df.con_code.str[:6]
    # df.rename(columns={'trade_date': 'date', }, inplace=True)
    # df = df[['index_code', 'ts_code', 'trade_date', 'weight']]

    print(df.head())
    writeSQL(df, 'chengfen')


def downGuzhi(ts_code):
    """ 下载单个股票估值数据， 保存并返回估值数据
    """
    url = urlGuzhi(ts_code)
    filename = filenameGuzhi(ts_code)
    logging.debug('write guzhi file: %s', filename)
    return dataToFile(url, filename)


def del_downKline(tradeDate):
    """
    使用tushare下载日交易数据
    :return:
    """
    strDate = tradeDate.strftime('%Y%m%d')
    logging.debug('downKline: %s', strDate)
    try:
        pro = ts.pro_api()
        df = pro.daily(trade_date=strDate)
        df['ts_code'] = df['ts_code'].str[:6]
        # df.date = (df['trade_date'].str[:4] + '-'
        #            + df['trade_date'].str[4:6] + '-'
        #            +df['trade_date'].str[6:])
        df['trade_date'] = df['trade_date'].map(lambda x:
                                                x[:4] + '-' + x[4:6] + '-' + x[
                                                                             6:])
        # df['treade_date'].map(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:])
        df.rename(columns={'trade_date': 'date', 'vol': 'volume'},
                  inplace=True)
        df = df[['ts_code', 'date', 'open', 'high', 'close', 'low', 'volume']]
        writeSQL(df, 'klinestock')
    except Exception as e:
        print(e)
        logging.error(e)


def __downKline(ts_code, startDate=None, endDate=None, retry_count=6):
    """
    待删除函数，改用tushare按日下载数据的方案
    下载单个股票K线历史写入数据库, 通过调用不同下载函数选择不同的数据源
    """
    logging.debug('download kline: %s', ts_code)

    # 数据源：　baostock
    # downKlineFromBaostock(ts_code, startDate, endDate, retry_count)

    # 数据源：　tushare
    # downKlineFromTushare(ts_code, startDate, endDate, retry_count)


def del_downKlineFromBaostock(ts_code, startDate=None,
                              endDate=None, retry_count=6):
    """下载单个股票K线历史写入数据库, 下载源为baostock
    :type ts_code: str
    :param startDate: date, 开始日期
    :param endDate: date, 结束日期
    :param retry_count:
    """
    # if startDate is None:  # startDate为空时取股票最后更新日期
    #     startDate = getStockKlineUpdateDate() + dt.timedelta(days=1)
    #         print ts_code, startDate
    startDate = startDate.strftime('%Y-%m-%d')
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
    #     retryCount = 0
    longID = longts_code(ts_code)

    for cur_retry in range(1, retry_count + 1):
        try:
            fieldsStr = "date,open,high,close,low,volume,peTTM,tradestatus"
            # fields = ['date', 'open', 'high', 'close', 'low', 'volume', 'ttmpe']
            rs = bs.query_history_k_data_plus(longID, fieldsStr, startDate,
                                              endDate)
            dataList = []
            while rs.next():
                dataList.append(rs.get_row_data())
            df = pd.DataFrame(dataList, columns=rs.fields)
            # assert isinstance(df, pd.DataFrame), 'df is not pd.DataFrame'
            df = df[df.tradestatus == '1'].copy()
            df['ts_code'] = ts_code
            df = df.rename({'peTTM': 'ttmpe'})
            df = df.iloc[0:, :-1]
            # df = ts.get_hist_data(ts_code, startDate, endDate, retry_count=1)
        except IOError:
            logging.warning('fail download %s Kline data %d times, retry....',
                            ts_code, cur_retry)
        else:
            if (df is None) or df.empty:
                return None
            else:
                writeSQL(df, 'klinestock')
                return
    logging.error('fail download %s Kline data!', ts_code)


def del_downKlineFromTushare(ts_code: str, startDate=None, endDate=None,
                             retry_count=6):
    """下载单个股票K线历史写入数据库, 下载源为tushare"""
    # logging.debug('download kline: %s', ts_code)
    # if startDate is None:  # startDate为空时取股票最后更新日期
    #     startDate = getStockKlineUpdateDate() + dt.timedelta(days=1)
    #         print ts_code, startDate
    # startDate = startDate.strftime('%Y-%m-%d')
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
    #     retryCount = 0
    if startDate > endDate:
        return
    for cur_retry in range(1, retry_count + 1):
        try:
            df = ts.get_hist_data(ts_code, startDate, endDate, retry_count=1)
        except IOError:
            logging.warning('fail download %s Kline data %d times, retry....',
                            ts_code, cur_retry)
        else:
            if (df is None) or df.empty:
                return None
            else:
                df['ts_code'] = ts_code
                writeSQL(df, 'klinestock')
                return
    logging.error('fail download %s Kline data!', ts_code)


def del_downloadLirun(date):
    """
    # 获取业绩报表数据
    """
    pass
    # return downloadLirunFromTushare(date)


#     return downloadLirunFromEastmoney(date)


# def del_downloadLirunFromEastmoney(stockList, date):
#     """
#     获取业绩报表数据,数据源为Eastmoney
#     Parameters
#     --------
#     stockList: list 股票代码列表
#     date: int 年度季度 e.g 20191表示2019年1季度
#     说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
#
#     Return
#     --------
#     DataFrame
#         code,代码
#         net_profits,净利润(万元)
#         report_date,发布日期
#
#     # 缺点： 数据中无准确的报表发布日期
#     """
#     lirunList = []
#     date = datatrans.transQuarterToDate(date).replace('-', '')
#     for ts_code in stockList:
#         lirun = None
#         # lirun = lirunFileToList(ts_code, date)
#         if lirun:
#             lirunList.append(lirun)
#     return DataFrame(lirunList)


#     return lirunList


# def del_downloadLirunFromTushare(date):
#     """
#     获取业绩报表数据,数据源为Tushare
#     Parameters
#     --------
#     date: int 年度季度 e.g 20191表示2019年1季度
#     说明：由于是从网站获取的数据，需要一页页抓取，速度取决于您当前网络速度
#
#     Return
#     --------
#     DataFrame
#         code,代码
#         net_profits,净利润(万元)
#         report_date,发布日期
#     """
#     year = date // 10
#     quarter = date % 10
#     #     df = ts.get_report_data(year, quarter)
#     # 因tushare的利润下载函数不支持重试和指定超时值，使用下面的改进版本
#     df = get_report_data(year, quarter)
#     if df is None:
#         return None
#     df = df.loc[:, ['code', 'net_profits', 'report_date']]
#     return datatrans.transLirunDf(df, year, quarter)


# def downGuben(ts_code='300445', date='2019-04-19'):
def downGuben(ts_code='300445', replace=False):
    """
    下载股本数据并保存到数据库
    2个可选数据源，tushare.pro和新浪财经
    tushare.pro数据错误，改用新浪数据
    :param ts_code:
    :return:
    """
    df = _downGubenSina(ts_code)
    # df = _downGubenTusharePro(ts_code)
    if df is not None:
        writeGubenToSQL(df, replace)


def del_downGubenTusharePro(ts_code='300445'):
    """
    从tushare.pro下载股本数据
    :param ts_code:
    :return:
    """
    print('start update guben: %s' % ts_code)
    # updateDate = gubenUpdateDate(ts_code)
    updateDate = None  # 暂时移除
    # print(type(updateDate))
    # print(updateDate.strftime('%Y%m%d'))
    startDate = updateDate.strftime('%Y%m%d')
    pro = ts.pro_api()
    code = tsCode(ts_code)
    # print(code)
    df = pro.daily_basic(ts_code=code, start_date=startDate,
                         fields='trade_date,total_share')
    if df.empty:
        return

    sql = (f'select date, totalshares from guben where ts_code="{ts_code}" '
           ' order by date desc limit 1;')
    result = engine.execute(sql).fetchone()

    gubenDate = []
    gubenValue = []
    pos = 0
    if result is None:
        gubenDate.append(df.trade_date[len(df) - 1])
        gubenValue.append(df.total_share[len(df) - 1])
    else:
        gubenDate.append(result[0])
        gubenValue.append(result[1])
    for idx in reversed(df.index):
        if df.total_share[idx] != gubenValue[pos]:
            gubenDate.append(dt.datetime.strptime(df.trade_date[idx - 1],
                                                  '%Y%m%d'))
            gubenValue.append(df.total_share[idx - 1] * 10000)
            pos += 1
    resultDf = pd.DataFrame({'ts_code': ts_code,
                             'date': gubenDate,
                             'totalshares': gubenValue})
    return resultDf


def del_downMainTable(ts_code):
    mainTableType = ['BalanceSheet', 'ProfitStatement', 'CashFlow']
    result = None
    for tableType in mainTableType:
        url = urlMainTable(ts_code, tableType)
        filename = filenameMainTable(ts_code, tableType)
        logging.debug('downMainTable %s, %s', ts_code, tableType)
        result = dataToFile(url, filename)
        if not result:
            logging.error('download fail: %s', url)
            continue
        time.sleep(1)
    return result


# def get_stock_basics():
#     """
#         确认无用后可删除
#         获取沪深上市公司基本情况
#     Return
#     --------
#     DataFrame
#                code,代码
#                name,名称
#                industry,细分行业
#                area,地区
#                pe,市盈率
#                outstanding,流通股本
#                totals,总股本(万)
#                totalAssets,总资产(万)
#                liquidAssets,流动资产
#                fixedAssets,固定资产
#                reserved,公积金
#                reservedPerShare,每股公积金
#                eps,每股收益
#                bvps,每股净资
#                pb,市净率
#                timeToMarket,上市日期
#     """
#     url = ct.ALL_STOCK_BASICS_FILE
#     req = getreq(url)
#     #     proxy = urllib2.ProxyHandler({'http': '127.0.0.1:8087'})
#     #     opener = urllib2.build_opener(proxy)
#     #     urllib2.install_opener(opener)
#     text = request.urlopen(req, timeout=30).read()
#     # text = text.decode('GBK')
#     # text = text.replace('--', '')
#     # df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
#     df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
#     df = df.set_index('code')
#     return df


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
                     retry_count=3, timeout=20):
    ct._write_console()
    for _ in range(retry_count):
        url = ct.REPORT_URL % (ct.P_TYPE['http'], ct.DOMAINS['vsf'],
                               ct.PAGES['fd'], year, quarter,
                               pageNo, ct.PAGE_NUM[1])
        url += '&order=code%7C2'
        #         url = ('http://vip.stock.finance.sina.com.cn/q/go.php/'
        #                'vFinanceAnalyze/kind/mainindex/index.phtml?'
        #                's_i=&s_a=&s_c=&reportdate=%s&quarter=%s&p=1&num=60' %
        #                (year, quarter))
        #         request = getreq(url)
        result = request.Request(url)
        #         print
        #         print url
        try:
            text = request.urlopen(result, timeout=timeout).read()
        #             print repr(text)
        #             print text
        except IOError as e:
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


def _downGubenSina(ts_code):
    """ 从新浪网下载股本数据
    """
    print('开始下载股本数据：', ts_code)
    timeout = 6
    socket.setdefaulttimeout(timeout)
    gubenURL = urlGubenSina(ts_code)
    useragent = (r'Mozilla / 5.0(Windows NT 10.0; Win64; x64) '
                 r'AppleWebKit / 537.36(KHTML, like Gecko) '
                 r'Chrome / 74.0.3729.169 Safari / 537.36')
    headers = {
        'User-Agent': useragent,
        'Referer': gubenURL,
        'Connection': 'keep-alive'
    }
    try:
        #         sock = urllib.urlopen(gubenURL)
        #         guben = sock.read()
        # req = getreq(gubenURL)
        req = request.Request(gubenURL, headers=headers)
        guben = request.urlopen(req).read()
    except IOError as e:
        print(e)
        print('数据下载失败： %s' % ts_code)
        return None
    # else:
    #         sock.close()
    #     print guben
    df = gubenDataToDfSina(ts_code, guben)
    return df


def del_downIndex(ID, startDate, endDate=None):
    """

    :param ID:
    :param startDate:
    :param endDate:
    :return:
    """
    # startDate = getKlineUpdateDate() + timedelta(days=1)
    if endDate is None:
        endDate = dt.datetime.today().date()
    pro = ts.pro_api()
    df = pro.index_daily(ts_code=ID,
                         start_date=startDate.strftime('%Y%m%d'),
                         end_date=endDate.strftime('%Y%m%d'))
    if df.empty:
        return

    # df = pro.index_daily(ts_code='399300.SZ', start_date='20180101',
    #                      end_date='20181010')
    df['id'] = df.ts_code.str[:6]
    df.rename(columns={'trade_date': 'date', 'vol': 'volume'},
              inplace=True)
    df = df[['id', 'date', 'open', 'high', 'close', 'low', 'volume']]

    print(df.head())
    writeSQL(df, 'klineindex')


def downIndexBasic():
    """
    从tushare下载指数基本信息
    市场代码	说明
    MSCI	MSCI指数
    CSI   	中证指数
    SSE	   上交所指数
    SZSE	深交所指数
    CICC	中金指数
    SW	    申万指数
    OTH	    其他指数
    :return:
    """
    pro = ts.pro_api()
    df_index_basic_sh = pro.index_basic(market='SSE')
    df_index_basic_sz = pro.index_basic(market='SZSE')
    writeSQL(df_index_basic_sh, 'index_basic')
    writeSQL(df_index_basic_sz, 'index_basic')


def downDaily(trade_date=None):
    """下载日K线数据

    :param trade_date:
    :return:
    """
    pro = ts.pro_api()
    dates = []
    if trade_date is None:
        sql = 'select max(trade_date) from daily'
        startDate = engine.execute(sql).fetchone()[0]
        assert isinstance(startDate, dt.date), 'startDate应为date类型'
        startDate += dt.timedelta(days=1)
        endDate = dt.datetime.now().date()
        dates = dateStrList(startDate.strftime('%Y%m%d'),
                            endDate.strftime('%Y%m%d'))
    else:
        dates.append(trade_date)
    for d in dates:
        logging.debug(f'下载日线:{d}')
        df = pro.daily(trade_date=d)
        writeSQL(df, 'daily')


def downDailyRepair():
    """修复日K线"""
    # stocks = readStockList()
    sql = ('select ts_code from stock_basic'
           ' where ts_code not in (select distinct ts_code from daily);')
    stocks = pd.read_sql(sql, engine)
    pro = ts.pro_api()
    for ts_code in stocks.ts_code.to_list():
        print('下载日K线：', ts_code)
        df = pro.daily(ts_code=ts_code)
        writeSQL(df, 'daily')


def downDailyBasic(ts_code=None, tradeDate=None, startDate=None, endDate=None):
    """
    从tushare下载股票每日指标
    :param ts_code: 股票代码
    :param tradeDate: 交易日期
    :param startDate: 开始日期
    :param endDate: 结束日期
    :return:
    """
    pro = ts.pro_api()
    df = None
    if ts_code is not None and startDate is not None:
        df = pro.daily_basic(ts_code=tsCode(ts_code),
                             start_date=startDate,
                             end_date=endDate)
    elif tradeDate is not None:
        df = pro.daily_basic(trade_date=tradeDate)
    if isinstance(df, pd.DataFrame):
        # df.rename(columns={'ts_code': 'ts_code', 'trade_date': 'date'},
        #           inplace=True)
        # df['ts_code'] = df['ts_code'].str[:6]
        df.set_index(keys=['ts_code'], inplace=True)
        sqlrw.writeSQL(df, 'daily_basic')
    return df


def downPledgeStat(ts_code):
    """获取股权质押统计数据

    :param ts_code:
    :return:
    """
    pro = ts.pro_api()
    df = pro.pledge_stat(ts_code=tsCode(ts_code))
    writeSQL(df, 'pledgestat')


def downIncome(ts_code, startDate='', endDate=''):
    """下载tushare利润表

    :param ts_code: str, 股票代码
    :param startDate: str, 开始日期, yyyymmdd
    :param endDate: str, 结束日期, yyyymmdd
    :return:
    """
    if len(ts_code) == 6:
        ts_code = tsCode(ts_code)
    pro = ts.pro_api()
    df = pro.income(ts_code=tsCode(ts_code), start_date=startDate,
                    end_date=endDate)
    print(df)
    writeSQL(df, 'income')


def downBalancesheet(ts_code, startDate='', endDate=''):
    """下载tushare资产负债表

    :param ts_code: str, 股票代码
    :param startDate: str, 开始日期, yyyymmdd
    :param endDate: str, 结束日期, yyyymmdd
    :return:
    """
    if len(ts_code) == 6:
        ts_code = tsCode(ts_code)
    pro = ts.pro_api()
    df = pro.balancesheet(ts_code=tsCode(ts_code), start_date=startDate,
                          end_date=endDate)
    print(df)
    return df
    # writeSQL(df, 'income')


def downloaderStock(tablename, stocks, perTimes=0, downLimit=0):
    """tushare用的下载器，可限制对tushare的访问量
    tushare下载限制，每perTimes秒限制下载downLimit次
    本函数只适用于股票类表格
    :return:
    """
    pro = ts.pro_api()
    times = []
    cnt = len(stocks)

    # tablename = 'income'
    for i in range(cnt):
        nowtime = dt.datetime.now()
        if perTimes > 0 and downLimit > 0 and i >= downLimit and (
                nowtime < times[i - downLimit] + dt.timedelta(
            seconds=perTimes)):
            _timedelta = nowtime - times[i - 50]
            sleeptime = 60 - _timedelta.seconds
            print(f'******暂停{sleeptime}秒******')
            time.sleep(sleeptime)
            nowtime = dt.datetime.now()
        times.append(nowtime)
        print(f'第{i}个，时间：{nowtime}')
        ts_code = stocks[i]
        print(ts_code)
        flag = True
        df = None
        fun = getattr(pro, tablename)
        while flag:
            try:
                # 下载质押统计表
                # df = downPledgeStat(ts_code)
                # 下载利润表
                # df = downIncome(ts_code)
                df = fun(ts_code=ts_code, stocks=stocks)
                flag = False
            except Exception as e:
                print(e)
                time.sleep(10)
        # print(df)
        time.sleep(1)
        if df is not None:
            writeSQL(df, tablename)


def downTradeCal(year):
    pro = ts.pro_api()
    df = pro.trade_cal(exchange='SSE', start_date=f'{year}0101')
    writeSQL(df, 'trade_cal')


def downIndexWeight():
    """
    下载指数成份和权重
    000001.SH	上证综指
    000005.SH	上证商业类指数
    000006.SH	上证房地产指数
    000016.SH	上证50
    000300.SH	沪深300
    000905.SH	中证500
    399001.SZ	深证成指
    399005.SZ	中小板指
    399006.SZ	创业板指
    399016.SZ	深证创新
    399300.SZ	沪深300
    399905.SZ	中证500

    :return:
    """
    pro = ts.pro_api()
    codeList = ['000001.SH',
                '000005.SH',
                '000006.SH',
                '000010.SH',
                '000016.SH',
                '000905.SH',
                '399001.SZ',
                '399005.SZ',
                '399006.SZ',
                '399016.SZ',
                '399300.SZ', ]
    times = []
    cur = 0
    perTimes = 60
    downLimit = 70
    for code in codeList:
        sql = (f'select max(trade_date) from index_weight'
               f' where index_code="{code}"')
        initDate = engine.execute(sql).fetchone()[0]
        if initDate is None:
            initDate = dt.date(2001, 1, 1)
        assert isinstance(initDate, dt.date)
        while initDate < dt.datetime.today().date():
            nowtime = dt.datetime.now()
            if (perTimes > 0 and downLimit > 0 and cur >= downLimit
                    and (nowtime < times[cur - downLimit] + dt.timedelta(
                        seconds=perTimes))):
                _timedelta = nowtime - times[cur - downLimit]
                sleeptime = perTimes - _timedelta.seconds
                print(f'******暂停{sleeptime}秒******')
                time.sleep(sleeptime)

            startDate = initDate.strftime('%Y%m%d')
            initDate += dt.timedelta(days=30)
            endDate = initDate.strftime('%Y%m%d')
            initDate += dt.timedelta(days=1)
            print(f'下载{code},日期{startDate}-{endDate}')
            df = pro.index_weight(index_code=code,
                                  start_date=startDate, end_date=endDate)
            writeSQL(df, 'index_weight')

            nowtime = dt.datetime.now()
            times.append(nowtime)
            cur += 1


def downIndexDaily():
    """
    下载指数每日指标
    000001.SH	上证综指
    000005.SH	上证商业类指数
    000006.SH	上证房地产指数
    000010.SH	上证180
    000016.SH	上证50
    000300.SH	沪深300
    000905.SH	中证500
    399001.SZ	深证成指
    399005.SZ	中小板指
    399006.SZ	创业板指
    399016.SZ	深证创新
    399300.SZ	沪深300
    399905.SZ	中证500

    :return:
    """
    pro = ts.pro_api()
    codeList = ['000001.SH',
                '000005.SH',
                '000006.SH',
                '000010.SH',
                '000016.SH',
                '399001.SZ',
                '399005.SZ',
                '399006.SZ',
                '399016.SZ',
                '399300.SZ',
                '399905.SZ', ]
    for code in codeList:
        sql = (f'select max(trade_date) from index_daily'
               f' where ts_code="{code}"')
        result = engine.execute(sql).fetchone()[0]
        startDate = None
        endDate = None
        if isinstance(result, dt.date):
            result = result + dt.timedelta(days=1)
            startDate = result.strftime('%Y%m%d')
        df = pro.index_daily(ts_code=code,
                             start_date=startDate, end_date=endDate)
        writeSQL(df, 'index_daily')


def downIndexDailyBasic():
    """
    下载指数每日指标
    000001.SH	上证综指
    000005.SH	上证商业类指数
    000006.SH	上证房地产指数
    000016.SH	上证50
    000300.SH	沪深300
    000905.SH	中证500
    399001.SZ	深证成指
    399005.SZ	中小板指
    399006.SZ	创业板指
    399016.SZ	深证创新
    399300.SZ	沪深300
    399905.SZ	中证500

    :return:
    """
    pro = ts.pro_api()
    codeList = ['000001.SH',
                '000005.SH',
                '000006.SH',
                '000016.SH',
                '399001.SZ',
                '399005.SZ',
                '399006.SZ',
                '399016.SZ',
                '399300.SZ',
                '399905.SZ', ]
    for code in codeList:
        # sql = (f'select max(trade_date) from index_dailybasic'
        #        f' where ts_code="{code}"')
        # result = engine.execute(sql).fetchone()[0]
        # startDate = None
        # if isinstance(result, type(datetime.date)):
        #     result = result + timedelta(days=1)
        #     startDate = result.strftime('YYYYmmdd')
        startDate = '20040101'
        endDate = '20080101'
        df = pro.index_dailybasic(ts_code=code,
                                  start_date=startDate, end_date=endDate)
        writeSQL(df, 'index_dailybasic')


if __name__ == '__main__':
    initlog()

    pass
    ts_code = '000651'
    startDate = '2019-04-01'
    # downKlineFromBaostock(ts_code, startDate)
