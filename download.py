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
from requests.exceptions import ConnectTimeout, ReadTimeout
import time
# import re

# import tushare
from lxml import etree
# import lxml
import datetime as dt
# from datetime import datetime, timedelta

import tushare as ts
import pandas as pd
# from pandas.core.frame import DataFrame
# from pandas.compat import StringIO
# from io import StringIO
# from tushare.stock import cons as ct
# import baostock as bs

from misc import (urlGuzhi, filenameGuzhi)
# import datatrans
from datatrans import dateStrList
# import hyanalyse
# import sqlrw
from sqlrw import (engine, writeSQL, writeHYToSQL, writeHYNameToSQL,
                   readTableFields)
from initlog import initlog
from misc import tsCode


class DownloaderQuarter:
    """限时下载器
    """

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
    fields = readTableFields('fina_indicator')

    def __init__(self, ts_code, startDate, tables=None,
                 replace=False, retry=3):
        pass
        self.ts_code = ts_code
        self.startDate = startDate
        if tables is None:
            self.tables = ['balancesheet', 'income', 'cashflow',
                           'fina_indicator']
        else:
            self.tables = tables
        self.replace = replace
        self.retry = retry

    # 每个股票一个下载器，下载第一张无数据时可跳过其他表
    # 下载限制由类静态成员记载与控制
    def run(self):
        pass
        for table in self.tables:
            # result = pd.DataFrame()
            perTimes = DownloaderQuarter.perTimes[table]
            limit = DownloaderQuarter.limit[table]
            cur = DownloaderQuarter.curcall[table]
            for _ in range(self.retry):
                nowtime = dt.datetime.now()
                if (perTimes > 0 and limit <= cur
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
                    kwargs = dict(table=table,
                                  ts_code=self.ts_code,
                                  start_date=self.startDate,
                                  replace=self.replace)
                    if table == 'fina_indicator':
                        kwargs['fields'] = DownloaderQuarter.fields
                    result = downStockQuarterData(**kwargs)
                except(socket.timeout, ConnectTimeout):
                    logging.warning(f'downloader timeout: '
                                    f'{table}-{self.ts_code}-{self.startDate}')
                else:
                    if result:
                        break
                    else:
                        return
                finally:
                    nowtime = dt.datetime.now()
                    DownloaderQuarter.times[table].append(nowtime)
                    DownloaderQuarter.curcall[table] += 1


# class DownloaderFinaIndicator:
#     """限时下载器
#     """
#
#     # 调用时间限制，如60秒调用80次
#     perTimes = {'balancesheet': 60,
#                 'income': 60,
#                 'cashflow': 60,
#                 'fina_indicator': 60}
#     # 一定时间内调用次数限制
#     limit = {'balancesheet': 80,
#              'income': 80,
#              'cashflow': 80,
#              'fina_indicator': 60}
#     # 下载开始时间，按表格保存不同的时间
#     times = {'balancesheet': [],
#              'income': [],
#              'cashflow': [],
#              'fina_indicator': []}
#     # 记录当前调用某个表的累计次数
#     curcall = {'balancesheet': 0,
#                'income': 0,
#                'cashflow': 0,
#                'fina_indicator': 0}
#     fields = readTableFields('fina_indicator')
#
#     def __init__(self, ts_code, tables=None, replace=False, retry=3):
#         pass
#         self.ts_code = ts_code
#         self.retry = retry
#         self.replace = replace
#         if tables == None:
#             self.tables = ['balancesheet', 'income', 'cashflow',
#                            'fina_indicator']
#         else:
#             self.tables = tables
#
#     # 每个股票一个下载器，下载第一张无数据时可跳过其他表
#     # 下载限制由类静态成员记载与控制
#     def run(self):
#         pass
#         table = 'fina_indicator'
#         # result = pd.DataFrame()
#         perTimes = DownloaderQuarter.perTimes[table]
#         limit = DownloaderQuarter.limit[table]
#         cur = DownloaderQuarter.curcall[table]
#         for _ in range(self.retry):
#             nowtime = dt.datetime.now()
#             delta = dt.timedelta(seconds=perTimes)
#             if (perTimes > 0 and limit <= cur
#                     and (nowtime < delta +
#                          DownloaderQuarter.times[table][cur - limit])):
#                 _timedelta = nowtime - DownloaderQuarter.times[table][
#                     cur - limit]
#                 sleeptime = DownloaderQuarter.perTimes[
#                                 table] - _timedelta.seconds
#                 print(f'******暂停{sleeptime}秒******')
#                 time.sleep(sleeptime)
#             try:
#                 kwargs = dict(table=table,
#                               ts_code=self.ts_code,
#                               fields=DownloaderFinaIndicator.fields,
#                               start_date='',
#                               replace=self.replace)
#                 result = downStockQuarterData(**kwargs)
#             except(socket.timeout, ConnectTimeout):
#                 logging.warning(f'downloader timeout: {table}-{self.ts_code}')
#             else:
#                 if result:
#                     break
#                 else:
#                     return
#             finally:
#                 nowtime = dt.datetime.now()
#                 DownloaderQuarter.times[table].append(nowtime)
#                 DownloaderQuarter.curcall[table] += 1
#
#
class DownloaderMisc:
    """限时下载器, 不定期更新
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
            except(socket.timeout, ConnectTimeout, ReadTimeout):
                logging.warning(f'downloader timeout: {table}')
            else:
                return result
            finally:
                nowtime = dt.datetime.now()
                self.times.append(nowtime)
                self.cur += 1


def downStockQuarterData(table, ts_code, start_date, fields='', replace=False):
    print(f'downStockQuarterData table:{table}, ts_code:{ts_code}')
    logging.debug(f'downStockQuarterData table:{table}, ts_code:{ts_code}')
    pro = ts.pro_api()
    fun = getattr(pro, table)
    kwargs = dict(ts_code=ts_code, start_date=start_date, fields=fields)
    df = fun(**kwargs)
    if df.empty:
        return False
    else:
        writeSQL(df, table, replace=replace)
        return True


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


def downStockList():
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
    result = engine.execute(sql).fetchall()
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


def downGuzhi(ts_code):
    """ 下载单个股票估值数据， 保存并返回估值数据
    """
    url = urlGuzhi(ts_code)
    filename = filenameGuzhi(ts_code)
    logging.debug('write guzhi file: %s', filename)
    return dataToFile(url, filename)


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
        writeSQL(df, 'daily_basic')
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
        delta = dt.timedelta(seconds=perTimes)
        if perTimes > 0 and 0 < downLimit <= i and (
                nowtime < times[i - downLimit] + delta):
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
            delta = dt.timedelta(seconds=perTimes)
            if (perTimes > 0 and 0 < downLimit <= cur
                    and (nowtime < times[cur - downLimit] + delta)):
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
                '000905.SH',

                '399001.SZ',
                '399005.SZ',
                '399006.SZ',
                '399016.SZ',
                '399300.SZ',
                ]
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
                '000905.SH',
                '399001.SZ',
                '399005.SZ',
                '399006.SZ',
                '399016.SZ',
                '399300.SZ',
                ]
    for code in codeList:
        sql = (f'select max(trade_date) from index_dailybasic'
               f' where ts_code="{code}"')
        result = engine.execute(sql).fetchone()[0]
        startDate = None
        if isinstance(result, dt.date):
            result = result + dt.timedelta(days=1)
            startDate = result.strftime('%Y%m%d')
        # startDate = '20040101'
        # endDate = '20080101'
        df = pro.index_dailybasic(ts_code=code, start_date=startDate)
        print('index_dailybasic:', startDate)
        print('ts_code:', code)
        print(df.head())
        writeSQL(df, 'index_dailybasic')


def downAdjFactor(trade_date, retry=3):
    """
    下载复权因子
    前复权 = 当日收盘价 × 当日复权因子 / 最新复权因子	qfq
    后复权 = 当日收盘价 × 当日复权因子	hfq
    :return:
    """
    logging.debug(f'下载复权因子: {trade_date}')
    pro = ts.pro_api()
    for _ in range(retry):
        try:
            df = pro.adj_factor(trade_date=trade_date)
            writeSQL(df, 'adj_factor')
        except (socket.timeout, ConnectTimeout):
            continue
        else:
            break


if __name__ == '__main__':
    initlog()

    pass
    ts_code = '000651'
    startDate = '2019-04-01'
    # downKlineFromBaostock(ts_code, startDate)
