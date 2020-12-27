# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 21:12:40 2017

@author: who8736

# 各类下载功能
"""

import datetime as dt
import logging
import os
import socket
import time
import zipfile
from requests.exceptions import ReadTimeout, ConnectTimeout

import pandas as pd
import tushare as ts

from ..config import DATAPATH
from ..db.sqlconn import engine
from ..db.sqlrw import (
    readCal, readTableFields, writeClassifyMemberToSQL, writeClassifyNameToSQL,
    writeSQL, readUpdate, setUpdate
)
from ..downloader.webRequest import WebRequest
from ..util.initlog import initlog
from ..util.misc import tsCode


class DownloaderQuarter:
    """限时下载器
    """

    # 调用时间限制，如60秒调用80次
    perTimes = {'balancesheet': 60,
                'income': 60,
                'cashflow': 60,
                'fina_indicator': 60}
    # 一定时间内调用次数限制
    limit = {'balancesheet': 50,
             'income': 50,
             'cashflow': 50,
             'fina_indicator': 50}
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

    def __init__(self, ts_code, startDate=None, tables=None, period=None,
                 replace=True, retry=3):
        pass
        assert startDate is not None or period is not None, '必须指定startDate或period'
        self.ts_code = ts_code
        self.startDate = startDate
        self.period = period
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
                    logging.debug(f'******暂停{sleeptime}秒******')
                    time.sleep(sleeptime)
                try:
                    kwargs = dict(table=table,
                                  ts_code=self.ts_code,
                                  replace=self.replace)
                    if self.period is None:
                        kwargs['start_date'] = self.startDate
                    else:
                        kwargs['period'] = self.period

                    if table == 'fina_indicator':
                        kwargs['fields'] = DownloaderQuarter.fields
                    result = downStockQuarterData(**kwargs)
                except(socket.timeout):
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


class DownloaderMisc:
    """限时下载器, 不定期更新

    """

    def __init__(self, perTimes, limit, retry=3):
        """

        :param perTimes: 单位为秒
        :param limit: 指定时间内限制的下载次数
        :param retry: 下载失败时的重试次数
        """
        pass
        self.perTimes = perTimes
        self.limit = limit
        self.retry = retry
        self.cur = 0
        self.times = []

    # 下载限制由类静态成员记载与控制
    def run(self, table, **kwargs):
        """
        :param table:
        :param kwargs:
        :return:
        :type table: str
        """
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
                logging.debug(f'******暂停{sleeptime}秒******')
                time.sleep(sleeptime)
            try:
                result = fun(**kwargs)
            except(socket.timeout, CONNECTTIMEOUT, ReadTimeout):
                logging.warning(f'downloader timeout: {table}')
            else:
                return result
            finally:
                nowtime = dt.datetime.now()
                self.times.append(nowtime)
                self.cur += 1


class Downloader:
    """通用限时下载器
    # TODO: 返回下载结果，由调用者写入数据库，并记录更新日期
    通过类变量记录下载限制并进行控制
    """

    def __init__(self, funcName, perTimes, limit, retry=3):
        """

        :param funcName: 调用下载函数的名称
        :param perTimes: 单位为秒
        :param limit: 指定时间内限制的下载次数
        :param retry: 下载失败时的重试次数
        """
        pass
        self.func = getattr(ts.pro_api(), funcName)
        self.perTimes = perTimes
        self.limit = limit
        self.retry = retry
        self.cur = 0
        self.times = []

    # 下载限制由类静态成员记载与控制
    def run(self, **kwargs):
        for _ in range(self.retry):
            nowtime = dt.datetime.now()
            if (self.cur >= self.limit
                    and (nowtime < self.times[self.cur - self.limit]
                         + dt.timedelta(seconds=self.perTimes))):
                _timedelta = nowtime - self.times[self.cur - self.limit]
                sleeptime = self.perTimes - _timedelta.seconds
                logging.debug(f'******暂停{sleeptime}秒******')
                time.sleep(sleeptime)
            try:
                result = self.func(**kwargs)
            except(socket.timeout, ConnectTimeout, ReadTimeout):
                logging.warning(f'downloader timeout: {table}')
            else:
                return result
            finally:
                nowtime = dt.datetime.now()
                self.times.append(nowtime)
                self.cur += 1


def downStockQuarterData(**kwargs):
    table = kwargs.get('table', '')
    ts_code = kwargs.get('ts_code', '')
    assert table, '表名不能为空'
    assert ts_code, '股票代码不能为空'
    replace = getattr(kwargs, 'replace', 'False')
    logging.debug(f'downStockQuarterData '
                  f'table:{table}, ts_code:{ts_code}')
    pro = ts.pro_api()
    fun = getattr(pro, table)
    df = fun(**kwargs)
    if df.empty:
        return False
    else:
        writeSQL(df, table, replace=replace)
        return True


def downStockList():
    """ 更新股票列表与行业列表
    """
    pro = ts.pro_api()
    df = pro.stock_basic(
        fields='ts_code, symbol, name, area, industry, fullname, enname, market, exchange, curr_type, list_status, list_date, delist_date, is_hs')
    writeSQL(df, 'stock_basic', replace=True)


def downClassify():
    """
    更新行业列表数据
    取已有行业数据的最后一天， 到更新数据前一日，
    调用下载行业文件函数，并更新到库
    """
    sql = 'select max(date) from classify_member'
    result = engine.execute(sql).fetchone()[0]
    if result is None:
        startDate = '20121203'
    else:
        startDate = result.strftime('%Y%m%d')
    endDate = dt.datetime.today().strftime('%Y%m%d')
    sql = f'''select cal_date from trade_cal 
                where is_open=1 
                and cal_date>"{startDate}" and cal_date<"{endDate}"'''
    dates = engine.execute(sql).fetchall()
    if dates:
        dates = [d[0].strftime('%Y%m%d') for d in dates]
        for _date in dates:
            logging.debug(f'下载行业数据：{_date}')
            filename = downClassifyFile(_date)
            if filename:
                writeClassifyMemberToSQL(_date, filename)
                writeClassifyNameToSQL(filename)


def downClassifyFile(_date):
    """ 从中证指数网下载行业数据文件
    下载按钮的xpath
    /html/body/div[3]/div/div/div[1]/div[1]/form/a[1]
    :type _date: str 'YYYYmmdd'
    """
    logging.debug(f'downClassifyFile: {_date}')
    url = f'http://47.97.204.47/syl/csi{_date}.zip'
    zipfilename = os.path.join(DATAPATH, f'csi{_date}.zip')
    datafilename = os.path.join(DATAPATH, f'csi{_date}.xls')
    try:
        req = WebRequest()
        req.get(url)
        req.save(zipfilename)
        zfile = zipfile.ZipFile(zipfilename, 'r')
        zfile.extract(os.path.basename(datafilename),
                      os.path.dirname(datafilename))
        return datafilename
    except Exception as e:
        logging.warning(e)


def getreq_del(url, includeHeader=False):
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


# def downGuzhi_del(ts_code):
#     """ 确认无用后可删除
#     # 下载单个股票估值数据， 保存并返回估值数据
#     """
#     logging.debug('downGuzhiToSQL: %s', ts_code)
#     url = urlGuzhi(ts_code)
#     data = downloadData(url)
#     if data is None:
#         logging.error('down %s guzhi data fail.', ts_code)
#         return None
#
#     # 保存至文件
#     filename = filenameGuzhi(ts_code)
#     mainFile = open(filename, 'wb')
#     try:
#         mainFile.write(data)
#     except IOError as e:
#         logging.error('[%s]写文件失败： %s', e, filename)
#     #         return False
#     finally:
#         mainFile.close()
#     return data


# def downGuzhi(ts_code):
#     """ 下载单个股票估值数据， 保存并返回估值数据
#     """
#     url = urlGuzhi(ts_code)
#     filename = filenameGuzhi(ts_code)
#     logging.debug('write guzhi file: %s', filename)
#     return dataToFile(url, filename)


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
        startDate = startDate.strftime('%Y%m%d')
        endDate = dt.datetime.now().strftime('%Y%m%d')
        dates = readCal(startDate, endDate)
    else:
        dates.append(trade_date)
    if dates:
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
        logging.info('下载日K线：', ts_code)
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
        # df.set_index(keys=['ts_code'], inplace=True)
        writeSQL(df, 'daily_basic')
    # return df


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
    logging.debug(f'下载利润表：{ts_code}, {startDate}-{endDate}')
    if len(ts_code) == 6:
        ts_code = tsCode(ts_code)
    pro = ts.pro_api()
    df = pro.income(ts_code=tsCode(ts_code), start_date=startDate,
                    end_date=endDate)
    writeSQL(df, 'income')


def downBalancesheet(ts_code, startDate='', endDate=''):
    """下载tushare资产负债表

    :param ts_code: str, 股票代码
    :param startDate: str, 开始日期, yyyymmdd
    :param endDate: str, 结束日期, yyyymmdd
    :return:
    """
    logging.debug(f'下载资产负债表：{ts_code}, {startDate}-{endDate}')
    if len(ts_code) == 6:
        ts_code = tsCode(ts_code)
    pro = ts.pro_api()
    df = pro.balancesheet(ts_code=tsCode(ts_code), start_date=startDate,
                          end_date=endDate)
    return df


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
            logging.debug(f'******暂停{sleeptime}秒******')
            time.sleep(sleeptime)
            nowtime = dt.datetime.now()
        times.append(nowtime)
        ts_code = stocks[i]
        logging.debug(f'使用限时下载器下载第{i}只股票：{ts_code}')
        flag = True
        df = None
        fun = getattr(pro, tablename)
        while flag:
            try:
                df = fun(ts_code=ts_code, stocks=stocks)
                flag = False
            except Exception as e:
                logging.warning(e)
                time.sleep(10)
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
    # pro = ts.pro_api()
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
    # times = []
    # cur = 0
    perTimes = 60
    limit = 70

    downloader = Downloader(funcName='index_weight',
                            perTimes=perTimes,
                            limit=limit,
                            )
    for code in codeList:
        dataName = f'weight_{code}'
        lastDate = readUpdate(dataName)
        if lastDate is None:
            startDate = dt.date(2001, 1, 1)
        else:
            startDate = dt.datetime.strptime(lastDate, '%Y%m%d') + dt.timedelta(days=1)
        endDate = dt.date.today() - dt.timedelta(days=1)

        # noinspection DuplicatedCode
        while startDate <= endDate:
            _startDate = startDate.strftime('%Y%m%d')
            startDate += dt.timedelta(days=30)
            _endDate = startDate.strftime('%Y%m%d')
            arg = {'index_code': code,
                   'start_date': _startDate,
                   'end_date': _endDate, }
            logging.debug(f'download index_weight {code} {_startDate}-{_endDate}')
            result = downloader.run(**arg)
            if result is not None:
                writeSQL(result, 'index_weight')
            setUpdate(dataName=dataName, _date=_endDate)
            startDate += dt.timedelta(days=1)


# for code in codeList:
#     dataName = f'weight_{code}'
#     lastDate = readUpdate(dataName)
#     if lastDate is None:
#         startDate = dt.date(2001, 1, 1)
#     else:
#         startDate = dt.datetime.strptime(lastDate, '%Y%m%d') + dt.timedelta(days=1)
#     endDate = dt.date.today() - dt.timedelta(days=1)
#
#     # noinspection DuplicatedCode
#     while startDate <= endDate:
#         nowtime = dt.datetime.now()
#         delta = dt.timedelta(seconds=perTimes)
#         if (perTimes > 0 and 0 < downLimit <= cur
#                 and (nowtime < times[cur - downLimit] + delta)):
#             _timedelta = nowtime - times[cur - downLimit]
#             sleeptime = perTimes - _timedelta.seconds
#             logging.debug(f'******暂停{sleeptime}秒******')
#             time.sleep(sleeptime)
#
#         _startDate = initDate.strftime('%Y%m%d')
#         initDate += dt.timedelta(days=30)
#         _endDate = initDate.strftime('%Y%m%d')
#         initDate += dt.timedelta(days=1)
#         logging.debug(f'下载指数成分和权重{code},日期{_startDate}-{_endDate}')
#         df = pro.index_weight(index_code=code,
#                               start_date=_startDate, end_date=_endDate)
#         writeSQL(df, 'index_weight')
#
#         nowtime = dt.datetime.now()
#         times.append(nowtime)
#         cur += 1


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
        logging.debug(f'下载指数每日指标:{code}, startDate:{startDate}')
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
        except (socket.timeout, CONNECTTIMEOUT):
            continue
        else:
            break


if __name__ == '__main__':
    initlog()

    pass
    ts_code = '000651'
    startDate = '2019-04-01'
    # downKlineFromBaostock(ts_code, startDate)
