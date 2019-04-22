# -*- coding: utf-8 -*-
"""
Created on Thu Oct 26 21:12:40 2017

@author: who8736

# 各类下载功能
"""

import logging
import zipfile
import socket
import urllib.request, urllib.error, urllib.parse
import time
import re
from lxml import etree
import lxml
import datetime as dt
from datetime import datetime

import tushare as ts
import pandas as pd
from pandas.core.frame import DataFrame
from pandas.compat import StringIO
from tushare.stock import cons as ct
import baostock as bs

from misc import urlGubenSina, urlGubenEastmoney, urlGuzhi, urlMainTable
from misc import filenameGuben, filenameMainTable, filenameGuzhi
from misc import longStockID, tsCode
import datatrans
import hyanalyse
import sqlrw
from sqlrw import klineUpdateDate, lirunFileToList
from sqlrw import writeKline, writeStockList
from sqlrw import writeHYToSQL, writeHYNameToSQL
from sqlrw import gubenUpdateDate, writeGubenToSQL
from initlog import initlog


# def downGubenToSQL(stockID, retry=3, timeout=10):
#     """下载单个股票股本数据写入数据库"""
#     logging.debug('downGubenToSQL: %s', stockID)
#     socket.setdefaulttimeout(timeout)
#     gubenURL = urlGuben(stockID)
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
#                             e, stockID)
# #             print type(e)
#             errorString = '%s' % e
#             if errorString == 'HTTP Error 456: ':
#                 print 'sleep 60 seconds...'
#                 time.sleep(60)
#         else:
#             gubenDf = datatrans.gubenDataToDf(stockID, guben)
#             tablename = 'guben'
#             lastUpdate = getGubenLastUpdateDate(stockID)
#             gubenDf = gubenDf[gubenDf.date > lastUpdate]
#             if not gubenDf.empty:
#                 writeSQL(gubenDf, tablename)
#             return
#     logging.error('fail download %s guben data.', stockID)


def downStockList(retry=10):
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
    writeStockList(sl)
#     clearStockList()
#     sl.to_sql(u'stocklist',
#               engine,
#               if_exists=u'append')

    # 读取行业表中的股票代码，与当前获取的股票列表比较，
    # 如果存在部分股票未列入行业表，则更新行业列表数据
    sql = 'select stockid from hangyestock;'
    result = sqlrw.engine.execute(sql)
    hystock = result.fetchall()
    hystock = [i[0] for i in hystock]
#     hystock = hyanalyse.getHYStock()
    noinhy = [i for i in sl if i not in hystock]
    # 股票列表中上市日期不为0，即为已上市
    # 且不在行业列表中，表示需更新行业数据
    slnoinhy = sl[(sl.timeToMarket != 0) & (sl.index.isin(noinhy))]
#     sl2 = sl1[sl1.index.isin(noinhy)]
    if not slnoinhy.empty:
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
    htmlresult = urllib.request.urlopen(req).read()
    myTree = etree.HTML(htmlresult)
    dateStr = myTree.xpath('''//html//body//div//div//div//div
                                //div//form//label//input//@value''')
    dateStr = dateStr[0]
#     print dateStr
#     print dateStr.split('-')
    dateStr = ''.join(dateStr.split('-'))
#     print dateStr

    # 下载并解压行业数据文件
    HYFileUrl = 'http://115.29.204.48/syl/csi%s.zip' % dateStr
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
        return urllib.request.Request(url, headers=headers)
    else:
        return urllib.request.Request(url)


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
            req = urllib.request.Request(url, headers=headers)
            content = urllib.request.urlopen(req).read()
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


def downloadClassified():
    """ 旧版下载行业分类， 计划删除本函数
    """
    classifiedDf = ts.get_industry_classified(standard='sw')
    classifiedDf = classifiedDf[['code', 'c_name']]
    classifiedDf.columns = ['stockid', 'cname']
    return classifiedDf


def downGubenOld(stockID, retry=3, timeout=10):
    """ 本函数待废除，如果新的downGuben正常
        下载单个股票股本数据写入数据库

    """
    # //*[@id="lngbbd_Table"]/tbody/tr[1]
    logging.debug('downGubenToSQL: %s', stockID)
    print('downGubenToSQL: %s' % stockID)
    socket.setdefaulttimeout(timeout)
    # gubenURL = urlGubenEastmoney(stockID)
    gubenURL = urlGubenSina(stockID)
    req = getreq(gubenURL)
#     downloadStat = False
    gubenDf = pd.DataFrame()

    # 使用代理抓取数据
#     proxy_handler = urllib2.ProxyHandler({"http": 'http://127.0.0.1:8087'})
#     opener = urllib2.build_opener(proxy_handler)
#     urllib2.install_opener(opener)

    for _ in range(retry):
        try:
            guben = urllib.request.urlopen(req).read()
        except IOError as e:
            logging.warning('[%s]:download %s guben data, retry...',
                            e, stockID)
#             print type(e)
            errorString = '%s' % e
            if errorString == 'HTTP Error 456: ':
                print('sleep 60 seconds...')
                time.sleep(60)
        else:
            gubenDf = datatrans.gubenDataToDfSina(stockID, guben)
            if sqlrw.writeGubenToSQL(stockID, gubenDf):
                logging.debug('download %s guben data final.', stockID)
                return
#             return gubenDf
    logging.error('fail download %s guben data.', stockID)
    return None


# def downloadGuben(stockID):
    """ 下载股本并保存到文件， 确认为无用函数后可删除
    """
    # url = urlGuben(stockID)
    # filename = filenameGuben(stockID)
    # return dataToFile(url, filename)


def downGuzhi_del(stockID):
    """ 确认无用后可删除
    # 下载单个股票估值数据， 保存并返回估值数据
    """
    logging.debug('downGuzhiToSQL: %s', stockID)
    url = urlGuzhi(stockID)
    data = downloadData(url)
    if data is None:
        logging.error('down %s guzhi data fail.', stockID)
        return None

    # 保存至文件
    filename = filenameGuzhi(stockID)
    try:
        mainFile = open(filename, 'wb')
        mainFile.write(data)
    except IOError as e:
        logging.error('[%s]写文件失败： %s', e, filename)
#         return False
    finally:
        mainFile.close()
    return data


def downGuzhi(stockID):
    """ 下载单个股票估值数据， 保存并返回估值数据
    """
    url = urlGuzhi(stockID)
    filename = filenameGuzhi(stockID)
    logging.debug('write guzhi file: %s', filename)
    return dataToFile(url, filename)


def downKline(stockID, startDate=None, endDate=None, retry_count=6):
    """ 下载单个股票K线历史写入数据库, 通过调用不同下载函数选择不同的数据源
    """
    # logging.debug('download kline: %s', stockID)

    # 数据源：　baostock
    # downKlineFromBaostock(stockID, startDate, endDate, retry_count)

    # 数据源：　tushare
    downKlineFromTushare(stockID, startDate, endDate, retry_count)

def downKlineFromBaostock(stockID, startDate=None, endDate=None, retry_count=6):
    """下载单个股票K线历史写入数据库, 下载源为baostock"""
    if startDate is None:  # startDate为空时取股票最后更新日期
        startDate = klineUpdateDate(stockID) + dt.timedelta(days=1)
    #         print stockID, startDate
    startDate = startDate.strftime('%Y-%m-%d')
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
    #     retryCount = 0
    longID = longStockID(stockID)

    for cur_retry in range(1, retry_count + 1):
        try:
            fieldsStr = "date,open,high,close,low,volume,peTTM,tradestatus"
            fields = ['date','open','high','close','low','volume','ttmpe']
            rs = bs.query_history_k_data_plus(longID, fieldsStr, startDate, endDate)
            dataList = []
            while rs.next():
                dataList.append(rs.get_row_data())
            df = pd.DataFrame(dataList, columns=rs.fields)
            df = df[df.tradestatus == '1'].copy()
            df = df.rename({'peTTM': 'ttmpe'})
            df = df.iloc[0:, :-1]
            # df = ts.get_hist_data(stockID, startDate, endDate, retry_count=1)
        except IOError:
            logging.warning('fail download %s Kline data %d times, retry....',
                            stockID, cur_retry)
        else:
            if (df is None) or df.empty:
                return None
            else:
                if writeKline(stockID, df):
                    return
    logging.error('fail download %s Kline data!', stockID)

def downKlineFromTushare(stockID, startDate=None, endDate=None, retry_count=6):
    """下载单个股票K线历史写入数据库, 下载源为tushare"""
    # logging.debug('download kline: %s', stockID)
    if startDate is None:  # startDate为空时取股票最后更新日期
        startDate = klineUpdateDate(stockID) + dt.timedelta(days=1)
#         print stockID, startDate
        startDate = startDate.strftime('%Y-%m-%d')
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y-%m-%d')
#     retryCount = 0
    if startDate > endDate:
        return
    for cur_retry in range(1, retry_count + 1):
        try:
            df = ts.get_hist_data(stockID, startDate, endDate, retry_count=1)
        except IOError:
            logging.warning('fail download %s Kline data %d times, retry....',
                            stockID, cur_retry)
        else:
            if (df is None) or df.empty:
                return None
            else:
                if writeKline(stockID, df):
                    return
    logging.error('fail download %s Kline data!', stockID)


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
    year = date // 10
    quarter = date % 10
#     df = ts.get_report_data(year, quarter)
    # 因tushare的利润下载函数不支持重试和指定超时值，使用下面的改进版本
    df = get_report_data(year, quarter)
    if df is None:
        return None
    df = df.loc[:, ['code', 'net_profits', 'report_date']]
    return datatrans.transLirunDf(df, year, quarter)

def downGuben(stockID='300445', date='2019-04-19'):
    print('start update guben: %s' % stockID)
    updateDate = gubenUpdateDate(stockID)
    # print(type(updateDate))
    # print(updateDate.strftime('%Y%m%d'))
    startDate = updateDate.strftime('%Y%m%d')
    pro = ts.pro_api()
    code = tsCode(stockID)
    # print(code)
    df = pro.daily_basic(ts_code=code, start_date=startDate,
                              fields='trade_date,total_share')
    # print(df)
    gubenDate = []
    gubenValue = []
    for idx in reversed(df.index):
        if idx > 0 and df.total_share[idx] != df.total_share[idx - 1]:
            # print(type(idx), idx, df.trade_date[idx], df.total_share[idx])
            # print(type(idx - 1), idx, df.trade_date[idx - 1], df.total_share[idx - 1])
            gubenDate.append(datetime.strptime(df.trade_date[idx - 1],
                                               '%Y%m%d'))
            gubenValue.append(df.total_share[idx - 1] * 10000)
    resultDf = pd.DataFrame({'stockid': stockID,
                             'date': gubenDate,
                             'totalshares': gubenValue})
    print(resultDf)
    writeGubenToSQL(stockID, resultDf)
    return resultDf

def downMainTable(stockID):
    mainTableType = ['BalanceSheet', 'ProfitStatement', 'CashFlow']
    for tableType in mainTableType:
        url = urlMainTable(stockID, tableType)
        filename = filenameMainTable(stockID, tableType)
        logging.debug('downMainTable %s, %s', stockID, tableType)
        result = dataToFile(url, filename)
        if not result:
            logging.error('download fail: %s', url)
            continue
        time.sleep(1)
    return result


def get_stock_basics():
    """
        确认无用后可删除
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
    text = urllib.request.urlopen(req, timeout=30).read()
    text = text.decode('GBK')
    text = text.replace('--', '')
    df = pd.read_csv(StringIO(text), dtype={'code': 'object'})
    df = df.set_index('code')
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
        request = urllib.request.Request(url)
#         print
#         print url
        try:
            text = urllib.request.urlopen(request, timeout=timeout).read()
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


# def gubenURLToDf(stockID):
#     """ 确认无用后可删除
#     """
#     gubenURL = urlGuben(stockID)
#     timeout = 6
#     try:
#         print('开始下载数据。。。')
#         socket.setdefaulttimeout(timeout)
# #         sock = urllib.urlopen(gubenURL)
# #         guben = sock.read()
#         req = getreq(gubenURL)
#         guben = urllib.request.urlopen(req).read()
#     except IOError as e:
#         print(e)
#         print('数据下载失败： %s' % stockID)
#         return None
#     else:
#         #         sock.close()
#         #     print guben
#         return datatrans.gubenDataToDf(stockID, guben)


if __name__ == '__main__':
    initlog()

    pass
    stockID = '000651'
    startDate = '2019-04-01'
    downKlineFromBaostock(stockID, startDate)
