# -*- coding: utf-8 -*-
"""
Created on 2016年1月10日

@author: who8736
"""

import datetime as dt
# import sys  # python的系统调用模块
# import os
import logging

import baostock as bs
import tushare as ts
from dateutil.relativedelta import relativedelta

# from . import analyse
from ..analyse import valuation
from ..analyse.classifyanalyse import (
    calClassifyPE, calClassifyStaticTTMProfit,
)
from ..analyse.compute import calAllPEHistory, calAllTTMProfits, calIndexPE
# from ..config import TUSHARETOKEN
from ..config import DATASOURCE, TUSHARETOKEN
from ..db.sqlconn import engine
from ..db.sqlrw import (
    readCal, readUpdate, setUpdate, readTTMProfitsUpdate, readStockList
)
from ..downloader.downloadbaostock import downTradeCalBaostock, downStockListBaostock
from ..downloader.downloadtushare import (
    DownloaderQuarterTushare, downAdjFactorTushare, downClassify, downDailyTushare,
    downDailyBasic,
    downIndexDaily, downIndexDailyBasic, downIndexWeight,
    downStockListTushare, downTradeCalTushare,
)
from ..downloader.downloadakshare import (
    downStockList, downTradeCal, downloadDaily, downloadDailyBasic, downloadIndexDaily, downloadIndexDailyIndicator, downloadETFDaily)
from ..util.check import checkQuarterData
from ..util.datatrans import classifyEndDate, quarterList
from ..util.initlog import initlog, logfun
from ..util.misc import dayDelta


@logfun
def startUpdate():
    """自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
    """
    if DATASOURCE == 'baostock':
        startUpdateBaostock()
    elif DATASOURCE == 'tushare':
        startUpdateTushare()
    elif DATASOURCE == 'akshare':
        startUpdateAkshare()
    else:
        logging.warning(
            'DATASCOURCE must be "tushare" or "baostock" or "akshare"')

    return

    # 更新复权因子
    updateAdjFacotrTushare()

    # 更新非季报表格
    # 财务披露表（另外单独更新）
    # 质押表（另外单独更新）
    # 业绩预告（另外单独更新）
    # 业绩快报（另外单独更新）
    # 分红送股（另外单独更新）

    # 更新股票季报数据
    # 资产负债表
    # 利润表
    # 现金流量表
    # 财务指标表
    updateQuarterData()

    # 更新股票TTM利润
    updateTTMProfits()

    # 更新行业列表
    updateClassifyList()

    # 更新行业利润
    updateClassifyProfits()

    # 计算行业PE
    updateClassifyPE()

    # 更新股票估值, 废弃, 用股票评分代替
    # updateGuzhiData()

    # 更新股票评分
    updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    updateAllMarketPE()


@logfun
def startUpdateBaostock():
    """自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
    """

    # 更新交易日历
    updateTradeCalBaostock()

    # 更新股票列表
    updateStockListBaostock()

    return
    # 更新股票日交易数据
    updateDailyTushare()

    # 更新每日指标
    updateDailybasicTushare()

    # 更新复权因子
    updateAdjFacotrTushare()

    # 更新非季报表格
    # 财务披露表（另外单独更新）
    # 质押表（另外单独更新）
    # 业绩预告（另外单独更新）
    # 业绩快报（另外单独更新）
    # 分红送股（另外单独更新）

    # 更新股票季报数据
    # 资产负债表
    # 利润表
    # 现金流量表
    # 财务指标表
    updateQuarterData()

    # 更新股票TTM利润
    updateTTMProfits()

    # 更新行业列表
    updateClassifyList()

    # 更新行业利润
    updateClassifyProfits()

    # 计算行业PE
    updateClassifyPE()

    # 更新股票估值, 废弃, 用股票评分代替
    # updateGuzhiData()

    # 更新股票评分
    updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    updateAllMarketPE()


def startUpdateTushare():
    """自动更新全部数据，包括K线历史数据、利润数据、K线表中的TTM市盈率
    """
    # 更新交易日历
    updateTradeCalTushare()

    # 更新股票列表
    updateStockListTushare()

    # 更新股票日交易数据
    updateDailyTushare()

    # 更新每日指标
    updateDailybasicTushare()

    # 更新复权因子
    updateAdjFacotrTushare()

    # 更新非季报表格
    # 财务披露表（另外单独更新）
    # 质押表（另外单独更新）
    # 业绩预告（另外单独更新）
    # 业绩快报（另外单独更新）
    # 分红送股（另外单独更新）

    # 更新股票季报数据
    # 资产负债表
    # 利润表
    # 现金流量表
    # 财务指标表
    updateQuarterData()

    # 更新股票TTM利润
    updateTTMProfits()

    # 更新行业列表
    updateClassifyList()

    # 更新行业利润
    updateClassifyProfits()

    # 计算行业PE
    updateClassifyPE()

    # 更新股票估值, 废弃, 用股票评分代替
    # updateGuzhiData()

    # 更新股票评分
    updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    updateAllMarketPE()


@logfun
def startUpdateAkshare():
    """自动更新全部数据,包括K线历史数据、利润数据、K线表中的TTM市盈率
    标星部分为等补充函数
    """
    pass

    # 更新交易日历
    # downTradeCal()
    # print('下载日历数据， AKShare')

    # 更新股票列表
    # downStockList()

    # 更新股票日交易数据
    downloadDaily()

    # 更新每日指标
    # downloadDailyBasic()

    # 更新复权因子*
    # updateAdjFacotrTushare()

    # 更新非季报表格*
    # 财务披露表（另外单独更新）*
    # 质押表（另外单独更新）*
    # 业绩预告（另外单独更新）*
    # 业绩快报（另外单独更新）*
    # 分红送股（另外单独更新）*

    # 更新股票季报数据*
    # 资产负债表*
    # 利润表*
    # 现金流量表*
    # 财务指标表*
    # updateQuarterData()

    # 更新股票TTM利润*
    # updateTTMProfits()

    # 更新行业列表*
    # updateClassifyList()

    # 更新行业利润*
    # updateClassifyProfits()

    # 计算行业PE*
    # updateClassifyPE()

    # 更新股票估值, 废弃, 用股票评分代替
    # updateGuzhiData()

    # 更新股票评分*
    # updatePf()

    # 更新指数数据及PE
    # downloadIndexDaily()
    # downloadIndexDailyIndicator()

    # 更新ETF每日净值
    # downloadETFDaily()

    # 更新全市PE*
    # updateAllMarketPE()


@logfun
def updateClassifyPE():
    sql = 'select max(date) from classify_pe'
    result = engine.execute(sql).fetchone()
    if result:
        startDate = (result[0] + dt.timedelta(days=1)).strftime('%Y%m%d')
    else:
        startDate = '20000101'
    endDate = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    dates = readCal(startDate, endDate)
    if dates:
        for _date in dates:
            calClassifyPE(_date)


@logfun
def updateAllMarketPE():
    """
    更新全市场PE
    :return:
    """
    dataName = 'index_all'
    startDate = readUpdate(dataName, offsetdays=1)
    calAllPEHistory(startDate)
    # setUpdate(dataName)


@logfun
def updateClassifyProfits(replace=False):
    """
    更新行业利润
    :return:
    """
    sql = 'select max(end_date) from classify_profits'
    result = engine.execute(sql).fetchone()
    if result is None:
        startDate = '20121231'
    else:
        startDate = result[0].strftime('%Y%m%d')
    endDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    _endDate = classifyEndDate(endDate)
    dates = quarterList(startDate, _endDate)
    for date in dates:
        calClassifyStaticTTMProfit(date, replace=replace)


@logfun
def updateIndex():
    """
    更新指数数据及PE
    :return:
    """
    # downIndexBasic()
    downIndexDaily()
    downIndexDailyBasic()
    downIndexWeight()

    ID = '000010.SH'
    # startDate = getIndexKlineUpdateDate() + dt.timedelta(days=1)
    # startDate = getIndexPEUpdateDate()
    startDate = readUpdate('index_000010.SH')
    # startDate = getIndexPEUpdateDate() + dt.timedelta(days=1)
    calIndexPE(ID, startDate)


@logfun
def updateClassifyList():
    downClassify()


@logfun
def updateQuarterData():
    """更新股票季报数据

    :return:
    """
    # 每支股票最后更新季报日期与每日指标中的最后日期计算的销售收入是否存在差异
    # 如有差异则说明需更新季报
    stocks = checkQuarterData()
    stocks.set_index('ts_code', inplace=True)

    for ts_code in stocks.index:
        e_date = stocks.loc[ts_code, 'e_date']
        datestr = None
        if e_date is not None:
            # _date = dt.datetime.strptime(e_date, '%Y%m%d')
            e_date += relativedelta(days=1)
            datestr = e_date.strftime('%Y%m%d')
        downloader = DownloaderQuarterTushare(ts_code=ts_code,
                                              startDate=datestr)
        downloader.run()


@logfun
def updatePf():
    """ 重算评分
    """
    sql = 'select max(date) from valuation'
    result = engine.execute(sql).fetchone()
    if result is None:
        startDate = '20090101'
    else:
        startDate = result[0] + dt.timedelta(days=1)
        startDate = startDate.strftime('%Y%m%d')
    endDate = dt.datetime.today() - dt.timedelta(days=1)
    endDate = endDate.strftime('%Y%m%d')
    dates = readCal(startDate, endDate)
    if dates:
        for date in dates:
            # print('计算评分：', date)
            logging.debug(f'计算评分： {date}')
            valuation.calpfnew(date)


def readStockListFromFile(filename):
    stockFile = open(filename, 'r')
    stockList = [i[:6] for i in stockFile.readlines()]
    print(stockList)
    return stockList


@logfun
def updateDailyBaostock():
    """更新日K线和每日指标
    本函数需重写
    """
    pass
    # bs.login()
    # sql = 'select max(trade_date) from daily'
    # startDate = engine.execute(sql).fetchone()[0]
    # assert isinstance(startDate, dt.date), 'startDate应为date类型'
    # startDate = (startDate + dt.timedelta(days=1)).strftime('%Y-%m-%d')
    # stocks = readStockList()
    # dates = readCal(startDate, endDate)
    # if dates:
    #     for d in dates:
    #         downDailyTushare(d)


@logfun
def updateDailyTushare():
    """更新日K线
    """
    sql = 'select max(trade_date) from daily'
    startDate = engine.execute(sql).fetchone()[0]
    assert isinstance(startDate, dt.date), 'startDate应为date类型'
    startDate = (startDate + dt.timedelta(days=1)).strftime('%Y%m%d')
    endDate = dt.datetime.now().strftime('%Y%m%d')
    dates = readCal(startDate, endDate)
    if dates:
        for d in dates:
            downDailyTushare(d)


@logfun
def updateDailybasicTushare():
    """更新每日指标
    """
    sql = 'select max(trade_date) from daily_basic'
    lastdate = engine.execute(sql).fetchone()[0]
    lastdate += dt.timedelta(days=1)
    startDate = lastdate.strftime('%Y%m%d')
    endDate = dt.datetime.today().date() - dt.timedelta(days=1)
    endDate = endDate.strftime('%Y%m%d')
    dates = readCal(startDate, endDate)
    if dates:
        for d in dates:
            downDailyBasic(tradeDate=d)


@logfun
def updateTradeCalBaostock():
    """更新交易日历
    """
    sql = 'select max(cal_date) from trade_cal'
    lastDate = engine.execute(sql).fetchone()[0]
    if lastDate < dt.date.today():
        downTradeCalBaostock(lastDate.strftime('%Y-%m-%d'))


@logfun
def updateTradeCalTushare():
    """更新交易日历
    """
    sql = 'select year(max(cal_date)) from trade_cal'
    lastYear = engine.execute(sql).fetchone()[0]
    if lastYear is None:
        downTradeCalTushare('1990')
    elif lastYear < dt.datetime.today().year:
        downTradeCalTushare(str(int(lastYear) + 1))


@logfun
def updateTTMProfits():
    """更新股票TTM利润
    上次更新日至当前日期间有新财务报表的，更新这几期财报的ttmprofits
    """
    pass
    startDate = readTTMProfitsUpdate()
    endDate = dayDelta(dt.datetime.today(), days=-1)
    dates = quarterList(startDate, endDate, includeStart=False)
    if dates is not None:
        for d in dates:
            calAllTTMProfits(d)
            # setUpdate('ttmprofits', d)


@logfun
def updateStockListTushare():
    downStockListTushare()


@logfun
def updateStockListBaostock():
    downStockListBaostock()


@logfun
def updateAdjFacotrTushare():
    """
    下载复权因子
    前复权 = 当日收盘价 × 当日复权因子 / 最新复权因子	qfq
    后复权 = 当日收盘价 × 当日复权因子	hfq
    :return:
    """
    sql = 'select max(trade_date) from adj_factor'
    result = engine.execute(sql).fetchone()
    if result is None or result[0] is None:
        startDate = '20090101'
    else:
        startDate = result[0].strftime('%Y%m%d')
    endDate = dt.datetime.today().strftime('%Y%m%d')
    dateList = readCal(startDate=startDate, endDate=endDate)
    for d in dateList:
        downAdjFactorTushare(d)


if __name__ == '__main__':
    initlog()

    #     logging.debug('This is debug message')
    #     logging.info('This is info message')
    #     logging.warning('This is warning message')

    # 使用baostock数据源时需先做登录操作
    lg = bs.login()
    if lg.error_code != '0':
        logging.error('login baostock failed')

    # 加载tushare.pro用户的token,需事先在sql.conf文件中设置
    ts.set_token(TUSHARETOKEN)
    print('设置访问tushare的token:', TUSHARETOKEN)

    #     updateDataTest()

    startUpdate()

    # updateLirun()
    # updatePf()

#     stockList = sqlrw.readts_codesFromSQL()
#     updateGubenSingleThread()
#     updateMainTableSingleThread(stockList)
#     stockList = stockList[:10]
#     threadNum = 20
#     updateGuzhi(stockList, threadNum)


#     ts_code = '000005'
#     sqlrw.downMainTable(ts_code)
