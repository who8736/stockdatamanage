# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 15:27:38 2019

@author: ff
"""

import pandas as pd
from pandas import DataFrame
from urllib.request import urlopen
from lxml import etree
from datetime import datetime
import baostock as bs
import tushare as ts

# from download import getreq
from download import *
from sqlrw import *
from sqlconn import engine
from misc import urlGubenEastmoney
from misc import *
from initlog import initlog


def downGubenFromEastmoney():
    """ 从东方财富下载总股本变动数据
    url: 
    """
    pass
    stockID = '600000'
    startDate = '2019-04-01'
    bs.login()
    # from misc import usrlGubenEastmoney
    # urlGubenEastmoney('600000')
    gubenURL = urlGubenEastmoney(stockID)
    # req = getreq(gubenURL, includeHeader=True)
    req = getreq(gubenURL)
    guben = urlopen(req).read()

    gubenTree = etree.HTML(guben)
    # //*[@id="lngbbd_Table"]/tbody/tr[1]/th[3]
    gubenData = gubenTree.xpath('//tr')
    gubenData = gubenTree.xpath('''//html//body//div//div
                                //div//div//table//tr//td
                                //table//tr//td//table//tr//td''')
    date = [gubenData[i][0].text for i in range(0, len(gubenData), 2)]
    date = [datetime.strptime(d, '%Y-%m-%d') for d in date]
    #     print date
    totalshares = [
        gubenData[i + 1][0].text for i in range(0, len(gubenData), 2)]
    #     print totalshares
    #     t = [i[:-2] for i in totalshares]
    #     print t
    try:
        totalshares = [float(i[:-2]) * 10000 for i in totalshares]
    except ValueError as e:
        # logging.error('stockID:%s, %s', stockID, e)
        print('stockID:%s, %s', stockID, e)
    #     print totalshares
    gubenDf = DataFrame({'stockid': stockID,
                         'date': date,
                         'totalshares': totalshares})
    return gubenDf

def urlGuben(stockID):
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/stockid'
            '/%s/stocktype/TotalStock.phtml' % stockID)

def downLiutongGubenFromBaostock():
    """ 从baostock下载每日K线数据，并根据成交量与换手率计算流通总股本
    """
    code = 'sz.000651'
    startDate = '2019-03-01'
    endDate = '2019-04-15'
    fields = "date,code,close,volume,turn,peTTM,tradestatus"

    lg = bs.login()
    rs = bs.query_history_k_data_plus(code, fields, startDate, endDate)
    dataList = []
    while rs.next():
        dataList.append(rs.get_row_data())
    result = pd.DataFrame(dataList, columns=rs.fields)
    print(result)
#    lg.logout()
    bs.logout()
    return result

def checkGuben(date='2019-04-19'):
    """ 以下方法用于从tushare.pro下载日频信息中的股本数据
        与数据库保存的股本数据比较，某股票的总股本存在差异时说明股本有变动
        返回需更新的股票列表
    """
    pro = ts.pro_api()
    tradeDate = '%s%s%s' % (date[:4], date[5:7], date[8:])
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
    dfUpdate = df[df.cha>=chaRate]
    print(dfUpdate)
    for stockID in dfUpdate['stockid']:
        sql = ('select max(date) from guben where stockid="%s" limit 1;'
              % stockID)
        dateA = getLastUpdate(sql)
        setGubenLastUpdate(stockID, dateA)

    # 对于需更新股本的股票，逐个更新股本并修改更新日期
    # 对于无需更新股本的股票，将其更新日期修改为上一交易日
    dfFinished = df[df.cha<chaRate]
    for stockID in dfFinished['stockid']:
        setGubenLastUpdate(stockID, date)
    # print(df3)
    return dfUpdate


def downGubenShuju(stockID='300445', date='2019-04-19'):
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



def downGubenTest():
    """ 仅做测试用，下载单个股本数据，验证股本下载函数是否正确"""
    downGuben('300445')


def resetKlineExtData():
    """

    :return:
    """
    stockList = sqlrw.readStockIDsFromSQL()
    print(type(stockList))
    print(stockList)
    for stockID in stockList:
        updateKlineEXTData(stockID, '2016-01-01')

    # for()

def resetLirun():
    stockID = '002087'
    startDate = '1990-01-01'
    fields = 'ts_code,ann_date,end_date,total_profit,n_income,n_income_attr_p'
    pro = ts.pro_api()

    stockID = tsCode(stockID)
    df = pro.income(ts_code=stockID, start_date=startDate, fields=fields)
    df['date'] = df['end_date'].apply(transTushareDateToQuarter)
    df['stockid'] = df['ts_code'].apply(lamba
    x: x[:6])
    df['stockid'] = df['ts_code'].apply(lambda x: x[:6])
    df['reportdate'] = df['ann_date'].apply(lambda x: '%s-%s-%s' % (x[:4], x[4:6], x[6:]))
    df.columns
    df.rename(columns={'n_income_attr_p': 'profits'})
    return df


if __name__ == "__main__":
    initlog()
    """072497"""
    pass
    # df = downLiutongGubenFromBaostock()

    # downGubenTest()
    # df['stockid'].apply(downGuben)

    # 检查股本信息，找出需要更新的股票
    # df = checkGuben()

    # print(df)

    # 下载指定股票股本信息
    # date = '2019-04-19'
    # gubenUpdateDf = checkGuben(date)
    # for stockID in gubenUpdateDf['stockid']:
    #     downGubenShuju(stockID)
    #     setGubenLastUpdate(stockID, date)
    #     time.sleep(1)  # tushare.pro每分钟最多访问接口200次
        # downGubenShuju('000157')

    # resetKlineExtData()

    # 重算TTMlirun
    # calAllTTMLirun(20114)

    # 重新下载lirun数据
    df = resetLirun()
