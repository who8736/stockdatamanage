# -*- coding: utf-8 -*-
"""
Created on Wed Oct 25 21:30:01 2017

@author: who8736

# 根据指定规则，对股票进行评价，并计算出一个分值
# 第一阶段，列出需评价的指标，每项分值为1
# 低市盈率
# 市盈率低于行业平均
# 近三年利润增长率算术平均值超10%

"""

import pandas as pd

import initsql
import hyanalyse
import dataanalyse
import sqlrw
from sqlrw import engine
from sqlrw import readStockListDf, readCurrentTTMPEs

# 定义指标常数
LOWPE = 20


def lowpe(stock):
    """ 判断某支股票为低市盈率时返回1，否则返回0
    """
    return 1 if stock.pe > 0 and stock.pe <= LOWPE else 0


def lowhype(stock):
    """ 判断某支股票低于行业市盈率时返回1，否则返回0
    """
    return 1 if stock.pe > 0 and stock.pe < stock.hype else 0


def wdzz(stock):
    """判断某支股票的利润增长是稳定
    任一期利润正增长，任一期利润增长与平均增长率的差的绝对值小于标准差
    """
    allplus = stock[1:] > 0
    if not all(allplus):
        return 0
    avg = stock[1:].mean()
    std = stock[1:].std()
    z = (stock[1:] - avg).abs() / std
    print stock.stockid
    print z
    return 1 if all(z < 1.5) else 0


def wdzz1(stock):
    """判断某支股票的利润增长是稳定
    任一期利润正增长，利润增长的平均标准差与平均增长率的比值， 小于0.6时判断为增长稳定
    """
    allplus = stock[1:] > 0
    if not all(allplus):
        return 0
    avg = stock[1:].mean()
    std = stock[1:].std()
    stdrate = std / avg
    return 1 if stdrate < 0.6 and avg > 10 else 0


def peZ(stock, dayCount):
    """ 计算一支股票指定日期PE的Z值，
        # 历史交易天数不足时，PE水平为-1
    """
    stockID = stock.stockid
    sql = ('select ttmpe from kline%(stockID)s order by `date` desc '
           'limit %(dayCount)s;') % locals()
    print sql
    peDf = pd.read_sql(sql, engine)
    # 如果历史交易天数不足，则本项指标为0
    if len(peDf.index) != dayCount:
        return 0
    pe = peDf.ttmpe[0]
    avg = peDf.ttmpe.mean()
    std = peDf.ttmpe.std()
    z = (pe - avg) / std
#    peDf['z'] = (peDf.ttmpe - avg) / std
    return z


def lowPEG(stock):
    return 1 if stock.pe > 0 and stock.peg < 1 else 0


def lowPEZ200(stock):
    return 1 if stock.pez200 < -1 else 0


def lowPEZ1000(stock):
    return 1 if stock.pez1000 < -1 else 0


def calpf():
    """ 根据各指标计算评分，分别写入文件和数据库
    """
#    stocks = readStockListDf()[:10]
    stocks = readStockListDf()
    print stocks
    # 低市盈率
    peDf = readCurrentTTMPEs(stocks.stockid.tolist())
    stocks = pd.merge(stocks, peDf, on='stockid', how='left')
    stocks['lowpe'] = stocks.apply(lowpe, axis=1)

    # 市盈率低于行业平均
    sql = 'select stockid, hyid from hangyestock;'
    hyDf = pd.read_sql(sql, engine)
    stocks = pd.merge(stocks, hyDf, on='stockid', how='left')
    hyPEDf = hyanalyse.getHYsPE()
    stocks = pd.merge(stocks, hyPEDf, on='hyid', how='left')
    stocks['lowhype'] = stocks.apply(lowhype, axis=1)

    # 过去6个季度利润稳定增长
    sectionNum = 6  # 取6个季度
    incDf = sqlrw.readLastTTMLirun(stocks.stockid.tolist(), sectionNum)
    stocks = pd.merge(stocks, incDf, on='stockid', how='left')
    stocks['avg'] = incDf.mean(axis=1).round(2)
    stocks['std'] = incDf.std(axis=1).round(2)
    stocks['wdzz'] = incDf.apply(wdzz, axis=1)

    # 利润增长的平均标准差与平均增长率的比值， 小于1时判断为增长稳定
    stocks['wdzz1'] = incDf.apply(wdzz1, axis=1)
    pass

    # 根据过去6季度TTM利润平均增长率与TTMPE计算PEG
    stocks['peg'] = stocks['pe'] / stocks['avg']
    stocks['peg'] = stocks['peg'].round(2)
    stocks['lowpeg'] = stocks.apply(lowPEG, axis=1)

    # 200天Z值小于-1
    stocks['pez200'] = stocks.apply(peZ, axis=1, args=(200, ))
    stocks['pez200'] = stocks['pez200'].round(2)
    stocks['lowpez200'] = stocks.apply(lowPEZ200, axis=1)

    # 1000天Z值小于-1
    stocks['pez1000'] = stocks.apply(peZ, axis=1, args=(1000, ))
    stocks['pez1000'] = stocks['pez1000'].round(2)
    stocks['lowpez1000'] = stocks.apply(lowPEZ1000, axis=1)

    # 计算pe200与pe1000
    stocks['pe200'] = dataanalyse.peHistRate(stocks.stockid.tolist(), 200)
    stocks['pe1000'] = dataanalyse.peHistRate(stocks.stockid.tolist(), 1000)

    # 计算总评分
    stocks['pf'] = stocks.lowpe
    stocks['pf'] += stocks.lowhype
    stocks['pf'] += stocks.wdzz1
    stocks['pf'] += stocks.lowpeg
    stocks['pf'] += stocks.lowpez200
    stocks['pf'] += stocks.lowpez1000
    stocks = stocks.sort_values(by='pf', ascending=False)

    # 设置输出列与列顺序
#     guzhiDf = guzhiDf[['stockid', 'name', 'pe',
#                        'incrate0', 'incrate1', 'incrate2',
#                        'incrate3', 'incrate4', 'incrate5',
#                        'avgrate', 'madrate', 'stdrate', 'pe200', 'pe1000'
#                        ]]

    mystocks = ['002508', '600261', '002285', '000488',
                '002573', '300072', '000910']
    mystockspf = stocks[stocks['stockid'].isin(mystocks)]
    mystockspf.set_index(['stockid'], inplace=True)
    mystockspf.to_csv('./data/valuationmystocks.csv')

    # 保存评价结果
    stocks.set_index(['stockid'], inplace=True)
    stocks.to_csv('./data/valuation.csv')
#    print stocks
    if initsql.existTable('valuation'):
        engine.execute('TRUNCATE TABLE valuation')
    stocks.to_sql('valuation', engine, if_exists='append')
    return stocks


if __name__ == '__main__':
    pass
    stockspf = calpf()