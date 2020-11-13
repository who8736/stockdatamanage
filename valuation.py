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
import os

import pandas as pd
import numpy as np

import analyse.report
import initsql
import classifyanalyse
import analyse
import sqlrw
from sqlconn import engine
from sqlrw import readLastTTMPEs, readStockList
import pushdata
from config import Config

# 定义指标常数
LOWPE = 20


def lowpe(stock):
    """ 判断某支股票为低市盈率时返回1，否则返回0
    """
    return 1 if 0 < stock.pe <= LOWPE else 0


def lowhype(stock):
    """ 判断某支股票低于行业市盈率时返回1，否则返回0
    """
    return 1 if (stock.pe is not None and stock.classify_pe is not None
                 and 0 < stock.pe < stock.classify_pe) else 0


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
    # print(stock.ts_code)
    # print(z)
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


def peZ(stock, dayCount, date=None):
    """ 计算一支股票指定日期PE的Z值，
        # 历史交易天数不足时，PE水平为-1
    """
    ts_code = stock.ts_code
    sql = f'select pe_ttm from daily_basic where ts_code="{ts_code}"'
    if date is not None:
        sql += f' and trade_date<="{date}"'
    sql += f' order by trade_date desc limit {dayCount};'
    # print(sql)
    peDf = pd.read_sql(sql, engine)
    # 如果历史交易天数不足，则本项指标为0
    if len(peDf.index) != dayCount:
        return 0
    pe = peDf.pe_ttm[0]
    avg = peDf.pe_ttm.mean()
    std = peDf.pe_ttm.std()
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
    stocks = readStockList()
    # print(stocks)
    # 低市盈率
    peDf = readLastTTMPEs(stocks.ts_code.tolist())
    stocks = pd.merge(stocks, peDf, on='ts_code', how='left')
    stocks['lowpe'] = stocks.apply(lowpe, axis=1)

    # 市盈率低于行业平均
    sql = 'select ts_code, hyid from hangyestock;'
    hyDf = pd.read_sql(sql, engine)
    stocks = pd.merge(stocks, hyDf, on='ts_code', how='left')
    hyPEDf = classifyanalyse.getClassifyPE()
    stocks = pd.merge(stocks, hyPEDf, on='hyid', how='left')
    stocks['lowhype'] = stocks.apply(lowhype, axis=1)

    # 过去6个季度利润稳定增长
    sectionNum = 6  # 取6个季度
    incDf = sqlrw.readLastTTMProfits(stocks.ts_code.tolist(), sectionNum)
    stocks = pd.merge(stocks, incDf, on='ts_code', how='left')
    stocks['avg'] = incDf.mean(axis=1).round(2)
    stocks['std'] = incDf.std(axis=1).round(2)
    stocks['wdzz'] = incDf.apply(wdzz, axis=1)

    # 利润增长的平均标准差与平均增长率的比值， 小于1时判断为增长稳定
    stocks['wdzz1'] = incDf.apply(wdzz1, axis=1)

    # 根据过去6季度TTM利润平均增长率与TTMPE计算PEG
    stocks['peg'] = stocks['pe'] / stocks['avg']
    stocks['peg'] = stocks['peg'].round(2)
    stocks['lowpeg'] = stocks.apply(lowPEG, axis=1)

    # 200天Z值小于-1
    stocks['pez200'] = stocks.apply(peZ, axis=1, args=(200,))
    stocks['pez200'] = stocks['pez200'].round(2)
    stocks['lowpez200'] = stocks.apply(lowPEZ200, axis=1)

    # 1000天Z值小于-1
    stocks['pez1000'] = stocks.apply(peZ, axis=1, args=(1000,))
    stocks['pez1000'] = stocks['pez1000'].round(2)
    stocks['lowpez1000'] = stocks.apply(lowPEZ1000, axis=1)

    # 计算pe200与pe1000
    # stocks['pe200'] = analyse.peHistRate(stocks.ts_code.tolist(), 200)
    # stocks['pe1000'] = analyse.peHistRate(stocks.ts_code.tolist(), 1000)
    df = analyse.report.peHistRate(stocks.ts_code.tolist(), 200)
    stocks = pd.merge(stocks, df, on='ts_code', how='left')
    df = analyse.report.peHistRate(stocks.ts_code.tolist(), 1000)
    stocks = pd.merge(stocks, df, on='ts_code', how='left')

    # 计算总评分
    stocks['pf'] = stocks.lowpe
    stocks['pf'] += stocks.lowhype
    stocks['pf'] += stocks.wdzz1
    stocks['pf'] += stocks.lowpeg
    stocks['pf'] += stocks.lowpez200
    stocks['pf'] += stocks.lowpez1000
    stocks = stocks.sort_values(by='pf', ascending=False)

    # 设置输出列与列顺序
    #     guzhiDf = guzhiDf[['ts_code', 'name', 'pe',
    #                        'incrate0', 'incrate1', 'incrate2',
    #                        'incrate3', 'incrate4', 'incrate5',
    #                        'avgrate', 'madrate', 'stdrate', 'pe200', 'pe1000'
    #                        ]]

    #     mystocks = ['002508', '600261', '002285', '000488',
    #                 '002573', '300072', '000910']
    #     mystockspf = stocks[stocks['ts_code'].isin(mystocks)]
    #     mystockspf.set_index(['ts_code'], inplace=True)
    #     mystockspf.to_csv('./data/valuationmystocks.csv')

    # 保存评价结果
    stocks.set_index(['ts_code'], inplace=True)
    stocks.to_csv('./data/valuation.csv')
    #    print stocks
    if initsql.existTable('valuation'):
        engine.execute('TRUNCATE TABLE valuation')
    stocks = stocks.dropna()

    # 当计算peg时，如果平均增长率为0，则结果为inf
    # 将inf替换为-9999
    stocks.replace([np.inf, -np.inf], -9999, inplace=True)

    stocks.to_sql('valuation', engine, if_exists='append')
    return stocks


# noinspection PyTypeChecker
def calpfnew(_date, replace=False):
    """ 根据各指标计算评分，分别写入文件和数据库
        新版，支持按指定日期计算评分，评分结果写入带日期的新表
    :param _date: str
    :param replace:  bool
        'YYYYmmdd'格式的日期
    :return:
    """
    #    stocks = readStockListDf()[:10]
    if not replace:
        sql = f'select count(1) from valuation where date="{_date}"'
        result = engine.execute(sql).fetchone()[0]
        if result > 0:
            return

    stocks = readStockList()
    # print(stocks)
    # 低市盈率
    peDf = readLastTTMPEs(stocks.ts_code.tolist(), _date)
    if peDf is None:
        return
    stocks = pd.merge(stocks, peDf, on='ts_code', how='inner')
    stocks['lowpe'] = stocks.apply(lowpe, axis=1)

    # 市盈率低于行业平均
    sql = f'select ts_code, classify_code from classify_member where date="{_date}";'
    classifyDf = pd.read_sql(sql, engine)
    stocks = pd.merge(stocks, classifyDf, on='ts_code', how='left')
    classifyPEDf = classifyanalyse.getClassifyPE(_date)
    if classifyPEDf is None or classifyPEDf.empty:
        classifyanalyse.calClassifyPE(_date)
        classifyPEDf = classifyanalyse.getClassifyPE(_date)
    stocks = pd.merge(stocks, classifyPEDf, on='classify_code', how='left')
    stocks['lowhype'] = stocks.apply(lowhype, axis=1)

    # 过去6个季度利润稳定增长
    sectionNum = 6  # 取6个季度
    incDf = sqlrw.readLastTTMProfits(stocks.ts_code.tolist(), sectionNum, _date)
    stocks = pd.merge(stocks, incDf, on='ts_code', how='left')
    stocks['avg'] = incDf.mean(axis=1).round(2)
    stocks['std'] = incDf.std(axis=1).round(2)
    stocks['wdzz'] = incDf.apply(wdzz, axis=1)

    # 利润增长的平均标准差与平均增长率的比值， 小于1时判断为增长稳定
    stocks['wdzz1'] = incDf.apply(wdzz1, axis=1)

    # 根据过去6季度TTM利润平均增长率与TTMPE计算PEG
    stocks['peg'] = stocks['pe'] / stocks['avg']
    stocks['peg'] = stocks['peg'].round(2)
    stocks['lowpeg'] = stocks.apply(lowPEG, axis=1)

    # 200天Z值小于-1
    stocks['pez200'] = stocks.apply(peZ, axis=1, args=(200, _date))
    stocks['pez200'] = stocks['pez200'].round(2)
    stocks['lowpez200'] = stocks.apply(lowPEZ200, axis=1)

    # 1000天Z值小于-1
    stocks['pez1000'] = stocks.apply(peZ, axis=1, args=(1000, _date))
    stocks['pez1000'] = stocks['pez1000'].round(2)
    stocks['lowpez1000'] = stocks.apply(lowPEZ1000, axis=1)
    # return stocks

    # 计算pe200与pe1000
    df = analyse.report.peHistRate(stocks.ts_code.tolist(), 200, _date)
    stocks = pd.merge(stocks, df, on='ts_code', how='left')
    df = analyse.report.peHistRate(stocks.ts_code.tolist(), 1000, _date)
    stocks = pd.merge(stocks, df, on='ts_code', how='left')

    # 计算总评分
    stocks['pf'] = stocks.lowpe
    stocks['pf'] += stocks.lowhype
    stocks['pf'] += stocks.wdzz1
    stocks['pf'] += stocks.lowpeg
    stocks['pf'] += stocks.lowpez200
    stocks['pf'] += stocks.lowpez1000
    stocks = stocks.sort_values(by='pf', ascending=False)

    # 保存评价结果
    stocks.set_index(['ts_code'], inplace=True)
    # stocks.to_csv('./data/valuation.csv')
    pfFilename = f'valuations{_date}.xlsx'
    stocks.to_excel(os.path.join('data', pfFilename))

    # 将评分发送到邮箱
    cf = Config()
    pushflag = cf.pushData
    if pushflag:
        mailTitle = f'评分{_date}'
        pushdata.push(mailTitle, pfFilename)
    #    print stocks
    # if initsql.existTable('valuation'):
    #     engine.execute('TRUNCATE TABLE valuation')
    stocks = stocks.dropna()
    stocks['date'] = _date

    # 当计算peg时，如果平均增长率为0，则结果为inf
    # 将inf替换为-9999
    stocks.replace([np.inf, -np.inf], -9999, inplace=True)

    sqlrw.writeSQL(stocks, 'valuation', replace)
    return stocks


if __name__ == '__main__':
    pass
    stockspf = calpfnew('20191231', replace=True)
    print('=' * 20)
    print(stockspf)
