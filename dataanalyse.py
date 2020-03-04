# -*- coding: utf-8 -*-
"""
Created on 2016年5月4日

@author: who8736
"""

import logging
import os
import datetime as dt
from datetime import datetime
# from functools import partial

import pandas as pd
import numpy as np

import datatrans
from datatrans import dateList
import sqlrw
from sqlrw import engine, Session
from initlog import initlog


# def getLowPEStockList(maxPE=40):
#     """选取指定范围PE的股票
#     maxPE: 最大PE
#     """


def calGuzhi(stockList=None):
    """生成估值水平评估列表，
    # 包括以下数据： peg, 未来三个PE预测， 过去6个季度TTM利润增长率， 平均增长率， 增长率方差
    Parameters
    --------
    stockList:list 股票列表 e.g:[600519, 600999]

    Return
    --------
    DataFrame
        stockid: 股票代码
        name: 股票名称
        pe: TTM市盈率
        peg: 股票PEG值
        next1YearPE: 下1年预测PE
        next2YearPE: 下2年预测PE
        next3YearPE: 下3年预测PE
        incrate0: 之前第6个季度TTM利润增长率
        incrate1: 之前第5个季度TTM利润增长率
        incrate2: 之前第4个季度TTM利润增长率
        incrate3: 之前第3个季度TTM利润增长率
        incrate4: 之前第2个季度TTM利润增长率
        incrate5: 之前第1个季度TTM利润增长率
        avgrate: 平均增长率
        madrate: 平均离差率， 按平均离差除以平均值计算，反应TTM利润增长率与平均增长率之间的偏离水平
                 # 该值越小，越体现TTM利润的稳定增长
        stdrate: 标准差差异率， 按标准差除以平均值计算
        pe200: 当前ttmpe在过去200个交易日中的水平，为0时表示当前ttmpe水平为200个交易日以来最低
                #为100时表示当前ttmpe水平为200个交易日以来最高
        pe1000: 参考pe200的说明
    """

    if stockList is None:
        stockList = sqlrw.getLowPEStockList().stockid.values

    #     print stockList.head()
    #     print type(stockList)
    # pe数据
    peDf = sqlrw.readLastTTMPEs(stockList)
    # 估值数据
    #     pegDf = sqlrw.readGuzhiFilesToDf(stockList)
    #     pegDf = sqlrw.readGuzhiSQLToDf(stockList)
    #     pegDf = pd.merge(peDf, pegDf, on='stockid', how='left')
    #     print pegDf.head()

    # TODO:　假设当前为第2季度，但第1季度上市公司的财务报告未公布，导致缺少数据如何处理
    sectionNum = 6  # 取6个季度
    # 新取TTM利润方法，取每支股票最后N季度数据
    incDf = sqlrw.readLastTTMLirun(stockList, sectionNum)
    #     print 'incDf:'
    #     print incDf
    guzhiDf = pd.merge(peDf, incDf, on='stockid', how='left')

    # 原取TTM利润方法，按当前日期计算取前N季度数据， 但第1季度上市公司的财务报告未公布，导致缺少数据无法处理
    #     endDate = datatrans.getLastQuarter()
    #     startDate = datatrans.quarterSub(endDate, sectionNum - 1)
    #     quarter  = (int(endDate / 10) * 4 + (endDate % 10)) - sectionNum
    #     QuarterList = datatrans.QuarterList(startDate, endDate)
    #     print QuarterList

    # 过去N个季度TTM利润增长率
    #     for i in range(sectionNum):
    #         incDf = sqlrw.readTTMLirunForDate(QuarterList[i])
    #         incDf = incDf[['stockid', 'incrate']]
    #         incDf.columns = ['stockid', 'incrate%d' % i]
    #         print incDf.head()
    #         pegDf = pd.merge(pegDf, incDf, on='stockid', how='left')
    #         pegDf = pd.merge(pegDf, incDf, on='stockid')

    #     print pegDf.head()
    # 平均利润增长率
    endfield = 'incrate%s' % (sectionNum - 1)
    guzhiDf['avgrate'] = guzhiDf.loc[:,
                         'incrate0':endfield].mean(axis=1).round(2)
    #     pegDf = pegDf.round(2)
    #     f = partial(Series.round, decimals=2)
    #     df.apply(f)

    # 平均利润增长率（另一种计算方法）
    #     pegDf['avgrate'] = 0
    #     for i in range(sectionNum):
    #         pegDf['avgrate'] += pegDf['incrate%d' % i]
    #     pegDf['avgrate'] /= sectionNum

    # 计算每行指定列的平均绝对离差
    lirunmad = guzhiDf.loc[:, 'incrate0':endfield].mad(axis=1)
    # 计算每行指定列的平均值
    #     lirunmean = df.loc[:, 'incrate0':'incrate5'].mean(axis=1).head()
    # 计算每行指定列的平均绝对离差率
    guzhiDf['madrate'] = lirunmad / abs(guzhiDf['avgrate'])
    guzhiDf['madrate'] = guzhiDf['madrate'].round(2)
    # 计算每行指定列的平均绝对离差率
    lirunstd = guzhiDf.loc[:, 'incrate0':endfield].std(axis=1)
    guzhiDf['stdrate'] = lirunstd / abs(guzhiDf['avgrate'])
    guzhiDf['stdrate'] = guzhiDf['stdrate'].round(2)
    #     print type(lirunstd / pegDf['avgrate'])

    # 当avgrate为0时，madrate和stdrate将无法计算，结果存为inf
    # 将inf替换为-9999
    guzhiDf.replace([np.inf, -np.inf], -9999, inplace=True)

    # 增加股票名称
    nameDf = sqlrw.readStockListDf()
    guzhiDf = pd.merge(guzhiDf, nameDf, on='stockid', how='left')
    #     print pegDf

    # 计算pe200与pe1000
    df = peHistRate(stockList, 200)
    guzhiDf = pd.merge(guzhiDf, df, on='stockid', how='left')
    df = peHistRate(stockList, 1000)
    guzhiDf = pd.merge(guzhiDf, df, on='stockid', how='left')
    #     print pegDf
    # 设置输出列与列顺序
    # 因无法取得数据，删除'peg', 'next1YearPE',  'next2YearPE',  'next3YearPE'
    guzhiDf = guzhiDf[['stockid', 'name', 'pe',
                       'incrate0', 'incrate1', 'incrate2',
                       'incrate3', 'incrate4', 'incrate5',
                       'avgrate', 'madrate', 'stdrate', 'pe200', 'pe1000'
                       ]]
    #     guzhiDf = guzhiDf[['stockid', 'name', 'pe', 'peg',
    #                        'next1YearPE',  'next2YearPE',  'next3YearPE',
    #                        'incrate0', 'incrate1', 'incrate2',
    #                        'incrate3', 'incrate4', 'incrate5',
    #                        'avgrate', 'madrate', 'stdrate', 'pe200', 'pe1000'
    #                        ]]
    return guzhiDf


def calHistoryStatus(stockID):
    TTMLirunDf = sqlrw.readTTMLirunForStockID(stockID)
    dates = TTMLirunDf['date']
    for _date in dates:
        #         print i
        result = _calHistoryStatus(stockID, TTMLirunDf, _date)
        integrity, seculargrowth, growthmadrate, averageincrement = result
        sql = ('insert ignore into guzhihistorystatus (`stockid`, `date`, '
               '`integrity`, `seculargrowth`, `growthmadrate`, '
               '`averageincrement`) '
               'values ("%(stockID)s", "%(_date)s", %(integrity)r, '
               '%(seculargrowth)r, "%(growthmadrate)s", '
               '"%(averageincrement)s");') % locals()
        print(sql)
        sqlrw.engine.execute(sql)


# def _calHistoryStatus(stockID, TTMLirunDf, date):
def _calHistoryStatus(TTMLirunDf, date):
    """
    """
    _startDate = datatrans.quarterSub(date, 11)
    print('_calHistoryStatus, current date: %s, start date: %s' % (date,
                                                                   _startDate))
    _TTMLirunDf = TTMLirunDf[(TTMLirunDf.date >= _startDate) &
                             (TTMLirunDf.date <= date)]
    print(len(_TTMLirunDf))
    if len(_TTMLirunDf) == 12:
        integrity = True
    else:
        integrity = False

    if len(_TTMLirunDf[_TTMLirunDf.incrate > 0]) == 12:
        seculargrowth = True
    else:
        seculargrowth = False

    # 计算利润增长率的平均绝对离差
    lirunMad = _TTMLirunDf['incrate'].mad()
    # 计算利润增长率的平均值
    lirunAverage = _TTMLirunDf['incrate'].mean()
    # 计算利润增长率的平均绝对离差率
    growthmadrate = round(lirunMad / abs(lirunAverage), 2)
    print(integrity, seculargrowth, growthmadrate)
    print(_TTMLirunDf[_TTMLirunDf.date == date])
    return [integrity, seculargrowth, growthmadrate, lirunAverage]


def peHistRate(stockList, dayCount, date=None):
    """ 计算一组股票在过去指定天数内的PE水平，
        # 最低为0，最高为100
        # 历史交易天数不足时，PE水平为-1
    """
    print(f'开始计算peHistRate: {dayCount}, stockList count:{len(stockList)}')
    perates = []
    for stockID in stockList:
        # print(stockID)
        sql = f'select ttmpe from klinestock where stockid="{stockID}" '
        if date is not None:
            sql += f' and date<="{date}"'
        sql += f'order by `date` desc limit {dayCount};'
        result = sqlrw.engine.execute(sql)
        peList = result.fetchall()
        # 如果历史交易天数不足，则历史PE水平为-1
        if len(peList) != dayCount or peList[0] is None:
            perates.append(-1)
        else:
            peList = [i[0] for i in peList]
            peCur = peList[0]
            perate = float(sum(1 for i in peList if
                               i is not None and i < peCur)) / dayCount * 100
            #             print stockID, perate, peList
            perates.append(perate)
    return pd.DataFrame({'stockid': stockList, f'pe{dayCount}': perates})


def youzhiSelect(pegDf):
    """ 从估值分析中筛选出各项指标都合格的
    # 筛选条件：1、 peg不为空，且大于0，小于1
             2、平均增长率大于0
             3、平均绝对离差率小于一定范围（待确定）
    Parameters
    --------
    pegDf:DataFrame 股票估值分析列表， 结构同calGuzhi()函数输出格式

    Return
    --------
    DataFrame: 筛选后的估值分析表格

    # 2017-07-28：　因无法取得peg数据， 临时取消peg筛选条件
    """
    #     print pegDf.head()
    pegDf = pegDf.dropna()
    #     pegDf = pegDf[pegDf.peg.notnull()]
    #     pegDf = pegDf[(pegDf.peg > 0) & (pegDf.peg < 1) & (pegDf.avgrate > 0)]
    pegDf = pegDf[pegDf.avgrate > 0]
    pegDf = pegDf[pegDf.pe < 30]
    pegDf = pegDf[(pegDf.pe200 < 20) & (pegDf.pe1000 < 30)]
    pegDf = pegDf[pegDf.madrate < 0.6]
    pegDf = pegDf.sort_values(by='pe')
    #     pegDf = pegDf[['stockid', 'pe', 'peg',
    #                    'next1YearPE',  'next2YearPE',  'next3YearPE',
    #                    'incrate0', 'incrate1', 'incrate2',
    #                    'incrate3', 'incrate4', 'incrate5',
    #                    'avgrate'
    #                    ]]
    #     print pegDf.head()
    #     print len(pegDf)
    return pegDf


# def dfToCsvFile(df, filename):
#    #     filename = u'.\\youzhi.csv'
#    return df.to_csv(filename)


def testChigu():
    #     youzhiSelect()
    #     inFilename = './data/chigustockid.txt'
    # outFilename = './data/chiguguzhi.csv'
    #     testStockList = ['600519', '600999', '000651', '000333']
    #     testStockList = sqlrw.readStockListFromFile(inFilename)
    stockList = sqlrw.loadChigu()
    #     print testStockList
    df = calGuzhi(stockList)
    #     df = calGuzhi()
    #    dfToCsvFile(df, outFilename)
    #     df.to_csv(outFilename)
    sqlrw.engine.execute('TRUNCATE TABLE chiguguzhi')
    #     df.index.name = 'stockid'
    #     clearStockList()
    #     df.set_index('stockid', inplace=True)
    #     print df.head()
    sqlrw.writeSQL(df, 'chiguguzhi')


#     df.to_sql(u'chiguguzhi',
#               sqlrw.engine,
#               if_exists=u'append')


def testShaixuan():
    stockList = sqlrw.readStockListDf().stockid.values
    df = calGuzhi(stockList)
    df = df.dropna()
    sqlrw.engine.execute('TRUNCATE TABLE guzhiresult')
    sqlrw.writeSQL(df, 'guzhiresult')
    df = youzhiSelect(df)
    print('youzhiSelect result:')
    print(df.head())
    outFilename = './data/youzhi.csv'
    #    dfToCsvFile(df, outFilename)
    df.to_csv(outFilename)
    #     outFilename = './data/youzhiid.txt'
    #     sqlrw.writeStockIDListToFile(df['stockid'], outFilename)
    sqlrw.engine.execute('TRUNCATE TABLE youzhiguzhi')
    sqlrw.writeSQL(df, 'youzhiguzhi')


def calAllPEHistory(startDate, endDate=None):
    """
    计算全市场TTMPE
    :param startDate:
    :param endDate:
    :return:
    """
    # startDate = datetime.strptime('2010-01-01', '%Y-%m-%d').date()
    endDate = datetime.today().date()
    session = Session()
    for tradeDate in dateList(startDate, endDate):
        sql = 'call calallpe("%(tradeDate)s");' % locals()
        print(sql)
        session.execute(sql)
    session.commit()
    session.close()


def calPEHistory(ID, startDate, endDate=None):
    """
    计算某一指数的TTMPE
    :param ID: 短格式的指数代码，如: 000010
    :param startDate:
    :param endDate:
    :return:
    """
    # startDate = datetime.strptime('2019-06-17', '%Y-%m-%d').date()
    # assert len(ID) == 9, '指数代码错误， 正确格式：000010.SH'
    assert len(ID) == 6, '指数代码错误， 正确格式：000010'
    ID = ID.upper()
    if endDate is None:
        endDate = datetime.today().date()
    # session = Session()
    for tradeDate in dateList(startDate, endDate):
        sql = 'call calchengfenpe("%(ID)s", "%(tradeDate)s");' % locals()
        print(sql)
        engine.execute(sql)
        # session.execute(sql)
    # session.commit()
    # session.close()


def analysePEHist(stockID, startDate, endDate, dayCount=200,
                  lowRate=20, highRate=80):
    """分析指定股票一段时期内PE水平，以折线图展示

    :param stockID:
    :param startDate:
    :param endDate:
    :param lowRate:
    :param highRate:
    :return:
    """
    sql = (f'select date, ttmpe from klinestock where stockid={stockID}'
           f' and date>="{startDate}" and date<="{endDate}";')
    result = engine.execute(sql).fetchall()
    tmpDates, tmpPEs = zip(*result)
    tmpDates = list(tmpDates)
    tmpPEs = list(tmpPEs)
    dates = []
    PEs = []
    lowPEs = []
    highPEs = []
    cnt = len(result)
    lowPos = dayCount * lowRate // 100 - 1
    highPos = dayCount * highRate // 100 - 1
    for i in range(dayCount - 1, cnt):
        dates.append(tmpDates[i])
        PEs.append(tmpPEs[i])
        start = i - dayCount + 1
        end = i + 1
        _tmpPEs = tmpPEs[start:end]
        _tmpPEs.sort()
        lowPE = _tmpPEs[lowPos]
        highPE = _tmpPEs[highPos]
        lowPEs.append(lowPE)
        highPEs.append(highPE)

    df = pd.DataFrame({'date': dates, 'pe': PEs,
                       'lowpe': lowPEs, 'highpe': highPEs})
    print(df)
    return df

if __name__ == '__main__':
    initlog()

    timec = dt.datetime.now()
    #    testStockID = u'601398'
    stockID = '000651'
    startDate = '20130101'
    endDate = '20191231'
    logging.info('===================start=====================')

    # 测试持股估值
    #     testChigu()

    # 测试筛选估值
    # testShaixuan()

    # 测试TTMPE直方图、概率分布
    #     ttmdf = sqlrw.readTTMPE(testStockID)
    #     ttmdf = ttmdf[-200:]
    #     ttmdf.plot()
    #     print ttmdf.head()
    #     print ttmdf.tail()
    #
    #     a = ttmdf.plot(kind='kde')
    #     print 'type a :', type(a)
    #
    #     b = ttmdf.hist(bins=20)
    #     print 'type b :', type(b)

    #    c = ttmdf.hist().get_figure()
    #    print 'type c :', type(c)

    # 生成历史估值状态
    #     calHistoryStatus('000333')

    # 测试估值计算函数
    #     calGuzhi()

    # 测试历史估值水平
    df = analysePEHist(stockID, startDate, endDate, dayCount=1000)
    df.plot()

    timed = dt.datetime.now()
    logging.info('datamanage test took %s' % (timed - timec))
    logging.info('===================end=====================')
