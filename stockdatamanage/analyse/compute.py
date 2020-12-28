import datetime as dt
import logging
from datetime import datetime

import numpy as np
import pandas as pd

# from stockdatamanage.analyse import peHistRate
from ..db import engine
from ..db.sqlrw import (
    readCal, readLastTTMPEs, readLastTTMProfits,
    readStockList, readTTMProfitsForStock,
)
from ..util import datatrans


def calAllTTMProfits(end_date, replace=False):
    """计算全部股票本期TTM利润并写入TTMLirun表
    date: 格式YYYYMMDD
    replace: True, 覆盖已有数据， False, 增量更新
    # 计算公式： TTM利润 = 本期利润 + 上年第四季度利润 - 上年同期利润
    # 计算原理：TTM利润为之前连续四个季度利润之和
    # 本期利润包含今年以来产生所有利润，上年第四季度利润 减上年同期利润为上年同期后一个季度至年末利润
    # 两者相加即为TTM利润
    # 举例：2016年1季度TTM利润 = 2016年1季度利润 + 2015年4季度利润  - 2015年1季度利润
    # 数据完整的情况下等同于：
    # 2015年2季度利润 + 2015年3季度利润 + 2015年4季度利润 + 2016年1季度利润
    # 当本期为第4季度时，计算公式仍有效， 如：
    # 2016年4季度TTM利润 = 2016年4季度利润 + 2015年4季度利润  - 2015年4季度利润
    """
    logging.debug(f'更新ttmprofits: {end_date}')
    end_date1 = f'{int(end_date[:4]) - 1}1231'
    end_date2 = f'{int(end_date[:4]) - 1}{end_date[4:]}'
    if replace:
        sql = f'delete from ttmprofits where end_date="{end_date}"'
        engine.execute(sql)
    sql = f'''
            insert into ttmprofits (ts_code, end_date, ttmprofits, ann_date)
            select a.ts_code, '{end_date}' as end_date, (a.p + b.p - c.p) ttmprofits, a.ann_date from 
            (select ts_code, n_income_attr_p p, ann_date from stockdata.income where end_date='{end_date}' and n_income_attr_p is not null) a,
            (select ts_code, n_income_attr_p p from stockdata.income where end_date='{end_date1}' and n_income_attr_p is not null) b,
            (select ts_code, n_income_attr_p p from stockdata.income where end_date='{end_date2}' and n_income_attr_p is not null) c,
            (select ts_code from stock_basic where ts_code not in
            (select ts_code from ttmprofits where end_date='{end_date}')) d
            where a.ts_code=d.ts_code and a.ts_code=b.ts_code and a.ts_code=c.ts_code and a.ann_date is not null
            '''
    engine.execute(sql)
    calTTMProfitsIncRate(end_date)



def calTTMProfitsIncRate(end_date, replace=False):
    """计算全部股票本期TTM利润增长率并写入TTMLirun表
    date: 格式YYYYMMDD
    # 计算公式： TTM利润增长率= (本期TTM利润  - 上年同期TTM利润) / TTM利润 * 100
    """
    sql = f'''
            update ttmprofits t1,
            (select ts_code, ttmprofits from ttmprofits 
                where end_date='{int(end_date[:4]) - 1}{end_date[4:]}') t2
            set t1.inc = round((t1.ttmprofits / t2.ttmprofits - 1) * 100, 2)
            where t1.end_date='{end_date}' 
                and t1.ts_code=t2.ts_code and t1.ttmprofits is not null 
                and t2.ttmprofits is not null
    '''
    if not replace:
        sql += ' and t1.inc is null '
    engine.execute(sql)


def calGuzhi(stocks=None):
    """生成估值水平评估列表，
    # 包括以下数据： peg, 未来三个PE预测， 过去6个季度TTM利润增长率， 平均增长率， 增长率方差
    Parameters
    --------
    stockList: DataFrame 股票列表

    Return
    --------
    DataFrame
        ts_code: 股票代码
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

    if stocks is None:
        stocks = getLowPEStockList()

    #     print stockList.head()
    #     print type(stockList)
    # pe数据
    peDf = readLastTTMPEs(stocks)
    # 估值数据
    #     pegDf = readGuzhiFilesToDf(stockList)
    #     pegDf = readGuzhiSQLToDf(stockList)
    #     pegDf = pd.merge(peDf, pegDf, on='ts_code', how='left')
    #     print pegDf.head()

    sectionNum = 6  # 取6个季度
    # 新取TTM利润方法，取每支股票最后N季度数据
    incDf = readLastTTMProfits(stocks, sectionNum)
    #     print 'incDf:'
    #     print incDf
    guzhiDf = pd.merge(peDf, incDf, on='ts_code', how='left')

    # 原取TTM利润方法，按当前日期计算取前N季度数据， 但第1季度上市公司的财务报告未公布，导致缺少数据无法处理
    #     endDate = datatrans.getLastQuarter()
    #     startDate = datatrans.quarterSub(endDate, sectionNum - 1)
    #     quarter  = (int(endDate / 10) * 4 + (endDate % 10)) - sectionNum
    #     QuarterList = datatrans.QuarterList(startDate, endDate)
    #     print QuarterList

    # 过去N个季度TTM利润增长率
    #     for i in range(sectionNum):
    #         incDf = readTTMLirunForDate(QuarterList[i])
    #         incDf = incDf[['ts_code', 'incrate']]
    #         incDf.columns = ['ts_code', 'incrate%d' % i]
    #         print incDf.head()
    #         pegDf = pd.merge(pegDf, incDf, on='ts_code', how='left')
    #         pegDf = pd.merge(pegDf, incDf, on='ts_code')

    #     print pegDf.head()
    # 平均利润增长率
    endfield = 'incrate%s' % (sectionNum - 1)
    guzhiDf['avgrate'] = guzhiDf.loc[:, 'incrate0':endfield].mean(axis=1).round(
        2)
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
    nameDf = readStockList()
    guzhiDf = pd.merge(guzhiDf, nameDf, on='ts_code', how='left')
    #     print pegDf

    # 计算pe200与pe1000
    df = peHistRate(stockList, 200)
    guzhiDf = pd.merge(guzhiDf, df, on='ts_code', how='left')
    df = peHistRate(stockList, 1000)
    guzhiDf = pd.merge(guzhiDf, df, on='ts_code', how='left')
    #     print pegDf
    # 设置输出列与列顺序
    # 因无法取得数据，删除'peg', 'next1YearPE',  'next2YearPE',  'next3YearPE'
    guzhiDf = guzhiDf[['ts_code', 'name', 'pe',
                       'incrate0', 'incrate1', 'incrate2',
                       'incrate3', 'incrate4', 'incrate5',
                       'avgrate', 'madrate', 'stdrate', 'pe200', 'pe1000'
                       ]]
    #     guzhiDf = guzhiDf[['ts_code', 'name', 'pe', 'peg',
    #                        'next1YearPE',  'next2YearPE',  'next3YearPE',
    #                        'incrate0', 'incrate1', 'incrate2',
    #                        'incrate3', 'incrate4', 'incrate5',
    #                        'avgrate', 'madrate', 'stdrate', 'pe200', 'pe1000'
    #                        ]]
    return guzhiDf


def calHistoryStatus(ts_code):
    TTMLirunDf = readTTMProfitsForStock(ts_code)
    dates = TTMLirunDf['date']
    for _date in dates:
        #         print i
        result = _calHistoryStatus(TTMLirunDf, _date)
        integrity, seculargrowth, growthmadrate, averageincrement = result
        sql = ('insert ignore into guzhihistorystatus (`ts_code`, `date`, '
               '`integrity`, `seculargrowth`, `growthmadrate`, '
               '`averageincrement`) '
               'values ("%(ts_code)s", "%(_date)s", %(integrity)r, '
               '%(seculargrowth)r, "%(growthmadrate)s", '
               '"%(averageincrement)s");') % locals()
        print(sql)
        engine.execute(sql)


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


def calAllPEHistory(startDate, endDate=None):
    """
    计算全市场TTMPE
    :param startDate: str, YYYYmmdd
    :param endDate: str, YYYYmmdd
    :return:
    """
    if endDate is None:
        endDate = (dt.datetime.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
    logging.debug(f'计算全市场PE:{startDate}-{endDate}')
    dates = readCal(startDate, endDate)
    if dates:
        for tradeDate in dates:
            sql = 'call calallpe("%(tradeDate)s");' % locals()
            engine.execute(sql)
            setUpdate('index_all', tradeDate.strftime('%Y%m%d'))


def calPEHistory(ID, startDate, endDate=None):
    """
    计算某一指数的TTMPE
    :param ID: 长格式的指数代码，如: 000010.SH
    :param startDate: str, YYYYMMDD
    :param endDate: str, YYYYMMDD
    :return:
    """
    if startDate is None:
        startDate = datetime.strptime('19900101', '%Y%m%d').date()
    assert len(ID) == 9, '指数代码错误， 正确格式：000010.SH'
    ID = ID.upper()
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y%m%d')
    for tradeDate in readCal(startDate, endDate):
        sql = f'call calchengfenpe("{ID}", "{tradeDate}");'
        logging.debug(f'calIndexPE: {tradeDate}')
        engine.execute(sql)
