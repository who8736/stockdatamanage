import datetime as dt
from datetime import datetime
import logging

import numpy as np
import pandas as pd

from .. import datatrans, sqlrw
# from .. import sqlrw
from ..classifyanalyse import (
    getHYProfitsIncRates, getStockForClassify, readClassifyForStock,
    readClassifyName,
)
from ..sqlconn import engine
from ..sqlrw import (
    readCal, loadChigu, readLastTTMPEs, readLastTTMProfits,
    readStockList, readTTMProfitsForStock, writeSQL,
    readProfitInc, readValuation,
)


# from ..valuation import ReportItem


def calGuzhi(stockList=None):
    """生成估值水平评估列表，
    # 包括以下数据： peg, 未来三个PE预测， 过去6个季度TTM利润增长率， 平均增长率， 增长率方差
    Parameters
    --------
    stockList:list 股票列表 e.g:[600519, 600999]

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

    if stockList is None:
        stockList = getLowPEStockList().ts_code.values

    #     print stockList.head()
    #     print type(stockList)
    # pe数据
    peDf = readLastTTMPEs(stockList)
    # 估值数据
    #     pegDf = readGuzhiFilesToDf(stockList)
    #     pegDf = readGuzhiSQLToDf(stockList)
    #     pegDf = pd.merge(peDf, pegDf, on='ts_code', how='left')
    #     print pegDf.head()

    sectionNum = 6  # 取6个季度
    # 新取TTM利润方法，取每支股票最后N季度数据
    incDf = readLastTTMProfits(stockList, sectionNum)
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


def peHistRate(stockList, dayCount, trade_date=None):
    """ 计算一组股票在过去指定天数内的PE水平，
        # 最低为0，最高为100
        # 历史交易天数不足时，PE水平为-1
    """
    # print(f'开始计算peHistRate: {dayCount}, stockList count:{len(stockList)}')
    perates = []
    for ts_code in stockList:
        # print(ts_code)
        sql = f'select pe_ttm from daily_basic where ts_code="{ts_code}" '
        if trade_date is not None:
            sql += f' and trade_date<="{trade_date}"'
        # sql += ' and pe_ttm is not null'
        sql += f' order by `trade_date` desc limit {dayCount};'
        result = engine.execute(sql).fetchall()
        peList = [i[0] for i in result if i[0] is not None]
        # 如果历史交易天数不足，则历史PE水平为-1
        if len(peList) != dayCount:
            perates.append(-1)
        else:
            peCur = peList[0]
            perate = sum(1 for i in peList if i < peCur) / dayCount * 100
            perates.append(perate)
    return pd.DataFrame({'ts_code': stockList, f'pe{dayCount}': perates})


def del_youzhiSelect(pegDf):
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
    #     pegDf = pegDf[['ts_code', 'pe', 'peg',
    #                    'next1YearPE',  'next2YearPE',  'next3YearPE',
    #                    'incrate0', 'incrate1', 'incrate2',
    #                    'incrate3', 'incrate4', 'incrate5',
    #                    'avgrate'
    #                    ]]
    #     print pegDf.head()
    #     print len(pegDf)
    return pegDf


def testChigu():
    #     youzhiSelect()
    #     inFilename = './data/chiguts_code.txt'
    # outFilename = './data/chiguguzhi.csv'
    #     testStockList = ['600519', '600999', '000651', '000333']
    #     testStockList = readStockListFromFile(inFilename)
    stockList = loadChigu()
    #     print testStockList
    df = calGuzhi(stockList)
    #     df = calGuzhi()
    #    dfToCsvFile(df, outFilename)
    #     df.to_csv(outFilename)
    engine.execute('TRUNCATE TABLE chiguguzhi')
    #     df.index.name = 'ts_code'
    #     clearStockList()
    #     df.set_index('ts_code', inplace=True)
    #     print df.head()
    writeSQL(df, 'chiguguzhi')


def testShaixuan():
    stockList = readStockList().ts_code.values
    df = calGuzhi(stockList)
    df = df.dropna()
    engine.execute('TRUNCATE TABLE guzhiresult')
    writeSQL(df, 'guzhiresult')
    # df = youzhiSelect(df)
    # print('youzhiSelect result:')
    # print(df.head())
    # outFilename = './data/youzhi.csv'
    #    dfToCsvFile(df, outFilename)
    # df.to_csv(outFilename)
    #     outFilename = './data/youzhiid.txt'
    #     writets_codeListToFile(df['ts_code'], outFilename)
    # engine.execute('TRUNCATE TABLE youzhiguzhi')
    # writeSQL(df, 'youzhiguzhi')


def calAllPEHistory(startDate, endDate=None):
    """
    计算全市场TTMPE
    :param startDate: str, YYYYmmdd
    :param endDate: str, YYYYmmdd
    :return:
    """
    # startDate = datetime.strptime('2010-01-01', '%Y-%m-%d').date()
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y%m%d')
    for tradeDate in readCal(startDate, endDate):
        sql = 'call calallpe("%(tradeDate)s");' % locals()
        print(sql)
        engine.execute(sql)


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
    # assert len(ID) == 6, '指数代码错误， 正确格式：000010'
    ID = ID.upper()
    if endDate is None:
        endDate = dt.datetime.today().strftime('%Y%m%d')
    # session = Session()
    for tradeDate in readCal(startDate, endDate):
        sql = f'call calchengfenpe("{ID}", "{tradeDate}");'
        logging.debug(f'calIndexPE: {tradeDate}')
        # result = engine.execute(sql)
        engine.execute(sql)
        # session.commit()
        # result = result.fetchall()
        # print(result)
        # session.execute(sql)
    # session.close()


def analysePEHist(ts_code, startDate, endDate, dayCount=200,
                  lowRate=20, highRate=80):
    """分析指定股票一段时期内PE水平，以折线图展示

    :param ts_code:
    :param startDate:
    :param endDate:
    :param dayCount:
    :param lowRate:
    :param highRate:
    :return:
    """
    sql = (f'select date, ttmpe from klinestock where ts_code={ts_code}'
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


def reportValuation(ts_code):
    """读取评估报告
    """
    valuation = readValuation(ts_code)
    # guzhi = getGuzhi(ts_code)

    # TODO: 需补充：根据平均绝对离差计算的增长率差异水平
    # myItem.profitsIncMad = guzhiData[14]

    lv4Code = readClassifyForStock(ts_code)
    lv3Code = lv4Code[:6]
    lv2Code = lv4Code[:4]
    lv1Code = lv4Code[:2]

    today = dt.datetime.today()
    startDate = f'{today.year - 3}1231'
    endDate = today.strftime('%Y%m%d')
    # 最近三年TTM利润增长率水平
    valuation['profitsInc'] = sqlrw.readProfitInc(startDate=startDate,
                                                  endDate=endDate,
                                                  code=ts_code,
                                                  reportType='year')
    # 所属1级行业
    valuation['lv1Code'] = lv1Code
    valuation['lv1Name'] = readClassifyName(lv1Code)
    valuation['lv1Inc'] = readProfitInc(startDate=startDate,
                                        endDate=endDate,
                                        code=lv1Code,
                                        ptype='classify',
                                        reportType='year')
    # 所属2级行业
    valuation['lv2Code'] = lv2Code
    valuation['lv2Name'] = readClassifyName(lv2Code)
    valuation['lv2Inc'] = readProfitInc(startDate=startDate,
                                        endDate=endDate,
                                        code=lv2Code,
                                        ptype='classify',
                                        reportType='year')
    # 所属3级行业
    valuation['lv3Code'] = lv3Code
    valuation['lv3Name'] = readClassifyName(lv3Code)
    valuation['lv3Inc'] = readProfitInc(startDate=startDate,
                                        endDate=endDate,
                                        code=lv3Code,
                                        ptype='classify',
                                        reportType='year')
    # 所属4级行业
    valuation['lv4Code'] = lv4Code
    valuation['lv4Name'] = readClassifyName(lv4Code)
    valuation['lv4Inc'] = readProfitInc(startDate=startDate,
                                        endDate=endDate,
                                        code=lv4Code,
                                        ptype='classify',
                                        reportType='year')

    # 同行业股票
    stocks = getStockForClassify(lv4Code)
    # print(f'223 line: {stockList}')

    incs = readProfitInc(startDate=startDate, endDate=endDate,
                         code=stocks.ts_code.to_list(),
                         reportType='year')
    stocks = stocks.merge(incs, on='ts_code')
    # print(f'230 stocks:', stocks)
    valuation['classifyStocks'] = stocks
    return valuation


def reportIndex(ID):
    myItem = ReportItem(ID)
    return myItem
