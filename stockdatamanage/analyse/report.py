import datetime as dt

import pandas as pd

# from .. import sqlrw
from .classifyanalyse import (
    # getHYProfitsIncRates,
    getStockForClassify, readClassifyCodeForStock,
    readClassifyName,
)
from ..db import sqlrw
from ..db.sqlconn import engine
from ..db.sqlrw import (
    readProfitInc,
    readValuation,
)


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


# def testChigu():
#     stocks = readChigu()
#     df = calGuzhi(stocks)
#     engine.execute('TRUNCATE TABLE chiguguzhi')
#     writeSQL(df, 'chiguguzhi')


# def testShaixuan():
#     stockList = readStockList().ts_code.values
#     df = calGuzhi(stockList)
#     df = df.dropna()
#     engine.execute('TRUNCATE TABLE guzhiresult')
#     writeSQL(df, 'guzhiresult')


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
    """生成评估报告
    """
    valuation = readValuation(ts_code)

    # TODO: 需补充：根据平均绝对离差计算的增长率差异水平
    # myItem.profitsIncMad = guzhiData[14]

    lv4Code = readClassifyCodeForStock(ts_code)
    if lv4Code is None:
        return valuation
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
    stocks = stocks.merge(incs, on='ts_code', how='left')
    # print(f'230 stocks:', stocks)
    stocks.rename(columns={'name': 'sname'}, inplace=True)
    valuation['classifyStocks'] = stocks
    return valuation


def reportIndex(ID):
    myItem = ReportItem(ID)
    return myItem
