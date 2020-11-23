# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/17 8:35

from stockdatamanage.datatrans import calDate
from stockdatamanage.classifyanalyse import (
    calClassifyStaticTTMProfitLow,
    calClassifyStaticTTMProfit,
    readClassifyPE,
    readStockProfitInc,
    # readClassifyProfitInc,
)


def test_calDate():
    pass
    _dates = [
        '20200331',
        '20200630',
        '20200930',
        '20201231',
    ]
    for _date in _dates:
        year, lastyear = calDate(_date)
        print(
            f'report date: {_date}; cal year: {year}; cal lastyear: {lastyear}')


def test_calClassifyStaticTTMProfitLow():
    end_date = '20200930'
    calClassifyStaticTTMProfitLow(end_date)


def test_calClassifyStaticTTMProfit():
    end_date = '20200930'
    calClassifyStaticTTMProfit(end_date)


def test_readClassifyPE():
    df = readClassifyPE('20200930')
    print(df)


def test_readStockProfitInc():
    df = readStockProfitInc('20150331', '20200930', reportType='year')
    print(df)


def test_readClassifyProfitInc():
    df = readClassifyProfitInc('01010101', period=3)
    print(df)


if __name__ == '__main__':
    # test_calDate()
    # test_calClassifyStaticTTMProfitLow()

    # test_calClassifyStaticTTMProfit()

    # test_readClassifyPE()

    test_readStockProfitInc()

    # test_readClassifyProfitInc()
