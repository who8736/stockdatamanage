# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/17 8:35

from stockdatamanage.analyse.classifyanalyse import (
    calClassifyStaticTTMProfitLow, calClassifyProfitsIncCompare,
    calClassifyStaticTTMProfit,
    readClassifyPE,
    # readClassifyProfitInc,
)
from stockdatamanage.db.sqlrw import readProfitInc
from stockdatamanage.util.datatrans import calDate


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


def test_readProfitInc():
    df = readProfitInc('20150331', '20200930', ptype='classify',
                       reportType='year')
    print(df)


def test_calClassifyProfitsIncCompare():
    calClassifyProfitsIncCompare()


if __name__ == '__main__':
    # test_calDate()
    # test_calClassifyStaticTTMProfitLow()

    # test_calClassifyStaticTTMProfit()

    # test_readClassifyPE()

    # test_readProfitInc()

    test_calClassifyProfitsIncCompare()