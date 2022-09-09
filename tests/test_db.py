#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2021/1/13 9:41
# software: PyCharm

from stockdatamanage.util.initlog import initlog
import datetime as dt

from stockdatamanage.db.sqlrw import readProfitInc, readStockUpdate


def test_readProfitInc():
    startDate = dt.date(dt.date.today().year - 2, 1, 1).strftime('%Y%m%d')
    endDate = dt.date.today().strftime('%Y%m%d')
    code = '000002.SZ'
    df = readProfitInc(startDate=startDate, endDate=endDate, code=code)
    print(df)


def test_readStockUpdate():
    readStockUpdate()


if __name__ == '__main__':
    pass
    initlog()

    # test_readProfitInc()
    test_readStockUpdate()
