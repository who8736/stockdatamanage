#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2021/1/12 9:25
# software: PyCharm

import logging

from stockdatamanage.db.sqlrw import readStockKline
from stockdatamanage.views.ajax_img import _klineimg


def test_stockklineimgnew():
    # ts_code = request.args.get('ts_code')
    ts_code = '000002.SZ'
    df = readStockKline(ts_code, days=1000)
    logging.info(f'stockklineimgnew: {ts_code}')
    return _klineimg(df)

if __name__ == '__main__':
    pass

    test_stockklineimgnew()
