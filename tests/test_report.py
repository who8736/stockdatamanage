#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2021/1/13 10:44
# software: PyCharm

from stockdatamanage.util.initlog import initlog
import datetime as dt

from stockdatamanage.analyse.report import reportValuation


def test_reportValuation():
    code = '000002.SZ'
    df = reportValuation(ts_code=code)
    print(df)


if __name__ == '__main__':
    pass
    initlog()

    test_reportValuation()
