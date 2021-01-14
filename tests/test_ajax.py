#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2021/1/14 10:00
# software: PyCharm
import logging
import datetime as dt

from pyecharts.charts import Bar

from stockdatamanage.db.sqlrw import readProfitInc
from stockdatamanage.util.initlog import initlog


def test_stockProfitsInc():
    ts_code = '000002.SZ'
    startDate = dt.date(dt.date.today().year - 2, 1, 1).strftime('%Y%m%d')
    endDate = dt.date.today().strftime('%Y%m%d')
    df = readProfitInc(startDate=startDate, endDate=endDate, code=ts_code)
    logging.debug(f'stock profits inc for {ts_code}')
    dates = [k[3:] for k in df.keys().to_list()[1:]]
    values = df.iloc[0, :].to_list()[1:]

    c = (
        Bar()
            .add_xaxis(dates)
            .add_yaxis("商家A", [randrange(0, 100) for _ in range(6)])
            .set_global_opts(title_opts=opts.TitleOpts(title="Bar-基本示例", subtitle="我是副标题"))
    )
    return c.dump_options_with_quotes()


if __name__ == '__main__':
    pass
    initlog()

    test_stockProfitsInc()
