# -*- coding: utf-8 -*-
"""独立成单独文件， 便于在指定时间单独下载股票不定期信息
    # 更新非季报表格
    # 财务披露表（另外单独更新）
    # 质押表（另外单独更新）
    # 业绩预告（另外单独更新）
    # 业绩快报（另外单独更新）
    # 分红送股（另外单独更新）

"""
from multiprocessing.dummy import Pool as ThreadPool

import pandas as pd
import tushare as ts

from sqlrw import readStockList, writeSQL
from sqlconn import engine
from download import DownloaderMisc
from datamanage import logfun
from initlog import initlog

tables = [
    ['pledge_stat', 60, 50],
    ['forecast', 60, 50],
    ['express', 60, 50],
    ['disclosure_date', 60, 50],
]
# def pledge_stat():
#     """下载股权质押信息
#
#     :return:
#     """
#     stocks = readStockList()
#     total = len(stocks)
#     cnt = 1
#     downloader = DownloaderMisc(60, 50)
#     pro = ts.pro_api()
#     # fun = pro.pledge_stat
#     for ts_code in stocks.ts_code:
#         print(f'下载股权质押{cnt}/{total}: {ts_code}')
#         df = downloader.run(pro.pledge_stat, ts_code=ts_code)
#         writeSQL(df, 'pledge_stat')
#         cnt += 1


# def _download(table, pertimes, limit):
@logfun
def _download(args):
    """下载非定期更新数据"""
    table, pertimes, limit = args
    stocks = readStockList()
    total = len(stocks)
    cnt = 1
    downloader = DownloaderMisc(pertimes, limit)
    for ts_code in stocks.ts_code:
        print(f'下载{table} {cnt}/{total}: {ts_code}')
        df = downloader.run(table, ts_code=ts_code)
        writeSQL(df, table)
        cnt += 1


# def __download(table):
#     # s = 'forecast'
#     # s = 'express'
#     table = 'disclosure_date'
#
#     stocks = readStockList()
#     total = len(stocks)
#     cnt = 1
#     pro = ts.pro_api()
#     fun = getattr(pro, table)
#     for ts_code in stocks.ts_code:
#         print(f'下载{table} {cnt}/{total}: {ts_code}')
#         df = fun(ts_code=ts_code)
#         writeSQL(df, table)
#         cnt += 1

@logfun
def runner():
    pool = ThreadPool(processes=4)
    pool.map(_download, tables)
    pool.close()
    pool.join()

if __name__ == '__main__':
    pass
    initlog()
    runner()

