# -*- coding: utf-8 -*-
# author:xx
# datetime:2020/4/7 9:07

import numpy as np
import tushare as ts
import pandas as pd
import talib
import matplotlib.pyplot as plt

from sqlconn import engine
from sqlrw import readStockList


def stockinc(ts_code):
    """一段时期内股票分段涨跌幅度
    20090804  起点，高位
    20140619  低位
    20150612  高位
    20160128  低位
    20180124  高位
    20190103  低位
    20190419  高位

    """
    dates = ['20090804',
             '20140619',
             '20150612',
             '20160128',
             '20180124',
             '20190103',
             '20190419', ]
    results = []
    for d in dates:
        sql = f"""select (a.close * b.adj_factor) s_close 
                    from daily a, adj_factor b
                    where a.ts_code="{ts_code}" and a.trade_date=
                        (select max(trade_date) from daily
                            where ts_code="{ts_code}" and trade_date<="{d}")
                        and a.ts_code=b.ts_code and a.trade_date=b.trade_date
                """
        result = engine.execute(sql).fetchone()
        if result is None or result[0] is None:
            row = dict(trade_date=d, s_close=np.nan)
        else:
            row = dict(trade_date=d, s_close=result[0])
        results.append(row)
    dfstock = pd.DataFrame(results)
    dfstock['s_inc'] = dfstock.s_close / dfstock.s_close.shift(1) * 100 - 100
    dic = {'ts_code': ts_code}
    for i in range(len(dates)):
        dic[f'close{dates[i]}'] = dfstock[dfstock.trade_date == dates[i]].s_close.values[0]
        dic[f'inc{dates[i]}'] = dfstock[dfstock.trade_date == dates[i]].s_inc.values[0]
    # print(dfstock)
    # print(dfstock.s_close)
    return dic


def indexinc(ts_code):
    """一段时期内股票分段涨跌幅度
    20090804  起点，高位
    20140619  低位
    20150612  高位
    20160128  低位
    20180124  高位
    20190103  低位
    20190419  高位

    """
    dates = ['20090804',
             '20140619',
             '20150612',
             '20160128',
             '20180124',
             '20190103',
             '20190419', ]
    results = []
    for d in dates:
        sql = f"""select close from index_daily
                    where ts_code="{ts_code}" and trade_date="{d}"
                """
        result = engine.execute(sql).fetchone()
        if result is None or result[0] is None:
            row = dict(trade_date=d, i_close=np.nan)
        else:
            row = dict(trade_date=d, i_close=result[0])
        results.append(row)
    dfstock = pd.DataFrame(results)
    dfstock['i_inc'] = dfstock.i_close / dfstock.i_close.shift(1) * 100 - 100
    dic = {'ts_code': ts_code}
    for i in range(len(dates)):
        dic[f'close{dates[i]}'] = dfstock[dfstock.trade_date == dates[i]].i_close.values[0]
        dic[f'inc{dates[i]}'] = dfstock[dfstock.trade_date == dates[i]].i_inc.values[0]
    print(dfstock)
    print(dfstock.i_close)
    return dic


def stockvsindex(ts_code, index_code, start_date=None, end_date=None):
    """比较一段时期内股票与指数的涨跌幅度
    20090804  起点，高位
    20140619  低位
    20150612  高位
    20160128  低位
    20180124  高位
    20190103  低位
    20190419  高位

    """
    pass
    dates = ['20090804',
             '20140619',
             '20150612',
             '20160128',
             '20180124',
             '20190103',
             '20190419', ]
    results = []
    for d in dates:
        sql = f"""select (a.close * b.adj_factor) s_close 
                    from daily a, adj_factor b
                    where a.ts_code="{ts_code}" and a.trade_date=
                        (select max(trade_date) from daily
                            where ts_code="{ts_code}" and trade_date<="{d}")
                        and a.ts_code=b.ts_code and a.trade_date=b.trade_date
                """
        result = engine.execute(sql).fetchone()
        if result is None or result[0] is None:
            continue
        row = dict(trade_date=d, s_close=result[0])
        results.append(row)
    dfstock = pd.DataFrame(results)
    dfstock['s_inc'] = dfstock.s_close / dfstock.s_close.shift(1) * 100 - 100

    results = []
    for d in dates:
        sql = f"""select close i_close from index_daily
                    where ts_code="{index_code}" and trade_date={d}
                """
        result = engine.execute(sql).fetchone()
        if result is None or result[0] is None:
            continue
        row = dict(trade_date=d, i_close=result[0])
        results.append(row)
    dfindex = pd.DataFrame(results)
    dfindex['i_inc'] = dfindex.i_close / dfindex.i_close.shift(1) * 100 - 100

    df = pd.merge(dfstock, dfindex, how='outer',
                  left_on='trade_date', right_on='trade_date')
    df['loss'] = df.s_inc < df.i_inc
    print(df)
    print('跑输大盘', df['loss'].values.sum(), '次')


def divRangeStock(ts_code='000002.SZ', startDate='20090101',
                  endDate='20131231'):
    sql = (f'select trade_date, close cl from daily'
           f' where ts_code="{ts_code}" and trade_date>="{startDate}"'
           f' and trade_date<="{endDate}"')
    df = pd.read_sql(sql, engine)

    sql = (f'select trade_date, adj_factor from adj_factor'
           f' where ts_code="{ts_code}" and trade_date>="{startDate}"'
           f' and trade_date<="{endDate}"')
    factor = pd.read_sql(sql, engine)
    df = pd.merge(df, factor, how='left', left_on='trade_date',
                  right_on='trade_date')
    df['close'] = df.cl * df.adj_factor
    df = df[['trade_date', 'close']]
    _divRange(df)


def divRangeIndex(ts_code='399300.SZ', startDate='20090101',
                  endDate='20131231'):
    sql = (f'select trade_date, close from index_daily'
           f' where ts_code="{ts_code}" and trade_date>="{startDate}"'
           f' and trade_date<="{endDate}"')
    df = pd.read_sql(sql, engine)

    _divRange(df)


def _divRange(df):
    df['ma60'] = talib.SMA(df.close, timeperiod=60)
    df['ma120'] = talib.SMA(df.close, timeperiod=120)

    df['flag'] = 0
    df.loc[(df.ma60.shift(1) < df.ma120.shift(1)) & (
            df.ma60 > df.ma120), 'flag'] = 1
    df.loc[(df.ma60.shift(1) > df.ma120.shift(1)) & (
            df.ma60 < df.ma120), 'flag'] = 2

    pos = 0
    while True:
        next = df[(df.index > pos) & (df.flag != 0)].index.min()
        if next is np.nan:
            break
        if next - pos <= 60:
            df.loc[pos, 'flag'] = 0
            df.loc[next, 'flag'] = 0
        pos = next
    ax = plt.subplot()
    ax.plot(df.close)
    ax.plot(df.ma60)
    ax.plot(df.ma120)
    ymin, ymax = plt.ylim()
    offset = (ymax - ymin) / 40
    ax.plot(df[df.flag == 1].index, df[df.flag == 1].ma120 - offset, 'r^')
    ax.plot(df[df.flag == 2].index, df[df.flag == 2].ma120 + offset, 'gv')

    plt.show()

    # ax = plt.subplot()
    # ax.plot(df.close)
    # ax.plot(df.ma60)
    # ax.plot(df.ma120)
    # plt.show()


if __name__ == '__main__':
    pass
    ts_code = '000002.SZ'
    index_code = '399300.SZ'
    startDate = '20090101'
    endDate = '20191231'
    # stockvsindex(ts_code=ts_code, index_code=index_code)
    dic = indexinc(ts_code=index_code)
    dic['name'] = '沪深300'
    print(dic)
    results = [dic]
    stocks = readStockList()
    # stocks = stocks[:6]
    for ts_code in stocks.ts_code:
        dic = stockinc(ts_code=ts_code)
        dic['name'] = stocks[stocks.ts_code==ts_code].name.values[0]
        print(dic)
        results.append(dic)

    df = pd.DataFrame(results)
    df_name = df.name
    df.drop('name', axis=1, inplace=True)
    df.insert(1, 'name', df_name)
    df = df.round(2)
    df.to_excel('../data/stockvsindex.xlsx')

    # divRangeStock()
    # divRangeIndex()

    # ts_code = '000651.SZ'
    # df = ts.pro_bar(ts_code=ts_code, adj='hfq')
    # filename = f'data/{ts_code[:6]}hfq.xlsx'
    # df.to_excel(filename)
