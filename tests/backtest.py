import datetime as dt

import backtrader as bt
import pandas as pd

# %matplotlib inline
import matplotlib.pyplot as plt

from stockdatamanage.db import engine


def read_data():
    # 沪深300指数数据
    sql = 'select trade_date, close, open from index_daily where code="399300" and trade_date>="20130101"'
    with engine.connect() as conn:
        df = pd.read_sql(text(sql), conn, parse_dates=['trade_date'])
    df['high'] = 0
    df['low'] = 0
    # df['open'] = 0
    df['volumn'] = 0
    df['openinterest'] = 0
    df = df[['trade_date', 'open', 'high', 'low',
             'close', 'volumn', 'openinterest']]

    # 沪深300滚动PE
    sql = 'select trade_date, pe from index_dailyindicator where code="399300.SZ" and trade_date>="20130101"'
    with engine.connect() as conn:
        pedf = pd.read_sql(text(sql), conn, parse_dates=['trade_date'])
    df = df.merge(pedf, how='left', on='trade_date')

    df.rename(columns={'trade_date': 'datetime'}, inplace=True)
    df.set_index('datetime', inplace=True)

    # 计算百分位
    df['quantile'] = df['pe'].rolling(window=600).apply(quantile_rate)
    df['q0'] = df.pe.rolling(600).quantile(0)
    df['q25'] = df.pe.rolling(600).quantile(.25)
    df['q75'] = df.pe.rolling(600).quantile(.75)
    df['q100'] = df.pe.rolling(600).quantile(1)
    df[~df['quantile'].isna()]

    return df


def quantile_rate(x):
    """ 计算百分位比率
    """
    return int(len(x[x <= x.iloc[-1]]) / len(x) * 100)


class TestStrategy(bt.Strategy):
    params = (
        ('pe', -1),
        # ('close', -1),
        # ('open', -1),
    )

    def log(self, txt, dt=None):
        ''' Logging function for this strategy'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # Keep a reference to the "close" line in the data[0] dataseries
        self.datape = self.data.pe
        self.dataclose = self.data.close
        self.dataopen = self.data.open
        self.dataquantile = self.data.quantile
        self.order = None

    def next(self):
        # Simply log the closing price of the series from the reference

        # self.log(f'PE, {self.datape[0]:.2f}')
        # self.log(f'Close, {self.dataclose[0]:.2f}')
        # self.log(f'Open, {self.dataopen[0]:.2f}')

        # if self.order:
        # return

        # self.log(f'position flag: {not self.position}')
        if not self.position:
            if self.dataquantile <= 10:
                self.log('BUY CREATE, %.2f' % self.dataclose[0])
                # 执行买入
                # self.order = self.buy()
                self.order = self.buy()
        else:
            if self.dataquantile >= 90:
                # 执行卖出条件判断：收盘价格跌破15日均线
                self.log('SELL CREATE, %.2f' % self.dataclose[0])
                # 执行卖出
                # self.order = self.sell()
                self.order = self.sell()
        # self.log(f'Position, {self.position}')

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Buy/Sell order submitted/accepted to/by broker - Nothing to do
            return

        # Check if an order has been completed
        # Attention: broker could reject order if not enough cash
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('BUY EXECUTED, %.2f' % order.executed.price)
            elif order.issell():
                self.log('SELL EXECUTED, %.2f' % order.executed.price)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Order Canceled/Margin/Rejected')

        # Write down: no pending order
        self.order = None


class IndexData(bt.feeds.PandasData):
    # lines = ('open', 'close', 'high', 'low', 'volumn', 'pe', 'quantile')
    lines = ('open', 'close', 'pe', 'quantile')
    params = (
        ('close', -1),
        ('open', -1),
        # ('high', -1),
        # ('low', -1),
        # ('volumn', -1),
        ('pe', -1),
        ('quantile', -1),

    )


if __name__ == '__main__':
    cerebro = bt.Cerebro()

    # 导入数据
    df = read_data()
    datafeed = IndexData(dataname=df, fromdate=dt.datetime(2013, 1, 1),
                         todate=dt.datetime(2022, 1, 1))
    # datafeed = bt.feeds.PandasData(dataname=df, fromdate=datetime.datetime(2019,1,2), todate=datetime.datetime(2021,1,28))
    cerebro.adddata(datafeed, name='hs300')  # 通过 name 实现数据集与股票的一一对应
    cerebro.broker.setcash(1000000)

    # 增加策略
    cerebro.addstrategy(TestStrategy)
    cerebro.addsizer(bt.sizers.FixedSize, stake=100)
    print('初始资金: %.2f' % cerebro.broker.getvalue())
    cerebro.run()
    print('结束资金: %.2f' % cerebro.broker.getvalue())
