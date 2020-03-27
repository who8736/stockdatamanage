# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 10:22:14 2019

@author: ff
"""

from os import path
import datetime as dt
import sys

import backtrader as bt
import pandas as pd

from sqlconn import engine
from sqlrw import readStockKline


class TestStrategy(bt.Strategy):
    """交易策略"""
    # 设置类参数， exitbars最大持有期
    params = (('maperiod', 15), ('printlog', False))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.datavolume = self.datas[0].volume
        # self.datape200 = self.datas[0].pe200
        # self.datape1000 = self.datas[0].pe1000

        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 添加移动平均指标
        self.sma = bt.indicators.SimpleMovingAverage(self.datas[0],
                                                     period=self.params.maperiod)

        # bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        # bt.indicators.WeightedMovingAverage(self.datas[0], period=25)
        # bt.indicators.StochasticSlow(self.datas[0])
        # bt.indicators.MACDHisto(self.datas[0])
        # rsi = bt.indicators.RSI(self.datas[0])
        # bt.indicators.SmoothedMovingAverage(self.datas[0])
        # bt.indicators.ATR(self.datas[0], plot=False)

    def notify_order(self, order):
        # 交易指令状态为submitted，accepted时不做任何操作
        if order.status in [order.Submitted, order.Accepted]:
            return

        # 交易指令状态为completed时执行交易
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log(f'BUY EXECUTED, price: {order.executed.price:.2f}'
                         f' cost: {order.executed.value:.2f}'
                         f' comm: {order.executed.comm:.2f}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            elif order.issell():
                self.log(f'SELL EXECUTED, price: {order.executed.price:.2f}'
                         f' cost: {order.executed.value:.2f}'
                         f' comm: {order.executed.comm:.2f}')

            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'Order Canceled/Margin/Rejected')

        self.order = None

    def log(self, txt, dt=None, doprint=False):
        """记录交易过程"""
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt} {txt}')

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f},'
                 f' NET {trade.pnlcomm:.2f}')

    def next(self):
        self.log(f'close: {self.dataclose[0]}')
        # self.log(f'close: {self.dataclose[0]}, pe200: {self.datape200[0]}'
        #          f' pe1000: {self.datape1000[0]}')

        if self.order:
            return

        # 检查是否开市
        if not self.position:
            # 开市前执行买入
            # if (self.dataclose[0] < self.dataclose[-1]
            #     and self.dataclose[-1] < self.dataclose[-2]):
            if self.dataclose[0] < self.sma[0]:
                # if self.datavolume[0] < 5:
                self.log(f'buy stock close {self.dataclose[0]}')
                # 追踪交易指令，避免发送重复指令
                self.order = self.buy()

        else:
            # 开市后执行卖出
            # if len(self) >= self.bar_executed + self.params.exitbars:
            #     self.log(f'SELL STOCK, {self.dataclose[0]}')
            if self.dataclose[0] > self.sma[0]:
                # if self.datavolume[0] > 95:
                # 追踪交易指令，避免发送重复指令
                self.order = self.sell()

    def stop(self):
        # print('stop')
        txt = (f'(MA period {self.params.maperiod})'
               f' Ending Value {self.broker.getvalue()}')
        self.log(txt, doprint = True)


def getData(ts_code, startDate):
    df = readStockKline(ts_code, startDate=startDate)
    df.set_index(keys='date', inplace=True)
    sql = (f'select date, pe200, pe1000 from valuation'
           f' where ts_code="{ts_code}" and date>="{startDate}"')
    dfpe = pd.read_sql(sql, engine)
    dfpe.date = pd.to_datetime(dfpe.date)
    dfpe = dfpe.set_index('date')
    dfpe.rename(columns={'pe200': 'volume'}, inplace=True)
    df = pd.merge(df, dfpe, how='left', left_index=True, right_index=True)
    return df

if __name__ == '__main__':
    ts_code = '000651.SZ'
    startDate = '20180101'
    cerebro = bt.Cerebro()

    modpath = path.dirname(path.abspath(sys.argv[0]))
    datapath = path.join(modpath, '../data/test.csv')

    df = getData(ts_code, startDate)
    # print(df.head())
    data = bt.feeds.PandasData(dataname=df)
    # 添加数据
    cerebro.adddata(data)
    # 添加交易策略
    # cerebro.addstrategy(TestStrategy)
    strats = cerebro.optstrategy(TestStrategy, maperiod=range(10, 31))
    # cerebro.addstrategy(strats)
    # 设置初始资金
    cerebro.broker.set_cash(100000)
    # 设置固定交易股数
    cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    # 设置交易费率，0.001=0.1%
    cerebro.broker.setcommission(commission=0.001)

    print(f'开始回测:资金{cerebro.broker.get_value():.2f}')
    cerebro.run()
    print(f'回测结束:资金{cerebro.broker.get_value():.2f}')
    # cerebro.plot(valume=False, valumes=False, volabel=False)
