# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 10:22:14 2019

@author: ff
"""

from os import path
import datetime as dt
import sys

import backtrader as bt
import backtrader.indicators as btind
import numpy as np
import pandas as pd
import tushare as ts

from sqlconn import engine
from sqlrw import readStockKline


class PandasData(bt.feeds.PandasData):
    lines = ('pe', 'pe200', 'pe1000')
    params = (('pe', 7), ('pe200', 8), ('pe1000', 9))


class MySizer(bt.Sizer):
    """
    ratio: float, 首次仓位比率，1：首次全仓买入， 0.5：首次半仓买入
    """
    params = (('ratio', 1),)

    def __init__(self):
        pass

    def _getsizing(self, comminfo, cash, data, isbuy):
        # print('-' * 80)
        # print(f'_getsizing')
        position = self.strategy.getposition(data)
        # print(f'size: {position.size} price: {position.price}')
        # print(f'cash: {self.broker.getcash()}')
        # print(f'value: {self.broker.getvalue()}')
        # print('-' * 80)
        if isbuy:
            if position.size == 0:
                size = cash * self.params.ratio // data[0] // 100 * 100
            else:
                size = cash // data[0] // 100 * 100
            return size
        elif position.size > 0:
            return position.size
        else:
            return 0


class TestStrategy(bt.Strategy):
    """交易策略"""
    # 设置类参数， exitbars最大持有期
    params = (('maperiod', 15),
              ('printlog', True),
              ('ssa_window', 15),
              ('ts_code', ''),
              ('startDate', None),
              ('endDate', None),
              ('pe200buy', 5),
              ('pe200sell', 95),
              ('pe1000buy', 5),
              ('pe1000sell', 95),
              )

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.datavolume = self.datas[0].volume
        # self.datape200 = self.datas[0].pe200
        # self.datape1000 = self.datas[0].pe1000

        self.ts_code = self.params.ts_code
        self.startDate = self.params.startDate
        self.endDate = self.params.endDate
        self.dividentData = None

        # 待处理的股利股息
        self.stk_div = None
        # 待处理的现金股息
        self.cash_div = None
        self.order = None
        self.buyprice = None
        self.buycomm = None

        # 读取除权除息数据
        self.getDivident()

        # 添加移动平均指标
        self.sma = bt.indicators.SimpleMovingAverage(self.datas[0],
                                                     period=self.params.maperiod)
        # 添加自定义指标
        # self.ssa = Ssa(ssa_window=self.params.ssa_window, subplot=False)
        # self.pehist = Pehist(subplot=False)

        btind.SMA(self.data.pe, period=1, subplot=True, plotname='pe')
        btind.SMA(self.data.pe200, period=1, subplot=True, plotname='pe200')
        btind.SMA(self.data.pe1000, period=1, subplot=True, plotname='pe1000')

        # bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        # bt.indicators.WeightedMovingAverage(self.datas[0], period=25)
        # bt.indicators.StochasticSlow(self.datas[0])
        # bt.indicators.MACDHisto(self.datas[0])
        # rsi = bt.indicators.RSI(self.datas[0])
        # bt.indicators.SmoothedMovingAverage(self.datas[0])
        # bt.indicators.ATR(self.datas[0], plot=False)

    def getDivident(self):
        sql = (f'select ann_date, stk_div, cash_div,'
               f' record_date, ex_date, pay_date, div_listdate'
               f' from dividend where ts_code="{self.ts_code}"')
        if self.startDate is not None:
            sql += f' and record_date>="{self.startDate}"'
        if self.endDate is not None:
            sql += (f' and (pay_date<="{self.endDate}"'
                    f' or div_listdate<="{self.endDate}")')
        self.dividentData = pd.read_sql(sql, engine)
        # self.log('除权除息数据：')
        # print('-' * 80)
        # print(self.dividentData)
        # print('-' * 80)

    def notify_order(self, order):
        # 交易指令状态为submitted，accepted时不做任何操作
        if order.status in [order.Submitted, order.Accepted]:
            return

        # 交易指令状态为completed时执行交易
        if order.status in [order.Completed]:
            if order.isbuy():
                # self.log(f'BUY EXECUTED, price: {order.executed.price:.2f}'
                #          f' cost: {order.executed.value:.2f}'
                #          f' comm: {order.executed.comm:.2f}')
                self.buyprice = order.executed.price
                self.buycomm = order.executed.comm
            elif order.issell():
                pass
                # self.log(f'SELL EXECUTED, price: {order.executed.price:.2f}'
                #          f' cost: {order.executed.value:.2f}'
                #          f' comm: {order.executed.comm:.2f}')

            self.bar_executed = len(self)
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log(f'Order Canceled/Margin/Rejected')

        self.order = None

    def log(self, txt, tradedate=None, doprint=False):
        """记录交易过程"""
        if self.params.printlog or doprint:
            tradedate = tradedate or self.datas[0].datetime.date(0)
            print(f'{tradedate} {txt}')

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        # self.log(f'OPERATION PROFIT, GROSS {trade.pnl:.2f},'
        #          f' NET {trade.pnlcomm:.2f}')

    def prenext(self):
        self.log('prenext')

    def prenext_open(self):
        self.log('prenext_open')

    def next_open(self):
        self.log('next_open')

    def isExDividentDate(self):
        if self.dividentData.empty:
            return False
        traderdate = self.datas[0].datetime.date(0)
        df = self.dividentData[self.dividentData.ex_date==traderdate]
        return not df.empty

    def exDivident(self):
        """除权除息"""
        tradedate = self.datas[0].datetime.date(0)
        size = self.position.size
        price = self.position.price
        df = self.dividentData[self.dividentData.ex_date == tradedate]
        adjsize = df.stk_div.values[0]
        adjprice = df.cash_div.values[0]
        exsize = size * adjsize
        newprice = round((price - adjprice) / (1 + adjsize), 2)
        excash = round(size * adjprice, 2)
        self.position.fix(size, newprice)
        # print(f'adjprice: {adjprice} newprice: {newprice}')
        # self.broker.add_cash(excash)
        if excash != 0:
            tradedate = df.pay_date.values[0]
            self.cash_div = dict(tradedate=tradedate, cash=excash)
        if exsize != 0:
            tradedate = df.div_listdate.values[0]
            self.stk_div = dict(tradedate=tradedate, right=exsize,
                                price=newprice)
            # self.stk_div = dict(tradedate=tradedate, right=exsize,
            #                     price=newprice)
        # self.log(f'除权除息: 现金股息{excash}元, 股利股息{exsize}股')

    def next(self):
        # self.log(f'close: {self.dataclose[0]}')
        # self.log(f'close: {self.dataclose[0]}, pe200: {self.datape200[0]}'
        #          f' pe1000: {self.datape1000[0]}')

        # print(self.order)
        if self.order:
            return

        traderdate = self.datas[0].datetime.date(0)
        # 在除权除息日除权除息
        if self.position and self.isExDividentDate():
            # self.log('-' * 20 + '开始除权除息' + '-' * 20)
            self.exDivident()
            # self.log('-' * 20 + '除权除息结束' + '-' * 20)

        # 派发股利股息
        if ((self.stk_div is not None)
                and traderdate == self.stk_div['tradedate']):
            # print(f'发放股利{self.stk_div["right"]}股')
            if self.position and self.position.size > 0:
                size = self.position.size + self.stk_div['right']
                price = self.position.price
                self.position.fix(size, price)
                self.position.upopened = size
            else:
                size = self.stk_div['right']
                price = self.stk_div['price']
                self.position.fix(size, price)
                self.position.upopened = size
                self.sell(size=size)
            self.stk_div = None

        # 派发现金股息
        if ((self.cash_div is not None)
                and traderdate == self.cash_div['tradedate']):
            # print(f'发放现金{self.cash_div["cash"]}元')
            self.broker.add_cash(self.cash_div['cash'])
            self.cash_div = None

        # 已持有仓位时不再买入
        if not self.position:
            # if (self.dataclose[0] < self.dataclose[-1]
            #     and self.dataclose[-1] < self.dataclose[-2]):
            # if self.dataclose[0] < self.sma[0]:
            # self.log(f'pe200:{self.datas[0].pe200[0]}')
            # if self.datas[0].pe1000 < 10 or self.datas[0].pe200 < 30:
            if self.datas[0].pe1000 <= self.params.pe1000buy:
                pass
                # self.log(f'buy stock close {self.dataclose[0]}')
                # 追踪交易指令，避免发送重复指令
                self.order = self.buy()

            # 测试用代码
            # buydate = dt.date(2010, 1, 4)
            # if self.datas[0].datetime.date(0) == buydate:
            #     self.log(f'buy stock close {self.dataclose[0]}')
                # 追踪交易指令，避免发送重复指令
                # self.order = self.buy()

        else:
            # if len(self) >= self.bar_executed + self.params.exitbars:
            #     self.log(f'SELL STOCK, {self.dataclose[0]}')
            # if self.dataclose[0] > self.sma[0]:
            # self.log(f'volume: {self.datavolume[0]}')
            # self.log(f'position: {self.position.__str__}', doprint=True)
            # self.position.size += 1

            # txt = (f'持仓: {self.position.size} 持仓价: {self.position.price}'
            #        f' upopened: {self.position.upopened}'
            #        f' cash: {self.broker.cash:.2f}')
            # self.log(txt, doprint=True)

            # if self.datas[0].pe1000 > 90 or self.datas[0].pe200 > 60:

            # 补仓
            if (self.datas[0].pe1000 <= self.params.pe1000buy
                    and self.datas[0].close <= self.position.price * 0.8):
                pass
                # self.log(f'buy stock close {self.dataclose[0]}')
                # 追踪交易指令，避免发送重复指令
                self.order = self.buy()

            # 卖出
            if self.datas[0].pe1000 >= self.params.pe1000sell:
                # self.log(f'sell stock close {self.dataclose[0]}')
                # 追踪交易指令，避免发送重复指令
                # self.order = self.sell(size=self.position.size)
                self.order = self.sell()
                pass

            # 测试用代码
            # selldate = dt.date(2019, 6, 27)
            # if self.datas[0].datetime.date(0) == selldate:
            #     self.log(f'sell stock close {self.dataclose[0]}')
            #     # 追踪交易指令，避免发送重复指令
            #     self.order = self.sell(size=self.position.size)
            #     self.order = self.sell()

    def stop(self):
        # print('stop')
        txt = (f'(pe1000buy {self.params.pe1000buy})'
               f'(pe1000sell {self.params.pe1000sell})'
               f' Ending Value {self.broker.getvalue():.2f}')
        self.log(txt, doprint=True)


class Pehist(bt.Indicator):
    lines = ('pehist',)

    def __init__(self):
        pass

    def next(self):
        data_serial = self.data.get(1)
        self.lines.pehist[0] = data_serial[-1]


class Ssa(bt.Indicator):
    lines = ('ssa',)

    def __init__(self, ssa_window):
        self.params.ssa_window = ssa_window
        # 这个很有用，会有 not maturity生成
        self.addminperiod(self.params.ssa_window * 2)

    def get_window_matrix(self, input_array, t, m):
        # 将时间序列变成矩阵
        temp = []
        n = t - m + 1
        for i in range(n):
            temp.append(input_array[i:i + m])
        window_matrix = np.array(temp)
        return window_matrix

    def svd_reduce(self, window_matrix):
        # svd分解
        u, s, v = np.linalg.svd(window_matrix)
        m1, n1 = u.shape
        m2, n2 = v.shape
        index = s.argmax()  # get the biggest index
        u1 = u[:, index]
        v1 = v[index]
        u1 = u1.reshape((m1, 1))
        v1 = v1.reshape((1, n2))
        value = s.max()
        new_matrix = value * (np.dot(u1, v1))
        return new_matrix

    def recreate_array(self, new_matrix, t, m):
        # 时间序列重构
        ret = []
        n = t - m + 1
        for p in range(1, t + 1):
            if p < m:
                alpha = p
            elif p > t - m + 1:
                alpha = t - p + 1
            else:
                alpha = m
            sigma = 0
            for j in range(1, m + 1):
                i = p - j + 1
                if i > 0 and i < n + 1:
                    sigma += new_matrix[i - 1][j - 1]
            ret.append(sigma / alpha)
        return ret

    def SSA(self, input_array, t, m):
        window_matrix = self.get_window_matrix(input_array, t, m)
        new_matrix = self.svd_reduce(window_matrix)
        new_array = self.recreate_array(new_matrix, t, m)
        return new_array

    def next(self):
        data_serial = self.data.get(size=self.params.ssa_window * 2)
        # self.lines.ssa[0] = \
        # self.SSA(data_serial, len(data_serial), int(len(data_serial) / 2))[-1]
        self.lines.ssa[0] = data_serial[-1] + 1


def getdf_sql(ts_code, startDate, endDate):
    df = readStockKline(ts_code, startDate=startDate, endDate=endDate)
    df.set_index(keys='date', inplace=True)
    return df


def getdf_tushare(ts_code, startDate):
    df = ts.pro_bar(ts_code='000651.SZ', start_date='20090101', adj='hfq',
                    adjfactor=True)
    df.rename(columns={'trade_date': 'date', 'vol': 'nouse'}, inplace=True)
    # df.date = pd.to_datetime(df.date)
    df.sort_values('date', inplace=True)
    # df.set_index(keys='date', inplace=True)
    # df.reset_index(inplace=True)
    df.set_index(keys='date', inplace=True)
    return df


def getData(ts_code, startDate=None, endDate=None):
    df = getdf_sql(ts_code, startDate, endDate)
    # df = getdf_tushare(ts_code, startDate)

    sql = (f'select date, pe200, pe1000 from valuation'
           f' where ts_code="{ts_code}" ')
    if startDate is not None:
        sql += f' and date>="{startDate}"'
    if startDate is not None:
        sql += f' and date<="{endDate}"'

    dfpe = pd.read_sql(sql, engine)
    dfpe.date = pd.to_datetime(dfpe.date)
    dfpe = dfpe.set_index('date')
    # dfpe.rename(columns={'pe200': 'volume'}, inplace=True)
    # dfpe.rename(columns={'pe1000': 'volume'}, inplace=True)
    df = pd.merge(df, dfpe, how='left', left_index=True, right_index=True)
    return df


def runstrat():
    # ts_code = '000651.SZ'
    # ts_code = '000002.SZ'
    ts_code = '600036.SH'
    startDate = '20100101'
    endDate = '20191231'
    cerebro = bt.Cerebro()

    # modpath = path.dirname(path.abspath(sys.argv[0]))
    # datapath = path.join(modpath, '../data/test.csv')
    df = getData(ts_code, startDate, endDate)
    # df['volume'] = 1
    # print(df.head())
    # print(df.dtypes)
    kwargs = dict(pe=4, pe200=5, pe1000=6, volume=None)
    data = PandasData(dataname=df, **kwargs)
    # 添加数据
    cerebro.adddata(data)
    # 添加交易策略
    # 单进程
    kwargs = dict(maperiod=1,
                  ssa_window=1,
                  ts_code='000651.SZ',
                  startDate=startDate,
                  endDate=endDate,
                  pe200buy=5,
                  pe200sell=95,
                  pe1000buy=10,
                  pe1000sell=90)
    cerebro.addstrategy(TestStrategy, **kwargs)

    # 多进程，用于参数优化
    # kwargs = dict(maperiod=1,
    #               ssa_window=1,
    #               ts_code='000651.SZ',
    #               startDate=startDate,
    #               endDate=endDate, )
    # strats = cerebro.optstrategy(TestStrategy, pe1000buy=range(0, 20),
    #                              pe1000sell=range(80, 100),
    #                              **kwargs)
    # cerebro.addstrategy(strats)
    # 设置初始资金
    cerebro.broker.set_cash(100000)
    # 设置固定交易股数
    cerebro.addsizer(MySizer)
    # cerebro.addsizer(bt.sizers.FixedSize, stake=1000)
    # 设置交易费率，0.001=0.1%
    cerebro.broker.setcommission(commission=0.001)

    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name='SharpeRatio')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')

    print(f'开始回测:资金{cerebro.broker.get_value():.2f}')
    result = cerebro.run()
    print(f'回测结束:资金{cerebro.broker.get_value():.2f}')

    # 回测结果评价
    strat = result[0]
    sharpe = strat.analyzers.SharpeRatio.get_analysis()["sharperatio"]
    if sharpe is not None:
        print(f'夏普比率: {sharpe:.2f}')
    else:
        print(f'夏普比率: {sharpe}')
    dw = strat.analyzers.DW.get_analysis()['max']['drawdown']
    print(f'DW: {dw:.2f}')
    cerebro.plot(volume=False, volabel=False)


if __name__ == '__main__':
    pass
    runstrat()
