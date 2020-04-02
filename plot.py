# -*- coding: utf-8 -*-
"""
Created on 2017年2月10日

@author: who8736
"""

# import datetime
import time
from io import BytesIO

import matplotlib
import pandas as pd
import matplotlib.pyplot as plt  # @IgnorePep8
# from matplotlib.finance import candlestick_ohlc  # @IgnorePep8
# from mplfinance import candlestick_ohlc  # @IgnorePep8
import mplfinance as mpf
import matplotlib.gridspec as gs  # @IgnorePep8
from matplotlib.dates import DateFormatter, MonthLocator  # @IgnorePep8
from matplotlib.ticker import FixedLocator  # @IgnorePep8
# 三维图需引用下面这句
from mpl_toolkits.mplot3d import Axes3D
import tushare  # @IgnorePep8
from bokeh.plotting import figure, show, output_file
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool
from bokeh.models import Slider, CustomJS

from sqlrw import engine, readStockKline  # @IgnorePep8
from datatrans import dateStrList  # @IgnorePep8

matplotlib.use('Agg')  # @UndefinedVariable


# matplotlib.use('Qt5Agg')  # @UndefinedVariable


def scatter(startDate, endDate):
    dateList = dateStrList(startDate, endDate)
    for date in dateList:
        print(date)
        sql = ('select pe, lirunincrease from pelirunincrease '
               f'where date="{date}";')
        result = engine.execute(sql)
        peList = []
        incrateList = []
        for pe, lirunincrease in result.fetchall():
            if pe is not None and lirunincrease is not None:
                peList.append(pe)
                incrateList.append(lirunincrease)
        if not peList:
            continue
        plt.scatter(incrateList, peList)
        plt.axes().set_xlim((-200, 200))
        plt.axes().set_ylim((-200, 200))
        filename = './data/plot/%(date)s.png' % locals()
        plt.savefig(filename)
        plt.clf()


#         plt.show()


# def plotKlineOld(ts_code):
#     #     return plotKline(ts_code)
#     #     ax2 = fig.add_subplot(2, 1, 2)
#     sql = ('select date, open, high, low, close, ttmpe '
#            'from klinestock where ts_code="%(ts_code)s" '
#            'order by date desc limit 1000;' % locals())
#     result = engine.execute(sql)
#     stockDatas = result.fetchall()
#     klineDatas = []
#     dates = []
#     peDatas = []
#     #     klineDatas = result.fetchall()
#     for date, _open, high, low, close, ttmpe in stockDatas:
#         klineDatas.append([time.mktime(date.timetuple()),
#                            _open, high, low, close])
#         dates.append(time.mktime(date.timetuple()))
#         peDatas.append(ttmpe)
#
#     print(dates)
#     print(peDatas)
#     gs1 = gs.GridSpec(3, 1)
#     gs1.update(hspace=0)
#     fig = plt.figure()
#     ax1 = fig.add_subplot(gs1[0:2, :])
#     candlestick_ohlc(ax1, klineDatas)
#     ax1.set_title(ts_code)
#     ax2 = fig.add_subplot(gs1[2:3, :])
#     ax2.plot(dates, peDatas)
#     ax2.xaxis.set_major_locator(MonthLocator())
#     ax2.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
#     fig.autofmt_xdate()
#
#     #     ax1.subplots_adjust(hspace=None)
#     #     fig.subplots_adjust(hspace=0)
#     #     plt.show()
#     imgData = BytesIO()
#     fig.savefig(imgData, format='png')
#     #     imgData.seek(0)
#
#     return imgData


#     datetime.date
#     datetime.timestamp()
#     datetime.


def plotKline(ID, type='stock', days=1000):
    """ 绘制K线与TTMPE图
    """
    if type == 'stock':
        return plotKlineStock(ID, days)
    elif type == 'index':
        return plotKlineIndex(ID, days)
    else:
        return None


def plotKlineIndex(ID, days):
    """

    :param ID:
    :param days:
    :return:
    """
    sql = (f'select a.trade_date, a.open, a.high, a.low, a.close, b.ttmpe '
           f'from daily a, daily_basic b '
           f'where a.ts_code="{ID}" and a.ts_code=b.ts_code and a.trade=b.trade'
           f'order by a.trade_date desc limit {days};')
    df = pd.read_sql(sql, engine)
    print(df.head())
    bokehplot = BokehPlot(ID, df)
    return bokehplot.plot()


def plotKlineStock(ID, days):
    """

    :param ID:
    :param days:
    :return:
    """
    # sql = (f'select a.trade_date, a.open, a.high, a.low, a.close, b.pe_ttm '
    #        f' from daily a, daily_basic b '
    #        f' where a.ts_code="{ID}" and a.ts_code=b.ts_code '
    #        f' and a.trade_date=b.trade_date'
    #        f' order by date desc limit {days};')
    # df = pd.read_sql(sql, engine)
    df = readStockKline(ts_code=ID, days=days)
    # print(df.head())
    bokehplot = BokehPlot(ID, df)
    return bokehplot.plot()


# def __del_plotKline():
#     sql = ('select date, open, high, low, close, ttmpe '
#            'from klinestock where ts_code="%(ts_code)s" '
#            'order by date desc limit %(days)s;' % locals())
#     result = engine.execute(sql).fetchall()
#     stockDatas = [i for i in reversed(result)]
#     klineDatas = []
#     dates = []
#     peDatas = []
#     indexes = list(range(len(stockDatas)))
#     for i in indexes:
#         date, _open, high, low, close, ttmpe = stockDatas[i]
#         klineDatas.append([i, _open, high, low, close])
#         dates.append(date.strftime("%Y-%m-%d"))
#         peDatas.append(ttmpe)
#
#     gs1 = gs.GridSpec(3, 1)
#     gs1.update(hspace=0)
#     fig = plt.figure()
#     ax1 = fig.add_subplot(gs1[0:2, :])
#     candlestick_ohlc(ax1, klineDatas)
#     # ax1.set_title(ID)
#     plt.grid(True)
#     ax2 = fig.add_subplot(gs1[2:3, :])
#     ax2.plot(indexes, peDatas)
#     ax1.set_xlim((0, len(stockDatas)))
#     ax2.set_xlim((0, len(stockDatas)))
#     tickerIndex, tickerLabels = getMonthIndex(dates)
#     locator = FixedLocator(tickerIndex)
#     ax1.xaxis.set_major_locator(locator)
#     ax2.xaxis.set_major_locator(locator)
#     ax2.set_xticklabels(tickerLabels)
#     #     for label in ax2.get_xticklabels():
#     #         label.set_rotation(45)
#     plt.grid(True)
#     plt.legend()
#     plt.show()
#     imgData = BytesIO()
#     fig.savefig(imgData, format='png')
#     return imgData


def plotPE(df):
    # klineDatas = []
    # dates = []
    # peDatas = []
    indexes = list(range(len(df)))
    # for i in indexes:
    #     date, _open, high, low, close, ttmpe = stockDatas[i]
    #     klineDatas.append([i, _open, high, low, close])
    #     dates.append(date.strftime("%Y-%m-%d"))
    #     peDatas.append(ttmpe)

    # gs1 = gs.GridSpec(3, 1)
    # gs1.update(hspace=0)
    fig = plt.figure()
    # ax1 = fig.add_subplot(gs1[0:2, :])
    ax1 = fig.add_subplot()

    # ax1.set_title(ID)
    # ax2 = fig.add_subplot(gs1[2:3, :])
    # ax2.plot(indexes, peDatas)
    ax1.plot(indexes, df.pe.to_list())
    ax1.set_xlim((0, len(df)))
    # ax2.set_xlim((0, len(stockDatas)))
    dates = [date.strftime('%Y%m%d') for date in df.trade_date]
    tickerIndex, tickerLabels = getMonthIndex(dates)
    locator = FixedLocator(tickerIndex)
    ax1.xaxis.set_major_locator(locator)
    # ax2.xaxis.set_major_locator(locator)
    ax1.set_xticklabels(tickerLabels)
    #     for label in ax2.get_xticklabels():
    #         label.set_rotation(45)
    plt.grid(True)
    plt.legend()
    plt.show()
    return
    # imgData = BytesIO()
    # fig.savefig(imgData, format='png')
    # return imgData


def getMonthIndex(dates):
    """

    :param dates: str, '20200320'
    :return:
    """
    month = ''
    monthIndex = []
    monthstr = []
    for i in range(len(dates)):
        date = dates[i]
        if month != date[:4]:
            month = date[:4]
            monthIndex.append(i)
            monthstr.append(month)
    return monthIndex, monthstr


def test():
    df = tushare.get_k_data('600000')
    df = df[-200:]
    ax = plt.subplot(111)
    print(df.head())
    ax.plot(df.index, df.close)
    monthIndex = getMonthIndex(df.date)
    tickerIndex = df.index[monthIndex]
    tickerLabels = df.date[monthIndex].str[:7]
    locator = FixedLocator(tickerIndex)
    ax.xaxis.set_major_locator(locator)
    ax.set_xticklabels(tickerLabels)
    for label in ax.get_xticklabels():
        label.set_rotation(45)
    plt.grid(True)
    #     plt.savefig('testplot.png')
    plt.legend()
    plt.show()


# def plotKlineBokeh(ts_code, days=1000):
#     """
#     绘制K线,pe走势图
#     :param ts_code: string, 股票代码, 600619
#     :param days: int, 走势图显示的总天数
#     :return:
#     """
#     df = readStockKlineDf(ts_code, days)
#     source = ColumnDataSource(df)
#     TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
#     width = 1000
#     klineHeight = int(width / 16 * 6)
#     peHeight = int(width / 16 * 3)
#     selectHeight = int(width / 16 * 1)
#
#     # 绘制K线图
#     dataLen = df.shape[0]
#     tooltips = [('date', '@date'), ('close', '@close')]
#     pkline = figure(x_axis_type="datetime", tools=TOOLS,
#                     plot_height=klineHeight,
#                     plot_width=width,
#                     x_axis_location="above",
#                     title="kline: %s" % ts_code,
#                     tooltips=tooltips,
#                     x_range=(dataLen - 200, dataLen - 1))
#     pkline.xaxis.major_label_overrides = df['date'].to_dict()
#     plotCandlestick(pkline, df)
#     print(type(pkline.y_range))
#     print(pkline.y_range)
#
#     tooltips = [('pe', '@pe')]
#     ppe = figure(x_axis_type="datetime", tools=TOOLS,
#                  plot_height=peHeight, plot_width=width,
#                  tooltips=tooltips,
#                  # x_axis_location=None,
#                  # x_axis_location="bottom",
#                  x_range=pkline.x_range)
#     ppe.xaxis.major_label_overrides = df['date'].to_dict()
#     plotPE(ppe, source)
#
#     select = figure(
#         # title="Drag the middle and edges of the selection box to change the range above",
#         plot_height=selectHeight,
#         plot_width=width,
#         # y_range=ppe.y_range,
#         # x_axis_type="datetime",
#         y_axis_type=None,
#         tools="", toolbar_location=None, background_fill_color="#efefef")
#     select.xaxis.major_label_overrides = df['date'].to_dict()
#     plotPE(select, source)
#
#     range_tool = RangeTool(x_range=pkline.x_range)
#     range_tool.overlay.fill_color = "navy"
#     range_tool.overlay.fill_alpha = 0.2
#     select.add_tools(range_tool)
#     select.toolbar.active_multi = range_tool
#
#     # 绘制滑条，用来控制k线上、下限
#     sliderMax = Slider(start=df.low.min(), end=df.high.max(),
#                        step=1, value=df.high.max())
#     sliderMin = Slider(start=df.low.min(), end=df.high.max(),
#                        step=1, value=df.high.min())
#     sliderMax.js_link('value', pkline.y_range, 'end')
#     sliderMin.js_link('value', pkline.y_range, 'start')
#     column_layout = column([pkline, ppe, select, sliderMin, sliderMax])
#     return column_layout
#     # output_file("kline.html", title="kline plot test")
#     # show(column_layout)  # open a browser


class BokehPlot:
    """
    绘制K线,pe走势图
    :param ts_code: string, 股票代码, 600619
    :param days: int, 走势图显示的总天数
    :return:
    """

    def __init__(self, ID, df):
        df['date'] = [i.strftime('%Y%m%d') for i in df.date]
        self.df = df
        days = df.shape[0]
        self.source = ColumnDataSource(self.df)

        TOOLS = 'pan,wheel_zoom,box_zoom,reset,save,crosshair'
        width = 1000
        klineHeight = int(width / 16 * 6)
        peHeight = int(width / 16 * 3)
        selectHeight = int(width / 16 * 1)

        # 绘制K线图
        dataLen = self.df.shape[0]
        tooltips = [('date', '@date'), ('close', '@close')]
        ymin = self.df.low[-200:].min()
        ymax = self.df.high[-200:].max()
        start = ymin - (ymax - ymin) * 0.05
        end = ymax + (ymax - ymin) * 0.05
        self.pkline = figure(x_axis_type="datetime", tools=TOOLS,
                             plot_height=klineHeight,
                             plot_width=width,
                             x_axis_location="above",
                             x_range=(dataLen - 200, dataLen),
                             y_range=(start, end),
                             tooltips=tooltips)
        self.pkline.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotCandlestick()

        tooltips = [('date', '@date'), ('pe', '@pe')]
        ymin = self.df.pe[-200:].min()
        ymax = self.df.pe[-200:].max()
        start = ymin - (ymax - ymin) * 0.05
        end = ymax + (ymax - ymin) * 0.05
        self.ppe = figure(x_axis_type="datetime", tools=TOOLS,
                          plot_height=peHeight, plot_width=width,
                          tooltips=tooltips,
                          x_range=self.pkline.x_range,
                          y_range=(start, end))
        self.ppe.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotPE(self.ppe)

        self.select = figure(plot_height=selectHeight,
                             plot_width=width,
                             x_range=(-2, days + 2),
                             y_axis_type=None,
                             tools="",
                             toolbar_location=None,
                             background_fill_color="#efefef")
        self.select.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotPE(self.select)

        range_tool = RangeTool(x_range=self.pkline.x_range)
        range_tool.overlay.fill_color = "navy"
        range_tool.overlay.fill_alpha = 0.2
        self.select.add_tools(range_tool)
        self.select.toolbar.active_multi = range_tool

        # kline和pe显示范围变动时自动更新y轴范围
        code = """
                var xstart = parseInt(ppe.x_range.start);
                if(xstart<0){xstart=0;}
                var xend = parseInt(ppe.x_range.end);
                if(xend>maxdays){xend=maxdays;}
                console.log('xstart: ', xstart);
                console.log('xend: ', xend);
                console.log('maxdays: ', maxdays);
                
                var data = source.data;
                var highdata = data['high'];
                var lowdata = data['low'];
                var klineymax = highdata[xstart];
                var klineymin = lowdata[xstart];
                var pedata = data['pe'];
                var peymax = pedata[xstart];
                var peymin = pedata[xstart];
                
                for (var i = xstart + 1; i <= xend; i++) {
                    klineymax =  Math.max(klineymax, highdata[i]);
                    klineymin =  Math.min(klineymin, lowdata[i]);
                    peymax =  Math.max(peymax, pedata[i]);
                    peymin =  Math.min(peymin, pedata[i]);
                    // console.log('pedata[i]: ', pedata[i]);
                    // console.log('i:', i);
                }
                
                pkline.y_range.start = klineymin - (klineymax - klineymin) * 0.05;
                pkline.y_range.end = klineymax + (klineymax - klineymin) * 0.05;
                ppe.y_range.start = peymin - (peymax - peymin) * 0.05;
                ppe.y_range.end = peymax + (peymax - peymin) * 0.05;
                console.log('klineymax: ', klineymax);
                console.log('klineymin: ', klineymin);
                console.log('peymax: ', peymax);
                console.log('peymin: ', peymin);
                """
        # code = """
        #         ppe.y_range.end = 30
        #        """
        callback = CustomJS(args=dict(ppe=self.ppe,
                                      pkline=self.pkline,
                                      source=self.source,
                                      maxdays=days - 1),
                            code=code)
        self.ppe.x_range.js_on_change('start', callback)
        self.ppe.x_range.js_on_change('end', callback)

        self.column_layout = column([self.pkline, self.ppe, self.select])
        # self.column_layout = column([self.pkline, self.ppe, self.select,
        #                              self.sliderKlineMin, self.sliderKlineMax,
        #                              self.sliderPEMin, self.sliderPEMax])

        # self.pkline.x_range.on_change('end',
        #                               callback=CustomJS.from_py_func(self.update))
        # output_file("kline.html", title="kline plot test")
        # show(self.column_layout)  # open a browser
        # return self.column_layout

    def plot(self):
        return self.column_layout

    def plotCandlestick(self):
        """绘制K线图"""
        inc = ((self.df.close > self.df.open) |
               ((self.df.open == self.df.close) &
                self.df.close.gt(self.df.close.shift())))
        dec = ~inc
        incSor = ColumnDataSource(self.df[inc])
        decSor = ColumnDataSource(self.df[dec])

        self.pkline.segment(x0='index', y0='high',
                            x1='index', y1='low',
                            source=incSor, color="red")
        self.pkline.segment(x0='index', y0='high',
                            x1='index', y1='low',
                            source=decSor, color="green")
        w = 0.6
        self.pkline.vbar(x='index', bottom='open', top='close',
                         width=w, source=incSor,
                         fill_color='red', line_color='red')
        self.pkline.vbar(x='index', bottom='close', top='open',
                         width=w, source=decSor,
                         fill_color='green', line_color='green')

    def plotPE(self, p):
        p.line(x='index', y='pe', source=self.source)

    def update(self):
        # print(attr, old, new)
        df = self.df[self.pkline.x_range.start:self.pkline.x_range.end + 1]
        klineMin = df.low.min()
        klineMax = df.low.max()
        peMin = df.pe.min()
        peMax = df.pe.max()
        self.pkline.y_range = (klineMin, klineMax)
        self.ppe.y_range = (peMin, peMax)


class PlotProfitsInc:
    """
    绘制K线,pe走势图
    :param ts_code: string, 股票代码, 600619
    :param days: int, 走势图显示的总天数
    :return:
    """

    def __init__(self, ts_code, startDate=None, endDate=None):
        sql = (f'select end_date date, dt_netprofit_yoy inc from fina_indicator'
               f' where ts_code="{ts_code}"')
        sql += f' and end_date>="{startDate}"' if startDate else ''
        sql += f' and end_date<="{endDate}"' if endDate else ''
        print(sql)
        df = pd.read_sql(sql, engine)
        df.dropna(inplace=True)
        df['date'] = [i.strftime('%Y%m%d') for i in df.date]
        print(df)
        # days = df.shape[0]
        self.source = ColumnDataSource(df)

        TOOLS = 'pan,wheel_zoom,box_zoom,reset,save,crosshair'
        width = 500
        height = int(width / 16 * 6)
        # peHeight = int(width / 16 * 3)
        # selectHeight = int(width / 16 * 1)

        # 绘制散点图
        dataLen = df.shape[0]
        tooltips = [('date', '@date'), ('inc', '@inc')]
        # ymin = df.low[-200:].min()
        # ymax = df.high[-200:].max()
        # start = ymin - (ymax - ymin) * 0.05
        # end = ymax + (ymax - ymin) * 0.05
        self.pinc = figure(x_axis_type="datetime", tools=TOOLS,
                           plot_height=height,
                           plot_width=width,
                           # x_axis_location="above",
                           # x_range=(dataLen - 200, dataLen),
                           # y_range=(start, end),
                           tooltips=tooltips)
        self.pinc.xaxis.major_label_overrides = df['date'].to_dict()
        incSor = ColumnDataSource(df)
        self.pinc.scatter(x='index', y='inc', source=incSor, color="red")
        # a = [1, 2, 3, 4, 5]
        # b = [1, 2, 3, 4, 5]
        # self.pinc.scatter(x=a, y=b)
        # self.plotCandlestick()

        xmin = min(df.index)
        xmax = max(df.index)
        print(xmin, xmax)
        filename = 'data/profits_inc_adf_linear.xlsx'
        linearDf = pd.read_excel(filename)
        intercept = linearDf[linearDf.ts_code == ts_code].intercept.values[0]
        coef = linearDf[linearDf.ts_code == ts_code].coef.values[0]
        r2 = linearDf[linearDf.ts_code == ts_code].r2.values[0]
        print(intercept, coef, r2)
        y = [intercept + x * coef for x in df.index]
        self.pinc.line(df.index, y, color='blue')

    def plot(self):
        return self.pinc


if __name__ == '__main__':
    startDate = '2017-01-01'
    endDate = '2017-03-31'
    #     k = dateStrList(startDate, endDate)
    #     print k
    #     scatter(startDate, endDate)
    # plotKline('600801')
    BokehPlot('600519')
#     test()
