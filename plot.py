# -*- coding: utf-8 -*-
"""
Created on 2017年2月10日

@author: who8736
"""

# import datetime
import time
from io import BytesIO

import matplotlib

import matplotlib.pyplot as plt  # @IgnorePep8
# from matplotlib.finance import candlestick_ohlc  # @IgnorePep8
from mpl_finance import candlestick_ohlc  # @IgnorePep8
import matplotlib.gridspec as gs  # @IgnorePep8
from matplotlib.dates import DateFormatter, MonthLocator  # @IgnorePep8
from matplotlib.ticker import FixedLocator  # @IgnorePep8
import tushare  # @IgnorePep8
from bokeh.plotting import figure, show, output_file
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool
from bokeh.models import Slider, CustomJS

from sqlrw import engine, readKlineDf  # @IgnorePep8
from datatrans import dateStrList  # @IgnorePep8

matplotlib.use('Agg')  # @UndefinedVariable


# matplotlib.use('Qt5Agg')  # @UndefinedVariable


def scatter(startDate, endDate):
    dateList = dateStrList(startDate, endDate)
    for date in dateList:
        print(date)
        sql = ('select pe, lirunincrease from pelirunincrease '
               'where date="%(date)s";' % locals())
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


def plotKlineOld(stockID):
    #     return plotKline(stockID)
    #     ax2 = fig.add_subplot(2, 1, 2)
    sql = ('select date, open, high, low, close, ttmpe '
           'from kline%(stockID)s '
           'order by date desc limit 1000;' % locals())
    result = engine.execute(sql)
    stockDatas = result.fetchall()
    klineDatas = []
    dates = []
    peDatas = []
    #     klineDatas = result.fetchall()
    for date, _open, high, low, close, ttmpe in stockDatas:
        klineDatas.append([time.mktime(date.timetuple()),
                           _open, high, low, close])
        dates.append(time.mktime(date.timetuple()))
        peDatas.append(ttmpe)

    print(dates)
    print(peDatas)
    gs1 = gs.GridSpec(3, 1)
    gs1.update(hspace=0)
    fig = plt.figure()
    ax1 = fig.add_subplot(gs1[0:2, :])
    candlestick_ohlc(ax1, klineDatas)
    ax1.set_title(stockID)
    ax2 = fig.add_subplot(gs1[2:3, :])
    ax2.plot(dates, peDatas)
    ax2.xaxis.set_major_locator(MonthLocator())
    ax2.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
    fig.autofmt_xdate()

    #     ax1.subplots_adjust(hspace=None)
    #     fig.subplots_adjust(hspace=0)
    #     plt.show()
    imgData = BytesIO()
    fig.savefig(imgData, format='png')
    #     imgData.seek(0)

    return imgData


#     datetime.date
#     datetime.timestamp()
#     datetime.


def plotKline(stockID):
    """ 绘制K线与TTMPE图
    """
    sql = ('select date, open, high, low, close, ttmpe '
           'from kline%(stockID)s '
           'order by date desc limit 1000;' % locals())
    result = engine.execute(sql).fetchall()
    stockDatas = [i for i in reversed(result)]
    klineDatas = []
    dates = []
    peDatas = []
    indexes = list(range(len(stockDatas)))
    for i in indexes:
        date, _open, high, low, close, ttmpe = stockDatas[i]
        klineDatas.append([i, _open, high, low, close])
        dates.append(date.strftime("%Y-%m-%d"))
        peDatas.append(ttmpe)

    gs1 = gs.GridSpec(3, 1)
    gs1.update(hspace=0)
    fig = plt.figure()
    ax1 = fig.add_subplot(gs1[0:2, :])
    candlestick_ohlc(ax1, klineDatas)
    ax1.set_title(stockID)
    plt.grid(True)
    ax2 = fig.add_subplot(gs1[2:3, :])
    ax2.plot(indexes, peDatas)
    ax1.set_xlim((0, len(stockDatas)))
    ax2.set_xlim((0, len(stockDatas)))
    tickerIndex, tickerLabels = getMonthIndex(dates)
    locator = FixedLocator(tickerIndex)
    ax1.xaxis.set_major_locator(locator)
    ax2.xaxis.set_major_locator(locator)
    ax2.set_xticklabels(tickerLabels)
    #     for label in ax2.get_xticklabels():
    #         label.set_rotation(45)
    plt.grid(True)
    plt.legend()
    plt.show()
    imgData = BytesIO()
    fig.savefig(imgData, format='png')
    return imgData


def getMonthIndex(dates):
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


# def plotKlineBokeh(stockID, days=1000):
#     """
#     绘制K线,pe走势图
#     :param stockID: string, 股票代码, 600619
#     :param days: int, 走势图显示的总天数
#     :return:
#     """
#     df = readKlineDf(stockID, days)
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
#                     title="kline: %s" % stockID,
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
    :param stockID: string, 股票代码, 600619
    :param days: int, 走势图显示的总天数
    :return:
    """

    def __init__(self, stockID, days=1000):
        self.df = readKlineDf(stockID, days)
        self.source = ColumnDataSource(self.df)

        TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
        width = 1000
        klineHeight = int(width / 16 * 6)
        peHeight = int(width / 16 * 3)
        selectHeight = int(width / 16 * 1)

        # 绘制K线图
        dataLen = self.df.shape[0]
        tooltips = [('date', '@date'), ('close', '@close')]
        self.pkline = figure(x_axis_type="datetime", tools=TOOLS,
                             plot_height=klineHeight,
                             plot_width=width,
                             x_axis_location="above",
                             x_range=(dataLen - 200, dataLen - 1),
                             tooltips=tooltips)
        self.pkline.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotCandlestick()

        tooltips = [('pe', '@pe')]
        self.ppe = figure(x_axis_type="datetime", tools=TOOLS,
                          plot_height=peHeight, plot_width=width,
                          tooltips=tooltips,
                          x_range=self.pkline.x_range)
        self.ppe.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotPE(self.ppe)

        self.select = figure(plot_height=selectHeight,
                             plot_width=width,
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

        # 绘制滑条，用来控制k线上、下限
        self.sliderKlineMax = Slider(start=self.df.high.min(), end=self.df.high.max(),
                                     step=0.1, value=self.df.high.max())
        self.sliderKlineMin = Slider(start=self.df.low.min(), end=self.df.low.max(),
                                     step=0.1, value=self.df.low.min())
        callback = CustomJS(args=dict(pkline=self.pkline), code="""
                            pkline.y_range.end = cb_obj.value
                            """)
        self.sliderKlineMax.js_on_change('value', callback)
        callback = CustomJS(args=dict(pkline=self.pkline), code="""
                            pkline.y_range.start = cb_obj.value
                            """)
        self.sliderKlineMin.js_on_change('value', callback)

        # 绘制滑条，用来控制pe线上、下限
        self.sliderPEMax = Slider(start=self.df.pe.min(), end=self.df.pe.max(),
                                  step=0.1, value=self.df.pe.max())
        self.sliderPEMin = Slider(start=self.df.pe.min(), end=self.df.pe.max(),
                                  step=0.1, value=self.df.pe.min())
        callback = CustomJS(args=dict(ppe=self.ppe), code="""
                            ppe.y_range.end = cb_obj.value
                            """)
        self.sliderPEMax.js_on_change('value', callback)
        callback = CustomJS(args=dict(ppe=self.ppe), code="""
                            ppe.y_range.start = cb_obj.value
                            """)
        self.sliderPEMin.js_on_change('value', callback)

        self.column_layout = column([self.pkline, self.ppe, self.select,
                                     self.sliderKlineMin, self.sliderKlineMax,
                                     self.sliderPEMin, self.sliderPEMax])

        self.pkline.x_range.on_change('start', self.update)
        self.pkline.x_range.on_change('end', self.update)
        # output_file("kline.html", title="kline plot test")
        # show(self.column_layout)  # open a browser
        # return self.column_layout

    def plot(self):
        return self.column_layout

    def plotCandlestick(self):
        inc = self.df.close > self.df.open
        dec = self.df.open > self.df.close
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

    def update(self, attr, old, new):
        print(attr, old, new)
        df = self.df[self.pkline.x_range.start:self.pkline.x_range.end + 1]
        klineMin = df.low.min()
        klineMax = df.low.max()
        peMin = df.pe.min()
        peMax = df.pe.max()
        self.pkline.y_range = (klineMin, klineMax)
        self.ppe.y_range = (peMin, peMax)


if __name__ == '__main__':
    startDate = '2017-01-01'
    endDate = '2017-03-31'
    #     k = dateStrList(startDate, endDate)
    #     print k
    #     scatter(startDate, endDate)
    # plotKline('600801')
    BokehPlot('600519')
#     test()
