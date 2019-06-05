# -*- coding: utf-8 -*-
"""
Created on 2019年5月10日

@author: who8736
"""

from math import pi

import pandas as pd
from bokeh.plotting import figure, show, output_file
from bokeh.sampledata.stocks import MSFT
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool
# from bokeh.layouts import row
# from bokeh.layouts import gridplot
# from bokeh.models import FixedFormatter

from sqlrw import engine  # @IgnorePep8


# df = pd.DataFrame(MSFT)
def plotCandlestick(p, df):
    inc = df.close > df.open
    dec = df.open > df.close
    incSor = ColumnDataSource(df[inc])
    decSor = ColumnDataSource(df[dec])

    p.segment(x0='index', y0='high', x1='index', y1='low', source=incSor, color="red")
    p.segment(x0='index', y0='high', x1='index', y1='low', source=decSor, color="green")
    w = 0.6
    p.vbar(x='index', bottom='open', top='close',
           width=w, source=incSor,
           fill_color='red', line_color='red')
    p.vbar(x='index', bottom='close', top='open',
           width=w, source=decSor,
           fill_color='green', line_color='green')

    # p.segment(df.index[inc], df.high[inc], df.index[inc], df.low[inc], color="red")
    # p.segment(df.index[dec], df.high[dec], df.index[dec], df.low[dec], color="green")
    # w = 0.6
    # p.vbar(df.index[inc], w, df.open[inc], df.close[inc],
    #        fill_color="red", line_color="red")
    # p.vbar(df.index[dec], w, df.open[dec], df.close[dec],
    #        fill_color="green", line_color="green")


def test():
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, title="MSFT Candlestick")
    p.xaxis.major_label_orientation = pi / 4
    p.grid.grid_line_alpha = 0.8

    df = pd.DataFrame(MSFT)[:50]
    df["date"] = pd.to_datetime(df["date"])

    plotCandlestick(p, df)
    output_file("candlestick.html", title="candlestick.py example")
    show(p)  # open a browser


# def testPlotKline(stockID, days=1000):
def readKlineDf(stockID, days):
    sql = ('select date, open, high, low, close, ttmpe '
           'from kline where stockid="%(stockID)s" '
           'order by date desc limit %(days)s;' % locals())
    result = engine.execute(sql).fetchall()
    stockDatas = [i for i in reversed(result)]
    # klineDatas = []
    dateList = []
    openList = []
    closeList = []
    highList = []
    lowList = []
    peList = []
    indexes = list(range(len(result)))
    for i in indexes:
        date, _open, high, low, close, ttmpe = stockDatas[i]
        dateList.append(date.strftime("%Y-%m-%d"))
        # QuarterList.append(date)
        openList.append(_open)
        closeList.append(close)
        highList.append(high)
        lowList.append(low)
        peList.append(ttmpe)
    klineDf = pd.DataFrame({'date': dateList,
                            'open': openList,
                            'close': closeList,
                            'high': highList,
                            'low': lowList,
                            'pe': peList})

    # print(klineDf)
    # for i in reversed(result):
    #     print(i)
    # plotDf(klineDf)
    return klineDf


def plotPE(p, source):
    p.line(x='index', y='pe', source=source)


def testPlotKline(stockID, days=1000):
    """
    绘制K线,pe走势图
    :param stockID: string, 股票代码, 600619
    :param days: int, 走势图显示的总天数
    :return:
    """
    df = readKlineDf(stockID, days)
    source = ColumnDataSource(df)
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
    width = 1000
    klineHeight = int(width / 16 * 6)
    peHeight = int(width / 16 * 3)
    selectHeight = int(width / 16 * 1)

    # 绘制K线图
    dataLen = df.shape[0]
    tooltips = [('date', '@date'), ('close', '@close')]
    pkline = figure(x_axis_type="datetime", tools=TOOLS,
                    plot_height=klineHeight,
                    plot_width=width,
                    x_axis_location="above",
                    title="kline: %s" % stockID,
                    tooltips=tooltips,
                    x_range=(dataLen - 200, dataLen - 1))
    pkline.xaxis.major_label_overrides = df['date'].to_dict()
    plotCandlestick(pkline, df)
    print(type(pkline.y_range))
    print(pkline.y_range)

    tooltips = [('pe', '@pe')]
    ppe = figure(x_axis_type="datetime", tools=TOOLS,
                 plot_height=peHeight, plot_width=width,
                 tooltips=tooltips,
                 # x_axis_location=None,
                 # x_axis_location="bottom",
                 x_range=pkline.x_range)
    ppe.xaxis.major_label_overrides = df['date'].to_dict()
    plotPE(ppe, source)

    select = figure(
                    # title="Drag the middle and edges of the selection box to change the range above",
                    plot_height=selectHeight,
                    plot_width=width,
                    # y_range=ppe.y_range,
                    # x_axis_type="datetime",
                    y_axis_type=None,
                    tools="", toolbar_location=None, background_fill_color="#efefef")
    select.xaxis.major_label_overrides = df['date'].to_dict()
    plotPE(select, source)

    range_tool = RangeTool(x_range=pkline.x_range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    column_layout = column([pkline, ppe, select])
    output_file("kline.html", title="kline plot test")
    show(column_layout)  # open a browser
    # show(pkline)  # open a browser


if __name__ == '__main__':
    # test()
    testPlotKline('600519')
    # df = testPlotKline('600519')
    # plotDf(df)
