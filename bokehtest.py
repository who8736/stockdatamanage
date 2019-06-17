# -*- coding: utf-8 -*-
"""
Created on 2019年5月10日

@author: who8736
"""
from abc import abstractmethod

from math import pi

import pandas as pd
from bokeh.plotting import figure, show, output_file
from bokeh.sampledata.stocks import MSFT
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, RangeTool
# from bokeh.layouts import row
# from bokeh.layouts import gridplot
# from bokeh.models import FixedFormatter
from bokeh.models import CustomJS

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


def plotIndexPE():
    """
    待删除
    :return:
    """
    sql = 'select date, pe from pehistory where name="all"'
    df = pd.read_sql(sql, engine)
    print(df)
    source = ColumnDataSource(df)
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
    width = 1000
    peHeight = int(width / 16 * 3)
    selectHeight = int(width / 16 * 1)

    # # 绘制K线图
    # tooltips = [('date', '@date'), ('close', '@close')]
    # pkline = figure(x_axis_type="datetime", tools=TOOLS,
    #                 plot_height=klineHeight,
    #                 plot_width=width,
    #                 x_axis_location="above",
    #                 title="kline: %s" % stockID,
    #                 tooltips=tooltips,
    # pkline.xaxis.major_label_overrides = df['date'].to_dict()
    # plotCandlestick(pkline, df)
    # print(type(pkline.y_range))
    # print(pkline.y_range)

    dataLen = df.shape[0]
    tooltips = [('pe', '@pe')]
    ppe = figure(x_axis_type="datetime", tools=TOOLS,
                 plot_height=peHeight, plot_width=width,
                 # x_axis_location=None,
                 # x_axis_location="bottom",
                 x_range=(dataLen - 200, dataLen - 1),
                 tooltips=tooltips)
    # ppe.xaxis.major_label_overrides = df['date'].to_dict()
    plotPE(ppe, source)

    select = figure(plot_height=selectHeight,
                    plot_width=width,
                    # y_range=ppe.y_range,
                    # x_axis_type="datetime",
                    y_axis_type=None,
                    tools="", toolbar_location=None, background_fill_color="#efefef")
    # select.xaxis.major_label_overrides = df['date'].to_dict()
    plotPE(select, source)

    range_tool = RangeTool(x_range=ppe.x_range)
    range_tool.overlay.fill_color = "navy"
    range_tool.overlay.fill_alpha = 0.2
    select.add_tools(range_tool)
    select.toolbar.active_multi = range_tool

    column_layout = column([ppe, select])
    output_file("pe.html", title="index pe plot test")
    show(column_layout)  # open a browser


class BokehPlot:
    """
    绘制K线,pe走势图
    :param stockID: string, 股票代码, 600619
    :param days: int, 走势图显示的总天数
    :return:
    """

    def __init__(self, stockID, days=1000):
        # self.df = readKlineDf(stockID, days)
        self.df = self._getDf(stockID, days)
        self.source = ColumnDataSource(self.df)

        TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
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
                             x_range=(dataLen - 200, dataLen - 1),
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
                             x_range=(0, days - 1),
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
                if(xend>maxdays - 1){xend=maxdays - 1;}
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
                
                for (var i = xstart + 1; i < xend; i++) {
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
                                      maxdays=days),
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

    def update(self):
        # print(attr, old, new)
        df = self.df[self.pkline.x_range.start:self.pkline.x_range.end + 1]
        klineMin = df.low.min()
        klineMax = df.low.max()
        peMin = df.pe.min()
        peMax = df.pe.max()
        self.pkline.y_range = (klineMin, klineMax)
        self.ppe.y_range = (peMin, peMax)

    @abstractmethod
    def _getDf(self, id, days):
        """
        本函数应在子类中实现，用于读取股票或指数的日K线数据和PE数据
        :param days:
        :return:
        """
        # please implemente in subclass


class BokehPlotStock(BokehPlot):
    def _getDf(self, id, days):
        return readKlineDf(id, days)


class BokehPlotPE:
    """
    绘制指数pe走势图
    :param days: int, 走势图显示的总天数
    :return:
    """

    def __init__(self, days=1000):
        sql = 'select date, pe from pehistory where name="all"'
        self.df = pd.read_sql(sql, engine)
        self.source = ColumnDataSource(self.df)

        TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
        width = 1000
        peHeight = int(width / 16 * 3)
        selectHeight = int(width / 16 * 1)

        # 绘制K线图
        dataLen = self.df.shape[0]
        # tooltips = [('index', '@index'), ('date', '@date'), ('close', '@close')]
        # ymin = self.df.low[-200:].min()
        # ymax = self.df.high[-200:].max()
        # start = ymin - (ymax - ymin) * 0.05
        # end = ymax + (ymax - ymin) * 0.05
        # self.pkline = figure(x_axis_type="datetime", tools=TOOLS,
        #                      plot_height=klineHeight,
        #                      plot_width=width,
        #                      x_axis_location="above",
        #                      x_range=(dataLen - 200, dataLen - 1),
        #                      y_range=(start, end),
        #                      tooltips=tooltips)
        # self.pkline.xaxis.major_label_overrides = self.df['date'].to_dict()
        # self.plotCandlestick()

        tooltips = [('pe', '@pe')]
        ymin = self.df.pe[-200:].min()
        ymax = self.df.pe[-200:].max()
        start = ymin - (ymax - ymin) * 0.05
        end = ymax + (ymax - ymin) * 0.05
        self.ppe = figure(x_axis_type="datetime", tools=TOOLS,
                          plot_height=peHeight, plot_width=width,
                          tooltips=tooltips,
                          x_range=(dataLen - 200, dataLen - 1),
                          # x_range=self.pkline.x_range,
                          y_range=(start, end))
        self.ppe.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotPE(self.ppe)

        self.select = figure(plot_height=selectHeight,
                             plot_width=width,
                             x_range=(0, days - 1),
                             y_axis_type=None,
                             tools="",
                             toolbar_location=None,
                             background_fill_color="#efefef")
        self.select.xaxis.major_label_overrides = self.df['date'].to_dict()
        self.plotPE(self.select)

        range_tool = RangeTool(x_range=self.ppe.x_range)
        range_tool.overlay.fill_color = "navy"
        range_tool.overlay.fill_alpha = 0.2
        self.select.add_tools(range_tool)
        self.select.toolbar.active_multi = range_tool

        # kline和pe显示范围变动时自动更新y轴范围
        code = """
                var xstart = parseInt(ppe.x_range.start);
                if(xstart<0){xstart=0;}
                var xend = parseInt(ppe.x_range.end);
                if(xend>maxdays - 1){xend=maxdays - 1;}
                console.log('xstart: ', xstart);
                console.log('xend: ', xend);
                console.log('maxdays: ', maxdays);
                
                var data = source.data;
                var highdata = data['high'];
                var lowdata = data['low'];
                var pedata = data['pe'];
                var peymax = pedata[xstart];
                var peymin = pedata[xstart];
                
                for (var i = xstart + 1; i < xend; i++) {
                    peymax =  Math.max(peymax, pedata[i]);
                    peymin =  Math.min(peymin, pedata[i]);
                    // console.log('pedata[i]: ', pedata[i]);
                    // console.log('i:', i);
                }
                
                ppe.y_range.start = peymin - (peymax - peymin) * 0.05;
                ppe.y_range.end = peymax + (peymax - peymin) * 0.05;
                console.log('peymax: ', peymax);
                console.log('peymin: ', peymin);
                """
        # code = """
        #         ppe.y_range.end = 30
        #        """
        callback = CustomJS(args=dict(ppe=self.ppe,
                                      source=self.source,
                                      maxdays=days),
                            code=code)
        self.ppe.x_range.js_on_change('start', callback)
        self.ppe.x_range.js_on_change('end', callback)

        self.column_layout = column([self.ppe, self.select])
        # output_file("kline.html", title="kline plot test")
        # show(self.column_layout)  # open a browser
        # return self.column_layout

    def plot(self):
        return self.column_layout

    def plotPE(self, p):
        p.line(x='index', y='pe', source=self.source)

    def update(self):
        # print(attr, old, new)
        df = self.df[self.ppe.x_range.start:self.ppe.x_range.end + 1]
        # klineMin = df.low.min()
        # klineMax = df.low.max()
        peMin = df.pe.min()
        peMax = df.pe.max()
        # self.pkline.y_range = (klineMin, klineMax)
        self.ppe.y_range = (peMin, peMax)


if __name__ == '__main__':
    pass
    # test()
    # testPlotKline('600519')
    # df = testPlotKline('600519')
    # plotDf(df)



