
from math import pi

import pandas as pd

from bokeh.plotting import figure, show, output_file
from bokeh.sampledata.stocks import MSFT


# df = pd.DataFrame(MSFT)
def plotCandlestick(p, df):
    inc = df.close > df.open
    dec = df.open > df.close
    # w = 12*60*60*1000 # half day in ms
    p.segment(df.date[inc], df.high[inc], df.date[inc], df.low[inc], color="red")
    p.segment(df.date[dec], df.high[dec], df.date[dec], df.low[dec], color="green")
    p.vbar(df.date[inc], w, df.open[inc], df.close[inc], fill_color="red", line_color="red")
    p.vbar(df.date[dec], w, df.open[dec], df.close[dec], fill_color="green", line_color="green")
    # p.segment(df.date, df.high, df.date, df.low, color="black")
    # p.vbar(df.date[inc], w, df.open[inc], df.close[inc], fill_color="#D5E1DD", line_color="black")
    # p.vbar(df.date[dec], w, df.open[dec], df.close[dec], fill_color="#F2583E", line_color="black")


def test():
    TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
    p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=1000, title="MSFT Candlestick")
    p.xaxis.major_label_orientation = pi/4
    p.grid.grid_line_alpha = 0.8

    df = pd.DataFrame(MSFT)[:50]
    df["date"] = pd.to_datetime(df["date"])

    plotCandlestick(p, df)
    output_file("candlestick.html", title="candlestick.py example")
    show(p)  # open a browser


if __name__ == '__main__':
    test()
