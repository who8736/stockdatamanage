# -*- coding: utf-8 -*-
'''
Created on 2016年12月14日

@author: who8736
'''

import numpy as np
from flask import render_template, redirect, url_for
from flask import send_file
from bokeh.embed import components
from bokeh.resources import INLINE
from bokeh.util.string import encode_utf8

from plot import plotKline, BokehPlot
from report import report1 as guzhiReport
from report import reportValuation
from report import reportIndex
from sqlrw import getChiguList, getGuzhiList, getYouzhiList
from sqlrw import getStockName, readLastTTMPE
from sqlrw import readCurrentClose, readCurrentPEG
from sqlrw import readPERate, readStockKline, readIndexKline
from sqlrw import readStockList, writeChigu
from sqlrw import readValuationSammary
from . import app
from .forms import StockListForm


@app.route('/')
def index():
    testText = 'testText, 中文'
    return render_template('index.html', testText=testText)


@app.route('/stocklist', methods=["GET", "POST"])
def setStockList():
    chigu = getChiguList()
    chiguStr = "|".join([i for i in chigu])
    form = StockListForm()
    #     form.stockList = stockListStr
    if form.validate_on_submit():
        chiguStr = form.stockList.data
        # print(stockListStr)
        chigu = chiguStr.split("|")
        allstocks = readStockList()
        checkFlag = True
        for ts_code in chigu:
            if ts_code not in allstocks:
                checkFlag = False
                print('%s is not a valid ts_code' % ts_code)
                break
        if checkFlag:
            print('all ok')
            writeChigu(chigu)
            return redirect(url_for('index'))
    return render_template('stocklist.html',
                           form=form,
                           stockListStr=chiguStr)


@app.route('/reporttype/<typeid>')
def reportnav(typeid):
    if typeid == 'chigu':
        stockList = getChiguList()
    elif typeid == 'youzhi':
        stockList = getYouzhiList()
    else:
        stockList = getGuzhiList()

    stockReportList = []
    for ts_code in stockList:
        stockName = getStockName(ts_code)
        stockClose = readCurrentClose(ts_code)
        pe = readLastTTMPE(ts_code)
        peg = readCurrentPEG(ts_code)
        pe200, pe1000 = readPERate(ts_code)
        stockReportList.append([ts_code, stockName,
                                stockClose, pe, peg, pe200, pe1000])
    return render_template('reportnav.html', stockList=stockReportList)


@app.route('/report/<ts_code>')
def reportView(ts_code):
    stockItem = guzhiReport(ts_code)
    #     reportstr = reportstr[:20]
    #     reportstr = 'test'
    return render_template('report.html',
                           stock=stockItem)


@app.route('/valuationtype/<typeid>')
def valuationNav(typeid):
    df = readValuationSammary()
    if typeid == 'chigu':
        df = df[df['ts_code'].isin(getChiguList())]
    elif typeid == 'youzhi':
        df = df[(df.pf >= 5) & (df.pe < 30)]
    # stockReportList = np.array(df).tolist()
    return render_template('valuationnav.html', stocksDf=df)


@app.route('/valuation/<ts_code>')
def valuationView(ts_code):
    stockItem = reportValuation(ts_code)
    #     reportstr = reportstr[:20]
    #     reportstr = 'test'
    return render_template('valuation.html',
                           stock=stockItem)


@app.route('/klineimg/<ts_code>')
def klineimg(ts_code):
    plotImg = plotKline(ts_code)
    plotImg.seek(0)
    return send_file(plotImg,
                     attachment_filename='img.png',
                     as_attachment=True)


@app.route('/stockklineimgnew/<ts_code>')
def stockklineimgnew(ts_code):
    df = readStockKline(ts_code, days=1000)
    return _klineimg(ts_code, df)

@app.route('/indexklineimgnew/<ID>')
def indexklineimgnew(ID):
    df = readIndexKline(ID, days=3000)
    return _klineimg(ID, df)

def _klineimg(ID, df):
    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    plotImg = BokehPlot(ID, df)
    scripts, div = components(plotImg.plot())
    # return render_template("plotkline.html", the_div=div, the_script=scripts)
    html = render_template(
        'plotkline.html',
        plot_script=scripts,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
    )
    return encode_utf8(html)


@app.route('/indexinfo/<ID>')
def indexInfo(ID):
    stockItem = reportIndex(ID)
    return render_template('indexinfo.html',
                           stock=stockItem)

