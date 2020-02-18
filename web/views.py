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
from sqlrw import readPERate, readStockKlineDf, readIndexKlineDf
from sqlrw import readStockIDsFromSQL, writeChigu
from sqlrw import readValuationSammary
from . import app
from .forms import StockListForm


@app.route('/')
def index():
    testText = 'testText, 中文'
    return render_template('index.html', testText=testText)


@app.route('/stocklist', methods=["GET", "POST"])
def setStockList():
    stockList = getChiguList()
    stockListStr = "|".join([i for i in stockList])
    form = StockListForm()
    #     form.stockList = stockListStr
    if form.validate_on_submit():
        stockListStr = form.stockList.data
        print(stockListStr)
        stockList = stockListStr.split("|")
        allStockID = readStockIDsFromSQL()
        checkFlag = True
        for stockID in stockList:
            if stockID not in allStockID:
                checkFlag = False
                print('%s is not a valid stockid' % stockID)
                break
        if checkFlag:
            print('all ok')
            writeChigu(stockList)
            return redirect(url_for('index'))
    return render_template('stocklist.html',
                           form=form,
                           stockListStr=stockListStr)


@app.route('/reporttype/<typeid>')
def reportnav(typeid):
    if typeid == 'chigu':
        stockList = getChiguList()
    elif typeid == 'youzhi':
        stockList = getYouzhiList()
    else:
        stockList = getGuzhiList()

    stockReportList = []
    for stockID in stockList:
        stockName = getStockName(stockID)
        stockClose = readCurrentClose(stockID)
        pe = readLastTTMPE(stockID)
        peg = readCurrentPEG(stockID)
        pe200, pe1000 = readPERate(stockID)
        stockReportList.append([stockID, stockName,
                                stockClose, pe, peg, pe200, pe1000])
    return render_template('reportnav.html', stockList=stockReportList)


@app.route('/report/<stockid>')
def reportView(stockid):
    stockItem = guzhiReport(stockid)
    #     reportstr = reportstr[:20]
    #     reportstr = 'test'
    return render_template('report.html',
                           stock=stockItem)


@app.route('/valuationtype/<typeid>')
def valuationNav(typeid):
    stocksDf = readValuationSammary()
    if typeid == 'chigu':
        stocksDf = stocksDf[stocksDf['stockid'].isin(getChiguList())]
    elif typeid == 'youzhi':
        stocksDf = stocksDf[(stocksDf.pf >= 5) &
                            (stocksDf.pe < 30)]
    # stockReportList = np.array(stocksDf).tolist()
    return render_template('valuationnav.html', stocksDf=stocksDf)


@app.route('/valuation/<stockid>')
def valuationView(stockid):
    stockItem = reportValuation(stockid)
    #     reportstr = reportstr[:20]
    #     reportstr = 'test'
    return render_template('valuation.html',
                           stock=stockItem)


@app.route('/klineimg/<stockID>')
def klineimg(stockID):
    plotImg = plotKline(stockID)
    plotImg.seek(0)
    return send_file(plotImg,
                     attachment_filename='img.png',
                     as_attachment=True)


@app.route('/stockklineimgnew/<stockID>')
def stockklineimgnew(stockID):
    df = readStockKlineDf(stockID, days=1000)
    return _klineimg(stockID, df)

@app.route('/indexklineimgnew/<ID>')
def indexklineimgnew(ID):
    df = readIndexKlineDf(ID, days=3000)
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

