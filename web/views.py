# -*- coding: utf-8 -*-
'''
Created on 2016年12月14日

@author: who8736
'''

import numpy as np
from flask import render_template, redirect, url_for
from flask import send_file
# from flask_login import login_required, current_user
from report import report1 as guzhiReport
from report import reportValuation
from sqlrw import getChiguList, getGuzhiList, getYouzhiList
from sqlrw import readStockIDsFromSQL, writeChigu
from sqlrw import getStockName, readCurrentTTMPE
from sqlrw import readCurrentClose, readCurrentPEG
from sqlrw import readPERate
from sqlrw import readValuationSammary, readValuation
from plot import plotKline

from . import app
from .forms import StockListForm
# import sys
#
# sys.setdefaultencoding('utf-8')
# reload(sys)


class testobj():

    def __init__(self):
        self.t1 = 'testtext1'
        self.t2 = 'testtext2'
        self.t3 = 'testtext3'
        self.t4 = ['t401', 't402', 't403', 't404', ]


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
        pe = readCurrentTTMPE(stockID)
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
#        sql = 'select stockid, name, pf, pe, peg, pe200, pe1000'

    stockReportList = np.array(stocksDf).tolist()
#    for stockID in stockList:
#        stockName = getStockName(stockID)
#        stockClose = readCurrentClose(stockID)
#        pe = readCurrentTTMPE(stockID)
#        peg = readCurrentPEG(stockID)
#        pe200, pe1000 = readPERate(stockID)
#        stockReportList.append([stockID, stockName,
#                                stockClose, pe, peg, pe200, pe1000])
    return render_template('valuationnav.html', stockList=stockReportList)


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
