# -*- coding: utf-8 -*-
'''
Created on 2016年12月14日

@author: who8736
'''

from flask import render_template, redirect, url_for
from flask import send_file
# from flask_login import login_required, current_user
from report import report1 as guzhiReport
from sqlrw import getChiguList, getGuzhiList, getYouzhiList
from sqlrw import saveChigu, readStockIDsFromSQL
from sqlrw import getStockName, readCurrentTTMPE
from sqlrw import readCurrentClose, readCurrentPEG
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
        print stockListStr
        stockList = stockListStr.split("|")
        allStockID = readStockIDsFromSQL()
        checkFlag = True
        for stockID in stockList:
            if stockID not in allStockID:
                checkFlag = False
                print '%s is not a valid stockid' % stockID
                break
        if checkFlag:
            print 'all ok'
            saveChigu(stockList)
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
        stockReportList.append([stockID, stockName,
                                stockClose, pe, peg])
    return render_template("reportnav.html", stockList=stockReportList)


@app.route('/report/<stockid>')
def reportView(stockid):
    stockItem = guzhiReport(stockid)
#     reportstr = reportstr[:20]
#     reportstr = 'test'
    return render_template("report.html",
                           stock=stockItem)


@app.route('/klineimg/<stockID>')
def klineimg(stockID):
    return send_file(plotKline(stockID))
