# -*- coding: utf-8 -*-
'''
Created on 2016年12月14日

@author: who8736
'''

from flask import render_template
# from flask_login import login_required, current_user
from report import report1 as guzhiReport
from sqlrw import getChiguList, getGuzhiList, getYouzhiList
from . import app

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


@app.route('/reporttype/<typeid>')
def reportnav(typeid):
    if typeid == 'chigu':
        stockList = getChiguList()
    elif typeid == 'youzhi':
        stockList = getYouzhiList()
    else:
        stockList = getGuzhiList()

    return render_template("reportnav.html", stockList=stockList)


@app.route('/report/<stockid>')
def reportView(stockid):
    stockItem = guzhiReport(stockid)
#     reportstr = reportstr[:20]
#     reportstr = 'test'
    return render_template("report.html",
                           stock=stockItem)
