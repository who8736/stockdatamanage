# -*- coding: utf-8 -*-
"""
Created on 2016年12月14日

@author: who8736
"""

from flask import (
    render_template, redirect, url_for, request, send_file, current_app,
)
from bokeh.embed import components
from bokeh.resources import INLINE
# from bokeh.util.string import encode_utf8
from flask_paginate import Pagination, get_page_parameter

from stockdatamanage.plot import plotKline, BokehPlot
from stockdatamanage.plot import PlotProfitsInc
from stockdatamanage.report import reportValuation
from stockdatamanage.report import reportIndex
from stockdatamanage.sqlrw import (
    readChigu, readStockKline, readIndexKline, readStockList, writeChigu,
    readValuationSammary, readProfitsIncAdf,
)
from stockdatamanage.misc import tsCode
from . import app
from .forms import StockListForm, HoldForm


@app.route('/')
def index():
    testText = 'testText, 中文'
    return render_template('index.html', testText=testText)


@app.route('/stocklist', methods=["GET", "POST"])
def setStockList():
    chigu = readChigu()
    chiguStr = "|".join([i for i in chigu])
    form = StockListForm()
    #     form.stockList = stockListStr
    if form.validate_on_submit():
        chiguStr = form.stockList.data
        # print(stockListStr)
        chigu = chiguStr.split("|")
        chigu = [tsCode(ts_code) for ts_code in chigu]
        allstocks = readStockList().ts_code.to_list()
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


# @app.route('/reporttype/<typeid>')
# def reportnav(typeid):
#     if typeid == 'chigu':
#         stockList = readChigu()
#     elif typeid == 'youzhi':
#         stockList = getYouzhiList()
#     else:
#         stockList = getGuzhiList()
#
#     stockReportList = []
#     for ts_code in stockList:
#         stockName = getStockName(ts_code)
#         stockClose = readCurrentClose(ts_code)
#         pe = readLastTTMPE(ts_code)
#         peg = readCurrentPEG(ts_code)
#         pe200, pe1000 = readPERate(ts_code)
#         stockReportList.append([ts_code, stockName,
#                                 stockClose, pe, peg, pe200, pe1000])
#     return render_template('reportnav.html', stockList=stockReportList)
#

# @app.route('/report/<ts_code>')
# def reportView(ts_code):
#     stockItem = guzhiReport(ts_code)
#     #     reportstr = reportstr[:20]
#     #     reportstr = 'tests'
#     return render_template('report.html',
#                            stock=stockItem)


@app.route('/valuationtype/<typeid>')
def valuationNav(typeid):
    df = readValuationSammary()
    if typeid == 'chigu':
        df = df[df['ts_code'].isin(readChigu())]
    elif typeid == 'youzhi':
        df = df[(df.pf >= 5) & (df.pe < 30)]
    # stockReportList = np.array(df).tolist()
    return render_template('valuationnav.html', stocksDf=df)


@app.route('/valuation/<ts_code>')
def valuationView(ts_code):
    stockItem = reportValuation(ts_code)
    #     reportstr = reportstr[:20]
    #     reportstr = 'tests'
    return render_template('valuation.html',
                           stock=stockItem)


@app.route('/test_table')
def test_table():
    data = list(range(5))
    q = request.args.get('q')
    if q:
        search = True

    page = request.args.get(get_page_parameter(), type=int, default=1)
    pagination = Pagination(page=page, per_page=10, total=100,
                            # search=True,
                            bs_version=4,
                            prev_label='上一页',
                            next_label='下一页',
                            display_msg='当前 <b>{start} - {end}</b> 条/共 <b>{total}</b> 条',
                            record_name='记录',
                            show_single_page=True)
    print('pagination.bs_version: ', pagination.bs_version)
    print('pagination.links: ', pagination.links)
    print('pagination.display_msg: ', pagination.display_msg)
    print('pagination.search_msg: ', pagination.search_msg)
    print('data: ', data)
    return render_template('test_table.html', data=data, pagination=pagination)


@app.route('/test2')
def test2():
    stocks = readProfitsIncAdf()
    return render_template('test2.html', stocks=stocks)


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
    # return encode_utf8(html)
    return html


@app.route('/profitsinc/<ts_code>')
def profitsIncImg(ts_code):
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    # plotImg = PlotProfitsInc(ts_code,
    #                          startDate='20150331',
    #                          endDate='20191231')
    plotImg = PlotProfitsInc(ts_code)
    scripts, div = components(plotImg.plot())
    # return render_template("plotkline.html", the_div=div, the_script=scripts)
    html = render_template(
        'plotkline.html',
        plot_script=scripts,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
    )
    return html


@app.route('/indexinfo/<ID>')
def indexInfo(ID):
    stockItem = reportIndex(ID)
    return render_template('indexinfo.html',
                           stock=stockItem)


@app.route('/holdsetting', methods=["GET", "POST"])
def holdsetting():
    form = HoldForm(request.form)
    hold = form.data["selected"]
    if hold:
        holdList = hold.split('|')
        current_app.logger.debug(f'保存股票清单：{holdList}')
        writeChigu(holdList)

    return render_template("holdsetting.html", form=form)


@app.route('/holdjson', methods=["GET", "POST"])
def holdjson():
    stocks = readStockList()
    # stocks = stocks[:10]
    stocks.rename(columns={'ts_code': 'id'}, inplace=True)
    stocks['text'] = stocks.id + ' ' + stocks.name
    stocks['selected'] = False
    chigu = readChigu()
    stocks.loc[stocks.id.isin(chigu), 'selected'] = True
    # print(chigu)
    current_app.logger.debug(f'持有的股票: {chigu}')

    stocksList = stocks.to_json(orient='records', force_ascii=False)
    return stocksList
