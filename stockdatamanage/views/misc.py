#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/12/14 10:50
# software: PyCharm
from flask import redirect, render_template, request, url_for, Blueprint
from flask_paginate import Pagination, get_page_parameter

from stockdatamanage.db.sqlrw import (
    readChigu, readProfitsIncAdf,
    readStockList, writeHold,
)
from stockdatamanage.util.misc import tsCode
from stockdatamanage.views.forms import StockListForm


misc = Blueprint('misc', __name__)

@misc.route('/test_table')
def test_table():
    data = list(range(5))
    # q = request.args.get('q')
    # if q:
    #     search = True

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


@misc.route('/test2')
def test2():
    stocks = readProfitsIncAdf()
    return render_template('test2.html', stocks=stocks)


@misc.route('/stocklist', methods=["GET", "POST"])
def del_setStockList():
    """废弃， 用holdsetting代替"""
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
            writeHold(chigu)
            return redirect(url_for('index'))
    return render_template('stocklist.html',
                           form=form,
                           stockListStr=chiguStr)
