#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/12/14 10:53
# software: PyCharm

"""
指数信息
"""
from flask import render_template, Blueprint

from stockdatamanage.analyse.report import reportIndex

indexinfo = Blueprint('indexinfo', __name__)

@indexinfo.route('/')
def indexInfo():
    ID = request.args.get('indexcode')
    stockItem = reportIndex(ID)
    return render_template('indexinfo.html',
                           stock=stockItem)
