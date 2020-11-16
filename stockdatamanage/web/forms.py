# -*- coding: utf-8 -*-
'''
Created on 2017年1月14日

@author: who8736
'''
from flask_wtf import Form
from wtforms import StringField
from wtforms.validators import DataRequired


class StockListForm(Form):
    stockList = StringField('stockList', validators=[DataRequired()])
