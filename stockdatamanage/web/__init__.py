# -*- coding: utf-8 -*-
"""
Created on 2016年12月14日

@author: who8736
"""
import importlib
import sys

from flask import Flask
from flask_bootstrap import Bootstrap
from flask_wtf.csrf import CSRFProtect

# noinspection PyTypeChecker
importlib.reload(sys)
# sys.setdefaultencoding('utf-8')  # @UndefinedVariable

app = Flask(__name__)
app.config.from_object('stockdatamanage.config')
# app.config.from_object('stockdata')
app.debug = True
CSRFProtect(app)
Bootstrap(app)
# app.config['SECRET_KEY'] = 'you never guess'

from . import views  # @IgnorePep8
# 现在通过app.config["VAR_NAME"]，我们可以访问到对应的变量
