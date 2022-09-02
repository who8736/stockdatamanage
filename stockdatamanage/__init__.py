# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/16 12:28
import importlib
import sys

from flask import Flask, render_template
from flask_bootstrap import Bootstrap4
from flask_wtf.csrf import CSRFProtect

from .views import misc
from .views import settings
from .views.ajax_data import ajax_data
from .views.ajax_img import ajax_img
from .views.classify import classify
from .views.indexinfo import indexinfo
from .views.valuation import valuation

importlib.reload(sys)

app = Flask(__name__)
app.config.from_object('stockdatamanage.config')
app.debug = True
CSRFProtect(app)
Bootstrap4(app)

app.register_blueprint(ajax_data, url_prefix='/ajax_data')
app.register_blueprint(ajax_img, url_prefix='/ajax_img')
app.register_blueprint(classify, url_prefix='/classify')
app.register_blueprint(indexinfo, url_prefix='/indexinfo')
app.register_blueprint(misc, url_prefix='/misc')
app.register_blueprint(settings, url_prefix='/settings')
app.register_blueprint(valuation, url_prefix='/valuation')


@app.route('/')
def index():
    return render_template('index.html')
