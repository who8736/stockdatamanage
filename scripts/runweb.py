# -*- coding: utf-8 -*-
"""
Created on 2016年12月14日

@author: who8736
"""
from stockdatamanage import app
from stockdatamanage.util.initlog import initlog
import sys

if __name__ == '__main__':
    initlog()
    app.run(host='0.0.0.0', threaded=True, debug=True)
