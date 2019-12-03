# -*- coding: utf-8 -*-
'''
Created on 2016年12月14日

@author: who8736
'''
from web import app
import sys
# import sys
#
# reload(sys)
# sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
    app.run(host='0.0.0.0', threaded=True, debug=True)
    for i in sys.path:
        print(i)
