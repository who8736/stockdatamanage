# -*- coding: utf-8 -*-
"""
Created on 2018年1月24日

@author: who8736
"""

import numpy as np
import jpush

from sqlrw import readValuationSammary, getChiguList


def readChiguInfo():
    stocksDf = readValuationSammary()
    stocksDf = stocksDf[stocksDf['ts_code'].isin(getChiguList())]
    stockReportList = np.array(stocksDf).tolist()
#     print stockReportList
#     msg = ''
#     msg = ';'.join(['|'.join([i[0], i[1], i[5], i[6]])
#                     for i in stockReportList])

    msgList = []
    # noinspection PyTypeChecker
    for stock in stockReportList:
        #         stockmsgs
        stockmsg = '%s%s,%s,%s' % (stock[0], stock[1], stock[5], stock[6])
        msgList.append(stockmsg)

    msg = '\n' + '\n'.join(msgList)
    print(msg)
    return msg


if __name__ == '__main__':
    msg = readChiguInfo()
    app_key = '67219dcea4186826a5d7dbd1'
    master_secret = '6ebebd37f18c1c12eed49212'
    _jpush = jpush.JPush(app_key, master_secret)
    push = _jpush.create_push()
    # if you set the logging level to "DEBUG",it will show the debug logging.
    _jpush.set_logging("DEBUG")
    push.audience = jpush.all_
    push.notification = jpush.notification(alert=msg)
    push.platform = jpush.all_
    response = push.send()
#     try:
#         response = push.send()
#     except common.Unauthorized:
#         raise common.Unauthorized("Unauthorized")
#     except common.APIConnectionException:
#         raise common.APIConnectionException("conn error")
#     except common.JPushFailure:
#         print ("JPushFailure")
#     except:
#         print ("Exception")
