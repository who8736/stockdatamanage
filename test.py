# -*- coding: utf-8 -*-
"""
Created on Mon Apr 15 15:27:38 2019

@author: ff
"""

#from sqlrw import 
import baostock as bs
import pandas as pd


def downGubenFromEastmoney():
    """ 从东方财富下载总股本变动数据
    url: 
    """
    pass
    stockID = '600000'
    startDate = '2019-04-01'
    bs.login()
    from misc import usrlGubenEastmoney
    from misc import urlGubenEastmoney
    urlGubenEastmoney('600000')
    gubenURL = urlGubenEastmoney(stockID)
    req = getreq(gubenURL)
    guben = urllib.request.urlopen(req).read()


def urlGuben(stockID):
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/stockid'
            '/%s/stocktype/TotalStock.phtml' % stockID)

def downLiutongGubenFromBaostock():
    """ 从baostock下载每日K线数据，并根据成交量与换手率计算流通总股本
    """
    code = 'sz.000651'
    startDate = '2019-03-01'
    endDate = '2019-04-15'
    fields = "date,code,close,volume,turn,peTTM,tradestatus"

    lg = bs.login()
    rs = bs.query_history_k_data_plus(code, fields, startDate, endDate)
    dataList = []
    while rs.next():
        dataList.append(rs.get_row_data())
    result = pd.DataFrame(dataList, columns=rs.fields)
    print(result)
#    lg.logout()
    bs.logout()
    return result

if __name__ == "__main__":
    """072497"""
    pass
    df = downLiutongGubenFromBaostock()
