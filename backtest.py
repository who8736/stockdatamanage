# -*- coding: utf-8 -*-
"""
Created on Mon Mar 25 10:22:14 2019

@author: ff
"""

import pandas as pd
import sqlrw
import baostock as bs

def histTTMPETrend(stockID, histLen=1000):
    """返回某股票PE水平
    histLen为参照时间长度，如当前TTMPE与前1000个交易日的TTMPE比较
    取值0-100
    为0时表示当前TTMPE处于历史最低位
    为100时表示当前TTMPE处于历史最高位
    """
#    添加一列记录pe1000
#    遍历1001至2000号记录， 计算每一天的pe1000
#    写入pe1000
#    ttmpe1.iat[0, 0] = 10
    
#    绘图， 调整双坐标系
    stockID = '000651'
    ttmpe = sqlrw.readTTMPE(stockID)
#    ttmpe = ttmpe[-15:]
    ttmpe = ttmpe.set_index('date')
    ttmpe['pe1000'] = 0
#    print(ttmpe)
    
#    histLen = 10
    for cur in range(histLen, len(ttmpe)):
        begin = cur - histLen
        tmp = ttmpe[begin:cur]
        tmp = tmp[tmp.ttmpe <= ttmpe.iloc[cur, 0]]
#        print('-----------------')
#        print(tmp)
#        print('cur ttmpe: %d' % ttmpe.iloc[cur, 0])
#        print(begin, cur, len(tmp))
        ttmpe.iloc[cur, 1] = len(tmp) / histLen * 100
    
    return ttmpe
        
def downHFQ(stockID):
    lg = bs.login()
    print 
    if lg.error_code != '0':
        return None
    if stockID[0] == "6":
        stockID = "sh." + stockID
    else:
        stockID = "sz." + stockID
        
    rs =  bs.query_history_k_data_plus(stockID,
    "date,close",
    frequency="d", adjustflag="3")
    reList = []
    while rs.next():
        reList.append(rs.get_row_data())
    result = pd.DataFrame(reList, columns=rs.fields)
    result = result.set_index('date')
    bs.logout()
    return result
        
if __name__ == '__main__':
    stockID = '000651'
    histLen = 200
#    ttmpe = histTTMPETrend(stockID, histLen)
#    print(ttmpe)
    
    histClose = downHFQ(stockID)
    