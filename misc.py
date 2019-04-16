# -*- coding: utf-8 -*-
'''
Created on 2017年10月26日

@author: who8736
'''


def filenameGuben(stockID):
    return './data/guben/%s.csv' % stockID


def filenameLirun(stockID):
    filename = './data/ProfitStatement/%s.csv' % stockID
    return filename


def filenameMainTable(stockID, tableType):
    filename = './data/%s/%s.csv' % (tableType, stockID)
    return filename


def filenameGuzhi(stockID):
    return './data/guzhi/%s.xml' % stockID


def urlGuzhi(stockID):
    '''
    估值数据文件下载地址
    '''
    url = 'http://f9.eastmoney.com/soft/gp72.php?code=%s' % stockID
    if stockID[0] == '6':
        url += '01'
    else:
        url += '02'
    return url


def urlGuben(stockID):
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/stockid'
            '/%s/stocktype/TotalStock.phtml' % stockID)


def urlMainTable(stockID, tableType):
    url = ('http://money.finance.sina.com.cn/corp/go.php'
           '/vDOWN_%(tableType)s/displaytype/4'
           '/stockid/%(stockID)s/ctrl/all.phtml' % locals())
    return url


def longStockID(stockID):
    if(len(stockID) == 9):
        return stockID
    if(stockID[0] == '6'):
        return 'sh.%s' % stockID
    else:
        return 'sz.%s' % stockID

if __name__ == '__main__':
    pass
