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


def urlGubenSina(stockID):
    """
    股本数据地址，数据源：sina
    """
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/stockid'
            '/%s/stocktype/TotalStock.phtml' % stockID)

def urlGubenEastmoney(stockID):
    """
    股本数据地址，数据源：eastmoney
    """
    if(stockID[0] == '6'):
        flag = 'sh'
    else:
        flag = 'sz'
    return('http://f10.eastmoney.com/f10_v2/CapitalStockStructure.aspx?'
           f'code={flag}{stockID}')

def urlMainTable(stockID, tableType):
    url = ('http://money.finance.sina.com.cn/corp/go.php'
           f'/vDOWN_{tableType}/displaytype/4'
           f'/stockid/{stockID}/ctrl/all.phtml')
    return url


def longStockID(stockID):
    """ 转换股票代码格式为baostock格式
        600000->sh.600000
        000651->sz.000651
    """
    if(stockID[0] == '6'):
        return 'sh.%s' % stockID
    else:
        return 'sz.%s' % stockID

def tsCode(stockID):
    """ 转换股票代码格式为tushare.pro格式
        600000->600000.SH
        000651->000651.SZ
    """
    if(len(stockID) == 9):
        return stockID
    if(stockID[0] == '6'):
        return '%s.SH' % stockID
    else:
        return '%s.SZ' % stockID

if __name__ == '__main__':
    pass
