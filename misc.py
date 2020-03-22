# -*- coding: utf-8 -*-
'''
Created on 2017年10月26日

@author: who8736
'''


def filenameGuben(ts_code):
    return './data/guben/%s.csv' % ts_code


def filenameLirun(ts_code):
    filename = './data/ProfitStatement/%s.csv' % ts_code
    return filename


def filenameMainTable(ts_code, tableType):
    filename = './data/%s/%s.csv' % (tableType, ts_code)
    return filename


def filenameGuzhi(ts_code):
    return './data/guzhi/%s.xml' % ts_code


def urlGuzhi(ts_code):
    '''
    估值数据文件下载地址
    '''
    url = 'http://f9.eastmoney.com/soft/gp72.php?code=%s' % ts_code
    if ts_code[0] == '6':
        url += '01'
    else:
        url += '02'
    return url


def urlGubenSina(ts_code):
    """
    股本数据地址，数据源：sina
    """
    return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
            '/vCI_StockStructureHistory/ts_code'
            '/%s/stocktype/TotalStock.phtml' % ts_code)

def urlGubenEastmoney(ts_code):
    """
    股本数据地址，数据源：eastmoney
    """
    if(ts_code[0] == '6'):
        flag = 'sh'
    else:
        flag = 'sz'
    return('http://f10.eastmoney.com/f10_v2/CapitalStockStructure.aspx?'
           'code=%(flag)s%(ts_code)s' % locals())

def urlMainTable(ts_code, tableType):
    url = ('http://money.finance.sina.com.cn/corp/go.php'
           '/vDOWN_%(tableType)s/displaytype/4'
           '/ts_code/%(ts_code)s/ctrl/all.phtml' % locals())
    return url


def longts_code(ts_code):
    """ 转换股票代码格式为baostock格式
        600000->sh.600000
        000651->sz.000651
    """
    if(ts_code[0] == '6'):
        return 'sh.%s' % ts_code
    else:
        return 'sz.%s' % ts_code

def tsCode(ts_code):
    """ 转换股票代码格式为tushare.pro格式
        600000->600000.SH
        000651->000651.SZ
    """
    if(len(ts_code) == 6):
        return ts_code + ('.SH' if ts_code[0] == '6' else '.SZ')
    else:
        return ts_code

if __name__ == '__main__':
    pass
