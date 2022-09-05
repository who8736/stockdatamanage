# -*- coding: utf-8 -*-
"""
Created on 2017年10月26日

@author: who8736
"""

import datetime as dt
from datetime import timedelta

INDEXLIST = {
    '000001.SH': '上证综指',
    '000005.SH': '上证商业类指数',
    '000006.SH': '上证房地产指数',
    '000016.SH': '上证50',
    '000905.SH': '中证500',
    '399001.SZ': '深证成指',
    '399005.SZ': '中小板指',
    '399006.SZ': '创业板指',
    '399016.SZ': '深证创新',
    '399300.SZ': '沪深300',
}


# from download import DownloaderQuarter
# from sqlrw import readStockList


# def filenameGuben(ts_code):
#     return './data/guben/%s.csv' % ts_code
#
#
# def filenameLirun(ts_code):
#     filename = './data/ProfitStatement/%s.csv' % ts_code
#     return filename
#
#
# def filenameMainTable(ts_code, tableType):
#     filename = './data/%s/%s.csv' % (tableType, ts_code)
#     return filename
#
#
# def filenameGuzhi(ts_code):
#     return './data/guzhi/%s.xml' % ts_code
#
#
# def urlGuzhi(ts_code):
#     '''
#     估值数据文件下载地址
#     '''
#     url = 'http://f9.eastmoney.com/soft/gp72.php?code=%s' % ts_code
#     if ts_code[0] == '6':
#         url += '01'
#     else:
#         url += '02'
#     return url


# def urlGubenSina(ts_code):
#     """
#     股本数据地址，数据源：sina
#     """
#     return ('http://vip.stock.finance.sina.com.cn/corp/go.php'
#             '/vCI_StockStructureHistory/ts_code'
#             '/%s/stocktype/TotalStock.phtml' % ts_code)
#
#
# def urlGubenEastmoney(ts_code):
#     """
#     股本数据地址，数据源：eastmoney
#     """
#     if (ts_code[0] == '6'):
#         flag = 'sh'
#     else:
#         flag = 'sz'
#     return ('http://f10.eastmoney.com/f10_v2/CapitalStockStructure.aspx?'
#             'code=%(flag)s%(ts_code)s' % locals())
#
#
# def urlMainTable(ts_code, tableType):
#     url = ('http://money.finance.sina.com.cn/corp/go.php'
#            '/vDOWN_%(tableType)s/displaytype/4'
#            '/ts_code/%(ts_code)s/ctrl/all.phtml' % locals())
#     return url


def longts_code(ts_code):
    """ 转换股票代码格式为baostock格式
        600000->sh.600000
        000651->sz.000651
    """
    if ts_code[0] == '6':
        return 'sh.%s' % ts_code
    else:
        return 'sz.%s' % ts_code


def tsCode(ts_code):
    """ 转换股票代码格式为tushare.pro格式
        600000->600000.SH
        000651->000651.SZ
    """
    if len(ts_code) == 6:
        return ts_code + ('.SH' if ts_code[0] == '6' else '.SZ')
    else:
        return ts_code


def dayDelta(sorDate, days, dateformat='%Y%m%d'):
    """字符串形式返回增减天数后的日期

    :parameter

    """
    if isinstance(sorDate, dt.date):
        return (sorDate + timedelta(days=days)).strftime(dateformat)
    elif isinstance(sorDate, str):
        _sorDate = dt.datetime.strptime(sorDate, '%Y%m%d')
        return (_sorDate + timedelta(days=days)).strftime(dateformat)


if __name__ == '__main__':
    pass