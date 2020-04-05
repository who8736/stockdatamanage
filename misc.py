# -*- coding: utf-8 -*-
'''
Created on 2017年10月26日

@author: who8736
'''
import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

from datatrans import lastQarterDate
# from download import DownloaderQuarter
# from sqlrw import readStockList
from sqlconn import engine


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
    if (ts_code[0] == '6'):
        flag = 'sh'
    else:
        flag = 'sz'
    return ('http://f10.eastmoney.com/f10_v2/CapitalStockStructure.aspx?'
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
    if (ts_code[0] == '6'):
        return 'sh.%s' % ts_code
    else:
        return 'sz.%s' % ts_code


def tsCode(ts_code):
    """ 转换股票代码格式为tushare.pro格式
        600000->600000.SH
        000651->000651.SZ
    """
    if (len(ts_code) == 6):
        return ts_code + ('.SH' if ts_code[0] == '6' else '.SZ')
    else:
        return ts_code


def checkQuarterData(ts_code):
    """
    按每日指标和利润表分别计算TTM利润，
    两者差异超过0.00001时表示需更新财务季报
    利润表应取最后一季，最后一季上年同期，上年年末计算得到profits_ttm
    :return:
    无利润表 返回1
    无每日指标 返回2
    利润表数据不全 返回3
    每日指标与利润表差异较大 返回4
    无差异 返回0
    """
    sql = f"""select end_date, n_income_attr_p from income 
                where ts_code="{ts_code}" 
                order by end_date desc limit 5;
            """
    df = pd.read_sql(sql, engine)
    if df.empty:
        return 1, None

    sql = f"""select total_mv/pe_ttm mvpettm from daily_basic
           where ts_code="{ts_code}" order by trade_date desc limit 1;
        """
    result = engine.execute(sql).fetchone()
    if result is None or result[0] is None:
        return 2, None
    mvpettm = result[0] * 10000

    # d1 最后一季日期
    # d2 上年末季日期
    # d3 最后一季上年同期日期
    # p1,p2,p3类似
    d1 = df.end_date.values[0]
    d2 = d1 - relativedelta(years=1, month=12, day=31)
    d3 = d1 - relativedelta(years=1)
    if (d2 not in df.end_date.values) or (d3 not in df.end_date.values):
        return 3, None
    p1 = df[df.end_date == d1].n_income_attr_p.values[0]
    p2 = df[df.end_date == d2].n_income_attr_p.values[0]
    p3 = df[df.end_date == d3].n_income_attr_p.values[0]
    profitsttm = p1 + p2 - p3
    div = abs(mvpettm / profitsttm - 1)
    # print(div)
    # return div > 0.00001
    if div > 0.00001:
        return 4, div
    else:
        return 0, div


if __name__ == '__main__':
    pass
