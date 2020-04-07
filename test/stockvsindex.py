# -*- coding: utf-8 -*-
# author:xx
# datetime:2020/4/7 9:07

def stockvsindex(ts_code, index_code, start_date, end_date):
    """比较一段时期内股票与指数的涨跌幅度
    20090804  起点，高位
    20140619  低位
    20150612  高位
    20160128  低位
    20180124  高位
    20190103  低位
    20190419  高位

    """
    pass


if __name__ == '__main__':
    ts_code = '000651.SZ'
    df = ts.pro_bar(ts_code=ts_code, adj='hfq')
    filename = f'data/{ts_code[:6]}hfq.xlsx'
    df.to_excel(filename)