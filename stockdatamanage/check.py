"""
检查和修复数据
"""
import os
import logging
import datetime as dt

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from .sqlconn import engine
from .sqlrw import readStockList, readCal
from .config import Config


# def checkGuben():
#     # ids = readts_codesFromSQL()
#     sql = 'select ts_code from klinestock'
#     # for ts_code in ids:


def checkQuarterData_del():
    """
    每支股票最后更新季报日期与每日指标中的最后日期计算的销售收入是否存在差异
    如有差异则说明需更新季报
    """
    sql = """
        select max(cal_date) from trade_cal 
        where exchange = 'SSE' and cal_date < (select max(ann_date) from income) 
            and is_open = 1;
            """
    startDate = engine.execute(sql).fetchone()[0].strftime('%Y%m%d')
    # print(startDate)
    sql = 'select max(trade_date) from daily_basic'
    endDate = engine.execute(sql).fetchone()[0].strftime('%Y%m%d')
    startDate = min(startDate, endDate)

    sql = f"""select a.ts_code, abs(a.tp/b.tp - 1) as cha from 
                (select ts_code, total_mv/ps tp 
                    from daily_basic where trade_date='{startDate}') a,
                (select ts_code, total_mv/ps tp 
                    from daily_basic where trade_date='{endDate}') b
                where a.ts_code=b.ts_code;"""
    df = pd.read_sql(sql, engine)

    sql = f"""select a.ts_code from income a,
                (select ts_code, max(end_date) edate from income 
                    group by ts_code) b,
                (select ts_code, total_mv/ps*10000 as rev from daily_basic 
                    where trade_date=
                        (select max(trade_date) from daily_basic)) c
                where a.ts_code=b.ts_code and a.end_date=b.edate 
                    and a.ts_code=c.ts_code and abs(a.revenue/c.rev-1)>0.0001;"""
    df1 = pd.read_sql(sql, engine)
    df.loc[df.ts_code.isin(df1.ts_code), 'cha'] = 1
    return df
    # print(df)
    # df = df[df.cha>0.001]
    # print(df)
    # return df.ts_code.values


def checkQuarterDataNew():
    """
    每支股票最后更新季报日期与每日指标中的最后日期计算的净资产是否存在差异
    如有差异则说明需更新季报
    如每日指标中无市净率的也应更新季报，因可能为新上市股票
    """
    end_date = '20200331'
    """
    查询balancesheet, cashflow, income, fina_indicator表
    报告期不同的，应更新报表
    报告期相同，则计算最后一期公告日的净利润，
        与前一日的净利润比较，有差异的更新报表
    如每日指标中无市净率的，根据当前日期计算需更新的报表
    """
    sql = f'''select ts_code, max(end_date) as e_date_balancesheet, 
                     max(ann_date) as a_date_balancesheet
                     from balancesheet group by ts_code'''
    df = pd.read_sql(sql, engine)

    sql = f'''select ts_code, max(end_date) as e_date_income, 
                     max(ann_date) as a_date_income
                     from income group by ts_code'''
    dftmp = pd.read_sql(sql, engine)
    df = df.merge(dftmp, how='outer', on='ts_code')

    sql = f'''select ts_code, max(end_date) as e_date_cashflow, 
                     max(ann_date) as a_date_cashflow
                     from cashflow group by ts_code'''
    dftmp = pd.read_sql(sql, engine)
    df = df.merge(dftmp, how='outer', on='ts_code')

    sql = f'''select ts_code, max(end_date) as e_date_fina_indicator, 
                     max(ann_date) as a_date_fina_indicator
                     from fina_indicator group by ts_code'''
    dftmp = pd.read_sql(sql, engine)
    df = df.merge(dftmp, how='outer', on='ts_code')

    # df['e_date']

    # sql = f'call checkquarterdata("{end_date}");'
    # df = pd.read_sql(sql, engine)
    # # print(df)
    # sql = f"""select ts_code from stock_basic where ts_code not in
    #             (select distinct ts_code from daily_basic
    #             where trade_date=(select max(trade_date) from daily_basic)
    #                 and pb is not null);
    #         """
    # df1 = pd.read_sql(sql, engine)
    # # print(df1)
    # df1['e_date'] = None
    # df1['networth_diff'] = 1
    # df = pd.concat([df, df1])
    # # df.reset_index(inplace=True)
    # # df = pd.merge(df, df1, how='left', on='ts_code')
    print(df)
    return df

def checkQuarterData():
    """
    每支股票最后更新季报日期与每日指标中的最后日期计算的净资产是否存在差异
    如有差异则说明需更新季报
    如每日指标中无市净率的也应更新季报，因可能为新上市股票
    """
    end_date = '20200331'
    sql = f'call checkquarterdata("{end_date}");'
    df = pd.read_sql(sql, engine)
    # print(df)
    sql = f"""select ts_code from stock_basic where ts_code not in
                (select distinct ts_code from daily_basic 
                where trade_date=(select max(trade_date) from daily_basic) 
                    and pb is not null);
            """
    df1 = pd.read_sql(sql, engine)
    # print(df1)
    df1['e_date'] = None
    df1['networth_diff'] = 1
    df = pd.concat([df, df1])
    # df.reset_index(inplace=True)
    # df = pd.merge(df, df1, how='left', on='ts_code')
    # print(df)
    return df


def checkClassifyMemberListdate():
    """校验行业分类清单中的股票是否存在未上市就已列入行业清单"""
    # cf = Config()
    # datapath = cf.datapath
    # for root, path, files in os.walk(datapath):
    #     print(root, path, files)

    sql = 'select ts_code, list_date from stock_basic'
    stocks = pd.read_sql(sql, engine)
    # print(stocks)

    dates = readCal('20200101', '20201101')
    # print(dates)
    for _date in dates:
        logging.debug(f'check classify_member list_date: {_date}')
        pass
        __date = dt.datetime.strptime(_date, '%Y%m%d').date()

        sql = f'''select ts_code from classify_member 
                    where date=(select max(date) from classify_member 
                        where date<="{_date}")
                '''
        members = pd.read_sql(sql, engine)
        # result = engine.execute(sql).fetchall()
        if members.empty:
            logging.warning(f'查询不到行业清单：{_date}')
            continue

        result = stocks[stocks.ts_code.isin(members.ts_code)
                        & (stocks.list_date > __date)]
        if not result.empty:
            logging.error(f'股票未上市时已列入行业清单：[code:{result}], [date:{_date}]')

def checkPath():
    cf = Config()
    if not os.path.isdir(cf.logpath):
        os.makedirs(cf.logpath)
    if not os.path.isdir(cf.datapath):
        os.makedirs(cf.datapath)
    linearpath = os.path.join(cf.datapath, 'linear_img')
    if not os.path.isdir(linearpath):
        os.makedirs(linearpath)

def repairQuarterData(stocks=None, startDate=None, endDate=None, replace=False):
    """修复指定报告期的股票季报数据

    TODO:

    :return:
    """
    if stocks is None:
        stocks = readStockList()

    for ts_code in stocks.index:
        downloader = DownloaderQuarter(ts_code=ts_code,
                                       startDate=startDate,
                                       endDate=startDate,
                                       replace=replace)
        downloader.run()
