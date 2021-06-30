"""
检查和修复数据
"""
import datetime as dt
import logging
import os

import pandas as pd

from ..config import DATAPATH, LOGPATH
from ..db import engine
from ..db.sqlrw import readCal
from ..downloader.downloadtushare import DownloaderQuarterTushare
from ..util.datatrans import lastQuarter, quarterList


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


def checkQuarterData():
    """
    每支股票最后更新季报日期与每日指标中的最后日期计算的净资产是否存在差异
    如有差异则说明需更新季报
    如每日指标中无市净率的也应更新季报，因可能为新上市股票
    """
    # end_date = '20200331'
    end_date = lastQuarter(dt.date.today())
    sql = f'call checkquarterdata("{end_date}");'
    df = pd.read_sql(sql, engine)
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
    """

    """
    if not os.path.isdir(LOGPATH):
        os.makedirs(LOGPATH)
    if not os.path.isdir(DATAPATH):
        os.makedirs(DATAPATH)
    linearpath = os.path.join(DATAPATH, 'linear_img')
    if not os.path.isdir(linearpath):
        os.makedirs(linearpath)


def repairLost(startDate, endDate):
    """修复缺失的股票季报数据
    1. 某股票上市后缺某季报表，如资产负债表等
    2. 财务指标中缺基本每股收益或扣非净利润则视为财务指标缺失

    :return:

    Parameters
    ----------
    startDate : str
    endDate : str
    replace : bool
    """
    # if stocks is None:
    #     stocks = readStockList()

    tables = ['balancesheet', 'income', 'cashflow', 'fina_indicator']
    dates = quarterList(startDate, endDate)
    for _date in dates:
        # print(_date)
        sql = f'select ts_code from stock_basic where list_date<="{_date}"'
        stocks = pd.read_sql(sql, engine)
        for table in tables:
            sql = f'select ts_code from {table} where end_date="{_date}"'
            df = pd.read_sql(sql, engine)
            lost = stocks[~stocks.ts_code.isin(df.ts_code)]
            if table == 'fina_indicator':
                sql = f'''select ts_code from fina_indicator
                            where end_date="{_date}" 
                                and (profit_dedt is null or eps is null);'''
                df = pd.read_sql(sql, engine)
                lost = pd.concat([lost, df])
                lost.drop_duplicates(['ts_code'])
            if table == 'income':
                sql = f'''select ts_code from income
                            where end_date="{_date}" 
                                and (n_income is null);'''
                df = pd.read_sql(sql, engine)
                lost = pd.concat([lost, df])
                lost.drop_duplicates(['ts_code'])
            for code in lost.ts_code.values:
                print(_date, table, code)
                downloader = DownloaderQuarterTushare(ts_code=code,
                                                      tables=[table],
                                                      period=_date,
                                                      replace=True)
                downloader.run()
