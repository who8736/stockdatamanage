import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta

from sqlconn import engine
from sqlrw import readStockList


def checkGuben():
    # ids = readts_codesFromSQL()
    sql = 'select ts_code from klinestock'
    # for ts_code in ids:


def checkQuarterData():
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
                    and a.ts_code=c.ts_code and (a.revenue/c.rev-1)>0.0001;"""
    df1 = pd.read_sql(sql, engine)
    df.loc[df.ts_code.isin(df1.ts_code), 'cha'] = 1
    return df
    # print(df)
    # df = df[df.cha>0.001]
    # print(df)
    # return df.ts_code.values


if __name__ == '__main__':
    pass
    checkQuarterData()
