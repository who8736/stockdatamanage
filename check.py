# from sqlrw
import pandas as pd
from dateutil.relativedelta import relativedelta

from sqlconn import engine


def checkGuben():
    # ids = readts_codesFromSQL()
    sql = 'select ts_code from klinestock'
    # for ts_code in ids:


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