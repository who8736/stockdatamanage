# -*- coding: utf-8 -*-
# author:xx
# datetime:2020/4/26 11:06
# 季报数据分析

from sqlrw import *


def profit_dedt(ts_code='600705.SH', startDate='20131231', endDate='20181231'):
    """扣非净利润增长水平"""
    sql = f"""select a.ts_code, c.name, 
                a.profit_dedt profita, b.profit_dedt profitb, 
                round(b.profit_dedt/a.profit_dedt, 2) inc from 
                (select ts_code, profit_dedt 
                    from fina_indicator where end_date='{startDate}') a, 
                (select ts_code, profit_dedt 
                    from fina_indicator where end_date='{endDate}') b,
                (select ts_code, name from stock_basic) c
                where a.ts_code=b.ts_code and a.profit_dedt>0 
                    and a.ts_code=c.ts_code and b.profit_dedt/a.profit_dedt>2
                order by inc desc;
            """
    df = pd.read_sql(sql, engine)
    print(df)


def check_income():
    sql = """
            select ts_code, total_revenue, revenue,
                total_cogs, oper_cost,
                sell_exp, admin_exp, fin_exp,
                oper_exp, operate_profit,
                non_oper_income, non_oper_exp,
                total_profit
            from income
            where end_date='20181231'
            ;
    """
    df = pd.read_sql(sql, engine)
    df.to_excel('../data/checkincome.xlsx')


if __name__ == '__main__':
    pass
    # profit_dedt()
    check_income()
