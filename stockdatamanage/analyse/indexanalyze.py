# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/4/6 12:02
"""
主要股指成分股历年总体利润增长率
"""

import pandas as pd

from ..db import engine


def allIndexProInc():
    codes = ['000016.SH',
             '399300.SZ',
             '000905.SH']
    results = []
    for index_code in codes:
        r = dict(index_code=index_code)
        for year in range(2009, 2020):
            print(index_code, year)
            result = indexProfitsInc(index_code, year)
            # r['profits1'] = result[0]
            # r['profits2'] = result[1]
            r[f'y{year - 1}'] = round(result[1] / result[0] * 100 - 100, 2)
        results.append(r)
    df = pd.DataFrame(results)
    print(df)
    df.to_excel('../data/indexprofitsinc.xlsx')


def indexProfitsInc(index_code='399300.SZ', year=2019):
    ann_date = f'{year}0501'
    end_date = [f'{year - 2}1231', f'{year - 1}1231']

    results = []
    for edate in end_date:
        sql = f"""
                SELECT 
                    ROUND(SUM(n_income_attr_p) / 10000 / 10000, 2)
                FROM
                    income
                WHERE
                    ts_code IN (SELECT 
                            con_code
                        FROM
                            index_weight
                        WHERE
                            index_code = '{index_code}'
                                AND trade_date = (SELECT 
                                    MAX(trade_date)
                                FROM
                                    stockdata.index_weight
                                WHERE
                                    index_code = '{index_code}' and
                                    trade_date <= '{ann_date}'))
                        AND end_date = '{edate}';
            """
        results.append(engine.execute(sql).fetchone()[0])

    # print(results)
    # print(round(results[1] / results[0] * 100 - 100, 2))
    return results


if __name__ == '__main__':
    pass
    allIndexProInc()
    # result = indexProfitsInc('000905.SH', 2009)
    # print(result)
