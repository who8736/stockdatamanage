import os

import pandas as pd

from stockdatamanage.db.sqlrw import readLastTTMPEs
from ..config import datapath


def profits_inc_linear_adf():
    df_adf = pd.read_excel(os.path.join(datapath, 'profits_inc_adf.xlsx'))
    df_adf.set_index('ts_code', inplace=True)
    df_adf = df_adf[['name', 'mean', 'std', 'sharp', 'flag']]

    df_linear = pd.read_excel(os.path.join(cf.datapath,
                                           'profits_inc_linear.xlsx'))
    df_linear.set_index('ts_code', inplace=True)
    df_linear = df_linear[['intercept', 'coef', 'r2']]

    df_pe = readLastTTMPEs(df_adf.index)
    df_pe.set_index('ts_code', inplace=True)
    # df = pd.merge(df_adf, df_linear, how='left',
    #               left_on='ts_code', right_on='ts_code')
    df = pd.merge(df_adf, df_linear, left_index=True, right_index=True)
    # df = pd.merge(df, df_pe, how='left',
    #               left_on='ts_code', right_on='ts_code')
    df = pd.merge(df, df_pe, left_index=True, right_index=True)

    df.to_excel(os.path.join(cf.datapath, 'profits_inc_adf_linear.xlsx'))