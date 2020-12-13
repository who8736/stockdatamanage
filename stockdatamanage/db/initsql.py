# -*- coding: utf-8 -*-
"""
Created on 2016年11月30日

@author: who8736
"""
import pandas as pd

from .sqlconn import engine


# from sqlrw import readts_codesFromSQL


def existTable(tablename):
    sql = 'show tables like "%s"' % tablename
    result = engine.execute(sql)
    return False if result.rowcount == 0 else True


def createTable():
    """读取createtable文件夹tablename.xlsx文件中的表名，
    根据相应表格格式的xlsx文件创建mysql表格

    :return:
    """
    # 读表名
    tablenameDf = pd.read_excel('database_struct/tablename.xlsx')
    tablenames = tablenameDf['table']

    # 读类型对应关系
    # typedf = pd.read_excel('database_struct/typetrans.xlsx')
    # typedf.set_index('tusharetype', inplace=True)

    for tablename in tablenames:
        if existTable(tablename):
            continue
        # 读字段名
        df = pd.read_excel(f'database_struct/{tablename}.xlsx')
        # 表头
        sql = f'CREATE TABLE `{tablename}` ('
        # 字段
        flag = False
        for _, row in df.iterrows():
            rowList = row.to_list()
            field = rowList[0]
            dataType = rowList[1]
            if flag:
                sql += ', '
            else:
                flag = True
            sql += f'`{field}` {dataType} DEFAULT NULL'
        # 表尾
        sql += ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;'
        print(sql)

        engine.execute(sql)
