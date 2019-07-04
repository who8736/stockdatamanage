# -*- coding: utf-8 -*-
import os

from sqlconn import sqlconn, engine


def export():
    """
    导出数据表结构和数据
    :return:
    """
    # 文件目标路径，如果不存在，新建一个
    exportPath = os.path.join(os.path.abspath('.'), 'exportdata')
    # print(exportPath)
    if not os.path.exists(exportPath):
        os.mkdir(exportPath)

    ip = sqlconn.ip
    user = sqlconn.user
    password = sqlconn.password
    database = sqlconn.database
    sql = 'SET GLOBAL local_infile = "ON";'
    engine.execute(sql)

    # 导出存储过程
    sql = (f'mysqldump -ntd -R -h{ip} -u{user} -p{password} '
           f'"{database}" > "{exportPath}/procedure.sql"')
    os.system(sql)

    sql = 'show tables;'
    res = engine.execute(sql)
    if res is None:
        return
    tables = [name[0] for name in res.fetchall()]
    # 保存表名到文件
    filename = os.path.join(exportPath, "tablename.txt")
    ofile = open(filename, 'w')
    ofile.write('\n'.join(tables))

    for table in tables:
        print('start proceses:', table)
        tableFileName = os.path.join(exportPath, f'{table}_struct.sql')
        tableFileName = tableFileName.replace('\\', '\\\\')
        sql = (f'mysqldump --no-data -h{ip} -u{user} -p{password} '
               f'{database} {table} > "{tableFileName}"')
        result = os.system(sql)
        # print('export sql: ', sql)
        # print('result:', result)
        if result:
            print(f'export {table}s struct fail')
            continue
        filename = os.path.join(exportPath, f'{table}.csv')
        filename = filename.replace('\\', '\\\\')
        if os.path.isfile(filename):
            os.remove(filename)
        # print('csv filename: ', filename)
        # return
        sql = (f'select * into outfile "{filename}" '
               f'FIELDS TERMINATED BY "," OPTIONALLY ENCLOSED BY "`" '
               f'ESCAPED BY "#" LINES TERMINATED BY "\\n" '
               f'from {table} ')
        # print('export sql: ', sql)
        res = engine.execute(sql)
        # print('export result: ', repr(res))
        if res:
            print(f'export {table} data ok')
        else:
            print(f'export {table} data fail')


def importData():
    """
    导入结构和数据
    :return:
    """
    exportPath = os.path.join(os.path.abspath('.'), 'exportdata')
    ip = sqlconn.ip
    user = sqlconn.user
    password = sqlconn.password
    database = sqlconn.database

    sql = 'SET GLOBAL local_infile = "ON";'
    engine.execute(sql)

    # 删除库中所有表
    sql = 'show tables;'
    res = engine.execute(sql)
    if res is None:
        return
    tables = [name[0] for name in res.fetchall()]
    for table in tables:
        sql = f'drop table if exists {table}'
        engine.execute(sql)
    # return

    # 导入存储过程
    sql = (f'mysql -h{ip} -u{user} -p{password} '
           f'{database} < "{exportPath}/procedure.sql"')
    os.system(sql)

    filename = os.path.join(exportPath, "tablename.txt")
    tables = open(filename, 'r').readlines()
    for table in tables:
        table = table.strip('\n')
        filename = os.path.join(exportPath, f'{table}_struct.sql')
        # filename = filename.replace('\\', '\\\\')
        print('start proceses:', filename)
        sql = (f'mysql -h{ip} -u{user} -p{password} '
               f'{database} < "{filename}"')
        # print('import struct: ', sql)
        os.system(sql)

        filename = os.path.join(exportPath, f'{table}.csv')
        filename = filename.replace('\\', '\\\\')
        sql = f'alter table {table} disable keys'
        engine.execute(sql)
        sql = (f'LOAD DATA LOCAL INFILE "{filename}" '
               f'REPLACE INTO TABLE {table} '
               f'FIELDS TERMINATED BY "," '
               f'ESCAPED BY "#" ENCLOSED BY "`" '
               f'LINES TERMINATED BY "\\n"')
        engine.execute(sql)
        sql = f'alter table {table} enable keys'
        engine.execute(sql)


if __name__ == '__main__':
    # export()
    importData()
