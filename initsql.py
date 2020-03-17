# -*- coding: utf-8 -*-
"""
Created on 2016年11月30日

@author: who8736
"""
import pandas as pd

import sqlrw
from sqlconn import engine
# from sqlrw import readts_codesFromSQL


def existTable(tablename):
    sql = 'show tables like "%s"' % tablename
    result = engine.execute(sql)
    return False if result.rowcount == 0 else True


def del_dropKlineTable():
    stockList = sqlrw.readStockList()
    for ts_code in stockList:
        tablename = 'kline%s' % ts_code
        print(tablename)
        if existTable(tablename):
            sql = 'drop table %s;' % tablename
            engine.execute(sql)


def createChiguGuzhiTable():
    sql = ('CREATE TABLE chiguguzhi('
           'ts_code VARCHAR(6),'
           'name VARCHAR(40),'
           'pe DOUBLE,'
           'peg DOUBLE,'
           'next1YearPE DOUBLE,'
           'next2YearPE DOUBLE,'
           'next3YearPE DOUBLE,'
           'incrate0 DOUBLE,'
           'incrate1 DOUBLE,'
           'incrate2 DOUBLE,'
           'incrate3 DOUBLE,'
           'incrate4 DOUBLE,'
           'incrate5 DOUBLE,'
           'avgrate DOUBLE,'
           'madrate DOUBLE,'
           'stdrate DOUBLE,'
           'pe200 DOUBLE,'
           'pe1000 DOUBLE,'
           'PRIMARY KEY ( ts_code )); ')
    result = engine.execute(sql)
    return result


def createHY():
    sql = ('CREATE TABLE hangyestock('
           'ts_code VARCHAR(6),'
           'hyid VARCHAR(8),'
           'PRIMARY KEY ( ts_code ),'
           'KEY `hyid` (`hyid`)); ')
    result = engine.execute(sql)
    return result


def createHYName():
    sql = ('CREATE TABLE hangyename('
           'hyid VARCHAR(8),'
           'hyname VARCHAR(50),'
           'hylevel INT,'
           'hylevel1id VARCHAR(2),'
           'hylevel2id VARCHAR(4),'
           'hylevel3id VARCHAR(6),'
           'PRIMARY KEY ( hyid ),'
           'KEY `hyid` (`hyid`),'
           'KEY `hylevel1id` (`hylevel1id`),'
           'KEY `hylevel2id` (`hylevel2id`),'
           'KEY `hylevel3id` (`hylevel3id`)); ')
    result = engine.execute(sql)
    return result


def createYouzhiGuzhiTable():
    sql = 'CREATE TABLE youzhiguzhi like chiguguzhi;'
    result = engine.execute(sql)
    return result


def createGuzhiResultTable():
    sql = 'CREATE TABLE guzhiresult like chiguguzhi;'
    result = engine.execute(sql)
    return result


def createTTMPETable(tablename):
    sql = 'create table %s like ttmpesimple' % tablename
    result = engine.execute(sql)
    return result


def createGuzhiTable():
    sql = ('CREATE TABLE guzhi('
           'ts_code VARCHAR(6),'
           'peg DOUBLE,'
           'next1YearPE DOUBLE,'
           'next2YearPE DOUBLE,'
           'next3YearPE DOUBLE,'
           'PRIMARY KEY ( ts_code )); ')
    result = engine.execute(sql)
    return result


def createHYProfitsTable():
    sql = ('CREATE TABLE classify_profits('
           'hyid VARCHAR(8),'
           'date INT(11),'
           'profitsInc DOUBLE,'
           'profitsIncRate DOUBLE,'
           'PRIMARY KEY ( hyid, date)); ')
    result = engine.execute(sql)
    return result


def createGuzhiHistoryStatusTable():
    """ 各字段定义 ：
    integrity： BOOL类型, 过去3年数据是否完整
    seculargrowth： BOOL类型, 是否保持持续增长，当某季TTM利润增长率为负时，该值为否
    growthmadrate： FLOAT类型, 利润增长平均离差率
    """
    sql = ('CREATE TABLE guzhihistorystatus('
           'ts_code VARCHAR(6),'
           'date INT(11),'
           'integrity BOOL,'
           'seculargrowth BOOL,'
           'growthmadrate FLOAT,'
           'averageincrement FLOAT,'
           'PRIMARY KEY ( ts_code, date)); ')
    result = engine.execute(sql)
    return result


def createPELirunIncreaseTable():
    """ 各字段定义 ：
    integrity： BOOL类型, 过去3年数据是否完整
    seculargrowth： BOOL类型, 是否保持持续增长，当某季TTM利润增长率为负时，该值为否
    growthmadrate： FLOAT类型, 利润增长平均离差率
    """
    sql = ('CREATE TABLE pelirunincrease('
           'ts_code VARCHAR(6),'
           'date DATE,'
           'pe FLOAT,'
           'lirunincrease FLOAT,'
           'PRIMARY KEY (date, ts_code)); ')
    result = engine.execute(sql)
    return result


def createGubenTable():
    sql = ("CREATE TABLE `guben` ("
           "`ts_code` varchar(6) NOT NULL,"
           " `date` date NOT NULL,"
           "  `totalshares` double DEFAULT NULL,"
           "  PRIMARY KEY (`ts_code`,`date`),"
           "  KEY `ix_guben_ts_code` (`ts_code`),"
           "  KEY `ix_guben_date` (`date`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
           )
    result = engine.execute(sql)
    return result


def createChiguTable():
    sql = 'CREATE TABLE chigu(ts_code VARCHAR(6),PRIMARY KEY (ts_code));'
    return engine.execute(sql)


# def createKlineTable(ts_code):
#     tableName = 'kline%s' % ts_code
#     sql = 'create table %s like klinesample' % tableName
#     return engine.execute(sql)


def createStocklist():
    sql = ("CREATE TABLE `stocklist` ("
           "`ts_code` varchar(6) NOT NULL,"
           "`name` varchar(20) DEFAULT NULL,"
           "`industry` varchar(20) DEFAULT NULL,"
           "`area` varchar(20) DEFAULT NULL,"
           "`pe` double DEFAULT NULL,"
           "`outstanding` double DEFAULT NULL,"
           "`totals` double DEFAULT NULL,"
           "`totalAssets` double DEFAULT NULL,"
           "`liquidAssets` double DEFAULT NULL,"
           "`fixedAssets` double DEFAULT NULL,"
           "`reserved` double DEFAULT NULL,"
           "`reservedPerShare` double DEFAULT NULL,"
           "`esp` double DEFAULT NULL,"
           "`bvps` double DEFAULT NULL,"
           "`pb` double DEFAULT NULL,"
           "`timeToMarket` bigint(20) DEFAULT NULL,"
           "`undp` double DEFAULT NULL,"
           "`perundp` double DEFAULT NULL,"
           "`rev` double DEFAULT NULL,"
           "`profit` double DEFAULT NULL,"
           "`gpr` double DEFAULT NULL,"
           "`npr` double DEFAULT NULL,"
           "`holders` double DEFAULT NULL,"
           "PRIMARY KEY (`ts_code`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
           )
    result = engine.execute(sql)
    return result


def createPEHistory():
    sql = ("CREATE TABLE `pehistory` ("
           "`name` varchar(45) NOT NULL,"
           "`date` date NOT NULL,"
           "`pe` decimal(10,2) NOT NULL,"
           "PRIMARY KEY (`name`,`date`),"
           "KEY `ix_name` (`name`),"
           "KEY `ix_date` (`date`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
           )
    result = engine.execute(sql)
    return result


def createIndexKline():
    sql = ("CREATE TABLE `indexkline` ("
           "`ts_code` varchar(9) COLLATE utf8mb4_bin NOT NULL,"
           "`date` date NOT NULL,"
           "`close` double DEFAULT NULL,"
           "`open` double DEFAULT NULL,"
           "`high` double DEFAULT NULL,"
           "`low` double DEFAULT NULL,"
           "`pre_close` double DEFAULT NULL,"
           "`change` double DEFAULT NULL,"
           "`pct_chg` double DEFAULT NULL,"
           "`vol` double DEFAULT NULL,"
           "`amount` double DEFAULT NULL,"
           "PRIMARY KEY (`ts_code`,`date`),"
           "KEY `ix_ts_code` (`ts_code`) /*!80000 INVISIBLE */,"
           "KEY `ix_date` (`date`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
           )
    result = engine.execute(sql)
    return result


def createHangyePE():
    sql = ("""CREATE TABLE `hangyepe` (
            `hyid` varchar(8) NOT NULL,
            `date` date NOT NULL,
            `hype` float DEFAULT NULL,
            PRIMARY KEY (`hyid`,`date`),
            KEY `index_date` (`date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            /*!50100 PARTITION BY RANGE (year(`date`))
            (PARTITION p00 VALUES LESS THAN (2000) ENGINE = InnoDB,
            PARTITION p01 VALUES LESS THAN (2001) ENGINE = InnoDB,
            PARTITION p02 VALUES LESS THAN (2002) ENGINE = InnoDB,
            PARTITION p03 VALUES LESS THAN (2003) ENGINE = InnoDB,
            PARTITION p04 VALUES LESS THAN (2004) ENGINE = InnoDB,
            PARTITION p05 VALUES LESS THAN (2005) ENGINE = InnoDB,
            PARTITION p06 VALUES LESS THAN (2006) ENGINE = InnoDB,
            PARTITION p07 VALUES LESS THAN (2007) ENGINE = InnoDB,
            PARTITION p08 VALUES LESS THAN (2008) ENGINE = InnoDB,
            PARTITION p09 VALUES LESS THAN (2009) ENGINE = InnoDB,
            PARTITION p10 VALUES LESS THAN (2010) ENGINE = InnoDB,
            PARTITION p11 VALUES LESS THAN (2011) ENGINE = InnoDB,
            PARTITION p12 VALUES LESS THAN (2012) ENGINE = InnoDB,
            PARTITION p13 VALUES LESS THAN (2013) ENGINE = InnoDB,
            PARTITION p14 VALUES LESS THAN (2014) ENGINE = InnoDB,
            PARTITION p15 VALUES LESS THAN (2015) ENGINE = InnoDB,
            PARTITION p16 VALUES LESS THAN (2016) ENGINE = InnoDB,
            PARTITION p17 VALUES LESS THAN (2017) ENGINE = InnoDB,
            PARTITION p18 VALUES LESS THAN (2018) ENGINE = InnoDB,
            PARTITION p19 VALUES LESS THAN (2019) ENGINE = InnoDB,
            PARTITION p20 VALUES LESS THAN (2020) ENGINE = InnoDB,
            PARTITION p21 VALUES LESS THAN (2021) ENGINE = InnoDB,
            PARTITION p22 VALUES LESS THAN (2022) ENGINE = InnoDB,
            PARTITION p23 VALUES LESS THAN (2023) ENGINE = InnoDB,
            PARTITION p24 VALUES LESS THAN (2024) ENGINE = InnoDB,
            PARTITION p25 VALUES LESS THAN (2025) ENGINE = InnoDB,
            PARTITION p26 VALUES LESS THAN (2026) ENGINE = InnoDB,
            PARTITION pnow VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
            """)
    result = engine.execute(sql)
    return result


def createValuation():
    sql = ("""CREATE TABLE `valuation` (
            `ts_code` varchar(6) NOT NULL,
            `date` date NOT NULL,
            `name` varchar(20) NOT NULL,
            `pf` int(11) DEFAULT NULL,
            `pe` float DEFAULT NULL,
            `lowpe` int(11) DEFAULT NULL,
            `hyid` varchar(8) DEFAULT NULL,
            `hype` float DEFAULT NULL,
            `lowhype` int(11) DEFAULT NULL,
            `incrate0` float DEFAULT NULL,
            `incrate1` float DEFAULT NULL,
            `incrate2` float DEFAULT NULL,
            `incrate3` float DEFAULT NULL,
            `incrate4` float DEFAULT NULL,
            `incrate5` float DEFAULT NULL,
            `avg` float DEFAULT NULL,
            `std` float DEFAULT NULL,
            `peg` float DEFAULT NULL,
            `lowpeg` int(11) DEFAULT NULL,
            `wdzz` int(11) DEFAULT NULL,
            `wdzz1` int(11) DEFAULT NULL,
            `pez200` float DEFAULT NULL,
            `lowpez200` int(11) DEFAULT NULL,
            `pez1000` float DEFAULT NULL,
            `lowpez1000` int(11) DEFAULT NULL,
            `pe200` int(11) DEFAULT NULL,
            `pe1000` int(11) DEFAULT NULL,
            PRIMARY KEY (`ts_code`,`date`),
            KEY `index_date` (`date`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            /*!50100 PARTITION BY RANGE (year(`date`))
            (PARTITION p00 VALUES LESS THAN (2000) ENGINE = InnoDB,
            PARTITION p01 VALUES LESS THAN (2001) ENGINE = InnoDB,
            PARTITION p02 VALUES LESS THAN (2002) ENGINE = InnoDB,
            PARTITION p03 VALUES LESS THAN (2003) ENGINE = InnoDB,
            PARTITION p04 VALUES LESS THAN (2004) ENGINE = InnoDB,
            PARTITION p05 VALUES LESS THAN (2005) ENGINE = InnoDB,
            PARTITION p06 VALUES LESS THAN (2006) ENGINE = InnoDB,
            PARTITION p07 VALUES LESS THAN (2007) ENGINE = InnoDB,
            PARTITION p08 VALUES LESS THAN (2008) ENGINE = InnoDB,
            PARTITION p09 VALUES LESS THAN (2009) ENGINE = InnoDB,
            PARTITION p10 VALUES LESS THAN (2010) ENGINE = InnoDB,
            PARTITION p11 VALUES LESS THAN (2011) ENGINE = InnoDB,
            PARTITION p12 VALUES LESS THAN (2012) ENGINE = InnoDB,
            PARTITION p13 VALUES LESS THAN (2013) ENGINE = InnoDB,
            PARTITION p14 VALUES LESS THAN (2014) ENGINE = InnoDB,
            PARTITION p15 VALUES LESS THAN (2015) ENGINE = InnoDB,
            PARTITION p16 VALUES LESS THAN (2016) ENGINE = InnoDB,
            PARTITION p17 VALUES LESS THAN (2017) ENGINE = InnoDB,
            PARTITION p18 VALUES LESS THAN (2018) ENGINE = InnoDB,
            PARTITION p19 VALUES LESS THAN (2019) ENGINE = InnoDB,
            PARTITION p20 VALUES LESS THAN (2020) ENGINE = InnoDB,
            PARTITION p21 VALUES LESS THAN (2021) ENGINE = InnoDB,
            PARTITION p22 VALUES LESS THAN (2022) ENGINE = InnoDB,
            PARTITION p23 VALUES LESS THAN (2023) ENGINE = InnoDB,
            PARTITION p24 VALUES LESS THAN (2024) ENGINE = InnoDB,
            PARTITION p25 VALUES LESS THAN (2025) ENGINE = InnoDB,
            PARTITION p26 VALUES LESS THAN (2026) ENGINE = InnoDB,
            PARTITION p27 VALUES LESS THAN (2027) ENGINE = InnoDB) */; """)
    result = engine.execute(sql)
    return result


def createTable():
    """读取createtable文件夹tablename.xlsx文件中的表名，
    根据相应表格格式的xlsx文件创建mysql表格

    :return:
    """
    # 读表名
    tablenameDf = pd.read_excel('createtable/tablename.xlsx')
    tablenames = tablenameDf['table']

    # 读类型对应关系
    # typedf = pd.read_excel('createtable/typetrans.xlsx')
    # typedf.set_index('tusharetype', inplace=True)

    for tablename in tablenames:
        if existTable(tablename):
            continue
        # 读字段名
        df = pd.read_excel(f'createtable/{tablename}.xlsx')
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


if __name__ == '__main__':
    if not existTable('chiguguzhi'):
        createChiguGuzhiTable()
    if not existTable('youzhiguzhi'):
        createYouzhiGuzhiTable()
    if not existTable('guzhi'):
        createGuzhiTable()
    if not existTable('hangyestock'):
        createHY()
    if not existTable('hangyename'):
        createHYName()
    if not existTable('classify_profits'):
        createHYProfitsTable()
    if not existTable('chigu'):
        createChiguTable()
    if not existTable('guzhiresult'):
        createGuzhiResultTable()
    if not existTable('guzhihistorystatus'):
        createGuzhiHistoryStatusTable()
    if not existTable('pelirunincrease'):
        createPELirunIncreaseTable()
    if not existTable('stocklist'):
        createStocklist()
    if not existTable('pehistory'):
        createPEHistory()
    if not existTable('indexkline'):
        createIndexKline()
    if not existTable('guben'):
        createGubenTable()
    if not existTable('hangyepe'):
        createHangyePE()
    if not existTable('valuation'):
        createValuation()
