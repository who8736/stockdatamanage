# -*- coding: utf-8 -*-
"""
Created on 2016年11月30日

@author: who8736
"""
import sqlrw


def existTable(tablename):
    sql = 'show tables like "%s"' % tablename
    result = sqlrw.engine.execute(sql)
    return False if result.rowcount == 0 else True


def dropKlineTable():
    stockList = sqlrw.readStockIDsFromSQL()
    for stockID in stockList:
        tablename = 'kline%s' % stockID
        print(tablename)
        if existTable(tablename):
            sql = 'drop table %s;' % tablename
            sqlrw.engine.execute(sql)


def createChiguGuzhiTable():
    sql = ('CREATE TABLE chiguguzhi('
           'stockid VARCHAR(6),'
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
           'PRIMARY KEY ( stockid )); ')
    result = sqlrw.engine.execute(sql)
    return result


def createHY():
    sql = ('CREATE TABLE hangyestock('
           'stockid VARCHAR(6),'
           'hyid VARCHAR(8),'
           'PRIMARY KEY ( stockid ),'
           'KEY `hyid` (`hyid`)); ')
    result = sqlrw.engine.execute(sql)
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
    result = sqlrw.engine.execute(sql)
    return result


def createYouzhiGuzhiTable():
    sql = 'CREATE TABLE youzhiguzhi like chiguguzhi;'
    result = sqlrw.engine.execute(sql)
    return result


def createGuzhiResultTable():
    sql = 'CREATE TABLE guzhiresult like chiguguzhi;'
    result = sqlrw.engine.execute(sql)
    return result


def createTTMPETable(tablename):
    sql = 'create table %s like ttmpesimple' % tablename
    result = sqlrw.engine.execute(sql)
    return result


def createGuzhiTable():
    sql = ('CREATE TABLE guzhi('
           'stockid VARCHAR(6),'
           'peg DOUBLE,'
           'next1YearPE DOUBLE,'
           'next2YearPE DOUBLE,'
           'next3YearPE DOUBLE,'
           'PRIMARY KEY ( stockid )); ')
    result = sqlrw.engine.execute(sql)
    return result


def createHYProfitsTable():
    sql = ('CREATE TABLE hyprofits('
           'hyid VARCHAR(8),'
           'date INT(11),'
           'profitsInc DOUBLE,'
           'profitsIncRate DOUBLE,'
           'PRIMARY KEY ( hyid, date)); ')
    result = sqlrw.engine.execute(sql)
    return result


def createGuzhiHistoryStatusTable():
    """ 各字段定义 ：
    integrity： BOOL类型, 过去3年数据是否完整
    seculargrowth： BOOL类型, 是否保持持续增长，当某季TTM利润增长率为负时，该值为否
    growthmadrate： FLOAT类型, 利润增长平均离差率
    """
    sql = ('CREATE TABLE guzhihistorystatus('
           'stockid VARCHAR(6),'
           'date INT(11),'
           'integrity BOOL,'
           'seculargrowth BOOL,'
           'growthmadrate FLOAT,'
           'averageincrement FLOAT,'
           'PRIMARY KEY ( stockid, date)); ')
    result = sqlrw.engine.execute(sql)
    return result


def createPELirunIncreaseTable():
    """ 各字段定义 ：
    integrity： BOOL类型, 过去3年数据是否完整
    seculargrowth： BOOL类型, 是否保持持续增长，当某季TTM利润增长率为负时，该值为否
    growthmadrate： FLOAT类型, 利润增长平均离差率
    """
    sql = ('CREATE TABLE pelirunincrease('
           'stockid VARCHAR(6),'
           'date DATE,'
           'pe FLOAT,'
           'lirunincrease FLOAT,'
           'PRIMARY KEY (date, stockid)); ')
    result = sqlrw.engine.execute(sql)
    return result


def createGubenTable():
    sql = ("CREATE TABLE `guben` ("
           "`stockid` varchar(6) NOT NULL,"
           " `date` date NOT NULL,"
           "  `totalshares` double DEFAULT NULL,"
           "  PRIMARY KEY (`stockid`,`date`),"
           "  KEY `ix_guben_stockid` (`stockid`),"
           "  KEY `ix_guben_date` (`date`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
           )
    result = sqlrw.engine.execute(sql)
    return result


def createChiguTable():
    sql = 'CREATE TABLE chigu(stockid VARCHAR(6),PRIMARY KEY (stockid));'
    return sqlrw.engine.execute(sql)


# def createKlineTable(stockID):
#     tableName = 'kline%s' % stockID
#     sql = 'create table %s like klinesample' % tableName
#     return sqlrw.engine.execute(sql)


def createStocklist():
    sql = ("CREATE TABLE `stocklist` ("
           "`stockid` varchar(6) NOT NULL,"
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
           "PRIMARY KEY (`stockid`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
           )
    result = sqlrw.engine.execute(sql)
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
    result = sqlrw.engine.execute(sql)
    return result


def createIndexKline():
    sql = ("CREATE TABLE `indexkline` ("
           "`stockid` varchar(9) COLLATE utf8mb4_bin NOT NULL,"
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
           "PRIMARY KEY (`stockid`,`date`),"
           "KEY `ix_stockid` (`stockid`) /*!80000 INVISIBLE */,"
           "KEY `ix_date` (`date`)"
           ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"
           )
    result = sqlrw.engine.execute(sql)
    return result


def createHangyePE():
    sql = ("""CREATE TABLE `hangyepe` (
            `hyid` varchar(8) NOT NULL,
            `date` date NOT NULL,
            `ttmpe` float DEFAULT NULL,
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
    result = sqlrw.engine.execute(sql)
    return result

def createValuation():
    sql = ("""CREATE TABLE `valuation` (
            `stockid` varchar(6) NOT NULL,
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
            PRIMARY KEY (`stockid`,`date`),
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
    result = sqlrw.engine.execute(sql)
    return result


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
    if not existTable('hyprofits'):
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
