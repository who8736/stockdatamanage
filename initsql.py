# -*- coding: utf-8 -*-
'''
Created on 2016年11月30日

@author: who8736
'''
import sqlrw


def existTable(tablename):
    sql = 'show tables like "%s"' % tablename
    result = sqlrw.engine.execute(sql)
    return False if result.rowcount == 0 else True


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
    sql = ('CREATE TABLE hangye('
           'stockid VARCHAR(6),'
           'level1 VARCHAR(2),'
           'level2 VARCHAR(4),'
           'level3 VARCHAR(6),'
           'level4 VARCHAR(8),'
           'PRIMARY KEY ( stockid )); ')
    result = sqlrw.engine.execute(sql)
    return result


def createHYName():
    sql = ('CREATE TABLE hangyename('
           'levelid VARCHAR(8),'
           'levelname VARCHAR(50),'
           'PRIMARY KEY ( levelid )); ')
    result = sqlrw.engine.execute(sql)
    return result


def createYouzhiGuzhiTable():
    sql = 'CREATE TABLE youzhiguzhi like chiguguzhi;'
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


def createGubenTable():
    pass  # TODO: 创建总股本表


def createChiguTable():
    sql = 'CREATE TABLE chigu(stockid VARCHAR(6),PRIMARY KEY (stockid));'
    return sqlrw.engine.execute(sql)


def createKlineTable(stockID):
    tableName = 'kline%s' % stockID
    sql = 'create table %s like klinesample' % tableName
    return sqlrw.engine.execute(sql)

if __name__ == '__main__':
    if not existTable('chiguguzhi'):
        createChiguGuzhiTable()
    if not existTable('youzhiguzhi'):
        createYouzhiGuzhiTable()
    if not existTable('guzhi'):
        createGuzhiTable()
    if not existTable('hangye'):
        createHY()
    if not existTable('hangyename'):
        createHYName()
    if not existTable('hyprofits'):
        createHYProfitsTable()
    if not existTable('chigu'):
        createChiguTable()
