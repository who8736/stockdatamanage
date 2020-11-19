# -*- coding: utf-8 -*-
'''
Created on 2016年12月5日

@author: who8736
'''
import codecs

from . import sqlrw
from .classifyanalyse import (getClassifyName, getHYProfitsIncRates,
                              getStockForClassify, getStockProfitsIncRates,
                              readClassify)
from . import datatrans


class ReportItem():

    def __init__(self, ts_code):
        self.ts_code = ts_code


def report(ts_code):
    reportStr = '股票代码: %s\n' % ts_code
    reportStr += '股票名称: %s\n\n' % sqlrw.getStockName(ts_code)

    reportStr += '估值数据：\n' + '-' * 20 + '\n'
    #     return reportStr
    guzhiData = sqlrw.getGuzhi(ts_code)
    print(guzhiData)
    reportStr += '当前TTMPE： %+6.2f\n' % guzhiData[2]
    reportStr += 'PEG：       %+6.2f\n' % guzhiData[3]
    reportStr += ('未来三年PE预计： %+10.2f%+10.2f%+10.2f\n' %
                  (guzhiData[4], guzhiData[5], guzhiData[6]))
    reportStr += ('最近6个季度TTM利润增长率：'
                  '%+10.2f%+10.2f%+10.2f%+10.2f%+10.2f%+10.2f\n' %
                  (guzhiData[7], guzhiData[8], guzhiData[9],
                   guzhiData[10], guzhiData[11], guzhiData[12]))
    reportStr += '最近6个季度TTM利润平均增长率： %+6.2f\n' % guzhiData[13]
    reportStr += '根据平均绝对离差计算的增长率差异水平： %+6.2f\n' % guzhiData[14]
    reportStr += '根据标准差计算的增长率差异水平： %+6.2f\n' % guzhiData[15]
    reportStr += '当前TTMPE参考最近200个工作日水平： %+6.2f\n' % guzhiData[16]
    reportStr += '当前TTMPE参考最近1000个工作日水平： %+6.2f\n' % guzhiData[17]

    hyIDlv4 = classifyanalyse.readClassify(ts_code)
    hyIDlv3 = hyIDlv4[:6]
    hyIDlv2 = hyIDlv4[:4]
    hyIDlv1 = hyIDlv4[:2]
    reportStr += '=' * 20 + '\n\n'
    reportStr += '行业比较：\n' + '-' * 20 + '\n'
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  classifyanalyse.getStockProfitsIncRates(ts_code))

    reportStr += '所属一级行业：%s\n' % classifyanalyse.getClassifyName(hyIDlv1)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  classifyanalyse.getHYProfitsIncRates(hyIDlv1))
    reportStr += '所属二级行业：%s\n' % classifyanalyse.getClassifyName(hyIDlv2)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  classifyanalyse.getHYProfitsIncRates(hyIDlv2))
    reportStr += '所属三级行业：%s\n' % classifyanalyse.getClassifyName(hyIDlv3)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  classifyanalyse.getHYProfitsIncRates(hyIDlv3))
    reportStr += '所属四级行业：%s\n' % classifyanalyse.getClassifyName(hyIDlv4)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  classifyanalyse.getHYProfitsIncRates(hyIDlv4))

    stockList = classifyanalyse.getStockForClassify(hyIDlv4)
    print(stockList)
    for sameHYts_code in stockList:
        if sameHYts_code[0] not in ['0', '3', '6']:
            continue
        print('sameHYts_code:', sameHYts_code)
        reportStr += '同行业股票代码: %s\t' % sameHYts_code
        reportStr += '股票名称: %s\n\n' % sqlrw.getStockName(sameHYts_code)
        reportStr += ('最近三年TTM利润增长率水平：%+10.2s%+10.2s%+10.2s\n\n' %
                      classifyanalyse.getStockProfitsIncRates(sameHYts_code))

    outFilename = './data/report%s.txt' % ts_code
    outfile = codecs.open(outFilename, 'wb', 'utf-8')
    outfile.write(reportStr)
    outfile.close()
    return reportStr


def report1(ts_code):
    myItem = ReportItem(ts_code)
    myItem.name = sqlrw.getStockName(ts_code)
    guzhiData = sqlrw.getGuzhi(ts_code)
    # 当前TTMPE
    myItem.curTTMPE = guzhiData[2]
    myItem.peg = guzhiData[3]
    # 未来三年PE预计
    myItem.PEYuji = [guzhiData[4], guzhiData[5], guzhiData[6]]
    # 最近6个季度TTM利润增长率
    myItem.profitsInc = [guzhiData[7], guzhiData[8], guzhiData[9],
                         guzhiData[10], guzhiData[11], guzhiData[12]]
    # 最近6个季度TTM利润平均增长率
    myItem.profitsIncAvg = guzhiData[13]
    # 根据平均绝对离差计算的增长率差异水平
    myItem.profitsIncMad = guzhiData[14]
    # 根据标准差计算的增长率差异水平
    myItem.profitsIncStand = guzhiData[15]
    # 当前TTMPE参考最近200个工作日水平
    myItem.PERate200 = guzhiData[16]
    # 当前TTMPE参考最近1000个工作日水平
    myItem.PERate1000 = guzhiData[17]

    hyIDlv4 = classifyanalyse.readClassify(ts_code)
    hyIDlv3 = hyIDlv4[:6]
    hyIDlv2 = hyIDlv4[:4]
    hyIDlv1 = hyIDlv4[:2]

    # 最近三年TTM利润增长率水平
    myItem.profitsInc3Years = classifyanalyse.getStockProfitsIncRates(ts_code)
    # 所属1级行业
    myItem.hyIDlv1 = hyIDlv1
    myItem.hyLv1 = classifyanalyse.getClassifyName(hyIDlv1)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv1 = classifyanalyse.getHYProfitsIncRates(hyIDlv1)
    # 所属2级行业
    myItem.hyIDlv2 = hyIDlv2
    myItem.hyLv2 = classifyanalyse.getClassifyName(hyIDlv2)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv2 = classifyanalyse.getHYProfitsIncRates(hyIDlv2)
    # 所属3级行业
    myItem.hyIDlv3 = hyIDlv3
    myItem.hyLv3 = classifyanalyse.getClassifyName(hyIDlv3)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv3 = classifyanalyse.getHYProfitsIncRates(hyIDlv3)
    # 所属4级行业
    myItem.hyIDlv4 = hyIDlv4
    myItem.hyLv4 = classifyanalyse.getClassifyName(hyIDlv4)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv4 = classifyanalyse.getHYProfitsIncRates(hyIDlv4)
    #
    stockList = classifyanalyse.getStockForClassify(hyIDlv4)
    #     print stockList
    sameHYList = []
    for sameHYts_code in stockList:
        if sameHYts_code[0] not in ['0', '3', '6']:
            continue
        #         print u'sameHYts_code:', sameHYts_code
        sameHYList.append([sameHYts_code,
                           sqlrw.getStockName(sameHYts_code),
                           classifyanalyse.getStockProfitsIncRates(
                               sameHYts_code)])
    myItem.sameHYList = sameHYList
    #     outFilename = u'./data/report%s.txt' % ts_code
    #     outfile = codecs.open(outFilename, 'wb', 'utf-8')
    #     outfile.write(reportStr)
    #     outfile.close()
    #     return reportStr
    return myItem


def reportValuation(ts_code):
    myItem = ReportItem(ts_code)
    myStockValuation = sqlrw.readValuation(ts_code)
    myItem.name = myStockValuation[2]
    guzhiData = sqlrw.getGuzhi(ts_code)

    # 股票评分
    myItem.pf = myStockValuation[3]
    myItem.lowpe = myStockValuation[5]
    myItem.lowhype = myStockValuation[8]
    myItem.lowpeg = myStockValuation[18]
    myItem.wdzz = myStockValuation[20]
    myItem.lowpez200 = myStockValuation[22]
    myItem.lowpez1000 = myStockValuation[24]

    # 当前TTMPE在近200、1000个工作日中的Z值
    myItem.pez200 = myStockValuation[21]
    myItem.pez1000 = myStockValuation[23]

    # 当前TTMPE
    myItem.curTTMPE = myStockValuation[4]
    myItem.peg = myStockValuation[17]
    myItem.hype = myStockValuation[7]
    # 未来三年PE预计
    myItem.PEYuji = [guzhiData[4], guzhiData[5], guzhiData[6]]
    # 最近6个季度TTM利润增长率
    myItem.profitsInc = [myStockValuation[9], myStockValuation[10],
                         myStockValuation[11], myStockValuation[12],
                         myStockValuation[13], myStockValuation[14]]
    # 最近6个季度TTM利润平均增长率
    myItem.profitsIncAvg = myStockValuation[15]
    # 根据平均绝对离差计算的增长率差异水平
    myItem.profitsIncMad = guzhiData[14]
    # 根据标准差计算的增长率差异水平
    myItem.profitsIncStand = myStockValuation[16]
    # 当前TTMPE参考最近200个工作日水平
    myItem.PERate200 = myStockValuation[25]
    myItem.PEZ200 = myStockValuation[21]
    # 当前TTMPE参考最近1000个工作日水平
    myItem.PERate1000 = myStockValuation[26]
    myItem.PEZ1000 = myStockValuation[23]

    lv4Code = readClassify(ts_code)
    lv3Code = lv4Code[:6]
    lv2Code = lv4Code[:4]
    lv1Code = lv4Code[:2]

    # 最近三年TTM利润增长率水平
    myItem.profitsInc3Years = getStockProfitsIncRates(ts_code)
    # 所属1级行业
    myItem.lv1Code = lv1Code
    myItem.lv1Name = getClassifyName(lv1Code)
    myItem.lv1Inc = getHYProfitsIncRates(lv1Code)
    # 所属2级行业
    myItem.lv2Code = lv2Code
    myItem.lv2Name = getClassifyName(lv2Code)
    myItem.lv2Inc = getHYProfitsIncRates(lv2Code)
    # 所属3级行业
    myItem.lv3Code = lv3Code
    myItem.lv3Name = getClassifyName(lv3Code)
    myItem.lv3Inc = getHYProfitsIncRates(lv3Code)
    # 所属4级行业
    myItem.lv4Code = lv4Code
    myItem.lv4Name = getClassifyName(lv4Code)
    myItem.lv4Inc = getHYProfitsIncRates(lv4Code)

    # 同行业股票
    stocks = getStockForClassify(lv4Code)
    # print(f'223 line: {stockList}')
    for code in stocks.ts_code:
        stocks.loc[stocks.ts_code == code, 'sname'] = sqlrw.getStockName(code)
        incs = getStockProfitsIncRates(code)
        stocks.loc[stocks.ts_code == code, 'inc1'] = incs[0]
        stocks.loc[stocks.ts_code == code, 'inc2'] = incs[1]
        stocks.loc[stocks.ts_code == code, 'inc3'] = incs[2]
    print(f'230 stocks:', stocks)
    myItem.classifyStocks = stocks
    return myItem


def reportIndex(ID):
    myItem = ReportItem(ID)
    return myItem


if __name__ == '__main__':
    ts_code = '000002'
    # report(ts_code)
