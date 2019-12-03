# -*- coding: utf-8 -*-
'''
Created on 2016年12月5日

@author: who8736
'''
import codecs

import sqlrw
import hyanalyse
import datatrans


class reportItem():

    def __init__(self, stockID):
        self.stockID = stockID


def report(stockID):
    reportStr = '股票代码: %s\n' % stockID
    reportStr += '股票名称: %s\n\n' % sqlrw.getStockName(stockID)

    reportStr += '估值数据：\n' + '-' * 20 + '\n'
#     return reportStr
    guzhiData = sqlrw.getGuzhi(stockID)
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

    hyIDlv4 = hyanalyse.getHYIDForStock(stockID)
    hyIDlv3 = hyIDlv4[:6]
    hyIDlv2 = hyIDlv4[:4]
    hyIDlv1 = hyIDlv4[:2]
    reportStr += '=' * 20 + '\n\n'
    reportStr += '行业比较：\n' + '-' * 20 + '\n'
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getStockProfitsIncRates(stockID))

    reportStr += '所属一级行业：%s\n' % hyanalyse.getHYName(hyIDlv1)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv1))
    reportStr += '所属二级行业：%s\n' % hyanalyse.getHYName(hyIDlv2)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv2))
    reportStr += '所属三级行业：%s\n' % hyanalyse.getHYName(hyIDlv3)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv3))
    reportStr += '所属四级行业：%s\n' % hyanalyse.getHYName(hyIDlv4)
    reportStr += ('最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv4))

    stockList = hyanalyse.getStockListForHY(hyIDlv4)
    print(stockList)
    for sameHYStockID in stockList:
        if sameHYStockID[0] not in ['0', '3', '6']:
            continue
        print('sameHYStockID:', sameHYStockID)
        reportStr += '同行业股票代码: %s\t' % sameHYStockID
        reportStr += '股票名称: %s\n\n' % sqlrw.getStockName(sameHYStockID)
        reportStr += ('最近三年TTM利润增长率水平：%+10.2s%+10.2s%+10.2s\n\n' %
                      hyanalyse.getStockProfitsIncRates(sameHYStockID))

    outFilename = './data/report%s.txt' % stockID
    outfile = codecs.open(outFilename, 'wb', 'utf-8')
    outfile.write(reportStr)
    outfile.close()
    return reportStr


def report1(stockID):
    myItem = reportItem(stockID)
    myItem.name = sqlrw.getStockName(stockID)
    guzhiData = sqlrw.getGuzhi(stockID)
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

    hyIDlv4 = hyanalyse.getHYIDForStock(stockID)
    hyIDlv3 = hyIDlv4[:6]
    hyIDlv2 = hyIDlv4[:4]
    hyIDlv1 = hyIDlv4[:2]

    # 最近三年TTM利润增长率水平
    myItem.profitsInc3Years = hyanalyse.getStockProfitsIncRates(stockID)
    # 所属1级行业
    myItem.hyIDlv1 = hyIDlv1
    myItem.hyLv1 = hyanalyse.getHYName(hyIDlv1)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv1 = hyanalyse.getHYProfitsIncRates(hyIDlv1)
    # 所属2级行业
    myItem.hyIDlv2 = hyIDlv2
    myItem.hyLv2 = hyanalyse.getHYName(hyIDlv2)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv2 = hyanalyse.getHYProfitsIncRates(hyIDlv2)
    # 所属3级行业
    myItem.hyIDlv3 = hyIDlv3
    myItem.hyLv3 = hyanalyse.getHYName(hyIDlv3)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv3 = hyanalyse.getHYProfitsIncRates(hyIDlv3)
    # 所属4级行业
    myItem.hyIDlv4 = hyIDlv4
    myItem.hyLv4 = hyanalyse.getHYName(hyIDlv4)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv4 = hyanalyse.getHYProfitsIncRates(hyIDlv4)
#
    stockList = hyanalyse.getStockListForHY(hyIDlv4)
#     print stockList
    sameHYList = []
    for sameHYStockID in stockList:
        if sameHYStockID[0] not in ['0', '3', '6']:
            continue
#         print u'sameHYStockID:', sameHYStockID
        sameHYList.append([sameHYStockID,
                           sqlrw.getStockName(sameHYStockID),
                           hyanalyse.getStockProfitsIncRates(sameHYStockID)])
    myItem.sameHYList = sameHYList
#     outFilename = u'./data/report%s.txt' % stockID
#     outfile = codecs.open(outFilename, 'wb', 'utf-8')
#     outfile.write(reportStr)
#     outfile.close()
#     return reportStr
    return myItem


def reportValuation(stockID):
    myItem = reportItem(stockID)
    myStockValuation = sqlrw.readValuation(stockID)
    myItem.name = myStockValuation[1]
    guzhiData = sqlrw.getGuzhi(stockID)

    # 股票评分
    myItem.pf = myStockValuation[2]
    myItem.lowpe = myStockValuation[4]
    myItem.lowhype = myStockValuation[7]
    myItem.lowpeg = myStockValuation[17]
    myItem.wdzz = myStockValuation[19]
    myItem.lowpez200 = myStockValuation[21]
    myItem.lowpez1000 = myStockValuation[23]

    # 当前TTMPE在近200、1000个工作日中的Z值
    myItem.pez200 = myStockValuation[20]
    myItem.pez1000 = myStockValuation[22]

    # 当前TTMPE
    myItem.curTTMPE = myStockValuation[3]
    myItem.peg = myStockValuation[16]
    myItem.hype = myStockValuation[6]
    # 未来三年PE预计
    myItem.PEYuji = [guzhiData[4], guzhiData[5], guzhiData[6]]
    # 最近6个季度TTM利润增长率
    myItem.profitsInc = [myStockValuation[8], myStockValuation[9],
                         myStockValuation[10], myStockValuation[11],
                         myStockValuation[12], myStockValuation[13]]
    # 最近6个季度TTM利润平均增长率
    myItem.profitsIncAvg = myStockValuation[14]
    # 根据平均绝对离差计算的增长率差异水平
    myItem.profitsIncMad = guzhiData[14]
    # 根据标准差计算的增长率差异水平
    myItem.profitsIncStand = myStockValuation[15]
    # 当前TTMPE参考最近200个工作日水平
    myItem.PERate200 = myStockValuation[24]
    myItem.PEZ200 = myStockValuation[20]
    # 当前TTMPE参考最近1000个工作日水平
    myItem.PERate1000 = myStockValuation[25]
    myItem.PEZ1000 = myStockValuation[22]

    hyIDlv4 = hyanalyse.getHYIDForStock(stockID)
    hyIDlv3 = hyIDlv4[:6]
    hyIDlv2 = hyIDlv4[:4]
    hyIDlv1 = hyIDlv4[:2]

    # 最近三年TTM利润增长率水平
    myItem.profitsInc3Years = hyanalyse.getStockProfitsIncRates(stockID)
    # 所属1级行业
    myItem.hyIDlv1 = hyIDlv1
    myItem.hyLv1 = hyanalyse.getHYName(hyIDlv1)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv1 = hyanalyse.getHYProfitsIncRates(hyIDlv1)
    # 所属2级行业
    myItem.hyIDlv2 = hyIDlv2
    myItem.hyLv2 = hyanalyse.getHYName(hyIDlv2)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv2 = hyanalyse.getHYProfitsIncRates(hyIDlv2)
    # 所属3级行业
    myItem.hyIDlv3 = hyIDlv3
    myItem.hyLv3 = hyanalyse.getHYName(hyIDlv3)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv3 = hyanalyse.getHYProfitsIncRates(hyIDlv3)
    # 所属4级行业
    myItem.hyIDlv4 = hyIDlv4
    myItem.hyLv4 = hyanalyse.getHYName(hyIDlv4)
    # 最近三年TTM利润增长率水平
    myItem.hyIncLv4 = hyanalyse.getHYProfitsIncRates(hyIDlv4)
#
    stockList = hyanalyse.getStockListForHY(hyIDlv4)
#     print stockList
    sameHYList = []
    for sameHYStockID in stockList:
        if sameHYStockID[0] not in ['0', '3', '6']:
            continue
#         print u'sameHYStockID:', sameHYStockID
        sameHYList.append([sameHYStockID,
                           sqlrw.getStockName(sameHYStockID),
                           hyanalyse.getStockProfitsIncRates(sameHYStockID)])
    myItem.sameHYList = sameHYList
#     outFilename = u'./data/report%s.txt' % stockID
#     outfile = codecs.open(outFilename, 'wb', 'utf-8')
#     outfile.write(reportStr)
#     outfile.close()
#     return reportStr
    return myItem


def reportIndex(ID):
    myItem = reportItem(ID)
    return myItem

if __name__ == '__main__':
    stockID = '000002'
    report(stockID)
