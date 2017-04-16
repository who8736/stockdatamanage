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
    reportStr = u'股票代码: %s\n' % stockID
    reportStr += u'股票名称: %s\n\n' % sqlrw.getStockName(stockID)

    reportStr += u'估值数据：\n' + u'-' * 20 + u'\n'
#     return reportStr
    guzhiData = sqlrw.getGuzhi(stockID)
    print guzhiData
    reportStr += u'当前TTMPE： %+6.2f\n' % guzhiData[2]
    reportStr += u'PEG：       %+6.2f\n' % guzhiData[3]
    reportStr += (u'未来三年PE预计： %+10.2f%+10.2f%+10.2f\n' %
                  (guzhiData[4], guzhiData[5], guzhiData[6]))
    reportStr += (u'最近6个季度TTM利润增长率：'
                  u'%+10.2f%+10.2f%+10.2f%+10.2f%+10.2f%+10.2f\n' %
                  (guzhiData[7], guzhiData[8], guzhiData[9],
                   guzhiData[10], guzhiData[11], guzhiData[12]))
    reportStr += u'最近6个季度TTM利润平均增长率： %+6.2f\n' % guzhiData[13]
    reportStr += u'根据平均绝对离差计算的增长率差异水平： %+6.2f\n' % guzhiData[14]
    reportStr += u'根据标准差计算的增长率差异水平： %+6.2f\n' % guzhiData[15]
    reportStr += u'当前TTMPE参考最近200个工作日水平： %+6.2f\n' % guzhiData[16]
    reportStr += u'当前TTMPE参考最近1000个工作日水平： %+6.2f\n' % guzhiData[17]

    hyIDlv4 = hyanalyse.getHYIDForStock(stockID)
    hyIDlv3 = hyIDlv4[:6]
    hyIDlv2 = hyIDlv4[:4]
    hyIDlv1 = hyIDlv4[:2]
    reportStr += u'=' * 20 + u'\n\n'
    reportStr += u'行业比较：\n' + u'-' * 20 + u'\n'
    reportStr += (u'最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getStockProfitsIncRates(stockID))

    reportStr += u'所属一级行业：%s\n' % hyanalyse.getHYName(hyIDlv1)
    reportStr += (u'最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv1))
    reportStr += u'所属二级行业：%s\n' % hyanalyse.getHYName(hyIDlv2)
    reportStr += (u'最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv2))
    reportStr += u'所属三级行业：%s\n' % hyanalyse.getHYName(hyIDlv3)
    reportStr += (u'最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv3))
    reportStr += u'所属四级行业：%s\n' % hyanalyse.getHYName(hyIDlv4)
    reportStr += (u'最近三年TTM利润增长率水平：%+10.2f%+10.2f%+10.2f\n\n' %
                  hyanalyse.getHYProfitsIncRates(hyIDlv4))

    stockList = hyanalyse.getStockListForHY(hyIDlv4)
    print stockList
    for sameHYStockID in stockList:
        if sameHYStockID[0] not in ['0', '3', '6']:
            continue
        print u'sameHYStockID:', sameHYStockID
        reportStr += u'同行业股票代码: %s\t' % sameHYStockID
        reportStr += u'股票名称: %s\n\n' % sqlrw.getStockName(sameHYStockID)
        reportStr += (u'最近三年TTM利润增长率水平：%+10.2s%+10.2s%+10.2s\n\n' %
                      hyanalyse.getStockProfitsIncRates(sameHYStockID))

    outFilename = u'./data/report%s.txt' % stockID
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

if __name__ == '__main__':
    stockID = u'000002'
    report(stockID)
