from stockdatamanage.util.datamanage import (
    updateAdjFacotr, updateAllMarketPE, updateClassifyList,
    updateClassifyProfits, updateDaily, updateDailybasic, updateIndex, updatePf,
    updateQuarterData, updateStockList, updateTradeCal,
)
from stockdatamanage.util.initlog import initlog
from stockdatamanage.downloader.download import downIndexWeight
from stockdatamanage.db.sqlrw import readUpdate
from stockdatamanage.analyse.compute import calIndexPE


def testUpdate():
    """测试专用函数:数据下载
    """
    pass
    # 更新交易日历
    # updateTradeCal()

    # 更新股票列表
    # updateStockList()

    # 更新股票日交易数据
    # updateDaily()

    # 更新每日指标
    # updateDailybasic()

    # 更新复权因子
    # updateAdjFacotr()

    # 更新非季报表格
    # 财务披露表（另外单独更新）
    # 质押表（另外单独更新）
    # 业绩预告（另外单独更新）
    # 业绩快报（另外单独更新）
    # 分红送股（另外单独更新）

    # 更新股票季报数据
    # 资产负债表
    # 利润表
    # 现金流量表
    # 财务指标表
    # updateQuarterData()

    # 更新股票TTM利润
    # updateTTMProfits()

    # 更新行业列表
    # updateClassifyList()

    # 更新行业利润
    # updateClassifyProfits()

    # 更新股票估值, 废弃, 用股票评分代替
    # updateGuzhiData()

    # 更新股票评分
    # updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    # updateAllMarketPE()


def test_download_index_weight():
    downIndexWeight()

def test_update_index_pe():
    ID = '000010.SH'
    startDate = readUpdate('index_000010.SH', offsetdays=1)
    calIndexPE(ID, startDate)


def test_update_all_pe():
    updateAllMarketPE()

if __name__ == '__main__':
    initlog()

    # testUpdate()
    # test_download_index_weight()
    # test_update_index_pe()
    test_update_all_pe()