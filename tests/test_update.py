from stockdatamanage.util.datamanage import (
    updateAdjFacotr, updateAllMarketPE, updateClassifyList,
    updateClassifyProfits, updateDaily, updateDailybasic, updateIndex, updatePf,
    updateQuarterData, updateStockList, updateTradeCal,
)


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
    updatePf()

    # 更新指数数据及PE
    updateIndex()

    # 更新全市PE
    updateAllMarketPE()


if __name__ == '__main__':
    testUpdate()
