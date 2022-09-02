#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:who8736
# datetime:2022/2/10 9:23
# software: PyCharm

from stockdatamanage.downloader.downloadakshare import (
    downStockList, downTradeCal
)

from stockdatamanage.util.initlog import initlog


def test_downStockList():
    downStockList()


def test_downTradeCal():
    downTradeCal()


if __name__ == '__main__':
    initlog()

    # test_downStockList()
    test_downTradeCal()

    # testUpdate()
    # test_download_index_weight()
    # test_update_index_pe()
    # test_update_all_pe()
