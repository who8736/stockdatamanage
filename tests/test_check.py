# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/16 20:41
import logging

from stockdatamanage.check import (
    checkClassifyMemberListdate, checkPath,
    repairLost,
)
from stockdatamanage.initlog import initlog

if __name__ == '__main__':
    pass
    initlog()
    # checkClassifyMemberListdate()

    # checkPath()

    # checkQuarterDataNew()

    startDate = '20100331'
    endDate = '20200930'
    repairLost(startDate=startDate, endDate=endDate)
