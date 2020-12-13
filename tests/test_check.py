# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/16 20:41

from stockdatamanage.util.check import (
    repairLost,
)
from stockdatamanage.util.initlog import initlog

if __name__ == '__main__':
    pass
    initlog()
    # checkClassifyMemberListdate()

    # checkPath()

    # checkQuarterDataNew()

    startDate = '20100331'
    endDate = '20200930'
    repairLost(startDate=startDate, endDate=endDate)
