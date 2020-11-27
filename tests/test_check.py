# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/16 20:41
import logging

from stockdatamanage.check import (
    checkClassifyMemberListdate, checkPath,
    checkQuarterDataNew,
)
from stockdatamanage.initlog import initlog

if __name__ == '__main__':
    pass
    initlog()
    # checkClassifyMemberListdate()

    # checkPath()

    checkQuarterDataNew()
