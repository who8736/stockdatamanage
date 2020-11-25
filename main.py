# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/16 18:15

from stockdatamanage import datamanage, initlog, check

if __name__ == '__main__':
    check.checkPath()
    initlog.initlog()
    datamanage.startUpdate()
