# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/16 18:15

import logging

from stockdatamanage.util import check, datamanage, initlog

if __name__ == '__main__':
    check.checkPath()
    initlog.initlog()

    try:
        datamanage.startUpdate()
    except Exception as e:
        logging.error(e)
