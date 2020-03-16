# -*- coding: utf-8 -*-
"""
独立成单独文件， 便于在指定时间单独下载股票质押信息
"""
# from  download import downPledgeStat
from download import downloaderStock


if __name__ == '__main__':
    pass
    downloaderStock('pledge_stat', perTimes=60, downLimit=50)