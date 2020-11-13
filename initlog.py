import logging
import os
# from datetime import datetime
import datetime as dt
from config import ROOTPATH

def initlog():
    # 取得当前年份，按年记录日志
    nowTime = dt.datetime.now()
    logDate = nowTime.strftime('%Y')
    logfilename = f'log/datamanage{logDate}.log'
    logfilename = os.path.join(ROOTPATH, logfilename)
    print(os.path.abspath(os.curdir))
    print(logfilename)

    formatStr = ('%(asctime)s %(filename)s[line:%(lineno)d] '
                 '%(levelname)s %(message)s')
    logging.basicConfig(level=logging.DEBUG,
                        format=formatStr,
                        #                     datefmt = '%Y-%m-%d %H:%M:%S',
                        filename=logfilename,
                        filemode='a')

    ##########################################################################
    # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，
    # 并将其添加到当前的日志处理对象#
    console = logging.StreamHandler()
#     console.setLevel(logging.INFO)
    console.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] '
                                  '%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    ##########################################################################
