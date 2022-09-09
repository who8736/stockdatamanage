import datetime as dt
import logging
import os
from functools import wraps

from ..config import LOGPATH


def initlog():
    # 取得当前年份，按年记录日志
    nowTime = dt.datetime.now()
    logDate = nowTime.strftime('%Y')
    logfilename = os.path.join(LOGPATH, f'datamanage{logDate}.log')

    formatStr = ('%(asctime)s %(filename)s[line:%(lineno)d] '
                 '%(levelname)s %(message)s')
    logging.basicConfig(level=logging.INFO,
                        format=formatStr,
                        filename=logfilename,
                        filemode='a',
                        force=True)

    ##########################################################################
    # 定义一个StreamHandler，将INFO级别或更高的日志信息打印到标准错误，
    # 并将其添加到当前的日志处理对象#
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter(formatStr)
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    ##########################################################################


def logfun(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        #         def _func(*args):
        #         print "hello, %s" % func.__name__
        logging.info('===========start %s===========', func.__name__)
        startTime = dt.datetime.now()
        func(*args, **kwargs)
        endTime = dt.datetime.now()
        logging.info('===========end %s===========', func.__name__)
        logging.info('%s cost time: %s ',
                     func.__name__, endTime - startTime)

    #         return _func
    return wrapper
