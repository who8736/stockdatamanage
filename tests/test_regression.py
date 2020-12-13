from stockdatamanage.util.initlog import initlog
from stockdatamanage.analyse.mean_reversoin import adfTestAllProfitsInc

if __name__ == '__main__':
    initlog()
    adfTestAllProfitsInc()
    # linearProfitInc()
    # profits_inc_linear_adf()