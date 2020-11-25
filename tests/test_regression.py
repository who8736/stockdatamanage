from stockdatamanage.initlog import initlog
from stockdatamanage.analyse.regression import profits_inc_linear_adf
from stockdatamanage.analyse.mean_reversoin import adfTestAllProfitsInc
from stockdatamanage.analyse.linear_regression import linearProfitInc

if __name__ == '__main__':
    initlog()
    adfTestAllProfitsInc()
    # linearProfitInc()
    # profits_inc_linear_adf()