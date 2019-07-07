import sqlrw

def checkGuben():
    ids = sqlrw.readStockIDsFromSQL()
    sql = 'select stockid from klinestock'
    for stockID in ids:
        pass
        # TODO:  检查每支股票收盘价是否存在异常, 如市盈率与上日比较差距10%以上

