from sqlrw

def checkGuben():
    ids = readStockIDsFromSQL()
    sql = 'select stockid from kline'
    for stockID in ids:

