from sqlrw

def checkGuben():
    ids = readStockIDsFromSQL()
    sql = 'select stockid from klinestock'
    for stockID in ids:

