from sqlrw

def checkGuben():
    ids = readStockIDsFromSQL()
    sql = 'select stockid from klinestockstock'
    for stockID in ids:

