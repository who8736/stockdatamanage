import logging

from flask import current_app, request, Blueprint

from ..db.sqlrw import readChigu, readClassifyProfit, readStockList
from ..util.datatrans import lastQuarter


ajax_data = Blueprint('ajax_data', __name__)

@ajax_data.route('/profitjson', methods=["GET", "POST"])
def classifyProfitJson():
    # print(request.args)
    year = request.args.get('year', '')
    quarter = request.args.get('quarter', '')
    logging.debug(
        f'ajax request classify profit year: {year}; quarter: {quarter}')
    qmonth = ['0331', '0630', '0930', '1231']
    if not ('2010' <= year <= '2030') or quarter not in '1234':
        # assert quarter in '1234', '季度参数不为1， 2， 3， 4'
        date = lastQuarter()
    else:
        date = f'{year}{qmonth[int(quarter) - 1]}'
    lv = request.args.get('lv', '')
    if lv and '1' <= lv <= '4':
        lv = int(lv)
    else:
        lv = ''
    # print(date, lv)
    df = readClassifyProfit(date, lv)
    stocks = df.to_json(orient='records', force_ascii=False)
    return stocks


@ajax_data.route('/holdjson', methods=["GET", "POST"])
def holdjson():
    stocks = readStockList()
    # stocks = stocks[:10]
    stocks.rename(columns={'ts_code': 'id'}, inplace=True)
    stocks['text'] = stocks.id + ' ' + stocks.name
    stocks['selected'] = False
    chigu = readChigu()
    stocks.loc[stocks.id.isin(chigu.ts_code), 'selected'] = True
    # print(chigu)
    current_app.logger.debug(f'持有的股票: {chigu}')

    stocksList = stocks.to_json(orient='records', force_ascii=False)
    return stocksList
