import json
import datetime as dt
import logging

from flask import current_app, request, Blueprint

from ..analyse.classifyanalyse import readClassifyCodeForStock, getStockForClassify
from ..db.sqlrw import readChigu, readClassifyProfit, readStockList, readProfitInc, readClassify
from ..util.datatrans import lastQuarter
from ..util.misc import INDEXLIST

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


@ajax_data.route('/stockprofitsinc', methods=["GET", "POST"])
def stockProfitsInc():
    logging.debug(f'stock profits inc')
    ts_code = request.args.get('ts_code')
    startDate = dt.date(dt.date.today().year - 2, 1, 1).strftime('%Y%m%d')
    endDate = dt.date.today().strftime('%Y%m%d')
    df = readProfitInc(startDate=startDate, endDate=endDate, code=ts_code)
    # incDict = df.to_dict(orient='records')[0]
    logging.debug(f'stock profits inc for {ts_code}')

    dates = [k[3:] for k in df.keys().to_list()[1:]]
    values = df.iloc[0, :].to_list()[1:]
    c = Bar()
    c.add_xaxis(dates)
    c.add_yaxis('利润增长率', values)
    c.set_global_opts(title_opts=opts.TitleOpts(title="近2年TTM利润增长率"))
    return c.dump_options_with_quotes()


@ajax_data.route('/classifystocks', methods=["GET", "POST"])
def calssifyStocks():
    logging.debug(f'stock profits inc')
    ts_code = request.args.get('ts_code')
    logging.debug(ts_code)

    lv4Code = readClassifyCodeForStock(ts_code)
    stocks = getStockForClassify(lv4Code)
    # print(f'223 line: {stockList}')

    startDate = f'{dt.date.today().year - 3}0101'
    endDate = dt.date.today().strftime('%Y%m%d')
    incs = readProfitInc(startDate=startDate, endDate=endDate,
                         code=stocks.ts_code.to_list(),
                         reportType='year')
    stocks = stocks.merge(incs, on='ts_code', how='left')

    # columns = {stocks.keys()[3]: 'inc0', stocks.keys()[4]: 'inc1'}
    # stocks.rename(columns=columns, inplace=True)
    for i in range(3, len(stocks.keys())):
        stocks.rename(columns={stocks.keys()[i]: f'inc{i - 3}'}, inplace=True)

    # return jsonify(data)
    return stocks.to_json(orient='records')


@ajax_data.route("/classifylist")
def classifylist():
    classify = readClassify()
    classify['text'] = classify.code + ' ' + classify.name
    classify.rename(columns={'code': 'id'}, inplace=True)
    return classify.to_json(orient='records', force_ascii=False)


@ajax_data.route("/indexcode")
def indexlist():
    data = []
    for key in INDEXLIST:
        data.append({'id': key, 'text': key + ' ' + INDEXLIST[key]})
    return json.dumps(data)

