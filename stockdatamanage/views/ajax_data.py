import datetime as dt
import logging

from flask import current_app, request, Blueprint
from pyecharts.charts import Bar
from pyecharts import options as opts

from ..db.sqlrw import readChigu, readClassifyProfit, readStockList, readProfitInc
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


@ajax_data.route('/stockprofitsinc', methods=["GET", "POST"])
def stockProfitsInc():
    logging.debug(f'stock profits inc')
    ts_code = request.args.get('ts_code')
    startDate = dt.date(dt.date.today().year - 2, 1, 1).strftime('%Y%m%d')
    endDate = dt.date.today().strftime('%Y%m%d')
    df = readProfitInc(startDate=startDate, endDate=endDate, code=ts_code)
    incDict = df.to_dict(orient='records')[0]
    logging.debug(f'stock profits inc for {ts_code}')

    dates = [k[3:] for k in df.keys().to_list()[1:]]
    values = df.iloc[0, :].to_list()[1:]
    c = (
        Bar()
            .add_xaxis(dates)
            .add_yaxis('利润增长率', values)
            .set_global_opts(title_opts=opts.TitleOpts(title="近2年TTM利润增长率"))
    )
    return c.dump_options_with_quotes()
