import json
import logging

from flask import Blueprint, render_template, request

from stockdatamanage.analyse.report import reportValuation
from stockdatamanage.db.sqlrw import readChigu, readValuationSammary

valuation = Blueprint('valuation', __name__)


@valuation.route('/')
def valuationNav():
    valu_type = request.args.get('valua_type')
    df = readValuationSammary()
    if valu_type == 'chigu':
        df = df[df['ts_code'].isin(readChigu())]
    elif valu_type == 'youzhi':
        df = df[
            (df.pf >= 5) & (df.pe < 30) & (df.pe200 >= 0) & (df.pe1000 >= 0) &
            (df.pe200 <= 20)]
    # stockReportList = np.array(df).tolist()
    df['trade_date'] = df['date'].apply(lambda x: x.strftime('%Y%m%d'))
    logging.debug(f'fina_date: [{type(df["fina_date"])}] {df["fina_date"]}')
    print(f'fina_date: [{type(df["fina_date"])}] {df["fina_date"]}')
    df['fina_date'] = df['fina_date'].apply(lambda x: x.strftime('%Y%m%d'))
    stocksStr = df.to_json(orient='records', force_ascii=False)
    stocks = json.loads(stocksStr)
    print(stocks)
    return render_template('valuationnav.html', stocks=stocks)


@valuation.route('/detail')
def valuationView():
    ts_code = request.args.get('ts_code')
    stockItem = reportValuation(ts_code)
    #     reportstr = reportstr[:20]
    #     reportstr = 'tests'
    return render_template('valuation.html',
                           stock=stockItem)
