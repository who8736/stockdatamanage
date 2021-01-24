import datetime as dt

from flask import render_template, Blueprint

from stockdatamanage.db.sqlrw import readClassifyProfit, readClassifyProfitInc
from stockdatamanage.util.datatrans import lastQuarter

classify = Blueprint('classify', __name__)

@classify.route('/profit', methods=["GET", "POST"])
def profit():
    date = lastQuarter()
    # df = readClassifyProfit(date)
    return render_template("classifyprofit.html")

@classify.route("/profit_inc_hist")
def profitIncHist():
    return render_template("classify_profit_inc_hist.html")


