from flask import render_template, Blueprint

from stockdatamanage.db.sqlrw import readClassifyProfit
from stockdatamanage.util.datatrans import lastQuarter

classify = Blueprint('classify', __name__)

@classify.route('/profit', methods=["GET", "POST"])
def classifyProfit():
    date = lastQuarter()
    # df = readClassifyProfit(date)
    return render_template("classifyprofit.html")

@classify.route("/profit_inc_hist")
def profitIncHist():
    # c = bar_base1()
    # return Markup(c.render_embed())
    return render_template("classify_proift_inc_hist.html")
