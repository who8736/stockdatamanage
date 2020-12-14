from flask import render_template, Blueprint

from stockdatamanage.db.sqlrw import readClassifyProfit
from stockdatamanage.util.datatrans import lastQuarter

classify = Blueprint('classify', __name__)

@classify.route('/classifyprofit', methods=["GET", "POST"])
def classifyProfit():
    date = lastQuarter()
    # df = readClassifyProfit(date)
    return render_template("classifyprofit.html")
