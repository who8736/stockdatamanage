from flask import Blueprint, current_app, render_template, request

from stockdatamanage.db.sqlrw import writeHold
from stockdatamanage.views.forms import HoldForm

settings = Blueprint('settings', __name__)


@settings.route('/holdsetting', methods=["GET", "POST"])
def holdsetting():
    form = HoldForm(request.form)
    hold = form.data["selected"]
    if hold:
        holdList = hold.split('|')
        current_app.logger.debug(f'保存股票清单：{holdList}')
        writeHold(holdList)

    return render_template("holdsetting.html", form=form)
