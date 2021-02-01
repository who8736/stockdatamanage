import datetime as dt
import json
import logging

from bokeh.embed import components
from bokeh.resources import INLINE
from flask import Blueprint, render_template, send_file, request
from pyecharts import options as opts
from pyecharts.charts import Bar, Line

from ..db.sqlrw import readIndexKline, readStockKline, readClassifyProfitInc, readClassifyName, readIndexPE
from ..util.bokeh_plot import BokehPlot, PlotProfitsInc, plotKline
from ..util.misc import INDEXLIST

ajax_img = Blueprint('ajax_img', __name__)


@ajax_img.route('/klineimg')
def klineimg():
    ts_code = request.args.get('ts_code')
    plotImg = plotKline(ts_code)
    plotImg.seek(0)
    return send_file(plotImg,
                     attachment_filename='img.png',
                     as_attachment=True)


@ajax_img.route('/stockkline')
def stockklineimgnew():
    ts_code = request.args.get('ts_code')
    df = readStockKline(ts_code, days=1000)
    logging.info(f'stockklineimgnew: {ts_code}')
    return _klineimg(df)


@ajax_img.route('/indexkline')
def indexklineimgnew():
    ID = request.args.get('indexcode')
    df = readIndexKline(ID, days=3000)
    return _klineimg(df)


def _klineimg(df):
    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    plotImg = BokehPlot(df)
    scripts, div = components(plotImg.plot())
    # return render_template("plotkline.html", the_div=div, the_script=scripts)
    return render_template(
        'plotkline.html',
        plot_script=scripts,
        plot_div=div,
        js_resources=js_resources,
        css_resources=css_resources,
    )


@ajax_img.route('/profitsinc_del')
def del_profitsIncImg():
    ts_code = request.args.get('profitsinc')
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()

    # plotImg = PlotProfitsInc(ts_code,
    #                          startDate='20150331',
    #                          endDate='20191231')
    try:
        plotImg = PlotProfitsInc(ts_code)
        scripts, div = components(plotImg.plot())
        # return render_template("plotkline.html", the_div=div, the_script=scripts)
        return render_template(
            'plotkline.html',
            plot_script=scripts,
            plot_div=div,
            js_resources=js_resources,
            css_resources=css_resources,
        )
    except Exception as e:
        logging.warning(e)
        return '<div>绘图失败</div>'


@ajax_img.route("/classify_profit_inc_hist")
def classifyProfitIncHist():
    args = request.args.get('codes')
    logging.debug(args)
    if args is not None:
        codes = json.loads(args)

        c = Bar()
        endDate = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
        startDate = f'{dt.date.today().year - 3}0101'
        df = readClassifyProfitInc(codes, startDate, endDate)
        stocks = []

        for key, data in df.iteritems():
            if key == 'end_date':
                c.add_xaxis(data.to_list())
            else:
                name = readClassifyName(key)
                title = f'{key} {name}'
                stocks.append(title)
                c.add_yaxis(title, data.to_list())
        c.set_global_opts(
            title_opts=opts.TitleOpts(title="Bar-基本示例",
                                      subtitle=', '.join(stocks)))
        return c.dump_options_with_quotes()


@ajax_img.route("/index_pe")
def indexPE():
    args = request.args.get('codes')
    logging.debug(args)
    if args is not None:
        codes = json.loads(args)

        c = Line()
        endDate = (dt.date.today() - dt.timedelta(days=1)).strftime('%Y%m%d')
        startDate = f'{dt.date.today().year - 3}0101'
        df = readIndexPE(codes, startDate, endDate)
        stocks = []

        for key, data in df.iteritems():
            if key == 'date':
                c.add_xaxis(data.to_list())
            else:
                code = key[2:] + ('.SH' if key[2] == '0' else '.SZ')
                name = INDEXLIST[code]
                title = f'{code} {name}'
                stocks.append(title)
                c.add_yaxis(title, data.to_list(),
                            label_opts=opts.LabelOpts(is_show=False))

        tool_js = """function (param) {return param.seriesName + ' — ' +param.data[3]+'<br/>' +'d0: '+param.data[0]+'<br/>' +'d1: '+param.data[1]+'<br/>' ;}"""
        c.set_global_opts(
            title_opts=opts.TitleOpts(title='指数PE', subtitle=', '.join(stocks)),
            xaxis_opts=opts.AxisOpts(
                type_='time',
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            # tooltip_opts=opts.TooltipOpts(formatter=JsCode(tool_js))
        )
        return c.dump_options_with_quotes()
