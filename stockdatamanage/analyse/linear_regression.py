"""

"""
import os
from collections import OrderedDict
from math import log, sqrt

# import matplotlib.mlab as mlab
import matplotlib.dates as mdates
import matplotlib.gridspec as gridspec
import matplotlib.pyplot as plt  # @IgnorePep8
import numpy as np
import pandas as pd
import tushare as ts
from matplotlib.dates import DateFormatter, YearLocator  # @IgnorePep8
from scipy.stats import normaltest, zscore
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import PowerTransformer
from statsmodels.tsa.stattools import adfuller

from stockdatamanage.db.sqlconn import engine
from stockdatamanage.db.sqlrw import readStockList, readStockName
from ..config import datapath


def studyTime():
    examDict = {
        '学习时间': [0.50, 0.75, 1.00, 1.25, 1.50, 1.75, 1.75, 2.00, 2.25,
                 2.50, 2.75, 3.00, 3.25, 3.50, 4.00, 4.25, 4.50, 4.75, 5.00,
                 5.50],
        '分数': [10, 22, 13, 43, 20, 22, 33, 50, 62,
               48, 55, 75, 62, 73, 81, 76, 64, 82, 90, 93]
    }
    examOrderDict = OrderedDict(examDict)
    exam = pd.DataFrame(examOrderDict)

    print(exam)

    # 从dataframe中把标签和特征导出来
    exam_X = exam['学习时间']
    exam_Y = exam['分数']

    # 绘制散点图，得出结果后记得注释掉以下4行代码
    plt.scatter(exam_X, exam_Y, color='green')
    # 设定X,Y轴标签和title
    plt.ylabel('Scores')
    plt.xlabel('Times(h)')
    plt.title('Exam Data')

    plt.show()


def _adfTest(data):
    """计算一组数据的均值及adf检测结果"""
    mean = np.mean(data)
    result = adfuller(data)
    return mean, result


# noinspection PyUnusedLocal
def _linear_trans(data, plot=False, title=None, filename=None):
    """线性回归通用函数，对一组数据进行线性回归
    数据经转换为监督学习序列后拟合
    :param data:
    :param plot: 是否显示绘图结果
    :param title: 绘图标题，plot为True时，title为必选项
    :param filename: 不为空时，自动保存图片
    :return:
    该组数据均值，均方差，adf检测结果
    该组数据一阶差分的均值，均方差，adf检测结果
    """
    pass
    cnt = len(data)
    if cnt < 10:
        return None
    x, y = series_to_supervised(data, n_in=3)
    # x = np.array(range(1, cnt + 1))
    # x = x[:, np.newaxis]
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.3)
    # print('x_train:\n', x_train)
    # print('x_test:\n', x_test)
    # print('y_train:\n', y_train)
    # print('y_test:\n', y_test)
    # print('-' * 80)
    # print('x_test len:', len(x_test))
    # print('y_test len:', len(y_test))

    # 拟合
    regr = linear_model.LinearRegression()
    regr.fit(x_train, y_train)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    # print(f'截距:{intercept}, 系数:{coef}')

    # 计算r2
    # y_predict = [intercept + i * coef for i in x]
    y_predictions = regr.predict(x_test)
    r2 = r2_score(y_test, y_predictions)
    return dict(intercept=intercept, coef=coef, r2=r2)


# noinspection PyUnusedLocal
def _linear(data, plot=False, title=None, filename=None):
    """线性回归通用函数，对一组数据进行线性回归
    :param data:
    :param plot: 是否显示绘图结果
    :param title: 绘图标题，plot为True时，title为必选项
    :param filename: 不为空时，自动保存图片
    :return:
    该组数据均值，均方差，adf检测结果
    该组数据一阶差分的均值，均方差，adf检测结果
    """
    pass
    cnt = len(data)
    if cnt < 10:
        return None
    x = np.array(range(1, cnt + 1))
    x = x[:, np.newaxis]

    # 拟合
    regr = linear_model.LinearRegression()
    regr.fit(x, data)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    # print(f'截距:{intercept}, 系数:{coef}')

    # 计算r2
    Y = [intercept + i * coef for i in x]
    r2 = r2_score(data, Y)
    return dict(intercept=intercept, coef=coef, r2=r2)


# noinspection PyUnusedLocal
def _linearHuber(data, plot=False, title=None, filename=None):
    """线性回归通用函数，对一组数据进行线性回归
    :param data:
    :param plot: 是否显示绘图结果
    :param title: 绘图标题，plot为True时，title为必选项
    :param filename: 不为空时，自动保存图片
    :return:
    该组数据均值，均方差，adf检测结果
    该组数据一阶差分的均值，均方差，adf检测结果
    """
    pass
    cnt = len(data)
    if cnt < 10:
        return None
    x = np.array(range(1, cnt + 1))
    x = x[:, np.newaxis]

    # 拟合
    regr = linear_model.HuberRegressor()
    regr.fit(x, data)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    print('huber loss:', regr.epsilon)
    # print(f'截距:{intercept}, 系数:{coef}')

    # 计算r2
    Y = [intercept + i * coef for i in x]
    r2 = r2_score(data, Y)
    return dict(intercept=intercept, coef=coef, r2=r2)


def linearProfitInc(startDate='20150331', endDate='20191231'):
    stocks = readStockList()
    # stocks = stocks[456:]

    if endDate is None:
        endDate = dt.date.today().strftime('%Y%m%d')
    if startDate is None:
        startDate = f'{int(endDate[:4]) - 3}0331'

    filename = os.path.join(datapath, 'profits_inc_linear.xlsx')

    resultList = []
    cnt = len(stocks)
    cur = 1
    for ts_code in stocks.ts_code:
        print(f'{cur}/{cnt}: {ts_code}')
        cur += 1
        result = _linearProfitInc(ts_code, startDate, endDate)
        if result is not None:
            resultList.append(result)
    df = pd.DataFrame(resultList)
    stocks = pd.merge(stocks, df, how='right',
                      left_on='ts_code', right_on='ts_code')

    stocks.to_excel(filename)


def _linearProfitInc(ts_code, startDate, endDate):
    """
    分析归属母公司股东的净利润-扣除非经常损益同比增长率(%)历年变化情况
    :param ts_code:
    :param startDate:
    :return:
    """
    pass
    sql = (f'select dt_netprofit_yoy inc from fina_indicator'
           f' where ts_code="{ts_code}" and end_date>="{startDate}"'
           f' and end_date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    df.dropna(inplace=True)
    result = _linear(df.inc.values)
    if result is not None:
        result['ts_code'] = ts_code
    return result


def _linearProfitIncDouble(ts_code, startDate, endDate):
    """
    比较最小二乘与HuberRegressor的区别
    分析归属母公司股东的净利润-扣除非经常损益同比增长率(%)历年变化情况
    :param ts_code:
    :param startDate:
    :return:
    """
    pass
    sql = (f'select dt_netprofit_yoy inc from fina_indicator'
           f' where ts_code="{ts_code}" and end_date>="{startDate}"'
           f' and end_date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    df.dropna(inplace=True)
    result_linear = _linear(df.inc.values)
    result_huber = _linearHuber(df.inc.values)
    print('linear:', result_linear)
    print('huber:', result_huber)
    return result_linear, result_huber


def _linearProfits(ts_code, startQuarter, fig):
    """
    对单一自变量进行线性回归，返回（截距， 系数，平均方差）
    :return:
    """
    pass
    sql = (f'SELECT ttmprofits FROM stockdata.ttmprofits'
           f' where ts_code="{ts_code}" and date>={startQuarter};')
    result = engine.execute(sql).fetchall()
    cnt = len(result)
    if cnt < 10:
        return None, None, None
    y = [i[0] / 10000 / 10000 for i in result]
    x = np.array(range(cnt))
    x = x[:, np.newaxis]

    # 拟合
    regr = linear_model.LinearRegression()
    regr.fit(x, y)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    # print(f'截距:{intercept}, 系数:{coef}')

    # 计算平均残差
    Y = [intercept + i * coef for i in x]
    try:
        cha = sum(map(lambda a, b: abs(sqrt((a - b) ** 2) / a), y, Y)) / cnt
    except ZeroDivisionError:
        return None, None, None

    # 绘图
    # ax = plt.subplot()
    ax = fig.add_subplot()
    ax.scatter(x, y)
    ax.plot(x, Y, color='r')
    name = readStockName(ts_code)
    plt.title(f'{ts_code} {name}', fontproperties='simsun', fontsize=26)
    # plt.show()
    filename = f'../data/linear_img/{ts_code[:6]}.png'
    plt.savefig(filename)
    plt.clf()

    return intercept, coef, cha


def plotPairs(df, intercept, coef):
    fig = plt.figure(figsize=(10, 5))
    gs = gridspec.GridSpec(1, 2)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[0, 1])
    # 绘制拆线图
    ax1.plot(df.index, df.ttmpea, color='blue', label='ts_codea')
    ax1.plot(df.index, df.ttmpeb, color='yellow', label='ts_codeb')
    ax1.legend()
    # 设置X轴的刻度间隔
    # 可选:YearLocator,年刻度; MonthLocator,月刻度; DayLocator,日刻度
    ax1.xaxis.set_major_locator(YearLocator())
    # 设置X轴主刻度的显示格式
    ax1.xaxis.set_major_formatter(DateFormatter('%Y'))
    # 设置鼠标悬停时，在左下显示的日期格式
    ax1.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
    # 自动调整X轴标签角度
    fig.autofmt_xdate()

    # 绘制散点图
    ax2.scatter(df.ttmpea, df.ttmpeb)

    plt.grid(True)

    x1 = int(min(df.ttmpea))
    x2 = int(max(df.ttmpea) + 1)
    X = [x1, x2]
    y1 = intercept + x1 * coef
    y2 = intercept + x2 * coef
    Y = [y1, y2]
    ax2.plot(X, Y, color='r')
    # print('X:', X)
    # print('Y:', Y)

    plt.show()


def findPairs(ts_codea, ts_codeb, startDate='20090101', endDate='20191231',
              plot=False):
    """两支股票的TTMPE是否存在协整关系
    时间区间为20090101至20191231
    1.两只股票TTMPE的折线图
    2.两只股票TTMPE的散点图
    3.两只股票的线性回归
    4.根据线性回归结果计算残差，返回残差的ADF检验结果
    """
    sql = (f'select trade_date, pe_ttm'
           f' from daily_basic where ts_code="{ts_codea}"'
           f' and trade_date>="{startDate}" and trade_date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    dfa.rename(columns={'pe_ttm': 'ttmpea', 'trade_date': 'date'}, inplace=True)
    dfa.set_index('date', inplace=True)
    # print(dfa)

    sql = (f'select trade_date, pe_ttm'
           f' from daily_basic where ts_code="{ts_codeb}"'
           f' and trade_date>="{startDate}" and trade_date<="{endDate}"')
    dfb = pd.read_sql(sql, engine)
    dfb.rename(columns={'pe_ttm': 'ttmpeb', 'trade_date': 'date'}, inplace=True)
    dfb.set_index('date', inplace=True)
    # print(dfb)

    df = pd.merge(dfa, dfb, left_index=True, right_index=True)
    # print(df)
    cnt = len(df)
    trainCnt = int(cnt * .8)
    xTrain = np.array(df.ttmpea.to_list()[:trainCnt])
    xTrain = xTrain[:, np.newaxis]
    xTest = np.array(df.ttmpea.to_list()[trainCnt:])
    xTest = xTest[:, np.newaxis]
    yTrain = df.ttmpeb.to_list()[:trainCnt]
    yTest = df.ttmpeb.to_list()[trainCnt:]

    regr = linear_model.LinearRegression()
    regr.fit(xTrain, yTrain)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    # print('截距 regr.intercept_:', regr.intercept_)
    # print('系数 regr.coef_:', regr.coef_)
    if plot:
        plotPairs(df, intercept, coef)

    # 使用测试集进行评分
    yPredict = regr.predict(xTest)
    score = r2_score(yTest, yPredict)
    print('score:', score)
    return score


# def findPairs1(ts_codea, ts_codeb, startDate='20090101', endDate='20191231'):
#     """两支股票的TTMPE是否存在协整关系
#     时间区间为20090101至20191231
#     1.两只股票TTMPE的折线图
#     2.两只股票TTMPE的散点图
#     3.两只股票的线性回归
#     4.根据线性回归结果计算残差，返回残差的ADF检验结果
#     """
#     sql = (f'select date, ttmpe from klinestock where ts_code="{ts_codea}"'
#            f' and date>="{startDate}" and date<="{endDate}"')
#     dfa = pd.read_sql(sql, engine)
#     dfa.rename(columns={'ttmpe': 'ttmpea'}, inplace=True)
#     dfa.set_index('date', inplace=True)
#     # print(dfa)
#
#     sql = (f'select date, ttmpe from klinestock where ts_code="{ts_codeb}"'
#            f' and date>="{startDate}" and date<="{endDate}"')
#     dfb = pd.read_sql(sql, engine)
#     dfb.rename(columns={'ttmpe': 'ttmpeb'}, inplace=True)
#     dfb.set_index('date', inplace=True)
#     # print(dfb)
#
#     df = pd.merge(dfa, dfb, left_index=True, right_index=True)
#     # print(df)
#
#     """
#     fig = plt.figure(figsize=(10, 5))
#     gs = gridspec.GridSpec(1, 2)
#     ax1 = plt.subplot(gs[0, 0])
#     ax2 = plt.subplot(gs[0, 1])
#     # 绘制拆线图
#     ax1.plot(dfa.index, dfa.ttmpea, color='blue', label=ts_codea)
#     ax1.plot(dfb.index, dfb.ttmpeb, color='yellow', label=ts_codeb)
#     ax1.legend()
#     # 设置X轴的刻度间隔
#     # 可选:YearLocator,年刻度; MonthLocator,月刻度; DayLocator,日刻度
#     ax1.xaxis.set_major_locator(YearLocator())
#     # 设置X轴主刻度的显示格式
#     ax1.xaxis.set_major_formatter(DateFormatter('%Y'))
#     # 设置鼠标悬停时，在左下显示的日期格式
#     ax1.fmt_xdata = mdates.DateFormatter('%Y-%m-%d')
#     # 自动调整X轴标签角度
#     fig.autofmt_xdate()
#
#     # 绘制散点图
#     ax2.scatter(df.ttmpea, df.ttmpeb)
#
#     plt.grid(True)
#     """
#
#     cnt = len(df)
#     trainCnt = int(cnt * .8)
#     xTrain = np.array(df.ttmpea.to_list()[:trainCnt])
#     xTrain = xTrain[:, np.newaxis]
#     xTest = np.array(df.ttmpea.to_list()[trainCnt:])
#     xTest = xTest[:, np.newaxis]
#     yTrain = df.ttmpeb.to_list()[:trainCnt]
#     yTest = df.ttmpeb.to_list()[trainCnt:]
#
#     regr = linear_model.LinearRegression()
#     regr.fit(xTrain, yTrain)
#     # print('截距 regr.intercept_:', regr.intercept_)
#     # print('系数 regr.coef_:', regr.coef_)
#     x1 = int(min(df.ttmpea))
#     x2 = int(max(df.ttmpea) + 1)
#     X = [x1, x2]
#     y1 = regr.intercept_ + x1 * regr.coef_[0]
#     y2 = regr.intercept_ + x2 * regr.coef_[0]
#     Y = [y1, y2]
#     # ax2.plot(X, Y, color='r')
#     # print('X:', X)
#     # print('Y:', Y)
#
#     # plt.show()
#
#     # 使用测试集进行评分
#     yPredict = regr.predict(xTest)
#     score = r2_score(yTest, yPredict)
#     # print('score:', score)
#     return score


def diabetesTest():
    # Load the diabetes dataset
    diabetes_X, diabetes_y = datasets.load_diabetes(return_X_y=True)

    # Use only one feature
    diabetes_X = diabetes_X[:, np.newaxis, 2]

    # Split the data into training/testing sets
    diabetes_X_train = diabetes_X[:-20]
    diabetes_X_test = diabetes_X[-20:]

    # Split the targets into training/testing sets
    diabetes_y_train = diabetes_y[:-20]
    diabetes_y_test = diabetes_y[-20:]

    # Create linear regression object
    regr = linear_model.LinearRegression()

    # Train the model using the training sets
    regr.fit(diabetes_X_train, diabetes_y_train)

    # Make predictions using the testing set
    diabetes_y_pred = regr.predict(diabetes_X_test)

    # The coefficients
    print('Coefficients: \n', regr.coef_)
    # The mean squared error
    print('Mean squared error: %.2f'
          % mean_squared_error(diabetes_y_test, diabetes_y_pred))
    # The coefficient of determination: 1 is perfect prediction
    print('Coefficient of determination: %.2f'
          % r2_score(diabetes_y_test, diabetes_y_pred))

    # Plot outputs
    plt.scatter(diabetes_X_test, diabetes_y_test, color='black')
    plt.plot(diabetes_X_test, diabetes_y_pred, color='blue', linewidth=3)

    plt.xticks(())
    plt.yticks(())

    plt.show()


def linearAll():
    """
    沪深300成份股中两支股票间的协整关系
    :return:
    """
    pro = ts.pro_api()
    indexDf = pro.index_weight(index_code='399300.SZ', start_date='20200301')
    codeList = indexDf.con_code.to_list()
    nameDf = pd.DataFrame(readStockList(), columns=['id', 'name'])
    # startDate = '20140101'
    # endDate = '20191231'
    # codeList = codeList[:6]

    total = len(codeList) * (len(codeList) - 1) // 2
    cnt = 0
    # resultList = []
    aList = []
    bList = []
    scoreList = []
    errorList = []
    for i in range(len(codeList) - 1):
        for j in range(i + 1, len(codeList)):
            cnt += 1
            ts_codea = codeList[i]
            ts_codeb = codeList[j]
            print(f'第{cnt}个，共{total}个：', ts_codea, ts_codeb)
            try:
                result = findPairs(ts_codea, ts_codeb)
            except Exception as e:
                print(f'ERROR {ts_codea}-{ts_codeb}:', e)
                errorList.append(f'ERROR {ts_codea}-{ts_codeb}: {e}')
            else:
                # resultList.append([ts_codea, ts_codeb, result])
                aList.append(ts_codea)
                bList.append(ts_codeb)
                scoreList.append(result)
    resultDf = pd.DataFrame({'ida': aList, 'idb': bList, 'score': scoreList})
    resultDf = pd.merge(resultDf, nameDf,
                        left_on='ida', right_on='id', how='left')
    resultDf.rename(columns={'name': 'namea'}, inplace=True)
    resultDf = pd.merge(resultDf, nameDf,
                        left_on='idb', right_on='id', how='left')
    resultDf.rename(columns={'name': 'nameb'}, inplace=True)
    resultDf = resultDf[['ida', 'namea', 'idb', 'nameb', 'score']]
    resultDf.sort_values('score', ascending=False, inplace=True)
    resultDf.to_excel('score.xlsx')

    stra = '\n'.join(errorList)
    f = open('error.log', 'w+')
    f.write(stra)
    f.close()


def linearProfits():
    """一定时期利润进行线性回归"""
    fig = plt.figure(figsize=(10, 10))
    stocks = readStockList()
    # stocks = stocks[455 + 1398:]
    intercept = []
    coef = []
    chas = []
    startQuarter = 20091
    cnt = len(stocks)
    cur = 1
    for ts_code in stocks.ts_code:
        # ts_code = '002161.SZ'
        _intercept, _coef, cha = _linearProfits(ts_code, startQuarter,
                                                fig)
        if _intercept is not None:
            print(f'{cur}/{cnt} {ts_code} 截距: {round(_intercept, 2)}'
                  f' 系数: {round(_coef, 2)}'
                  f' 平均残差率: {round(cha, 2)}')
        else:
            print(f'{cur}/{cnt} {ts_code} 无数据')
        intercept.append(_intercept)
        coef.append(_coef)
        chas.append(cha)
        cur += 1

    stocks['intercept'] = intercept
    stocks['coef'] = coef
    stocks['cha'] = chas
    stocks.to_excel('../data/profits_linear_regression.xlsx')


def series_to_supervised(data, n_in=1, n_out=1, dropnan=True):
    """
    将时间序列重构为监督学习数据集.
    参数:
        data: 观测值序列，类型为列表或Numpy数组。
        n_in: 输入的滞后观测值(X)长度。
        n_out: 输出观测值(y)的长度。
        dropnan: 是否丢弃含有NaN值的行，类型为布尔值。
    返回值:
        经过重组后的Pandas DataFrame序列.
    """
    n_vars = 1
    if isinstance(data, np.ndarray) and len(data.shape) >= 2:
        n_vars = data.shape[1]
    df = pd.DataFrame(data)
    cols, names = list(), list()
    # 输入序列 (t-n, ... t-1)
    for i in range(n_in, 0, -1):
        cols.append(df.shift(i))
        names += [('var%d(t-%d)' % (j + 1, i)) for j in range(n_vars)]
    # 预测序列 (t, t+1, ... t+n)
    for i in range(0, n_out):
        cols.append(df.shift(-i))
        if i == 0:
            names += [('var%d(t)' % (j + 1)) for j in range(n_vars)]
        else:
            names += [('var%d(t+%d)' % (j + 1, i)) for j in range(n_vars)]
    # 将列名和数据拼接在一起
    agg = pd.concat(cols, axis=1)
    agg.columns = names
    # 丢弃含有NaN值的行
    if dropnan:
        agg.dropna(inplace=True)
    return agg.iloc[:, 0:n_in].values, agg.iloc[:, -1].values


def trans_series():
    """将单自变量时间序列转换为3自变量监督学习序列后线性回归"""
    ts_code = '000651.SZ'
    startDate = '20090101'
    endDate = '20191231'
    sql = (f'select dt_netprofit_yoy inc from fina_indicator'
           f' where ts_code="{ts_code}" and end_date>="{startDate}"'
           f' and end_date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    df.dropna(inplace=True)
    results = _linear_trans(df.inc.values)
    print(f'转换序列拟合结果')
    print(f'intercept:{results["intercept"]}')
    print(f'coef:{results["coef"]}')
    print(f'r2:{results["r2"]}')
    print('-' * 80)

    results = _linear(df.inc.values)
    print(f'非转换序列拟合结果')
    print(f'intercept:{results["intercept"]}')
    print(f'coef:{results["coef"]}')
    print(f'r2:{results["r2"]}')


def profits_inc_lof(ts_code, startDate='20150101', endDate='20191231'):
    """用非监督学习方法检测利润增长率和历年ttm利润是否有离群值
    绘图比较，原始数据，离群检测结果，幂转换结果
    """
    sql = (f'select dt_netprofit_yoy inc from fina_indicator'
           f' where ts_code="{ts_code}" and end_date>="{startDate}"'
           f' and end_date<="{endDate}"')
    df = pd.read_sql(sql, engine)
    df.dropna(inplace=True)
    print(df)

    # a = np.ones(10)
    # a = np.append(a, 1000)
    # df = pd.DataFrame({'inc': a})

    param_n = 10
    model = LocalOutlierFactor(n_neighbors=param_n)
    data = df.inc.values.reshape(-1, 1)
    # data = data.reshape(-1, 1)
    y_predict = model.fit_predict(data)
    print(data)
    print('y_predict:\n', y_predict)
    print('异常性得分:')
    print(model.negative_outlier_factor_)

    transer = PowerTransformer()
    data = transer.fit_transform(data)
    model1 = LocalOutlierFactor(n_neighbors=param_n)
    y_predict = model1.fit_predict(data)
    print(data)
    print('y_predict:\n', y_predict)
    print('异常性得分:')
    print(model.negative_outlier_factor_)

    # numpy计算四分位数
    # q1, q2, q3 = np.percentile(a, [25, 50, 75])

    ax = plt.subplot()
    colors = np.array(['r', 'g'])
    ax.scatter(df.index, df.inc, color=colors[(y_predict + 1) // 2])
    plt.show()


def zscoretest():
    b = np.array([[0.3148, 0.0478, 0.6243, 0.4608],
                  [0.7149, 0.0775, 0.6072, 0.9656],
                  [0.6341, 0.1403, 0.9759, 0.4064],
                  [0.5918, 0.6948, 0.904, 0.3721],
                  [0.0921, 0.2481, 0.1188, 0.1366]])

    resultdefault = zscore(b)
    result = zscore(b, axis=None)
    result0 = zscore(b, axis=0)
    result1 = zscore(b, axis=1)
    print('resultdefault:\n', resultdefault)
    print('result:\n', result)
    print('result0:\n', result0)
    print('result1:\n', result1)
    print('=' * 80)

    # for row in b:


def _normaltest(data):
    result = normaltest(data)
    # print('正态检验结果:', result)
    return round(result[1], 2)


def stocknormaltest(ts_code, startDate='20090101', endDate='20191231'):
    """正态分布检验
    ttmpe的分布情况，计算均值及+/-2倍标准差
    取对数后的分布情况，计算均值及+/-2倍标准差，还原后的数值
    """
    sql = (f'select pe_ttm from daily_basic where ts_code="{ts_code}"'
           f' and trade_date>="{startDate}" and trade_date<="{endDate}"')

    result = engine.execute(sql).fetchall()
    data = np.reshape(result, -1)
    if len(data) < 1000:
        return None, None
    # mu = np.mean(data)
    # sigma = np.std(data)
    p1 = _normaltest(data)
    # print(data)
    # print(f'均值:{mu:.2f}, 低值:{mu - 2 * sigma:.2f}, 高值:{mu + 2 * sigma:.2f}')

    # gs = gridspec.GridSpec(1, 2, hspace=0.8, wspace=0.65)
    # ax1 = plt.subplot(gs[0, 0])
    # n, bins, patches = ax1.hist(data, bins=20)
    # ax1twin = ax1.twinx()
    # y = norm.pdf(bins, mu, sigma)
    # ax1twin.plot(bins, y, 'r--')

    data1 = [log(i) for i in data]
    # mu1 = np.mean(data1)
    # sigma1 = np.std(data1)
    p2 = _normaltest(data1)
    # print(f'mu1={mu1:.2f}, sigma1={sigma1:.2f}')
    # print(f'对数均值:{mu1:.2f}, ',
    #       f'低值:{mu1 - 2 * sigma1:.2f}, ',
    #       f'高值:{mu1 + 2 * sigma1:.2f}')
    # print(f'还原均值:{exp(mu1):.2f}, ',
    #       f'低值:{exp(mu1) - 2 * exp(sigma1):.2f}, '
    #       f'高值:{exp(mu1) + 2 * exp(sigma1):.2f}')
    # print(f'还原均值:{exp(mu1):.2f}, ',
    #       f'低值:{exp(mu1 - 2 * sigma1):.2f}, '
    #       f'高值:{exp(mu1 + 2 * sigma1):.2f}')
    # ax2 = plt.subplot(gs[0, 1])
    # n, bins, patches = ax2.hist(data1, bins=20)
    # n, bins, patches = ax.hist(data, cumulative=True)
    # ax2twin = ax2.twinx()
    # y = norm.pdf(bins, mu1, sigma1)
    # ax2twin.plot(bins, y, 'r--')

    # plt.show()
    return p1, p2


def _testoutlier(data):
    """按第一、第三个四分位加减差值过滤奇异值
    返回:
    过滤后的数列 data1
    最低值 low
    第一个四分位数 q1
    第三个四分位数 q3
    最高值 high
    均值 mean
    标准差 std
    减1倍标准差 down
    加1倍标准差 up"""
    if not isinstance(data, pd.Series):
        data = pd.Series(data)
    q1 = data.quantile(.25)
    q3 = data.quantile(.75)
    diff = q3 - q1
    low = q1 - diff * 1.5
    high = q3 + diff * 1.5
    # noinspection PyTypeChecker
    data1 = data[(data >= low) & (data <= high)]
    cnt = len(data1)
    mean = np.mean(data1)
    std = np.std(data1)
    down = mean - std
    up = mean + std
    dic = dict(cnt=cnt, low=low, q1=q1, q3=q3, high=high,
               mean=mean, std=std, down=down, up=up)
    return data1, dic


def testoutlier(end_date=2017):
    """迭代过滤奇异值"""
    sql = (f'select dt_netprofit_yoy from fina_indicator '
           f' where end_date="{end_date}1231"')
    result = engine.execute(sql).fetchall()
    data = np.reshape(result, -1)
    results = []
    for i in range(10):
        data, dic = _testoutlier(data)
        results.append(dic)

    df = pd.DataFrame(results)
    print(df)
    df.to_excel('../data/testoutlier.xlsx')
    plt.hist(data, bins=30)
    plt.show()


if __name__ == '__main__':
    pass
    # ts_codea = '601985'
    # ts_codeb = '600170'
    # startDate = '20140101'
    # endDate = '20191231'
    # findPairs(ts_codea, ts_codeb, startDate=startDate)
    # linearAll()
    # diabetesTest()

    # linearProfitInc()

    # 比较最小二乘与HuberRegressor的区别
    # ts_code = '600340.SH'
    # startDate = '20150331'
    # endDate = '20191231'
    # result = _linearProfitIncDouble(ts_code=ts_code,
    #                                 startDate=startDate, endDate=endDate)
    # print(result)

    # 利润增长率奇异值检验
    # profits_inc_lof('000651.SZ')

    # zscoretest()

    # ts_code = '000333.SZ'
    # startDate = '20140101'
    # endDate = '20191231'
    # stocknormaltest(ts_code, startDate=startDate, endDate=endDate)

    # stocks = readStockList()
    # stocks = stocks[:20]
    # results = []
    # for stock in stocks.ts_code:
    #     try:
    #         print(f'ts_code: {stock}')
    #         p1, p2 = stocknormaltest(stock)
    #         d = dict(ts_code=stock, p1=p1, p2=p2)
    #         results.append(d)
    #     except Exception as e:
    #         print(f'ts_code:{stock} ', e)
    #
    # df = pd.DataFrame(results)
    # df.to_excel('../data/normaltest.xlsx')

    testoutlier(2018)

    # TODO: 选股的基本判断条件
    # 1.利润稳步增长，表现近5年或10年增长水平无异常波动，如何界定异常待确定
    # 2.考虑PE是否具正态分布，该条暂不作为必要条件
    #   正态分布条件下，低于2倍标准差为买入点，高出2倍标准差为卖出点
    # 3.试验一：绘制沪深300指数收盘价，上下2倍标准差为通道
    # 4.试验二：绘制沪深300指数PE，上下2倍标准差为通道
    # 5.检验正态分布的条件与方法
    # 6.z-score应用于PE和利润增长分析
