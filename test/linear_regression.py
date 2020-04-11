"""

"""
import os
from math import sqrt

from collections import OrderedDict
import matplotlib.dates as mdates
import matplotlib.pyplot as plt  # @IgnorePep8
import matplotlib.gridspec as gridspec
from matplotlib.dates import DateFormatter
from matplotlib.dates import YearLocator  # @IgnorePep8
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
import tushare as ts

from sqlconn import engine
from sqlrw import readStockList
from sqlrw import getStockName


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
    return (mean, result)


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


def linearPlot(data, plot=False, title=None, filename=None):
    """绘图函数，接受_linear产生的数据绘制原始数据和一阶差分的散点图和回归直线"""
    pass
    cnt = len(data)
    x = list(range(cnt))

    # 绘图
    # ax = plt.subplot()
    fig = plt.figure(figsize=(10, 5))
    gs = gridspec.GridSpec(1, 2)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[0, 1])

    # 拟合
    trainx = np.array(x)
    trainx = trainx[:, np.newaxis]
    regr = linear_model.LinearRegression()
    regr.fit(trainx, data)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    y = [intercept + coef * i for i in x]
    ax1.scatter(x, data)
    ax1.plot(x, y, color='r')

    diff_data = np.diff(data)
    diff_cnt = len(diff_data)
    diff_x = list(range(diff_cnt))
    diff_trainx = np.array(diff_x)
    diff_trainx = diff_trainx[:, np.newaxis]
    regr = linear_model.LinearRegression()
    regr.fit(diff_trainx, diff_data)
    intercept = regr.intercept_
    coef = regr.coef_[0]
    diff_y = [intercept + coef * i for i in diff_x]
    ax2.scatter(diff_x, diff_data)
    ax2.plot(diff_x, diff_y, color='r')

    plt.title(title, fontproperties='simsun', fontsize=26)
    if plot:
        plt.show()
    if filename is not None:
        plt.savefig(os.path.join('../data/linear_img', filename))
    plt.close()


def linearProfitInc():
    stocks = readStockList()
    # stocks = stocks[456:]
    startDate = '20150331'
    endDate = '20191231'
    filename = '../data/profits_inc_linear.xlsx'

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
    return (result_linear, result_huber)


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
        return (None, None, None)
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
        return (None, None, None)

    # 绘图
    # ax = plt.subplot()
    ax = fig.add_subplot()
    ax.scatter(x, y)
    ax.plot(x, Y, color='r')
    name = getStockName(ts_code)
    plt.title(f'{ts_code} {name}', fontproperties='simsun', fontsize=26)
    # plt.show()
    filename = f'../data/linear_img/{ts_code[:6]}.png'
    plt.savefig(filename)
    plt.clf()

    return (intercept, coef, cha)


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
    startDate = '20140101'
    endDate = '20191231'
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
    return (agg.iloc[:, 0:n_in].values, agg.iloc[:, -1].values)


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

    # 将单变量时间序列转换为监督学习数据序列
    # data = list(range(100))
    # df = series_to_supervised(data, n_in=3)
    # x, y= series_to_supervised(data, n_in=3)
    # print(x)
    # print('-' * 80)
    # print(y)

    # 将单自变量时间序列转换为3自变量监督学习序列后线性回归
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
