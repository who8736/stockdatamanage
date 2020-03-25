"""

"""
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


def linearRegressionTest(ts_code, startQuarter, fig):
    """
    对单一自变量进行线性回归，返回（截距， 系数，平均方差）
    :return:
    """
    pass
    sql = (f'SELECT ttmprofits FROM stockdata.ttmprofits'
           f' where ts_code="{ts_code}" and date>={startQuarter};')
    result = engine.execute(sql).fetchall()
    cnt = len(result)
    if cnt<10:
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


def findPairs(ts_codea, ts_codeb, startDate='20090101', endDate='20191231'):
    """两支股票的TTMPE是否存在协整关系
    时间区间为20090101至20191231
    1.两只股票TTMPE的折线图
    2.两只股票TTMPE的散点图
    3.两只股票的线性回归
    4.根据线性回归结果计算残差，返回残差的ADF检验结果
    """
    sql = (f'select date, ttmpe from klinestock where ts_code="{ts_codea}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    dfa.rename(columns={'ttmpe': 'ttmpea'}, inplace=True)
    dfa.set_index('date', inplace=True)
    # print(dfa)

    sql = (f'select date, ttmpe from klinestock where ts_code="{ts_codeb}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfb = pd.read_sql(sql, engine)
    dfb.rename(columns={'ttmpe': 'ttmpeb'}, inplace=True)
    dfb.set_index('date', inplace=True)
    # print(dfb)

    df = pd.merge(dfa, dfb, left_index=True, right_index=True)
    # print(df)

    fig = plt.figure(figsize=(10, 5))
    gs = gridspec.GridSpec(1, 2)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[0, 1])
    # 绘制拆线图
    ax1.plot(dfa.index, dfa.ttmpea, color='blue', label=ts_codea)
    ax1.plot(dfb.index, dfb.ttmpeb, color='yellow', label=ts_codeb)
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
    # print('截距 regr.intercept_:', regr.intercept_)
    # print('系数 regr.coef_:', regr.coef_)
    x1 = int(min(df.ttmpea))
    x2 = int(max(df.ttmpea) + 1)
    X = [x1, x2]
    y1 = regr.intercept_ + x1 * regr.coef_[0]
    y2 = regr.intercept_ + x2 * regr.coef_[0]
    Y = [y1, y2]
    ax2.plot(X, Y, color='r')
    # print('X:', X)
    # print('Y:', Y)

    plt.show()

    # 使用测试集进行评分
    yPredict = regr.predict(xTest)
    score = r2_score(yTest, yPredict)
    print('score:', score)
    return score


def findPairs1(ts_codea, ts_codeb, startDate='20090101', endDate='20191231'):
    """两支股票的TTMPE是否存在协整关系
    时间区间为20090101至20191231
    1.两只股票TTMPE的折线图
    2.两只股票TTMPE的散点图
    3.两只股票的线性回归
    4.根据线性回归结果计算残差，返回残差的ADF检验结果
    """
    sql = (f'select date, ttmpe from klinestock where ts_code="{ts_codea}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    dfa.rename(columns={'ttmpe': 'ttmpea'}, inplace=True)
    dfa.set_index('date', inplace=True)
    # print(dfa)

    sql = (f'select date, ttmpe from klinestock where ts_code="{ts_codeb}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfb = pd.read_sql(sql, engine)
    dfb.rename(columns={'ttmpe': 'ttmpeb'}, inplace=True)
    dfb.set_index('date', inplace=True)
    # print(dfb)

    df = pd.merge(dfa, dfb, left_index=True, right_index=True)
    # print(df)

    """
    fig = plt.figure(figsize=(10, 5))
    gs = gridspec.GridSpec(1, 2)
    ax1 = plt.subplot(gs[0, 0])
    ax2 = plt.subplot(gs[0, 1])
    # 绘制拆线图
    ax1.plot(dfa.index, dfa.ttmpea, color='blue', label=ts_codea)
    ax1.plot(dfb.index, dfb.ttmpeb, color='yellow', label=ts_codeb)
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
    """

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
    # print('截距 regr.intercept_:', regr.intercept_)
    # print('系数 regr.coef_:', regr.coef_)
    x1 = int(min(df.ttmpea))
    x2 = int(max(df.ttmpea) + 1)
    X = [x1, x2]
    y1 = regr.intercept_ + x1 * regr.coef_[0]
    y2 = regr.intercept_ + x2 * regr.coef_[0]
    Y = [y1, y2]
    # ax2.plot(X, Y, color='r')
    # print('X:', X)
    # print('Y:', Y)

    # plt.show()

    # 使用测试集进行评分
    yPredict = regr.predict(xTest)
    score = r2_score(yTest, yPredict)
    # print('score:', score)
    return score


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
                result = findPairs1(ts_codea, ts_codeb)
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


if __name__ == '__main__':
    pass
    ts_codea = '601985'
    ts_codeb = '600170'
    startDate = '20140101'
    endDate = '20191231'
    # findPairs(ts_codea, ts_codeb, startDate=startDate)
    # linearAll()
    # diabetesTest()

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
        _intercept, _coef, cha = linearRegressionTest(ts_code, startQuarter,
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
