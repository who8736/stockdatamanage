"""

"""
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
from sqlrw import readStockListFromSQL


def studyTime():
    examDict={
        '学习时间':[0.50,0.75,1.00,1.25,1.50,1.75,1.75,2.00,2.25,
                2.50,2.75,3.00,3.25,3.50,4.00,4.25,4.50,4.75,5.00,5.50],
        '分数':    [10,  22,  13,  43,  20,  22,  33,  50,  62,
                  48,  55,  75,  62,  73,  81,  76,  64,  82,  90,  93]
    }
    examOrderDict=OrderedDict(examDict)
    exam=pd.DataFrame(examOrderDict)

    print(exam)

    #从dataframe中把标签和特征导出来
    exam_X = exam['学习时间']
    exam_Y = exam['分数']

    #绘制散点图，得出结果后记得注释掉以下4行代码
    plt.scatter(exam_X, exam_Y, color = 'green')
    #设定X,Y轴标签和title
    plt.ylabel('Scores')
    plt.xlabel('Times(h)')
    plt.title('Exam Data')

    plt.show()


def linearRegressionTest():
    pass


def findPairs(stockIDa, stockIDb, startDate='20090101', endDate='20191231'):
    """两支股票的TTMPE是否存在协整关系
    时间区间为20090101至20191231
    1.两只股票TTMPE的折线图
    2.两只股票TTMPE的散点图
    3.两只股票的线性回归
    4.根据线性回归结果计算残差，返回残差的ADF检验结果
    """
    sql = (f'select date, ttmpe from klinestock where stockid="{stockIDa}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    dfa.rename(columns={'ttmpe': 'ttmpea'}, inplace=True)
    dfa.set_index('date', inplace=True)
    # print(dfa)

    sql = (f'select date, ttmpe from klinestock where stockid="{stockIDb}"'
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
    ax1.plot(dfa.index, dfa.ttmpea, color='blue', label=stockIDa)
    ax1.plot(dfb.index, dfb.ttmpeb, color='yellow', label=stockIDb)
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


def findPairs1(stockIDa, stockIDb, startDate='20090101', endDate='20191231'):
    """两支股票的TTMPE是否存在协整关系
    时间区间为20090101至20191231
    1.两只股票TTMPE的折线图
    2.两只股票TTMPE的散点图
    3.两只股票的线性回归
    4.根据线性回归结果计算残差，返回残差的ADF检验结果
    """
    sql = (f'select date, ttmpe from klinestock where stockid="{stockIDa}"'
           f' and date>="{startDate}" and date<="{endDate}"')
    dfa = pd.read_sql(sql, engine)
    dfa.rename(columns={'ttmpe': 'ttmpea'}, inplace=True)
    dfa.set_index('date', inplace=True)
    # print(dfa)

    sql = (f'select date, ttmpe from klinestock where stockid="{stockIDb}"'
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
    ax1.plot(dfa.index, dfa.ttmpea, color='blue', label=stockIDa)
    ax1.plot(dfb.index, dfb.ttmpeb, color='yellow', label=stockIDb)
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
    startDate = '20140101'
    endDate = '20191231'
    pro = ts.pro_api()
    df1 = pro.index_weight(index_code='399300.SZ', start_date='20200301')
    codeList = df1.con_code.str[:6].to_list()
    # codeList = codeList[:6]

    total = (1 + len(codeList) - 1) * ((len(codeList) - 1) / 2)
    cnt = 0
    # resultList = []
    aList = []
    bList = []
    scoreList = []
    for i in range(len(codeList) - 1):
        for j in range(i + 1, len(codeList)):
            cnt += 1
            stockIDa = codeList[i]
            stockIDb = codeList[j]
            print(f'第{cnt}个，共{total}个：', stockIDa, stockIDb)
            try:
                result = findPairs1(stockIDa, stockIDb)
            except Exception as e:
                print(f'ERROR {stockIDa}-{stockIDb}:', e)
            else:
                # resultList.append([stockIDa, stockIDb, result])
                aList.append(stockIDa)
                bList.append(stockIDb)
                scoreList.append(result)
    df = pd.DataFrame({'a': aList, 'b': bList, 'score': scoreList})
    df.to_excel('score.xlsx')


if __name__ == '__main__':
    pass
    stockIDa = '601985'
    stockIDb = '601186'
    startDate = '20140101'
    endDate = '20191231'
    findPairs(stockIDa, stockIDb, startDate=startDate)
    # linearAll()
    # diabetesTest()
