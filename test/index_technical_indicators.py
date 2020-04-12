# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/4/11 15:42

"""计算指数的技术分析指标，并按技术指标做线性回归或分类"""

import pandas as pd
import numpy as np
from sklearn.linear_model import (LinearRegression, Ridge, Lasso,
                                    RidgeCV, LassoCV)
from sklearn.svm import LinearSVC
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
from sklearn.utils import check_X_y
import talib as ta
from talib import MA_Type

from sqlconn import engine


def getdata(ts_code, startDate='20090101', endDate='20191231',
            type='linear'):
    """取得指数数据和特征值，返回训练集和测试集
    type: linear 线性归回类数据， classifier 分类器数据
    """
    # 基础数据
    sql = (f'SELECT trade_date, close, open, high, low, pct_chg, vol volume'
           f' FROM stockdata.index_daily where ts_code = "{ts_code}"'
           f' and trade_date>="{startDate}" and trade_date<="{endDate}";')
    df = pd.read_sql(sql, engine, index_col='trade_date')

    dftalib = ta_indicators(df)
    df = pd.merge(df, dftalib, left_index=True, right_index=True)

    # 均线
    df['ma5'] = ta.SMA(df.close, timeperiod=5)
    df['ma10'] = ta.SMA(df.close, timeperiod=10)
    df['ma20'] = ta.SMA(df.close, timeperiod=20)
    df['maflag5_10up'] = ((df.ma5.shift(1) < df.ma10.shift(1)) &
                          (df.ma5 > df.ma10))
    df['maflag5_10down'] = ((df.ma5.shift(1) > df.ma10.shift(1)) &
                            (df.ma5 < df.ma10))
    df['maflag10_20up'] = ((df.ma10.shift(1) < df.ma20.shift(1)) &
                           (df.ma10 > df.ma20))
    df['maflag10_20down'] = ((df.ma10.shift(1) > df.ma20.shift(1)) &
                             (df.ma10 < df.ma20))

    if type == 'linear':
        # 以次日涨跌幅为目标
        df['y'] = df.pct_chg.shift(-1)
    elif type == 'classifier':
        # 次日是否涨跌为目标
        df['y'] = df.pct_chg.shift(-1) > 0
    print('data shape:', df.shape)
    print(df)

    # 返回训练集与测试集
    df.dropna(inplace=True)
    x = df.iloc[:, :-1]
    y = df.iloc[:, -1]
    # print('y shape:', y.shape)
    # print(y.values)
    x, y = check_X_y(x, y)
    return train_test_split(x, y, test_size=0.2)


def ta_indicators(df):
    dfa = pd.DataFrame()
    # BBANDS               Bollinger Bands
    # 函数名：BBANDS
    # 名称： 布林线指标
    # 简介：其利用统计原理，求出股价的标准差及其信赖区间，从而确定股价的波动
    # 范围及未来走势，利用波带显示股价的安全高低价位，因而也被称为布林带。
    # 分析和应用： 百度百科 同花顺学院
    # upperband, middleband, df.lowerband = BBANDS(
    # df.close, timeperiod=5, nbdevup=2, nbdevdn=2, matype=0)

    dfa["BBANDS_upper"], dfa["BBANDS_middle"], dfa["BBANDS_lower"] = ta.BBANDS(
        df.close, matype=MA_Type.T3)

    # DEMA - Double Exponential Moving Average 双移动平均线
    # 函数名：DEMA
    # 名称： 双移动平均线
    # 简介：两条移动平均线来产生趋势信号，较长期者用来识别趋势，较短期者用来选择时机。正是两条平均线及价格三者的相互作用，才共同产生了趋势信号。
    # real = DEMA(df.close, timeperiod=30)
    dfa["DEMA"] = ta.DEMA(df.close, timeperiod=30)
    # MA - Moving average 移动平均线
    # 函数名：MA
    # 名称： 移动平均线
    # 简介：移动平均线，Moving Average，简称MA，原本的意思是移动平均，
    # 由于我们将其制作成线形，所以一般称之为移动平均线，简称均线。
    # 它是将某一段时间的收盘价之和除以该周期。 比如日线MA5指5天内的收盘价除以5 。
    # real = MA(df.close, timeperiod=30, matype=0)
    dfa["MA"] = ta.MA(df.close, timeperiod=30, matype=0)
    # EMA和MACD
    # 调用ta计算6日指数移动平均线的值
    dfa['EMA12'] = ta.EMA(np.array(df.close), timeperiod=6)
    dfa['EMA26'] = ta.EMA(np.array(df.close), timeperiod=12)
    # KAMA - Kaufman Adaptive Moving Average 考夫曼的自适应移动平均线
    # 函数名：KAMA
    # 名称： 考夫曼的自适应移动平均线
    # 简介：短期均线贴近价格走势，灵敏度高，但会有很多噪声，产生虚假信号；
    # 长期均线在判断趋势上一般比较准确 ，但是长期均线有着严重滞后的问题。
    # 我们想得到这样的均线，当价格沿一个方向快速移动时，短期的移动平均线是
    # 最合适的；当价格在横盘的过程中，长期移动平均线是合适的。
    # NOTE: The KAMA function has an unstable period.
    dfa["KAMA"] = ta.KAMA(df.close, timeperiod=30)

    # SMA （简单移动平均线）
    # （参数1：收盘价序列，参数2：时间周期（均线的计算长度 即 几日均线））
    # SAREXT - Parabolic SAR - Extended
    # real = SAREXT(df.high, df.low, startvalue=0, offsetonreverse=0,
    # accelerationinitlong=0, accelerationlong=0, accelerationmaxlong=0,
    # accelerationinitshort=0, accelerationshort=0, accelerationmaxshort=0)
    # SMA - Simple Moving Average 简单移动平均线
    # 函数名：SMA
    # 名称： 简单移动平均线
    # 简介：移动平均线，Moving Average，简称MA，原本的意思是移动平均，
    # 由于我们将其制作成线形，所以一般称之为移动平均线，简称均线。
    # 它是将某一段时间的收盘价之和除以该周期。 比如日线MA5指5天内的收盘价除以5 。
    # 百度百科 同花顺学院
    # real = SMA(df.close, timeperiod=30)
    dfa['SMA'] = ta.SMA(df.close, timeperiod=30)
    # MIDPRICE - Midpoint Price over period
    dfa["MIDPOINT"] = ta.MIDPOINT(df.close, timeperiod=14)
    # SAR - Parabolic SAR 抛物线指标
    # 函数名：SAR
    # 名称： 抛物线指标
    # 简介：抛物线转向也称停损点转向，是利用抛物线方式，随时调整停损点位置
    # 以观察买卖点。由于停损点（又称转向点SAR）以弧形的方式移动，
    # 故称之为抛物线转向指标 。
    # real = SAR(df.high, df.low, acceleration=0, maximum=0)
    dfa["SAR"] = ta.SAR(df.high, df.low, acceleration=0, maximum=0)
    # T3 - Triple Exponential Moving Average (T3) 三重指数移动平均线
    # 函数名：T3
    # 名称：三重指数移动平均线
    # 简介：TRIX长线操作时采用本指标的讯号，长时间按照本指标讯号交易，
    # 获利百分比大于损失百分比，利润相当可观。 比如日线MA5指5天内的收盘价除以5 。
    # NOTE: The T3 function has an unstable period
    # real = T3(df.close, timeperiod=5, vfactor=0)
    dfa["T3"] = ta.T3(df.close, timeperiod=5, vfactor=0)
    # TEMA - Triple Exponential Moving Average
    # 函数名：TEMA（T3 区别？） 名称：三重指数移动平均线
    # real = TEMA(df.close, timeperiod=30)
    dfa["TEMA"] = ta.TEMA(df.close, timeperiod=30)
    # MIDPRICE - Midpoint Price over period
    # real = MIDPRICE(df.high, df.low, timeperiod=14)
    dfa["MIDPRICE"] = ta.MIDPRICE(df.high, df.low, timeperiod=14)
    # TRIMA - Triangular Moving Average
    dfa["TRIMA"] = ta.TRIMA(df.close, timeperiod=30)
    # dfa["SAREXT"]=ta.SAREXT(df.high, df.low, maximum=0)
    # SAREXT - Parabolic SAR - Extended
    dfa["SAREXT"] = ta.SAREXT(df.high, df.low, startvalue=0, offsetonreverse=0,
                              accelerationinitlong=0, accelerationlong=0,
                              accelerationmaxlong=0, accelerationinitshort=0,
                              accelerationshort=0, accelerationmaxshort=0)
    # WMA - Weighted Moving Average 移动加权平均法
    # 函数名：WMA
    # 名称：加权移动平均线
    # 简介：移动加权平均法是指以每次进货的成本加上原有库存存货的成本，
    # 除以每次进货数量与原有库存存货的数量之和，据以计算加权平均单位成本，
    # 以此为基础计算当月发出存货的成本和期末存货的成本的一种方法。
    dfa["WMA"] = ta.WMA(df.close, timeperiod=30)
    # Volatility Indicator Functions 波动率指标函数
    # ATR - Average True Range
    # 函数名：ATR
    # 名称：真实波动幅度均值
    # 简介：真实波动幅度均值（ATR)是以N天的指数移动平均数平均後的交易波动幅度。
    # 计算公式：一天的交易幅度只是单纯地 最大值 - 最小值。
    # 而真实波动幅度则包含昨天的收盘价，若其在今天的幅度之外：
    # 真实波动幅度 = max(最大值,昨日收盘价) − min(最小值,昨日收盘价) 真实波动
    # 幅度均值便是「真实波动幅度」的 N 日 指数移动平均数。
    # 特性：：
    # 波动幅度的概念表示可以显示出交易者的期望和热情。
    # 大幅的或增加中的波动幅度表示交易者在当天可能准备持续买进或卖出股票。
    # 波动幅度的减少则表示交易者对股市没有太大的兴趣。
    # NOTE: The ATR function has an unstable period.
    # real = ATR(df.high, df.low, df.close, timeperiod=14)
    # ATR（平均真实波幅）
    # （参数1：最高价序列，参数2：最低价序列，参数3：收盘价序列，参数4：时间周期）
    dfa["ATR"] = ta.ATR(df.high, df.low, df.close, timeperiod=14)

    # NATR - Normalized Average True Range
    # 函数名：NATR
    # 名称：归一化波动幅度均值
    # 简介：归一化波动幅度均值（NATR)是
    # NOTE: The NATR function has an unstable period.
    # real = NATR(df.high, df.low, df.close, timeperiod=14)
    dfa["NATR"] = ta.NATR(df.high, df.low, df.close, timeperiod=14)
    # TRANGE - True Range
    # 函数名：TRANGE
    # 名称：真正的范围
    # real = TRANGE(df.high, df.low, df.close)
    dfa["TRANGE"] = ta.TRANGE(df.high, df.low, df.close)
    # Volume Indicators 成交量指标

    # AD - Chaikin A/D Line 量价指标
    # 函数名：AD
    # 名称：Chaikin A/D Line 累积/派发线（Accumulation/Distribution Line）
    # 简介：Marc Chaikin提出的一种平衡交易量指标，以当日的收盘价位来估算成交流量
    # 用于估定一段时间内该证券累积的资金流量。
    # 计算公式：
    # 多空对比 = [（收盘价- 最低价） - （最高价 - 收盘价）] / （最高价 - 最低价)
    # 若最高价等于最低价： 多空对比 = （收盘价 / 昨收盘） - 1
    # 研判：
    # 1、A/D测量资金流向，向上的A/D表明买方占优势，而向下的A/D表明卖方占优势
    # 2、A/D与价格的背离可视为买卖信号，即底背离考虑买入，顶背离考虑卖出
    # 3、应当注意A/D忽略了缺口的影响，事实上，跳空缺口的意义是不能轻易忽略的
    # A/D指标无需设置参数，但在应用时，可结合指标的均线进行分析
    # real = AD(df.high, df.low, df.close, df.volume)
    dfa["AD"] = ta.AD(df.high, df.low, df.close, df.volume)
    # ADOSC - Chaikin A/D Oscillator
    # 函数名：ADOSC
    # 名称：Chaikin A/D Oscillator Chaikin震荡指标
    # 简介：将资金流动情况与价格行为相对比，检测市场中资金流入和流出的情况
    # 计算公式：fastperiod A/D - slowperiod A/D
    # 研判：
    # 1、交易信号是背离：看涨背离做多，看跌背离做空
    # 2、股价与90天移动平均结合，与其他指标结合
    # 3、由正变负卖出，由负变正买进
    # real = ADOSC(df.high, df.low, df.close, df.volume,
    # fastperiod=3, slowperiod=10)
    dfa["ADOSC"] = ta.ADOSC(df.high, df.low, df.close, df.volume, fastperiod=3,
                            slowperiod=10)
    # OBV - On Balance Volume
    # 函数名：OBV
    # 名称：On Balance Volume 能量潮
    # 简介：Joe Granville提出，通过统计成交量变动的趋势推测股价趋势
    # 计算公式：以某日为基期，逐日累计每日上市股票总成交量，
    # 若隔日指数或股票上涨 ，则基期OBV加上本日成交量为本日OBV。
    # 隔日指数或股票下跌， 则基期OBV减去本日成交量为本日OBV
    # 研判：
    # 1、以“N”字型为波动单位，一浪高于一浪称“上升潮”，下跌称“跌潮”；上升潮买进，跌潮卖出
    # 2、须配合K线图走势
    # 3、用多空比率净额法进行修正，但不知TA-Lib采用哪种方法
    # 计算公式： 多空比率净额= [（收盘价－最低价）－（最高价-收盘价）] ÷（ 最高价－最低价）×成交量
    # real = OBV(df.close, df.volume)
    dfa["OBV"] = ta.OBV(df.close, df.volume)
    # Cycle Indicator Functions
    # HT_DCPERIOD - Hilbert Transform - Dominant Cycle Period
    # 函数名：HT_DCPERIOD
    # 名称： 希尔伯特变换-主导周期
    # 简介：将价格作为信息信号，计算价格处在的周期的位置，作为择时的依据。
    # NOTE: The HT_DCPERIOD function has an unstable period.
    # real = HT_DCPERIOD(df.close)
    dfa["HT_DCPERIOD"] = ta.HT_DCPERIOD(df.close)
    # HT_DCPHASE - Hilbert Transform - Dominant Cycle Phase
    # 函数名：HT_DCPHASE
    # 名称： 希尔伯特变换-主导循环阶段
    # NOTE: The HT_DCPHASE function has an unstable period.
    # real = HT_DCPHASE(df.close)
    dfa["HT_DCPHASE"] = ta.HT_DCPHASE(df.close)

    # HT_PHASOR - Hilbert Transform - Phasor Components
    # 函数名：HT_DCPHASE
    # 名称： 希尔伯特变换-希尔伯特变换相量分量
    # NOTE: The HT_PHASOR function has an unstable period.
    # inphase, quadrature = HT_PHASOR(df.close)
    dfa["HT_PHASOR_inphase"], dfa["HT_PHASOR_quadrature"] = ta.HT_PHASOR(
        df.close)
    # HT_SINE - Hilbert Transform - SineWave
    # 函数名：HT_DCPHASE
    # 名称： 希尔伯特变换-正弦波 NOTE: The HT_SINE function has an unstable period.
    # sine, leadsine = HT_SINE(df.close)
    dfa["HT_SINE_sine"], dfa["HT_SINE_leadsine"] = ta.HT_SINE(df.close)
    # HT_TRENDMODE - Hilbert Transform - Trend vs Cycle Mode
    # 函数名：HT_DCPHASE
    # 名称： 希尔伯特变换-趋势与周期模式
    # NOTE: The HT_TRENDMODE function has an unstable period.
    # integer = HT_TRENDMODE(df.close)
    dfa["HT_TRENDMODE"] = ta.HT_TRENDMODE(df.close)
    # Price Transform Functions
    # AVGPRICE - Average Price
    # 函数名：AVGPRICE
    # 名称：平均价格函数
    # real = AVGPRICE(open, df.high, df.low, df.close)
    dfa["AVGPRICE"] = ta.AVGPRICE(df.open, df.high, df.low, df.close)
    # MEDPRICE - Median Price
    # 函数名：MEDPRICE
    # 名称：中位数价格
    # real = MEDPRICE(df.high, df.low)
    dfa["MEDPRICE"] = ta.MEDPRICE(df.high, df.low)

    # TYPPRICE - Typical Price
    # 函数名：TYPPRICE
    # 名称：代表性价格
    # real = TYPPRICE(df.high, df.low, df.close)
    dfa["TYPPRICE"] = ta.TYPPRICE(df.high, df.low, df.close)

    # WCLPRICE - Weighted Close Price
    # 函数名：WCLPRICE
    # 名称：加权收盘价
    # real = WCLPRICE(df.high, df.low, df.close)
    dfa["WCLPRICE"] = ta.WCLPRICE(df.high, df.low, df.close)
    # Momentum Indicator Functions
    # ADX - Average Directional Movement Index
    # 函数名：ADX
    # 名称：平均趋向指数
    # 简介：使用ADX指标，指标判断盘整、振荡和单边趋势。
    # 公式：
    # 一、先决定股价趋势（Directional Movement，DM）是上涨或下跌：
    # “所谓DM值，今日股价波动幅度大于昨日股价波动幅部分的最大值，可能是创高价的部分或创低价的部分；如果今日股价波动幅度较前一日小，则DM = 0。”
    # 若股价高点持续走高，为上涨趋势，记作 +DM。
    # 若为下跌趋势，记作 -DM。-DM的负号（–）是表示反向趋势（下跌），并非数值为负数。
    # 其他状况：DM = 0。
    # 二、寻找股价的真实波幅（True Range，TR）：
    # 所谓真实波幅（TR）是以最高价，最低价，及前一日收盘价三个价格做比较，求出当日股价波动的最大幅度。
    # 三、趋势方向需经由一段时间来观察，研判上才有意义。一般以14天为指标的观察周期：
    # 先计算出 +DM、–DM及TR的14日算术平均数，得到 +DM14、–DM14及TR14三组数据作为起始值，再计算各自的移动平均值（EMA）。
    #     +DI14 = +DM/TR14*100
    #     -DI14 = +DM/TR14*100
    #     DX = |(+DI14)-(-DI14)| / |(+DI14)+(-DI14)|
    #     DX运算结果取其绝对值，再将DX作移动平均，得到ADX。
    # 特点：
    # ADX无法告诉你趋势的发展方向。
    # 如果趋势存在，ADX可以衡量趋势的强度。不论上升趋势或下降趋势，
    # ADX看起来都一样。
    # ADX的读数越大，趋势越明显。衡量趋势强度时，需要比较几天的ADX 读数，
    # 观察ADX究竟是上升或下降。ADX读数上升，代表趋势转强；
    # 如果ADX读数下降，意味着趋势转弱。
    # 当ADX曲线向上攀升，趋势越来越强，应该会持续发展。
    # 如果ADX曲线下滑，代表趋势开始转弱，反转的可能性增加。
    # 单就ADX本身来说，由于指标落后价格走势，所以算不上是很好的指标，
    # 不适合单就ADX进行操作。可是，如果与其他指标配合运用，
    # ADX可以确认市场是否存在趋势，并衡量趋势的强度。
    # 指标应用：
    # +DI与–DI表示多空相反的二个动向，当据此绘出的两条曲线彼此纠结相缠时，
    # 代表上涨力道与下跌力道相当，多空势均力敌。当 +DI与–DI彼此穿越时，
    # 由下往上的一方其力道开始压过由上往下的另一方，此时出现买卖讯号。
    # ADX可作为趋势行情的判断依据，当行情明显朝多空任一方向进行时，
    # ADX数值都会显著上升，趋势走强。
    # 若行情呈现盘整格局时，ADX会低于 +DI与–DI二条线。
    # 若ADX数值低于20，则不论DI表现如何，均显示市场没有明显趋势。
    # ADX持续偏高时，代表“超买”（Overbought）或“超卖”（Oversold）的现象，
    # 行情反转的机会将增加，此时则不适宜顺势操作。
    # 当ADX数值从上升趋势转为下跌时，则代表行情即将反转；
    # 若ADX数值由下跌趋势转为上升时，行情将止跌回升。
    # 总言之，DMI指标包含4条线：+DI、-DI、ADX和ADXR。
    # +DI代表买盘的强度、-DI代表卖盘的强度；
    # ADX代表趋势的强度、ADXR则为ADX的移动平均。
    # NOTE: The ADX function has an unstable period.
    # real = ADX(df.high, df.low, df.close, timeperiod=14)
    dfa["ADX"] = ta.ADX(df.high, df.low, df.close, timeperiod=14)
    # ADXR- Average Directional Movement Index Rating
    # 函数名：ADXR
    # 名称：平均趋向指数的趋向指数
    # 简介：使用ADXR指标，指标判断ADX趋势。
    # NOTE: The ADXR function has an unstable period.
    # real = ADXR(df.high, df.low, df.close, timeperiod=14)
    dfa["ADXR"] = ta.ADXR(df.high, df.low, df.close, timeperiod=14)

    # APO - Absolute Price Oscillator
    # real = APO(df.close, fastperiod=12, slowperiod=26, matype=0)
    dfa["APO"] = ta.APO(df.close, fastperiod=12, slowperiod=26, matype=0)
    # AROON - Aroon
    # 函数名：AROON
    # 名称：阿隆指标
    # 简介：该指标是通过计算自价格达到近期最高值和最低值以来所经过的期间数，
    # 阿隆指标帮助你预测价格趋势到趋势区域
    # （或者反过来，从趋势区域到趋势）的变化。
    # 计算公式：
    # Aroon(上升)=[(计算期天数-最高价后的天数)/计算期天数]*100
    # Aroon(下降)=[(计算期天数-最低价后的天数)/计算期天数]*100
    # 指数应用
    # 1、极值0和100
    # 当UP线达到100时，市场处于强势；如果维持在70100之间，表示一个上升趋势。
    # 同样，如果Down线达到0，表示处于弱势，如果维持在030之间，表示处于下跌趋势。
    # 如果两条线同处于极值水平，则表明一个更强的趋势。
    # 2、平行运动
    # 如果两条线平行运动时，表明市场趋势被打破。
    # 可以预期该状况将持续下去，只到由极值水平或交叉穿行西安市出方向性运动为止。
    # 3、交叉穿行
    # 当下行线上穿上行线时，表明潜在弱势，预期价格开始趋于下跌。
    # 反之，表明潜在强势，预期价格趋于走高。
    # aroondown, aroonup = AROON(df.high, df.low, timeperiod=14)
    dfa["AROON_aroondown"], dfa["AROON_aroonup"] = ta.AROON(df.high, df.low,
                                                            timeperiod=14)

    # AROONOSC - Aroon Oscillator
    # 函数名：AROONOSC
    # 名称：阿隆振荡
    # 简介：
    # real = AROONOSC(df.high, df.low, timeperiod=14)
    dfa["AROONOSC"] = ta.AROONOSC(df.high, df.low, timeperiod=14)
    # BOP - Balance Of Power 均势
    # 函数名：BOP
    # 名称：均势指标
    # 简介
    # real = BOP(open, df.high, df.low, df.close)
    dfa["BOP"] = ta.BOP(df.open, df.high, df.low, df.close)

    # CCI - Commodity Channel Index
    # 函数名：CCI
    # 名称：顺势指标
    # 简介：CCI指标专门测量股价是否已超出常态分布范围
    # 指标应用
    # 1.当CCI指标曲线在+100线～-100线的常态区间里运行时,CCI指标参考意义不大，
    # 可以用KDJ等其它技术指标进行研判。
    # 2.当CCI指标曲线从上向下突破+100线而重新进入常态区间时，
    # 表明市场价格的上涨阶段可能结束，将进入一个比较长时间的震荡整理阶段，
    # 应及时平多做空。
    # 3.当CCI指标曲线从上向下突破-100线而进入另一个非常态区间（超卖区）时，
    # 表明市场价格的弱势状态已经形成，将进入一个比较长的寻底过程，
    # 可以持有空单等待更高利润。如果CCI指标曲线在超卖区运行了相当长的一段
    # 时间后开始掉头向上，表明价格的短期底部初步探明，可以少量建仓。
    # CCI指标曲线在超卖区运行的时间越长，确认短期的底部的准确度越高。
    # 4.CCI指标曲线从下向上突破-100线而重新进入常态区间时，表明市场价格的
    # 探底阶段可能结束，有可能进入一个盘整阶段，可以逢低少量做多。
    # 5.CCI指标曲线从下向上突破+100线而进入非常态区间(超买区)时，
    # 表明市场价格已经脱离常态而进入强势状态，如果伴随较大的市场交投，
    # 应及时介入成功率将很大。
    # 6.CCI指标曲线从下向上突破+100线而进入非常态区间(超买区)后，
    # 只要CCI指标曲线一直朝上运行，表明价格依然保持强势可以继续持有待涨。
    # 但是，如果在远离+100线的地方开始掉头向下时，则表明市场价格的强势状态
    # 将可能难以维持，涨势可能转弱，应考虑卖出。如果前期的短期涨幅过高同时
    # 价格回落时交投活跃，则应该果断逢高卖出或做空。
    # CCI主要是在超买和超卖区域发生作用，对急涨急跌的行情检测性相对准确。
    # 非常适用于股票、外汇、贵金属等市场的短期操作。[1]
    # real = CCI(df.high, df.low, df.close, timeperiod=14)
    dfa["CCI"] = ta.CCI(df.high, df.low, df.close, timeperiod=14)
    # CMO - Chande Momentum Oscillator 钱德动量摆动指标
    # 函数名：CMO
    # 名称：钱德动量摆动指标
    # 简介：与其他动量指标摆动指标如相对强弱指标（RSI）和随机指标（KDJ）不同，
    # 钱德动量指标在计算公式的分子中采用上涨日和下跌日的数据。
    # 计算公式：CMO=（Su－Sd）*100/（Su+Sd）
    # 其中：Su是今日收盘价与昨日收盘价（上涨日）差值加总。
    # 若当日下跌，则增加值为0；
    # Sd是今日收盘价与做日收盘价（下跌日）差值的绝对值加总。
    # 若当日上涨，则增加值为0；
    # 指标应用
    # 本指标类似RSI指标。
    # 当本指标下穿-50水平时是买入信号，上穿+50水平是卖出信号。
    # 钱德动量摆动指标的取值介于-100和100之间。
    # 本指标也能给出良好的背离信号。
    # 当股票价格创出新低而本指标未能创出新低时，出现牛市背离；
    # 当股票价格创出新高而本指标未能创出新高时，当出现熊市背离时。
    # 我们可以用移动均值对该指标进行平滑。
    # NOTE: The CMO function has an unstable period.
    # real = CMO(df.close, timeperiod=14)
    dfa["CMO"] = ta.CMO(df.close, timeperiod=14)

    # DX - Directional Movement Index DMI指标又叫动向指标或趋向指标
    # 函数名：DX
    # 名称：动向指标或趋向指标
    # 简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，
    # 即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，
    # 从而提供对趋势判断依据的一种技术指标。
    # 分析和应用：百度百科 维基百科 同花顺学院
    # NOTE: The DX function has an unstable period.
    # real = DX(df.high, df.low, df.close, timeperiod=14)
    dfa["DX"] = ta.DX(df.high, df.low, df.close, timeperiod=14)

    # MACD - Moving Average Convergence/Divergence
    # 函数名：MACD
    # 名称：平滑异同移动平均线
    # 简介：利用收盘价的短期（常用为12日）指数移动平均线与长期（常用为26日）
    # 指数移动平均线之间的聚合与分离状况，对买进、卖出时机作出研判的技术指标。
    # 分析和应用：百度百科 维基百科 同花顺学院
    # macd, macdsignal, macdhist = MACD(df.close, fastperiod=12,
    #                                   slowperiod=26, signalperiod=9)
    dfa["MACD_macd"], dfa["MACD_macdsignal"], dfa["MACD_macdhist"] = ta.MACD(
        df.close, fastperiod=12, slowperiod=26, signalperiod=9)

    # MACDEXT - MACD with controllable MA type
    # 函数名：MACDEXT (这个是干啥的(⊙o⊙)?)
    # 名称：
    # macd, macdsignal, macdhist = MACDEXT(df.close, fastperiod=12,
    #                                      fastmatype=0, slowperiod=26,
    #                                      slowmatype=0, signalperiod=9,
    #                                      signalmatype=0)
    dfa["MACDEXT_macd"], dfa["MACDEXT_macdsignal"], dfa[
        "MACDEXT_macdhist"] = ta.MACDEXT(df.close, fastperiod=12, fastmatype=0,
                                         slowperiod=26, slowmatype=0,
                                         signalperiod=9, signalmatype=0)
    # MACDFIX - Moving Average Convergence/Divergence Fix 12/26
    # macd, macdsignal, macdhist = MACDFIX(df.close, signalperiod=9)
    dfa["MACDFIX_macd"], dfa["MACDFIX_macdsignal"], dfa[
        "MACDFIX_macdhist"] = ta.MACDFIX(df.close, signalperiod=9)
    # MFI - Money Flow Index 资金流量指标
    # 函数名：MFI
    # 名称：资金流量指标
    # 简介：属于量价类指标，反映市场的运行趋势
    # 分析和应用：百度百科 同花顺学院
    # NOTE: The MFI function has an unstable period.
    # real = MFI(df.high, df.low, df.close, df.volume, timeperiod=14)
    dfa["MFI"] = ta.MFI(df.high, df.low, df.close, df.volume, timeperiod=14)

    # MINUS_DI - Minus Directional Indicator
    # 函数名：DMI 中的DI指标 负方向指标
    # 名称：下升动向值
    # 简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，
    # 即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，
    # 从而提供对趋势判断依据的一种技术指标。
    # 分析和应用：百度百科 维基百科 同花顺学院
    # NOTE: The MINUS_DI function has an unstable period.
    # real = MINUS_DI(df.high, df.low, df.close, timeperiod=14)
    dfa["MINUS_DI"] = ta.MINUS_DI(df.high, df.low, df.close, timeperiod=14)

    # MINUS_DM - Minus Directional Movement
    # 函数名：MINUS_DM
    # 名称： 上升动向值 DMI中的DM代表正趋向变动值即上升动向值
    # 简介：通过分析股票价格在涨跌过程中买卖双方力量均衡点的变化情况，
    # 即多空双方的力量的变化受价格波动的影响而发生由均衡到失衡的循环过程，
    # 从而提供对趋势判断依据的一种技术指标。
    # 分析和应用：百度百科 维基百科 同花顺学院
    # NOTE: The MINUS_DM function has an unstable period.
    # real = MINUS_DM(df.high, df.low, timeperiod=14)
    dfa["MINUS_DM"] = ta.MINUS_DM(df.high, df.low, timeperiod=14)

    # MOM - Momentum 动量
    # 函数名：MOM
    # 名称： 上升动向值
    # 简介：投资学中意思为续航，指股票(或经济指数)持续增长的能力。研究发现，
    # 赢家组合在牛市中存在着正的动量效应，输家组合在熊市中存在着负的动量效应。
    # real = MOM(df.close, timeperiod=10)
    dfa["MOM"] = ta.MOM(df.close, timeperiod=10)

    # PLUS_DI - Plus Directional Indicator
    # NOTE: The PLUS_DI function has an unstable period.
    # real = PLUS_DI(df.high, df.low, df.close, timeperiod=14)
    dfa["PLUS_DI"] = ta.PLUS_DI(df.high, df.low, df.close, timeperiod=14)

    # PLUS_DM - Plus Directional Movement
    # NOTE: The PLUS_DM function has an unstable period.
    # real = PLUS_DM(df.high, df.low, timeperiod=14)
    dfa["PLUS_DM"] = ta.PLUS_DM(df.high, df.low, timeperiod=14)

    # PPO - Percentage Price Oscillator 价格震荡百分比指数
    # 函数名：PPO 名称： 价格震荡百分比指数
    # 简介：价格震荡百分比指标（PPO）是一个和MACD指标非常接近的指标。
    # PPO标准设定和MACD设定非常相似：12,26,9和PPO，
    # 和MACD一样说明了两条移动平均线的差距，
    # 但是它们有一个差别是PPO是用百分比说明。
    # real = PPO(df.close, fastperiod=12, slowperiod=26, matype=0)
    dfa["PPO"] = ta.PPO(df.close, fastperiod=12, slowperiod=26, matype=0)

    # ROC - Rate of change : ((price/prevPrice)-1)*100 变动率指标
    # 函数名：ROC
    # 名称： 变动率指标
    # 简介：ROC是由当天的股价与一定的天数之前的某一天股价比较，
    # 其变动速度的大小,来反映股票市变动的快慢程度
    # 分析和应用：百度百科 同花顺学院
    # real = ROC(df.close, timeperiod=10)
    dfa["ROC"] = ta.ROC(df.close, timeperiod=10)
    # ROCP - Rate of change Percentage: (price-prevPrice)/prevPrice
    # real = ROCP(df.close, timeperiod=10)
    dfa["ROCP"] = ta.ROCP(df.close, timeperiod=10)
    # ROCR - Rate of change ratio: (price/prevPrice)
    # real = ROCR(df.close, timeperiod=10)
    dfa["ROCR"] = ta.ROCR(df.close, timeperiod=10)
    # ROCR100 - Rate of change ratio 100 scale: (price/prevPrice)*100
    # real = ROCR100(df.close, timeperiod=10)
    dfa["ROCR100"] = ta.ROCR100(df.close, timeperiod=10)

    # RSI - Relative Strength Index 相对强弱指数
    # 函数名：RSI
    # 名称：相对强弱指数
    # 简介：是通过比较一段时期内的平均收盘涨数和平均收盘跌数来分析市场买沽盘的
    # 意向和实力，从而作出未来市场的走势。
    # NOTE: The RSI function has an unstable period.
    # real = RSI(df.close, timeperiod=14)
    dfa["RSI"] = ta.RSI(df.close, timeperiod=14)

    # STOCH - Stochastic 随机指标,俗称KD
    # 函数名：STOCH
    # 名称：随机指标,俗称KD
    # slowk, slowd = STOCH(df.high, df.low, df.close, fastk_period=5,
    #                      slowk_period=3, slowk_matype=0, slowd_period=3,
    #                      slowd_matype=0)
    dfa["STOCH_slowk"], dfa["STOCH_slowd"] = ta.STOCH(df.high, df.low, df.close,
                                                      fastk_period=5,
                                                      slowk_period=3,
                                                      slowk_matype=0,
                                                      slowd_period=3,
                                                      slowd_matype=0)

    # STOCHF - Stochastic Fast
    # fastk, fastd = STOCHF(df.high, df.low, df.close, fastk_period=5,
    #                       fastd_period=3, fastd_matype=0)
    dfa["STOCHF_fastk"], dfa["STOCHF_fastd"] = ta.STOCHF(df.high, df.low,
                                                         df.close,
                                                         fastk_period=5,
                                                         fastd_period=3,
                                                         fastd_matype=0)

    # STOCHRSI - Stochastic Relative Strength Index
    # NOTE: The STOCHRSI function has an unstable period.
    # fastk, fastd = STOCHRSI(df.close, timeperiod=14, fastk_period=5,
    #                         fastd_period=3, fastd_matype=0)
    dfa["STOCHRSI_fastk"], dfa["STOCHRSI_fastd"] = ta.STOCHRSI(df.close,
                                                               timeperiod=14,
                                                             fastk_period=5,
                                                             fastd_period=3,
                                                             fastd_matype=0)

    # TRIX - 1-day Rate-Of-Change (ROC) of a Triple Smooth EMA
    # real = TRIX(df.close, timeperiod=30)
    dfa["TRIX"] = ta.TRIX(df.close, timeperiod=30)

    # ULTOSC - Ultimate Oscillator 终极波动指标
    # 函数名：ULTOSC
    # 名称：终极波动指标
    # 简介：UOS是一种多方位功能的指标，除了趋势确认及超买超卖方面的作用之外，
    # 它的“突破”讯号不仅可以提供最适当的交易时机之外，
    # 更可以进一步加强指标的可靠度。
    # 分析和应用：百度百科 同花顺学院
    # real = ULTOSC(df.high, df.low, df.close, timeperiod1=7, timeperiod2=14,
    #               timeperiod3=28)
    dfa["ULTOSC"] = ta.ULTOSC(df.high, df.low, df.close, timeperiod1=7,
                              timeperiod2=14,
                              timeperiod3=28)

    # WILLR - Williams' %R 威廉指标
    # 函数名：WILLR
    # 名称：威廉指标
    # 简介：WMS表示的是市场处于超买还是超卖状态。
    # 股票投资分析方法主要有如下三种：基本分析、技术分析、演化分析。
    # 在实际应用中，它们既相互联系，又有重要区别。
    # 分析和应用：百度百科 维基百科 同花顺学院
    # real = WILLR(df.high, df.low, df.close, timeperiod=14)
    dfa["WILLR"] = ta.WILLR(df.high, df.low, df.close, timeperiod=14)

    # Statistic Functions 统计学指标
    # BETA - Beta
    # 函数名：BETA
    # 名称：β系数也称为贝塔系数
    # 简介：一种风险指数，用来衡量个别股票或股票基金相对于整个股市的价格波动情况
    # 贝塔系数衡量股票收益相对于业绩评价基准收益的总体波动性，是一个相对指标。
    # β 越高，意味着股票相对于业绩评价基准的波动性越大。
    # β 大于 1 ， 则股票的波动性大于业绩评价基准的波动性。反之亦然。
    # 用途：
    # 1）计算资本成本，做出投资决策（只有回报率高于资本成本的项目才应投资）；
    # 2）计算资本成本，制定业绩考核及激励标准；
    # 3）计算资本成本，进行资产估值（Beta是现金流贴现模型的基础）；
    # 4）确定单个资产或组合的系统风险，用于资产组合的投资管理，
    # 特别是股指期货或其他金融衍生品的避险（或投机）
    # real = BETA(df.high, df.low, timeperiod=5)
    dfa["BETA"] = ta.BETA(df.high, df.low, timeperiod=5)

    # CORREL - Pearson's Correlation Coefficient (r)
    # 函数名：CORREL
    # 名称：皮尔逊相关系数
    # 简介：用于度量两个变量X和Y之间的相关（线性相关），其值介于-1与1之间
    # 皮尔逊相关系数是一种度量两个变量间相关程度的方法。
    # 它是一个介于 1 和 -1 之间的值，
    # 其中，1 表示变量完全正相关， 0 表示无关，-1 表示完全负相关。
    # real = CORREL(df.high, df.low, timeperiod=30)
    dfa["CORREL"] = ta.CORREL(df.high, df.low, timeperiod=30)

    # LINEARREG - Linear Regression
    # 直线回归方程：当两个变量x与y之间达到显著地线性相关关系时,
    # 应用最小二乘法原理确定一条最优直线的直线方程y=a+bx,
    # 这条回归直线与个相关点的距离比任何其他直线与相关点的距离都小,
    # 是最佳的理想直线. 回归截距a：表示直线在y轴上的截距,代表直线的起点.
    # 回归系数b：表示直线的斜率,他的实际意义是说明x每变化一个单位时,
    # 影响y平均变动的数量. 即x每增加1单位,y变化b个单位.
    # 函数名：LINEARREG
    # 名称：线性回归
    # 简介：来确定两种或两种以上变量间相互依赖的定量关系的一种统计分析方法
    # 其表达形式为y = w'x+e，e为误差服从均值为0的正态分布。
    # real = LINEARREG(df.close, timeperiod=14)
    dfa["LINEARREG"] = ta.LINEARREG(df.close, timeperiod=14)
    # LINEARREG_ANGLE - Linear Regression Angle
    # 函数名：LINEARREG_ANGLE
    # 名称：线性回归的角度
    # 简介：来确定价格的角度变化. 参考
    # real = LINEARREG_ANGLE(df.close, timeperiod=14)
    dfa["LINEARREG_ANGLE"] = ta.LINEARREG_ANGLE(df.close, timeperiod=14)
    # LINEARREG_INTERCEPT - Linear Regression Intercept
    # 函数名：LINEARREG_INTERCEPT
    # 名称：线性回归截距
    # real = LINEARREG_INTERCEPT(df.close, timeperiod=14)
    dfa["LINEARREG_INTERCEPT"] = ta.LINEARREG_INTERCEPT(df.close, timeperiod=14)
    # LINEARREG_SLOPE - Linear Regression Slope
    # 函数名：LINEARREG_SLOPE
    # 名称：线性回归斜率指标
    # real = LINEARREG_SLOPE(df.close, timeperiod=14)
    dfa["LINEARREG_SLOPE"] = ta.LINEARREG_SLOPE(df.close, timeperiod=14)
    # STDDEV - Standard Deviation
    # 函数名：STDDEV
    # 名称：标准偏差
    # 简介：种量度数据分布的分散程度之标准，用以衡量数据值偏离算术平均值的程度。
    # 标准偏差越小，这些值偏离平均值就越少，反之亦然。
    # 标准偏差的大小可通过标准偏差与平均值的倍率关系来衡量。
    # real = STDDEV(df.close, timeperiod=5, nbdev=1)
    dfa["STDDEV"] = ta.STDDEV(df.close, timeperiod=5, nbdev=1)
    # TSF - Time Series Forecast
    # 函数名：TSF
    # 名称：时间序列预测
    # 简介：一种历史资料延伸预测，也称历史引伸预测法。
    # 是以时间数列所能反映的社会经济现象的发展过程和规律性，
    # 进行引伸外推，预测其发展趋势的方法
    # real = TSF(df.close, timeperiod=14)
    dfa["TSF"] = ta.TSF(df.close, timeperiod=14)

    # VAR - VAR
    # 函数名： VAR 名称：方差
    # 简介：方差用来计算每一个变量（观察值）与总体均数之间的差异。
    # 为避免出现离均差总和为零，离均差平方和受样本含量的影响，
    # 统计学采用平均离均差平方和来描述变量的变异程度
    # real = VAR(df.close, timeperiod=5, nbdev=1)
    dfa["VAR"] = ta.VAR(df.close, timeperiod=5, nbdev=1)
    return dfa


def model_main_linear():
    ts_code = '399300.SZ'
    x_train, x_test, y_train, y_test = getdata(ts_code, type='linear')
    # return
    # print(x_train.shape)
    # print(x_train)
    # print(x_test.shape)
    # print(y_train.shape)
    # print(y_train)
    # print(y_test.shape)

    # 线性回归
    model = LinearRegression()
    # # 线性支持向量机 linearSVC
    # model = LinearSVC(C=0.01)

    model.fit(x_train, y_train)
    y_predictions = model.predict(x_test)
    r2 = r2_score(y_test, y_predictions)
    print('intercept:', model.intercept_)
    print('coef:', model.coef_)
    # print('y_test:\n', y_test)
    # print('y_predictions:\n', y_predictions)
    print('r2:', r2)

    print('ridge regression:')
    model_ridge = RidgeCV()
    model_ridge.fit(x_train, y_train)
    y_predictions_ridge = model_ridge.predict(x_test)
    r2_ridge = r2_score(y_test, y_predictions_ridge)
    print('intercept:', model_ridge.intercept_)
    print('coef:', model_ridge.coef_)
    print('r2:', r2_ridge)

    print('lasso regression:')
    model_lasso = LassoCV()
    model_lasso.fit(x_train, y_train)
    y_predictions_lasso = model_lasso.predict(x_test)
    r2_lasso = r2_score(y_test, y_predictions_lasso)
    print('intercept:', model_lasso.intercept_)
    print('coef:', model_lasso.coef_)
    print('r2:', r2_lasso)


def model_main_classifier():
    ts_code = '399300.SZ'
    x_train, x_test, y_train, y_test = getdata(ts_code, type='classifier')
    # return
    # print(x_train.shape)
    # print(x_train)
    # print(x_test.shape)
    # print(y_train.shape)
    # print(y_train)
    # print(y_test.shape)

    # 线性回归
    # model = LinearRegression()
    # 线性支持向量机 linearSVC
    model = LinearSVC(C=0.1)

    model.fit(x_train, y_train)
    y_predictions = model.predict(x_test)
    # r2 = r2_score(y_test, y_predictions)
    print('intercept:', model.intercept_)
    print('coef:', model.coef_)
    print('y_test:\n', y_test)
    print('y_predictions:\n', y_predictions)
    # print('r2:', r2)

    # print('ridge regression:')
    # model_ridge = Ridge()
    # model_ridge.fit(x_train, y_train)
    # y_predictions_ridge = model_ridge.predict(x_test)
    # r2_ridge = r2_score(y_test, y_predictions_ridge)
    # print('intercept:', model_ridge.intercept_)
    # print('coef:', model_ridge.coef_)
    # print('r2:', r2_ridge)
    #
    # print('lasso regression:')
    # model_lasso = Lasso()
    # model_lasso.fit(x_train, y_train)
    # y_predictions_lasso = model_lasso.predict(x_test)
    # r2_lasso = r2_score(y_test, y_predictions_lasso)
    # print('intercept:', model_lasso.intercept_)
    # print('coef:', model_lasso.coef_)
    # print('r2:', r2_lasso)


if __name__ == '__main__':
    pass
    model_main_linear()
    # model_main_classifier()
