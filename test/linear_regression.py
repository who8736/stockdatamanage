from collections import OrderedDict
import matplotlib.pyplot as plt
import pandas as pd


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