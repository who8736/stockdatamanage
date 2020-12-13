# -*- coding:utf-8 -*-
# author:who8736
# datetime:2020/11/17 14:21

from stockdatamanage.util.datatrans import quarterList, classifyEndDate


def test_quarterList():
    start = '20010101'
    end = '20201114'
    dates = quarterList(start, end)
    for d in dates:
        print(d)


if __name__ == '__main__':
    # test_quarterList()
    for m in range(1, 13):
        classifyEndDate(m)
