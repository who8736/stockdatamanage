# -*- coding: utf-8 -*-
'''
Created on 2016年11月26日

@author: who8736
'''

import os


def createPath(filePath):
    if not os.path.exists(filePath):
        os.makedirs(filePath)

if __name__ == '__main__':
    filePaths = ['./data/BalanceSheet',
                 './data/CashFlow',
                 './data/ProfitStatement',
                 './data/guzhi', ]
    for filePath in filePaths:
        createPath(filePath)
    os.system('chown -R apache:apache data')

    os.system('chmod +x startstockmanage')
    os.system('chmod +x run.sh')
