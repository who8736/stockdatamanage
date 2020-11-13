# -*- coding: utf-8 -*-
"""
Created on 2016年12月14日

@author: who8736
"""
import os
import sys
import configparser

SECRET_KEY = 'Sm9obiBTY2hyb20ga2lja3MgYXNz'

# options格式：类成员变量名，配置文件中的段，配置文件中的选项，
#              缺省值, 值类型
options = [
    ['secretkey', 'main', 'secretkey', 'Sm9obiBTY2hyb20ga2lja3MgYXNz', 'str'],
    ['rootpath', 'main', 'rootpath', '', 'str'],
    ['sqlUser', 'sql', 'sqluser', '', 'str'],
    ['sqlPassword', 'sql', 'sqlpassword', '', 'str'],
    ['sqlIp', 'sql', 'sqlip', '127.0.0.1', 'str'],
    ['tushareToken', 'sql', 'tusharetoken', '', 'str'],
    ['pushData', 'mail', 'pushdata', 'False', 'bool'],
    ['mailServer', 'mail', 'mailserver', 'smtp.163.com', 'str'],
    ['mailPort', 'mail', 'mailport', '25', 'str'],  # 设为空表示不需指定端口
    ['mailUser', 'mail', 'mailuser', 'test@163.com', 'str'],
    ['mailPassword', 'mail', 'mailpassword', 'testpassword', 'str'],
    ['sendTo', 'mail', 'sendto', 'test@qq.com|test1@qq.com', 'str'],
]

ROOTPATH = os.path.split(os.path.abspath(__file__))[0]
CFILE = os.path.join(ROOTPATH, 'stockdata.conf')


class Config:
    def __init__(self):
        self.cf = configparser.ConfigParser()
        if not os.path.isfile(CFILE):
            # self.initConfig()
            print('请修改配置stockdata.conf后，重新运行程序')
            sys.exit(1)
        else:
            self.readConfig()
            # self.saveConf()

    def initConfig(self):
        """初始化配置文件，
        初始化步骤：生成空配置文件, 生成缺省配置, 保存缺省配置
        """
        self.saveConf()
        self.readConfig()
        self.saveConf()

    def readConfig(self):
        self.cf.read(CFILE)
        for varName, section, option, defaultValue, valueType in options:
            if not self.cf.has_section(section):
                self.cf.add_section(section)
            if not self.cf.has_option(section, option):
                self.cf.set(section, option, defaultValue)

            if valueType == 'bool':
                setattr(self, varName, self.cf.getboolean(section, option))
            else:
                setattr(self, varName, self.cf.get(section, option))

    def saveConf(self):
        # write to file
        self.cf.write(open(CFILE, 'w+'))


if __name__ == '__main__':
    mycnf = Config()
    print(mycnf.sendTo)
