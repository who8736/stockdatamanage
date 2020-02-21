# from  download import downPledgeStat
from download import downloader


if __name__ == '__main__':
    downloader('pledge_stat', perTimes=60, downLimit=50)