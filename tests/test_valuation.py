from stockdatamanage.valuation import calpfnew
from stockdatamanage.initlog import initlog


def test_calpfnew():
    calpfnew('20201123')


if __name__ == '__main__':
    # item = ReportItem('000002.SZ')
    # item = reportValuation('000002.SZ')
    # print(item)
    initlog()

    test_calpfnew()
    print('完成')
