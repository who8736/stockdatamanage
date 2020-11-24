from stockdatamanage.valuation import ReportItem
from stockdatamanage.analyse.report import readValuation, reportValuation

if __name__ == '__main__':
    # item = ReportItem('000002.SZ')
    item = reportValuation('000002.SZ')
    print(item)
