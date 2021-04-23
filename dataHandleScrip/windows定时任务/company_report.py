import sys
sys.path.append('../')
from company_report_to_mysql_all import *


if __name__ == '__main__':
    time_now = datetime.datetime.now() - datetime.timedelta(days=1)
    data = CompanyReport(time_now)
    data.main()
