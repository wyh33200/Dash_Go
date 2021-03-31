import sys
sys.path.append('../')
from per_gov_report_all import *


if __name__ == "__main__":
    time_now = (datetime.datetime.now() - datetime.timedelta(days=1))
    date = DailyReport(time_now)
    date.main()
