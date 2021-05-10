import sys
sys.path.append('../')
from company_report_to_mysql_all import *


if __name__ == '__main__':
    time_now = datetime.datetime.now() - datetime.timedelta(days=1)
    data = CompanyReport(time_now)
    data.main()

    # start_date = datetime.datetime.strptime('2021-04-30', "%Y-%m-%d")  # 开始日期
    # end_date = datetime.datetime.strptime('2021-05-09', "%Y-%m-%d")  # 结束日期
    # while start_date < end_date:
    #     start_date += timedelta(days=1)
    #     data = CompanyReport(start_date)
    #     data.main()
    #     logging.info("数据清洗执行完毕")
