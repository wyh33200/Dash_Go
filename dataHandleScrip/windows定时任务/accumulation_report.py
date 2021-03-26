from accumulation_report_all import *


if __name__ == '__main__':
    time_now = datetime.datetime.now() - datetime.timedelta(days=1)
    data = Weekly(time_now)
    data.main()



