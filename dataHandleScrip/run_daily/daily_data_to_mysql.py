import pandas as pd
import time
from sqlalchemy import create_engine
from per_xxz_daily_info import per_xxz_data_clean, data_to_mysql

cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


def per_xxz_data_get(date_):
    table_date = date_[:6]
    table = 'per_wxapp_xxz_log_behavior_'+table_date

    date_ = list(str(int(date_)-1))
    print(date_)
    date_.insert(4, '-')
    date_.insert(7, '-')
    date_ = ''.join(date_)
    sql = r"select distinct pid,DATE_FORMAT(logtime,'%Y-%m-%d') date,count(1) handle_num from " \
          "{table} where logtime like '%{date_}%' group by 1,2".format(table=table, date_=date_)
    print(sql)
    data = pd.read_sql(sql, cnn_xxz_log)
    print('获取{time}的数据'.format(time=date_))
    return data


# 增加定时任务
if __name__ == '__main__':
    while True:
        print("\r程序已启动,当前时间：{now}".format(now=time.strftime("%H:%M:%S")), end="", flush=True)
        time.sleep(1)
        if time.strftime('%H:%M') == '03:30':
            print('开始执行程序')
            time_now = time.strftime('%Y%m%d')
            df = per_xxz_data_clean(per_xxz_data_get(time_now))
            data_to_mysql(df)
            print("{time}数据处理完成".format(time=time))
            time.sleep(61)
