import pandas as pd
import time
from sqlalchemy import create_engine
import sys
sys.path.append('../')
import datetime
from per_xxz_daily_info import per_xxz_data_clean, data_to_mysql

cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


def per_xxz_data_get(date_):
    table_date = date_[:6]
    table = 'per_wxapp_xxz_log_behavior_'+table_date
    date_ = list(date_)
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
    time_now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    df = per_xxz_data_get(time_now)
    data_to_mysql(per_xxz_data_clean(df))
    print("脚本结束")
