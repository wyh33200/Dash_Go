import pandas as pd
from sqlalchemy import create_engine
from data_to_mangodb import per_xxz_data_clean, data_to_mysql


cnn_xxz_log = create_engine("mysql+pymysql://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_xxz_base = create_engine("mysql+pymysql://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
cnn_root = create_engine("mysql+pymysql://root:root@localhost:3306/dash")
# cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


def per_xxz_data_get(date):
    # date = '20210303'
    table_date = date[:6]
    table = 'per_wxapp_xxz_log_behavior_'+table_date
    date = list(date)
    date.insert(4, '-')
    date.insert(7, '-')
    date = ''.join(date)
    sql = "select distinct pid,left(logtime,10)date,count(1) handle_num from {table} where logtime like '%{date}%'" \
          " group by 1,2".format(table=table, date=date)
    print(sql)
    data = pd.read_sql(sql, cnn_xxz_log)
    print('获取{time}的数据'.format(time=date))
    return data


if __name__ == '__main__':
    for time in ['20210305', '20210306', '20210307']:
        df = per_xxz_data_clean(per_xxz_data_get(time))
        data_to_mysql(df)
        print("{time}数据处理完成".format(time=time))
    print("==" * 60)
    print("数据处理完成")
