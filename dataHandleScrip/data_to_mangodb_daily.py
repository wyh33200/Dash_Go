import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

from data_to_mangodb import per_xxz_data_clean,data_to_mangodb

cnn_xxz_log = create_engine("mysql+pymysql://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_xxz_base = create_engine("mysql+pymysql://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")


def per_xxz_data_get(date):
    # date = '20210303'
    table_date = date[:6]
    table = 'per_wxapp_xxz_log_behavior_'+table_date
    date = list(date)
    date.insert(4, '-')
    date.insert(7, '-')
    date = ''.join(date)
    sql = "select distinct pid,left(logtime,10)date,count(1) hanldNum from {table} where logtime like '%{date}%' group by 1,2".format(table=table, date=date)
    print(sql)
    data = pd.read_sql(sql, cnn_xxz_log)
    print('获取{time}的数据'.format(time=date))
    return data


if __name__ == '__main__':
    for time in ['20210301', '20210302', '20210303', '20210304']:
        df = per_xxz_data_clean(per_xxz_data_get(time))
        data_to_mangodb(df)
        print("{time}数据处理完成".format(time=time))
    print("==" * 60)
    print("数据处理完成")
