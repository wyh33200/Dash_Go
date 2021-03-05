import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient

import data_to_mangodb

cnn_xxz_log = create_engine("mysql+pymysql://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_xxz_base = create_engine("mysql+pymysql://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
def per_xxz_data_get(date='202009'):
    table = 'per_wxapp_xxz_log_behavior_'+date
    sql = "select distinct pid,left(logtime,10)date,count(1) hanldNum from {table} group by 1,2".format(table=table)
    data = pd.read_sql(sql, cnn_xxz_log)
    print('获取{time}的数据'.format(time=date))
    return data