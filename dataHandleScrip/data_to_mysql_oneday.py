import pandas as pd
from sqlalchemy import create_engine
from data_to_mysql import per_xxz_data_clean, data_to_mysql


cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_xxz_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


def per_xxz_data_get(date):
    # date = '20210303'
    table_date = date[:6]
    table = 'per_wxapp_xxz_log_behavior_'+table_date
    date = list(date)
    date.insert(4, '-')
    date.insert(7, '-')
    date = ''.join(date)
    sql = "select distinct pid,DATE_FORMAT(logtime,'%Y-%m-%d') date,count(1) handle_num from " \
          "{table} where logtime like '%%{date}%%' group by 1,2".format(table=table, date=date)
    print(sql)
    data = pd.read_sql(sql, cnn_xxz_log)
    print('获取{time}的数据'.format(time=date))
    return data


# 单独增加某天
if __name__ == '__main__':
    for time in ['20210406']:
        df = per_xxz_data_clean(per_xxz_data_get(time))
        data_to_mysql(df)
        print("{time}数据处理完成".format(time=time))
    print("==" * 30)
    print("数据处理完成")
