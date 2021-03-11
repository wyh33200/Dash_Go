import pandas as pd
from sqlalchemy import create_engine

cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")
cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_xxz_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")


# 获取用户的Log表，统计每日登录的用户群体
def per_xxz_data_get(date='202009'):
    table = 'per_wxapp_xxz_log_behavior_'+date
    sql = "select distinct pid,left(logtime,10)date,count(1) handle_num from {table} group by 1,2".format(table=table)
    data = pd.read_sql(sql, cnn_xxz_log)
    print('获取{time}的数据'.format(time=date))
    return data


# 数据清洗，把用户的属性附加给原表
# 目前有个人ID，性别，学历，学校名称，院系名称，专业名称，毕业年份，学历来源
def per_xxz_data_clean(data):
    pid = str(list(data['pid'].unique())).replace('[', '(').replace(']', ')')
    sql = '''
    select account_id,gender,degree,school_name,school_college_name,special_name,grade,degree_from from 
    (select * from per_degree_info order by id desc)tb 
    where account_id in {pid}
    group by 1
    '''.format(pid=pid)
    per_info = pd.read_sql(sql, cnn_xxz_base)
    data = pd.merge(data, per_info, how='left', left_on='pid', right_on='account_id')
    print('数据清洗完成')
    return data


# 数据转存进入mangoDB
def data_to_mysql(data):
    data.to_sql('per_xxz_daily_info', cnn_root, if_exists='append')
    print('数据转储完成')


if __name__ == '__main__':
    for time in ['202009', '202010', '202011', '202012', '202101', '202102', '202102', '202103']:
        df = per_xxz_data_clean(per_xxz_data_get(time))
        data_to_mysql(df)
        print("{time}数据处理完成".format(time=time))
    print("=="*60)
    print("数据处理完成")
