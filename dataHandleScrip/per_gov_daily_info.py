import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", )


class GovPerInfo:
    def __init__(self, date):
        self.cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")
        self.cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
        self.cnn_xxz_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
        self.date = date

    # 获取用户的Log表，统计每日登录的用户群体
    def per_gov_data_get(self):
        # data 的格式是 "YYYYmm"
        table = 'per_wxapp_gov_log_behavior_'+self.date
        sql = "select distinct pid,left(logtime,10)date,count(1) handle_num from {table} group by 1,2".format(
            table=table)
        data = pd.read_sql(sql, self.cnn_xxz_log)
        logging.info(f"获取{self.date}数据")
        return data

    # 数据清洗，把用户的属性附加给原表
    # 目前有个人ID，性别，学历，学校名称，院系名称，专业名称，毕业年份，学历来源
    def per_gov_data_clean(self, data):
        pid = tuple(data['pid'].unique())
        sql = '''
            select account_id,gender,degree,school_name,school_college_name,special_name,grade,degree_from from 
            (select * from per_degree_info order by id desc)tb 
            where account_id in {pid}
            group by 1
            '''.format(pid=pid)
        per_info = pd.read_sql(sql, self.cnn_xxz_base)
        data = pd.merge(data, per_info, how='left', left_on='pid', right_on='account_id')
        print('数据清洗完成')
        return data

    # 数据储存
    def data_to_mysql(self, data):
        data.to_sql('per_gov_daily_info', self.cnn_root, if_exists='append')
        print('数据转储完成')


if __name__ == '__main__':
    for time in ['202010', '202011', '202012', '202101', '202102', '202102', '202103']:
        gpi = GovPerInfo(time)
        df = gpi.per_gov_data_clean(gpi.per_gov_data_get())
        gpi.data_to_mysql(df)
        print("{time}数据处理完成".format(time=time))
    print("=="*60)
    print("数据处理完成")
