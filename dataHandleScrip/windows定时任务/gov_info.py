import sys
import datetime
sys.path.append('../')
from data_to_mysql_gov import *


class GPI(GovPerInfo):
    cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")
    cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
    cnn_xxz_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")

    def __init__(self, date):
        GovPerInfo.__init__(self, date)

    def per_gov_data_get(self):
        table = 'per_wxapp_gov_log_behavior_'+self.date[:7].replace("-", "")
        logger.info(table)
        sql = f"select distinct pid,left(logtime,10)date,count(1) handle_num from {table}" \
              f" where logtime like '%{self.date}%' group by 1,2"
        data = pd.read_sql(sql, self.cnn_xxz_log)
        logging.info(f"获取{self.date}数据")
        return data


if __name__ == '__main__':
    time = (datetime.datetime.now() - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    gpi = GPI(time)
    df = gpi.per_gov_data_clean(gpi.per_gov_data_get())
    gpi.data_to_mysql(df)
    print("{time}数据处理完成".format(time=time))
    print("=="*60)
    print("数据处理完成")
