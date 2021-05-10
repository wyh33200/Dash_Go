import datetime
import logging
from datetime import timedelta

import pandas as pd
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", )

cnn_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


class CompanyReport:
    """
    企业端日报；company_report_daily
    """

    def __init__(self, date):
        """
        :param date:
        datetime.datetime
        date = '2020-09-02 00:00:00'
        """
        self.date = date.strftime("%Y-%m-%d")
        self.table = f'com_pc_log_behavior_{self.date.replace("-","")[:6]}'

    def data_get(self):
        """
        获取behavior_log表数据
        去除我们的ip

        :return: data (DataFrame)
        """
        logger.info(f"开始获取{self.date}数据...")
        sql = f'select * from {self.table} where logtime like "%{self.date}%"'
        data = pd.read_sql(sql, cnn_log)
        data['date'] = data['logtime'].dt.date
        data = data[~data['ip'].str.contains('119.145.111.42|192.168')].query("comId != 0")
        logger.info(f"{self.date}数据获取成功...")
        return data

    def data_handle(self, data):
        """
        清洗数据

        :param data:
        :return: com_report (DataFrame)
        """
        com_report = pd.DataFrame(data[~data['cmd_cn'].str.contains('邮箱')]
                                  .drop_duplicates(['date', 'comId'])['date'].value_counts().sort_index())
        # 查看简历，不含邮箱查看
        _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[
            data['cmd_cn'].str.contains('查看') & ~data['cmd_cn'].str.contains('邮箱')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd_cn'].str.contains('查看') & ~data['cmd_cn'].str.contains('邮箱')
                     ].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 邮箱查看简历
        data['cmd_cn'].replace('打开邮箱查看附件简历', '打开邮箱查看简历', inplace=True)
        _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[data['cmd_cn'].str.contains('邮箱')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd_cn'].str.contains('邮箱')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 下载简历
        _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[data['cmd_cn'].str.contains('下载')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd_cn'].str.contains('下载')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 点击招聘会
        _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[
            data['cmd_cn'].str.contains('点击招聘会场次')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd_cn'].str.contains('点击招聘会场次')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 发布职位
        _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/jobinput')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd'].str.contains('/jobinput')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 沟通页面
        _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/chat')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd'].str.contains('/chat')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 简历中心
        _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[
            data['cmd'].str.contains('/resume/center')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd'].str.contains('/resume/center')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 公司主页
        data['cmd'].replace('/company/edit', '/company', inplace=True)
        _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/company')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd'].str.contains('/company')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        # 录入企业信息
        _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/comInfo')].groupby('date')[
            'comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        _ = data.loc[data['cmd'].str.contains('/comInfo')].groupby('date')['comId'].count()
        com_report = pd.concat([com_report, _], axis=1)
        com_report.columns = ['UV', '查看简历', '查看简历次数', '邮箱查看简历', '邮箱查看简历次数', '下载简历', '下载简历次数',
                              '点击招聘会', '招聘会次数', '录入职位', '录入职位次数', '沟通页面', '沟通页面次数', '简历中心',
                              '简历中心次数', '企业中心', '企业中心次数', '录入企业信息', '录入企业信息次数']
        com_report.index.name = 'date'
        logging.info(f"{self.date}数据清洗完成...")
        return com_report

    def data_to_mysql(self, data):
        """
        持久化数据
        :param data:
        :return: null
        """
        logging.info(f"开始储存数据")
        data.to_sql('company_report_daily', cnn_root, if_exists='append')
        logging.info(f"{self.date}数据数据注入成功")

    def main(self):
        """
        主函数
        """
        data = self.data_get()
        data = self.data_handle(data)
        self.data_to_mysql(data)


if __name__ == '__main__':
    start_date = datetime.datetime.strptime('2020-11-01', "%Y-%m-%d")
    end_date = datetime.datetime.strptime('2021-04-22', "%Y-%m-%d")
    while start_date < end_date:
        start_date += timedelta(days=1)
        com_data = CompanyReport(start_date)
        com_data.main()
        logging.info("数据清洗执行完毕")


# def data_get(date_):
#     # 获取数据
#     # date = '202103'
#     print('开始获取{date_}数据'.format(date_=date_))
#     sql = r'select * from com_pc_log_behavior_{date_}'.format(date_=date_)
#     data = pd.read_sql(sql, cnn_log)
#     data['date'] = data['logtime'].dt.date
#     data = data[~data['ip'].str.contains('119.145.111.42|192.168')].query("comId != 0")
#     print('{date_}数据成功获取'.format(date_=date_))
#     print('▉' * 10)
#     return data
#
#
# def data_handle(data):
#     # 数据清洗
#     print('开始清洗数据')
#     com_report = pd.DataFrame(data[~data['cmd_cn'].str.contains('邮箱')]
#                               .drop_duplicates(['date', 'comId'])['date'].value_counts().sort_index())
#     # 查看简历，不含邮箱查看
#     _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[
#         data['cmd_cn'].str.contains('查看') & ~data['cmd_cn'].str.contains('邮箱')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd_cn'].str.contains('查看') & ~data['cmd_cn'].str.contains('邮箱')
#                  ].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 邮箱查看简历
#     data['cmd_cn'].replace('打开邮箱查看附件简历', '打开邮箱查看简历', inplace=True)
#     _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[data['cmd_cn'].str.contains('邮箱')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd_cn'].str.contains('邮箱')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 下载简历
#     _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[data['cmd_cn'].str.contains('下载')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd_cn'].str.contains('下载')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 点击招聘会
#     _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[data['cmd_cn'].str.contains('点击招聘会场次')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd_cn'].str.contains('点击招聘会场次')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 发布职位
#     _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/jobinput')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd'].str.contains('/jobinput')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 沟通页面
#     _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/chat')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd'].str.contains('/chat')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 简历中心
#     _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[
#     data['cmd'].str.contains('/resume/center')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd'].str.contains('/resume/center')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 公司主页
#     data['cmd'].replace('/company/edit', '/company', inplace=True)
#     _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/company')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd'].str.contains('/company')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     # 录入企业信息
#     _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/comInfo')].groupby('date')[
#         'comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     _ = data.loc[data['cmd'].str.contains('/comInfo')].groupby('date')['comId'].count()
#     com_report = pd.concat([com_report, _], axis=1)
#     com_report.columns = ['UV', '查看简历', '查看简历次数', '邮箱查看简历', '邮箱查看简历次数', '下载简历', '下载简历次数',
#                           '点击招聘会', '招聘会次数', '录入职位', '录入职位次数', '沟通页面', '沟通页面次数', '简历中心',
#                           '简历中心次数', '企业中心', '企业中心次数', '录入企业信息', '录入企业信息次数']
#     print('数据清洗完毕')
#     print('▉'*20)
#     return com_report
#
#
# def data_to_mysql(data):
#     print('开始存储数据')
#     data.to_sql('company_report_daily', cnn_root, if_exists='append')
#     print('数据储存完毕')
#     print('▉' * 30)


# if __name__ == '__main__':
#     for date in ['202011', '202012', '202101', '202102', '202103']:
#         df = data_get(date)
#         data_to_mysql(data_handle(df))
#         print('{date}月数据执行完成'.format(date=date))
#         print('▉' * 40)
