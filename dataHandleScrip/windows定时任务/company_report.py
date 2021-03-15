import pandas as pd
from sqlalchemy import create_engine
import datetime

cnn_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


def data_get(time_):
    # 获取数据
    table_date = time_[:6]
    time_ = list(time_)
    time_.insert(4, '-')
    time_.insert(7, '-')
    time_ = ''.join(time_)
    print('开始获取{time_}数据'.format(time_=time_))

    sql = r'select * from com_pc_log_behavior_{table_date} where logtime like "%{time_}%" '.format(
        table_date=table_date, time_=time_)
    data = pd.read_sql(sql, cnn_log)
    data['date'] = data['logtime'].dt.date
    data = data[~data['ip'].str.contains('119.145.111.42|192.168')].query("comId != 0")
    print('{time_}数据成功获取'.format(time_=time_))
    print('▉' * 10)
    return data


def data_handle(data):
    # 数据清洗
    print('开始清洗数据')
    com_report = pd.DataFrame(
        data[~data['cmd_cn'].str.contains('邮箱')].drop_duplicates(['date', 'comId'])['date'].value_counts().sort_index())
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
    _ = data.drop_duplicates(['date', 'comId', 'cmd_cn']).loc[data['cmd_cn'].str.contains('点击招聘会场次')].groupby('date')[
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
    _ = data.drop_duplicates(['date', 'comId', 'cmd']).loc[data['cmd'].str.contains('/resume/center')].groupby('date')[
        'comId'].count()
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
    print('数据清洗完毕')
    print('▉'*20)
    return com_report


def data_to_mysql(data):
    print('开始存储数据')
    data.to_sql('company_report_daily', cnn_root, if_exists='append')
    print('数据储存完毕')
    print('▉' * 30)


if __name__ == '__main__':
    time_now = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    df = data_handle(data_get(time_now))
    data_to_mysql(df)