import pandas as pd
import time
from sqlalchemy import create_engine
import datetime
from datetime import timedelta


class DailyReport:
    """
    莞就业日报
    整体数据结构类似，基本只换了表名，但是重构了方法。用类的形式去表达
    """
    def __init__(self, time_):
        self.table_name = "per_wxapp_gov_log_behavior_" + time_.strftime("%Y%m-%d")[:6]
        self.time_ = time_.strftime("%Y-%m-%d")
        self.cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
        self.cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")

    def behavior_handle_data(self):
        sql = r'''
            SELECT
                tb1.date,
                tb1.uniqueVistor,
                tb1.comView,
                tb2.comViewNum,
                tb1.positionView,
                tb2.positionViewNum,
                tb1.positionSearch,
                tb2.positionSearchNum,
                tb1.ircView,
                tb2.ircViewNum,
                tb1.resumeEdit,
                tb2.resumeEditNum,
                tb1.resumeSend,
                tb2.resumeSendNum,
                tb1.slideClick,
                tb2.slideClickNum,
                tb1.studentAssessClick,
                tb2.studentAssessClickNum,
                tb1.positionGuideClick,
                tb2.positionGuideClickNum,
                tb1.resumeDiagnoseClick,
                tb2.resumeDiagnoseClickNum,
                tb1.employmentCoachClick,
                tb2.employmentCoachClickNum 
            FROM
                (
                SELECT
                    date,
                    count( DISTINCT pid ) uniqueVistor,
                    sum( CASE WHEN cmd_cn LIKE '%查看职位%' THEN 1 ELSE 0 END ) positionView,
                    sum( CASE WHEN cmd_cn LIKE '%查看企业%' THEN 1 ELSE 0 END ) comView,
                    sum( CASE WHEN cmd_cn LIKE '%职位搜索%' THEN 1 ELSE 0 END ) positionSearch,
                    sum( CASE WHEN cmd_cn LIKE '%访问招聘会详情%' THEN 1 ELSE 0 END ) ircView,
                    sum( CASE WHEN cmd_cn LIKE '%轮播图%' THEN 1 ELSE 0 END ) slideClick,
                    sum( CASE WHEN cmd_cn LIKE '%编辑简历%' THEN 1 ELSE 0 END ) resumeEdit,
                    sum( CASE WHEN cmd_cn LIKE '%投%' THEN 1 ELSE 0 END ) resumeSend,
                    sum( CASE WHEN cmd_cn LIKE '%人才测评【点击】%' THEN 1 ELSE 0 END ) studentAssessClick,
                    sum( CASE WHEN cmd_cn LIKE '%职业规划【点击】%' THEN 1 ELSE 0 END ) positionGuideClick,
                    sum( CASE WHEN cmd_cn LIKE '%简历诊断【点击】%' THEN 1 ELSE 0 END ) resumeDiagnoseClick,
                    sum( CASE WHEN cmd_cn LIKE '%就业辅导【点击】%' THEN 1 ELSE 0 END ) employmentCoachClick 
                FROM
                    (
                    SELECT DISTINCT
                        DATE_FORMAT( logtime, '%Y-%m-%d' ) date,
                        pid,
                        cmd_cn 
                    FROM
                    {table} where logtime like '%{time_}%' ) tb 

                GROUP BY
                    1 
                ) tb1
                LEFT JOIN (
                SELECT
                    DATE_FORMAT( logtime, '%Y-%m-%d' ) date,
                    sum( CASE WHEN cmd_cn LIKE '%查看职位%' THEN 1 ELSE 0 END ) positionViewNum,
                    sum( CASE WHEN cmd_cn LIKE '%查看企业%' THEN 1 ELSE 0 END ) comViewNum,
                    sum( CASE WHEN cmd_cn LIKE '%职位搜索%' THEN 1 ELSE 0 END ) positionSearchNum,
                    sum( CASE WHEN cmd_cn LIKE '%访问招聘会详情%' THEN 1 ELSE 0 END ) ircViewNum,
                    sum( CASE WHEN cmd_cn LIKE '%轮播图%' THEN 1 ELSE 0 END ) slideClickNum,
                    sum( CASE WHEN cmd_cn LIKE '%编辑简历%' THEN 1 ELSE 0 END ) resumeEditNum,
                    sum( CASE WHEN cmd_cn LIKE '%投%' THEN 1 ELSE 0 END ) resumeSendNum,
                    sum( CASE WHEN cmd_cn LIKE '%人才测评【点击】%' THEN 1 ELSE 0 END ) studentAssessClickNum,
                    sum( CASE WHEN cmd_cn LIKE '%职业规划【点击】%' THEN 1 ELSE 0 END ) positionGuideClickNum,
                    sum( CASE WHEN cmd_cn LIKE '%简历诊断【点击】%' THEN 1 ELSE 0 END ) resumeDiagnoseClickNum,
                    sum( CASE WHEN cmd_cn LIKE '%就业辅导【点击】%' THEN 1 ELSE 0 END ) employmentCoachClickNum 
                FROM
                    {table} where logtime like '%{time_}%'
                GROUP BY
                    1 
                ) tb2 
                ON tb1.date = tb2.date
            '''.format(table=self.table_name, time_=self.time_)
        data_ = pd.read_sql(sql, self.cnn_xxz_log)
        print("{table_date}表1数据提取成功".format(table_date=self.table_name))
        return data_

    def click_handle_data(self):
        sql_cmd = r'''
            select tb1.date,tb1.homeClick,tb2.homeClickNum,tb1.myClick,tb2.myClickNum,tb1.messageClick,tb2.messageClickNum FROM 
            (select date,sum(case when cmd = '/pages/index/index' then 1 else 0 end) homeClick,
            sum(case when cmd = '/pages/per/index/index' then 1 else 0 end) myClick,
            sum(case when cmd = '/pages/new/index/index' then 1 else 0 end) messageClick FROM
            (select distinct DATE_FORMAT(logtime,'%Y-%m-%d') date,pid,cmd from {table} 
            where logtime like '%{time_}%') tb 
            GROUP BY 1) tb1
            left join
            (select DATE_FORMAT(logtime,'%Y-%m-%d') date,
            sum(case when cmd = '/pages/index/index' then 1 else 0 end) homeClickNum,
            sum(case when cmd = '/pages/per/index/index' then 1 else 0 end) myClickNum,
            sum(case when cmd = '/pages/new/index/index' then 1 else 0 end) messageClickNum 
            FROM {table} where logtime like '%{time_}%' GROUP BY 1 ) tb2 on tb1.date=tb2.date
            '''.format(table=self.table_name, time_=self.time_)
        data_cmd_ = pd.read_sql(sql_cmd, self.cnn_xxz_log)
        print("{table_date}表2数据提取成功".format(table_date=self.table_name))
        return data_cmd_

    def merge_data(self):
        data_ = self.behavior_handle_data()
        data_cmd_ = self.click_handle_data()
        data_merge = pd.merge(data_, data_cmd_, on=['date'])
        columns = ['日期', '日活', '查看公司', '查看公司次数', '查看职位', '查看职位次数', '职位搜索', '职位搜索次数', '查看招聘会', '查看招聘会次数', '编辑简历',
                   '编辑简历次数',
                   '投递简历', '投递简历次数', '点击轮播图', '轮播图点击次数', '学生测评', '学生测评点击次数', '职业规划', '职业规划点击次数', '简历诊断', '简历诊断点击次数',
                   '就业辅导',
                   '就业辅导点击次数', '首页', '首页次数', '我的', '我的次数', '消息', '消息点击次数']
        data_merge.columns = columns
        print('输出{table_date}数据'.format(table_date=self.time_))
        # data_merge.to_sql('per_gov_student_report', self.cnn_root, if_exists='append')
        print("数据处理完成")
        return data_merge

    def main(self):
        data_merge = self.merge_data()
        data_merge.to_sql('per_gov_student_report', self.cnn_root, if_exists='append')
        print("{}数据注入完成".format(self.time_))
        time.sleep(3)


if __name__ == "__main__":
    start_date = datetime.datetime.strptime('2020-10-26', "%Y-%m-%d")
    end_date = datetime.datetime.strptime('2021-03-29', "%Y-%m-%d")
    while start_date <= end_date:
        data = DailyReport(start_date)
        start_date += timedelta(days=1)
        data.main()
        print('{date}数据清洗执行完毕'.format(date=start_date))
    # date = DailyReport(datetime.datetime.strptime('2021-03-28', "%Y-%m-%d"))
    # # date.main()
    # print(date.main())
