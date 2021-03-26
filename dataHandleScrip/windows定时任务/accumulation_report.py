from sqlalchemy import create_engine
from datetime import timedelta
import pandas as pd
import datetime
import time
import warnings
warnings.filterwarnings('ignore')

cnn_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")
cnn_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")
cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")


class Weekly:
    """
    每个函数对应不同功能,
    规定第一个注释为返回数据的属性，如累加统计,
    第二个注释为函数的中文含义,
    第三个注释为筛选字段的描述,
    """

    def __init__(self, date):
        # 默认属性，统计时间，一般为周一
        # date = '2020-09-02 00:00:00'
        self.date = date.strftime("%Y-%m-%d")
        data_ = datetime.datetime.strptime(self.date, "%Y-%m-%d").date()
        data_ = data_ - timedelta(days=1)
        data_ = data_.strftime("%Y-%m-%d")
        self.yesterday = data_

    def school_consult(self):
        # 累加统计
        # 学校咨询量
        # 筛选申请日期小于当前时间
        school_trial = '''select count(1) from school_apply_trial_info 
        where apply_time <"{date}"'''.format(date=self.date)
        data_school_trial = pd.read_sql(school_trial, cnn_base)
        school_trial_num = data_school_trial.loc[0][0]
        print('{date}学校咨询量统计完成'.format(date=self.date))
        yield school_trial_num

    def irc_num(self):
        # 累加统计
        # 招聘会新发布量
        # 招聘会状态不为取消，未被删除, 如需剔除莞就业 'and school_id !=5722'
        irc_prod = '''SELECT count(1) FROM irc_prod WHERE prod_status <>-2 AND del_status=0 and create_time<'
        {date} ' '''.format(date=self.date)
        data_irc_prod = pd.read_sql(irc_prod, cnn_base)
        irc_prod_num = data_irc_prod.loc[0][0]
        print('{date}招聘会新发布量统计完成'.format(date=self.date))
        yield irc_prod_num

    def student_num(self):
        # 累加统计
        # 用户量
        # 仅统计2020年以后的
        per_num = '''SELECT count(1) FROM per_user where create_date > '2020-01-01' and create_date <
         "{date}" and status = 0'''.format(date=self.date)
        per_num = pd.read_sql(per_num, cnn_base)
        per_num = per_num .loc[0][0]
        print('{date}用户量统计完成'.format(date=self.date))
        yield per_num

    def resume_num(self):
        # 累加统计，筛选时间的'fre_date'为临时字段
        # 简历量，包含附件简历。
        # 筛选2020年之前，教育经历不为空，简历状态筛选大于等于0
        resume = '''
        SELECT count(account_id) FROM
        (SELECT account_id FROM per_resume where fre_date >'2020-01-01' and pass>='0' and education_info is not null and
        fre_date <{date}
        UNION
        SELECT per_user_id FROM per_attach_resume 
        WHERE del_status = '0' and upload_date <{date}) as tb1'''.format(date=self.date)
        resume = pd.read_sql(resume, cnn_base)
        resume = resume.loc[0][0]
        print('{date}简历量统计完成'.format(date=self.date))
        yield resume

    def com_num(self):
        # 累加统计
        # 公司数量
        # 筛选状态大于0的，创建时间小于data
        sql = '''
        SELECT count(*) FROM com_info where status>=0 and create_date< "{today}"
        '''.format(today=self.date)
        data_com_register = pd.read_sql(sql, cnn_base)
        data_com_register = data_com_register.loc[0][0]
        print('{date}公司数量统计完成'.format(date=self.date))
        yield data_com_register

    def position_num(self):
        # 累加数据
        # 岗位数量
        # 删除状态为0，岗位状态等于9，创建时间小于data
        # 岗位在线时间为90天，90天后就会下架，所以累加数据会呈现减少的趋势
        sql = '''
        SELECT count(1) FROM com_position WHERE del_status = '0' AND pos_status = 9  and create_date< '{today}'
        '''.format(today=self.date)
        data_position = pd.read_sql(sql, cnn_base)
        data_position_num = data_position.loc[0][0]
        print('{date}岗位数量统计完成'.format(date=self.date))
        yield data_position_num

    def receive_num(self):
        # 累加数据
        # 简历接受量
        # 筛选创建时间
        sql = '''
        select count(1) FROM xxz_action.com_receive where create_date < '{today}'
        '''.format(today=self.date)
        data_receive = pd.read_sql(sql, cnn_base)
        data_receive_num = data_receive.loc[0][0]
        print('{date}简历接受量统计完成'.format(date=self.date))
        yield data_receive_num

    def school_register(self):
        # 累加数据
        # 学校注册量
        # 筛选名称中包含学院或者大学的，筛选创建日期
        sql = '''
        SELECT count(1) FROM `xxz_base`.`sys_user` WHERE `name` LIKE '%学院%' OR
         `name` LIKE '%大学%' and create_date <'{today}'
        '''.format(today=self.date)
        school_register = pd.read_sql(sql, cnn_base).loc[0][0]
        print('{date}学校注册量统计完成'.format(date=self.date))
        yield school_register

    def com_pos_view(self):
        # 累加数据
        # 岗位、企业浏览量

        table = 'act_view_behavior_log_'+self.date[:7].replace('-', '')
        com_sql = r'''
        SELECT count(1) AS `sum` FROM {table} WHERE `action_cn` LIKE '%查看企业%' and create_date like '%{date}%'
        '''.format(table=table, date=self.date)
        com_view = pd.read_sql(com_sql, cnn_log).loc[0][0]
        pos_sql = r'''
        SELECT count(1) AS `sum` FROM {table} WHERE `action_cn` LIKE '%查看职位%' and create_date like '%{date}%'
        '''.format(table=table, date=self.date)
        position_view = pd.read_sql(pos_sql, cnn_log).loc[0][0]
        print('{date}中com_view:{com_view} \n '
              'position_view:{position_view}'.format(date=self.date, com_view=com_view, position_view=position_view))

        # 读取数据库中，前一天的数据，防止第一次运行时报错
        try:
            sql = "select `公司查看量`,`岗位查看量` from accumulation_report " \
                  "where 日期 = '{yesterday}'".format(yesterday=self.yesterday)
            _ = pd.read_sql(sql, cnn_root)
            com_view = int(com_view)+int(_.loc[0][0])
            position_view = int(position_view)+int(_.loc[0][1])
        except KeyError:
            pass
        print('{date}岗位、企业浏览量统计完成'.format(date=self.date))
        yield com_view, position_view

    def link_num(self):
        # 累加数据
        # 跳转到简历优帮，师兄职路
        # 先运行第一天，后续再读取增加
        table = 'per_wxapp_xxz_log_behavior_'+self.date[:7].replace('-', '')
        doctor_sql = '''
        select count(id) from {table} where cmd_cn ='跳转到简历优帮' and logtime like '%{date}%'
        '''.format(table=table, date=self.date)
        doctor_num = pd.read_sql(doctor_sql, cnn_log).loc[0][0]
        guide_sql = '''
        select count(id) from {table} where cmd_cn ='跳转到师兄职路' and logtime like '%{date}%'
        '''.format(table=table, date=self.date)
        guide_num = pd.read_sql(guide_sql, cnn_log).loc[0][0]

        # 读取数据库中，前一天的数据，防止第一次运行时报错
        try:
            sql = "select `跳转到简历优帮`,`跳转到师兄职路` from accumulation_report " \
                  "where 日期 = '{yesterday}'".format(yesterday=self.yesterday)
            _ = pd.read_sql(sql, cnn_root)
            doctor_num = int(doctor_num)+int(_.loc[0][0])
            guide_num = int(guide_num)+int(_.loc[0][0])
        except KeyError:
            pass
        print('{date}跳转到简历优帮，师兄职路量统计完成'.format(date=self.date))
        yield doctor_num, guide_num

    def weekly_num(self):
        # 周度累加数据
        # 跳转到简历优帮，师兄职路
        # 先运行第一天，后续再读取增加
        date = datetime.datetime.strptime(self.date, "%Y-%m-%d").date()  # 转成日期格式
        week_start = (date - timedelta(days=date.weekday())).strftime("%Y-%m-%d")
        week_end = (date + timedelta(days=6)).strftime("%Y-%m-%d")
        if week_start[:7].replace('-', '') != self.date[:7].replace('-', ''):
            table_start = week_start[:7].replace('-', '')
            table_end = self.date[:7].replace('-', '')
            sql = '''
            select count(distinct tb.per_user_id) from (select * from act_view_behavior_log_{table_start} 
            union all 
            select * from act_view_behavior_log_{table_end}) tb
            where tb.create_date between '{week_start}' and '{week_end}'
            '''.format(table_start=table_start, table_end=table_end, week_end=week_end, week_start=week_start)
            week_num = pd.read_sql(sql, cnn_log).loc[0][0]
        else:
            table = week_start[:7].replace('-', '')
            sql = '''
            select count(distinct per_user_id) from act_view_behavior_log_{table} 
            where create_date between '{week_start}' and '{week_end}'
            '''.format(table=table, week_end=week_end, week_start=week_start)
            week_num = pd.read_sql(sql, cnn_log).loc[0][0]
        print('{date}周度累加数据统计完成'.format(date=self.date))
        yield week_num

    def main(self):
        # 综合函数，数据处理
        # 所有yield 需要next返回
        school_consult_ = next(self.school_consult())  # 学校咨询量
        irc_num_ = next(self.irc_num())  # 招聘会发布量
        student_num_ = next(self.student_num())  # 用户量
        resume_num_ = next(self.resume_num())  # 简历量
        com_num_ = next(self.com_num())  # 公司量
        position_num_ = next(self.position_num())  # 岗位量
        receive_num_ = next(self.receive_num())  # 投递简历量
        school_register_ = next(self.school_register())  # 学校注册量
        com_view_, pos_view_ = next(self.com_pos_view())  # 公司查看，岗位查看量
        doctor_num_, guide_num_ = next(self.link_num())  # 跳转到简历优帮，师兄职路
        week_num_ = next(self.weekly_num())  # 每周活跃数据
        list_ = [[self.date, school_consult_, irc_num_, student_num_, resume_num_, com_num_, position_num_,
                  receive_num_, school_register_, com_view_, pos_view_, doctor_num_, guide_num_, week_num_]]
        _ = pd.DataFrame(list_)
        print('{date}数据集和完成'.format(date=self.date))
        _.columns = ['日期', '学校咨询量', '招聘会发布量', '用户量', '简历量', '公司量', '岗位量', '投递简历量', '学校注册量',
                     '公司查看量', '岗位查看量', '跳转到简历优帮', '跳转到师兄职路', '每周活跃数据']
        _.to_sql('accumulation_report', cnn_root, if_exists='append')
        print('{date}数据注入完成'.format(date=self.date))
        print('睡眠3秒')
        time.sleep(3)
        return _


if __name__ == '__main__':
    time_now = datetime.datetime.now() - datetime.timedelta(days=1)
    data = Weekly(time_now)
    data.main()



