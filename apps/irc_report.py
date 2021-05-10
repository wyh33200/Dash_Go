import dash_core_components as dcc
import dash_html_components as html
import dash_table
import pandas as pd
from dash.dependencies import Input, Output

from app import app
from cm import cnn_xxz_base


def irc_list():
    # 获取招聘会列表
    # {'label': '164-粤菜师傅餐饮企业招聘专区', 'value': '164'},
    sql = 'select prod_id,prod_name from irc_prod where del_status = 0 AND prod_status <> -2 ORDER BY prod_id DESC'
    data = pd.read_sql(sql, cnn_xxz_base)
    data = data.astype(str)
    data['data'] = data['prod_id'] + '-' + data['prod_name']
    irc_list_ = []
    for j, i in zip(data['prod_id'], data['data']):
        irc_list_.append({'label': i, 'value': j})
    return irc_list_


# 整体布局
layout_index = html.Div([
    html.Div([
        dcc.Dropdown(id='irc-dropdown', options=irc_list(), placeholder="选取你想查阅的招聘会场次", optionHeight=40, value='165'),
        # 选择招聘会
    ], style={'width': '50%'}, ),
    html.P(),
    html.Div([html.Div("当前招聘会为："), html.Div(id='irc_info'), ], style={"display": "flex", 'align-items': "flex-end"}),

    html.Div(id='div_data'),
    html.P(),
    dash_table.DataTable(id='overview_table')
])


@app.callback([Output('irc_info', 'children'),
               Output('irc_info', 'style')
               ],
              Input('irc-dropdown', 'value'))
def irc_info_function(irc_id):
    sql = "select grade, " \
          "case when prod_type = 0 then '线下招聘会' " \
          "when prod_type = 1 then '线上招聘会' " \
          "when prod_type = 2 then '线下/线上招聘会' end prod_type, sponsor, begin_time, end_time, enroll_begin_time," \
          f"enroll_end_time from irc_prod where prod_id = {irc_id}"
    irc = pd.read_sql(sql, cnn_xxz_base)
    irc_type = irc["prod_type"].loc[0]
    if irc_type == '线上招聘会':
        irc_type_style = {"font-size": '30px', 'color': 'SpringGreen '}
    elif irc_type == '线下招聘会':
        irc_type_style = {"font-size": '30px', 'color': 'GoldenRod  '}
    else:
        irc_type_style = {"font-size": '30px', 'color': 'SlateBlue  '}
    irc_grade = irc["grade"].loc[0]
    return irc_type, irc_type_style
