import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from sqlalchemy import create_engine
import dash_bootstrap_components as dbc

import pandas as pd
import plotly.graph_objects as go
import datetime
from app import app

cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")

layout_index = html.Div([
    # 时间筛选
    html.Div([
        dcc.DatePickerRange(
            id='my-date-picker-range',
            min_date_allowed=datetime.date(2020, 9, 18),
            max_date_allowed=datetime.date(2021, 12, 31),
            display_format='Y-MM-DD',
            month_format='Y-MM',
            start_date=datetime.date.today()-datetime.timedelta(days=7),
            end_date=datetime.date.today()-datetime.timedelta(days=1)
        ),
        html.Div(id='output-container-date-picker-range')
    ]),
    html.P(),
    dcc.Store(id='memory-output'),
    # html.Div(dcc.Graph(id='1'), style={'width': '24%', 'display': 'inline-block', }),
    # html.Div(dcc.Graph(id='2'), style={'width': '25%', 'display': 'inline-block', }),
    html.Div(id='bug'),
    html.Div([
        dcc.Graph(id='school_value', )
            ], style={'width': '49%', 'display': 'inline-block', 'padding': '0 20'}
    ),
    html.Div([
        dcc.Graph(id='department_value'), dcc.Graph(id='major_value')
    ], style={'display': 'inline-block', 'width': '49%'})

])


@app.callback(Output('memory-output', 'data'),
              [Input('my-date-picker-range', 'start_date'),
               Input('my-date-picker-range', 'end_date')])
def store_func(start_date, end_date):
    sql = r"SELECT * from per_xxz_daily_info where date between '{start_date}' and '{end_date}'".format(
        start_date=start_date, end_date=end_date)
    df = pd.read_sql(sql, cnn_root)
    df = df.drop_duplicates('pid')
    df['gender'] = df['gender'].fillna('无性别')
    df['degree'] = df['degree'].fillna('无学历')
    df['school_name'] = df['school_name'].fillna('无学校名称')
    df['school_college_name'] = df['school_college_name'].fillna('无院系名称')
    df['special_name'] = df['special_name'].fillna('无专业名称')
    df['grade'] = df['grade'].fillna('无毕业年份')
    df['degree_from'] = df['degree_from'].fillna('无导入信息')
    return df.to_dict('records')


@app.callback(Output('school_value', 'hoverData'),
              Input('memory-output', 'data'))
def main_hover_data(data):
    df = pd.DataFrame(data)
    _ = df['school_name'].value_counts().index[0]
    return {'points': [{'label': _}]}


@app.callback([Output('school_value', 'figure'), ],
              Input('memory-output', 'data'))
def school_func(data):
    # 学校柱状图
    df = pd.DataFrame(data)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df['school_name'].value_counts().index[:8],
        y=df['school_name'].value_counts().values[:8],
        text=df['school_name'].value_counts().values[:8],
        textposition='auto',
        # hover_name=df['school_college_name'],
    ))
    fig.update_layout(title_text='学校使用人数', template='none', height=675,)

    return fig,


@app.callback([Output('department_value', 'figure'),
              Output('department_value', 'hoverData')],
              [Input('memory-output', 'data'),
              Input('school_value', 'hoverData')])
def department_value(data, hoverData):
    # 院系柱状图
    school_name = hoverData['points'][0]['label']
    df = pd.DataFrame(data).query("school_name == @school_name")['school_college_name'].value_counts()
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df.index[:8],
        y=df.values[:8],
        text=df.values[:8],
        textposition='auto',
    ))
    fig.update_layout(title_text='{school_name}院系分布'.format(school_name=school_name), template='none',)
    _ = df.index[0]
    return fig, {'points': [{'label': _}]}


@app.callback([Output('major_value', 'figure'),
              Output('major_value', 'hoverData')],
              [Input('memory-output', 'data'),
               Input('department_value', 'hoverData'),
               Input('school_value', 'hoverData')])
def major_value(data, department_name, school_name):
    # 院系柱状图
    department = department_name['points'][0]['label']
    school_name = school_name['points'][0]['label']

    df = pd.DataFrame(data).query("school_name == @school_name & school_college_name == @department")[
        'special_name'].value_counts()
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df.index[:8],
        y=df.values[:8],
        text=df.values[:8],
        textposition='auto',
    ))
    fig.update_layout(title_text='{department}专业分布'.format(department=department), template='none',)
    _ = df.index[0]
    return fig, {'points': [{'label': _}]}


# @app.callback(Output('bug', 'Children'),
#   Debug
#               Input('department_value', 'hoverData'))
# def print_(data):
#     print(data)
#     return str(data)
