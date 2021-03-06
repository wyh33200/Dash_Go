import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
from dash.exceptions import PreventUpdate
from sqlalchemy import create_engine

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import datetime

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
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
            end_date=datetime.date.today()
        ),
        html.Div(id='output-container-date-picker-range')
    ]),
    html.P(),
    # 数据
    html.Div(className='row', style={'align-items': 'center'}, children=[
        html.Div('日期范围内一共登录', style={'font-size': '26px'}),
        html.Span(id='uv', style={'font-size': '36px', 'font-weight': '600'}),
        html.Div('人(去除重复登录人数)。页面共浏览', style={'font-size': '26px'}),
        html.Span(id='pv', style={'font-size': '36px', 'font-weight': '600'}),
        html.Div('次。平均人均浏览', style={'font-size': '26px'}),
        html.Span(id='pv-uv', style={'font-size': '36px', 'font-weight': '600'}),
        html.Div('个页面', style={'font-size': '26px'})
    ]),
    html.Div([dcc.Graph(id='graph-with-uv'),
              dcc.Graph(id='graph-with-pv'),
              dcc.Graph(id='graph-with-pv-uv'),
              dcc.Graph(id='graph-with-avg'), ]
             )
])


def uv_func(df):
    # 每日活跃用户折线图
    per_num_trace = go.Scatter(
        x=df['date'].unique(),
        y=df.groupby('date').agg({'pid': 'count'})['pid'],
    )
    per_num_trace_fig = go.Figure(layout={'title': '每日活跃用户',
                                          'xaxis': {'tickformat': '%Y-%m-%d'},
                                          'template': 'none'})
    per_num_trace_fig.add_trace(per_num_trace)
    return per_num_trace_fig


# def pv_fix(df):
#     # pv 拟合直线
#     poly_reg = PolynomialFeatures(degree=3)
#     y = df.groupby('date').agg({'handle_num': 'sum'})['handle_num'].values.reshape(-1, 1)
#     y = poly_reg.fit_transform(y)
#     X = np.arange(len(df['date'].unique())).reshape(-1, 1)
#     regr = LinearRegression()
#     regr.fit(X, y)
#     formula = str(np.round(regr.coef_[0], 2)) + 'x+' + str(np.round(regr.intercept_, 2))
#     return regr.predict(X), formula


def pv_func(df):
    # 每日页面浏览量
    handle_num_trace_fig = go.Figure(layout={'title': '每日页面浏览量',
                                             'xaxis': {'tickformat': '%Y-%m-%d'},
                                             'template': 'none',
                                             })
    per_num_trace = go.Scatter(
        x=df['date'].unique(),
        y=df.groupby('date').agg({'handle_num': 'sum'})['handle_num'],
    )
    # pv_fix_trace = go.Scatter(
    #     x=df['date'].unique(),
    #     y=pv_fix(df)[0],
    #     name=avg_fix(df)[1],
    #     line=dict(color='#7EFF99', width=4, dash='dash')
    # )
    # handle_num_trace_fig.add_traces([per_num_trace, pv_fix_trace])
    handle_num_trace_fig.add_trace(per_num_trace)
    return handle_num_trace_fig


def pv_uv_func(df):
    # 两天直线在一张图
    # 每人，每日页面浏览量
    pv_num_trace = go.Scatter(
        x=df['date'].unique(),
        y=df.groupby('date').agg({'handle_num': 'sum'})['handle_num'],
        name='每日页面浏览量',
        # line_shape='spline'
    )
    per_num_trace = go.Scatter(
        x=df['date'].unique(),
        y=df.groupby('date').agg({'pid': 'count'})['pid'],
        yaxis='y2',
        name='每日登录人数',
        # line_shape='spline',
    )
    trace = [pv_num_trace, per_num_trace, ]
    figure = go.Figure(data=trace, layout=go.Layout(title='每日UV和PV',
                                                    xaxis={'tickformat': '%Y-%m-%d'},
                                                    # yaxis2=dict(overlaying='y', side='right',),
                                                    yaxis2={'overlaying': 'y', 'side': 'right', "showgrid": False},
                                                    template='none',
                                                    # showlegend=False, # 图例
                                                    ))

    return figure


def avg_fix(df):
    # 平均页面浏览量线性回归直线，
    # formula(str) 拟合公式
    # regr.predict(X)(array) 拟合值
    df['per_ave'] = df.groupby('date')['handle_num'].transform('sum') / df.groupby('date')[
        'pid'].transform('count')
    df = df[['date', 'per_ave']].drop_duplicates('date')
    X = np.arange(len(df)).reshape(len(df), -1)
    y = df['per_ave']
    regr = LinearRegression()
    regr.fit(X, y)
    formula = str(round(regr.coef_[0], 2))+'x+'+str(round(regr.intercept_, 2))
    return regr.predict(X), formula


def avg_func(df):
    # per_num_trace 平均每人页面浏览量
    # per_fix_trace 拟合直线
    avg_trace_fig = go.Figure(
                              layout={'title': '每日每人平均页面浏览量',
                                      'xaxis': {'tickformat': '%Y-%m-%d'},
                                      'template': 'none',
                                      'showlegend': True,  # 图例
                                      })
    per_num_trace = go.Scatter(
        x=df['date'].unique(),
        y=df.groupby('date').agg({'handle_num': 'sum'})['handle_num']/df.groupby('date').agg({'pid': 'count'})['pid'],
        line=dict(color='royalblue'),
        name='平均页面浏览量'
    )
    per_fix_trace = go.Scatter(
        x=df['date'].unique(),
        y=avg_fix(df)[0],
        name=avg_fix(df)[1],
        line=dict(color='#7EFF99', width=4, dash='dash')
    )
    avg_trace_fig.add_traces([per_num_trace, per_fix_trace])
    # avg_trace_fig.add_trace()
    return avg_trace_fig


@app.callback([Output('uv', 'children'),
               Output('pv', 'children'),
               Output('pv-uv', 'children'),
               Output('graph-with-uv', 'figure'),
               Output('graph-with-pv', 'figure'),
               Output('graph-with-pv-uv', 'figure'),
               Output('graph-with-avg', 'figure'), ],
              [Input('my-date-picker-range', 'start_date'),
               Input('my-date-picker-range', 'end_date')]
              )
def _func(start_date, end_date):
    sql = '''
    select date, pid, handle_num from per_xxz_daily_info where date > '{start_date}' and date <'{end_date}'
    '''.format(start_date=start_date, end_date=end_date)
    filtered = pd.read_sql(sql, cnn_root)
    per_num = len(filtered['pid'].unique())
    handle_num = filtered['handle_num'].sum()
    per_handle_mean = round(handle_num/per_num, 2)
    # 每日活跃用户
    per_num_trace_fig = uv_func(filtered)
    # 每日操作数
    handle_num_trace_fig = pv_func(filtered)
    figure = pv_uv_func(filtered)
    avg_trace_fig = avg_func(filtered)
    return per_num, handle_num, per_handle_mean, per_num_trace_fig, handle_num_trace_fig, figure, avg_trace_fig
