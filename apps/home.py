import logging
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

from app import app

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", )

cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")
layout_index = html.Div([
    # 时间筛选
    html.Div([
        dcc.DatePickerSingle(
            id='my-date-picker',
            min_date_allowed=datetime.date(2020, 9, 18),
            max_date_allowed=datetime.date(2021, 12, 31),
            display_format='Y-MM-DD',
            month_format='Y-MM',
            date=datetime.date.today()-datetime.timedelta(days=1),
        ),
        html.Div(id='output-container-date-picker-range'),
        html.Div("校校招", style={"font-family": "微软雅黑", "font-size": "150%",
                               "padding-top": "1.5rem"})
        ]),
    dbc.Row([
        dbc.Col(dbc.Card([  # 校校招
            dbc.CardHeader(id="uv_title", children=["整体日活"], style={"font-size": "10px"}),
            dbc.CardBody([
                html.H2(id="uv_h2", className="card-title", ),
                html.H4(id="uv_h4", className="card-title",),
                html.H4(id="uv_h4_1", className="card-title",), ]
            )])),
        dbc.Col(dbc.Card([
            dbc.CardHeader(id="weekly_title", children=["整体周活"], style={"font-size": "10px"}),
            dbc.CardBody([
                html.H2(id="weekly_h2", className="card-title", ),
                html.H4(id="weekly_h4", className="card-title",),
                html.H4(id="weekly_h4_1", className="card-title",), ]
            )])),
        dbc.Col(dbc.Card([
            dbc.CardHeader(id="month_title", children=["整体月活"], style={"font-size": "10px"}),
            dbc.CardBody([
                html.H2(id="month_h2", className="card-title", ),
                html.H4(id="month_h4", className="card-title",),
                html.H4(id="month_h4_1", className="card-title",), ]
            )])),
        ]),
    html.Div("莞就业", style={"font-family": "微软雅黑", "font-size": "150%",
                           "padding-top": "1.5rem"}),
    dbc.Row([  # 莞就业
        dbc.Col(dbc.Card([
            dbc.CardHeader(id="uv_title_gov", children=["整体日活"], style={"font-size": "10px"}),
            dbc.CardBody([
                html.H2(id="uv_h2_gov", className="card-title", ),
                html.H4(id="uv_h4_gov", className="card-title", ),
                html.H4(id="uv_h4_1_gov", className="card-title", ), ]
            )])),
        dbc.Col(dbc.Card([
            dbc.CardHeader(id="weekly_title_gov", children=["整体周活"], style={"font-size": "10px"}),
            dbc.CardBody([
                html.H2(id="weekly_h2_gov", className="card-title", ),
                html.H4(id="weekly_h4_gov", className="card-title", ),
                html.H4(id="weekly_h4_1_gov", className="card-title", ), ]
            )])),
        dbc.Col(dbc.Card([
            dbc.CardHeader(id="month_title_gov", children=["整体月活"], style={"font-size": "10px"}),
            dbc.CardBody([
                html.H2(id="month_h2_gov", className="card-title", ),
                html.H4(id="month_h4_gov", className="card-title", ),
                html.H4(id="month_h4_1_gov", className="card-title", ), ]
            )])),
        ])
])


def compare_data(today, yesterday):
    # 环比数据
    link_relative_ratio = round(((today-yesterday)/yesterday)*100, 2)
    if link_relative_ratio > 0:
        link_relative_ratio = "上升"+str(link_relative_ratio)+"%"
        title_style = {"background-color": "PaleGreen"}
        font_style = {"color": "MediumSeaGreen", "font-size": "20px"}
    else:
        link_relative_ratio = (str(link_relative_ratio) + "%").replace("-", "下降")
        title_style = {"background-color": "Tomato"}
        font_style = {"color": "FireBrick", "font-size": "20px"}
    return link_relative_ratio, title_style, font_style


@app.callback([Output('uv_title', 'style'),
               Output('uv_h2', 'children'),
               Output('uv_h4', 'children'),
               Output('uv_h4', 'style'),
               Output('uv_h4_1', 'children'),
               Output('uv_h4_1', 'style'), ],
              [Input('my-date-picker', 'date'), ]
              )
def daily_func(date):
    sql = f'''
    select count(distinct pid) from per_xxz_daily_info where date = '{date}'
    '''
    uv = pd.read_sql(sql, cnn_root).loc[0][0]
    date = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=1)
    sql = f'''
        select count(distinct pid) from per_xxz_daily_info where date = '{date}'
        '''
    yest = pd.read_sql(sql, cnn_root).loc[0][0]
    daily_ratio, daily_title_style, daily_font_style = compare_data(uv, yest)
    uv_h2_children = f"当日日活为：{uv}"
    uv_h4_children = f"前一日日活为{yest}，"
    uv_h4_1_children = f"环比{daily_ratio}"
    return daily_title_style, uv_h2_children, uv_h4_children, daily_font_style, uv_h4_1_children, daily_font_style


@app.callback([Output('weekly_title', 'style'),
               Output('weekly_h2', 'children'),
               Output('weekly_h4', 'children'),
               Output('weekly_h4', 'style'),
               Output('weekly_h4_1', 'children'),
               Output('weekly_h4_1', 'style'), ],
              [Input('my-date-picker', 'date'), ]
              )
def weekly_func(date):
    date_ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=7)
    sql = f'''
    select count(distinct pid) from per_xxz_daily_info where date between '{date_}' and '{date}'
    '''
    weekly = pd.read_sql(sql, cnn_root).loc[0][0]
    date_ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=8)
    date__ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=15)
    sql = f'''
        select count(distinct pid) from per_xxz_daily_info where date between '{date__}' and '{date_}'
        '''
    last_week = pd.read_sql(sql, cnn_root).loc[0][0]
    weekly_ratio, weekly_title_style, weekly_font_style = compare_data(weekly, last_week)
    weekly_h2_children = f"当日周活为：{weekly}"
    weekly_h4_children = f"前一周周活为{last_week}，"
    weekly_h4_1_children = f"环比{weekly_ratio}"
    return weekly_title_style, weekly_h2_children, weekly_h4_children, weekly_font_style, \
        weekly_h4_1_children, weekly_font_style


@app.callback([Output('month_title', 'style'),
               Output('month_h2', 'children'),
               Output('month_h4', 'children'),
               Output('month_h4', 'style'),
               Output('month_h4_1', 'children'),
               Output('month_h4_1', 'style'), ],
              [Input('my-date-picker', 'date'), ]
              )
def month_func(date):
    month = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=30)
    month_sql = f'''
    select count(distinct pid) from per_xxz_daily_info where date between '{month}' and '{date}'
    '''
    month = pd.read_sql(month_sql, cnn_root).loc[0][0]
    date_ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=31)
    date__ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=60)
    sql = f'''
        select count(distinct pid) from per_xxz_daily_info where date between '{date__}' and '{date_}'
        '''
    last_week = pd.read_sql(sql, cnn_root).loc[0][0]
    month_ratio, month_title_style, month_font_style = compare_data(month, last_week)
    month_h2_children = f"当日月活为：{month}"
    month_h4_children = f"前一月月活为{last_week}，"
    month_h4_1_children = f"环比{month_ratio}"
    return month_title_style, month_h2_children, month_h4_children, month_font_style, \
        month_h4_1_children, month_font_style


@app.callback([Output('uv_title_gov', 'style'),
               Output('uv_h2_gov', 'children'),
               Output('uv_h4_gov', 'children'),
               Output('uv_h4_gov', 'style'),
               Output('uv_h4_1_gov', 'children'),
               Output('uv_h4_1_gov', 'style'), ],
              [Input('my-date-picker', 'date'), ]
              )
def daily_func_gov(date):
    sql = f'''
    select count(distinct pid) from per_gov_daily_info where date = '{date}'
    '''
    uv = pd.read_sql(sql, cnn_root).loc[0][0]
    date = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=1)
    sql = f'''
        select count(distinct pid) from per_gov_daily_info where date = '{date}'
        '''
    yest = pd.read_sql(sql, cnn_root).loc[0][0]
    daily_ratio, daily_title_style, daily_font_style = compare_data(uv, yest)
    uv_h2_children = f"当日日活为：{uv}"
    uv_h4_children = f"前一日日活为{yest}，"
    uv_h4_1_children = f"环比{daily_ratio}"
    return daily_title_style, uv_h2_children, uv_h4_children, daily_font_style, uv_h4_1_children, daily_font_style


@app.callback([Output('weekly_title_gov', 'style'),
               Output('weekly_h2_gov', 'children'),
               Output('weekly_h4_gov', 'children'),
               Output('weekly_h4_gov', 'style'),
               Output('weekly_h4_1_gov', 'children'),
               Output('weekly_h4_1_gov', 'style'), ],
              [Input('my-date-picker', 'date'), ]
              )
def weekly_func(date):
    date_ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=7)
    sql = f'''
    select count(distinct pid) from per_gov_daily_info where date between '{date_}' and '{date}'
    '''
    weekly = pd.read_sql(sql, cnn_root).loc[0][0]
    date_ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=8)
    date__ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=15)
    sql = f'''
        select count(distinct pid) from per_gov_daily_info where date between '{date__}' and '{date_}'
        '''
    last_week = pd.read_sql(sql, cnn_root).loc[0][0]
    weekly_ratio, weekly_title_style, weekly_font_style = compare_data(weekly, last_week)
    weekly_h2_children = f"当日周活为：{weekly}"
    weekly_h4_children = f"前一周周活为{last_week}，"
    weekly_h4_1_children = f"环比{weekly_ratio}"
    return weekly_title_style, weekly_h2_children, weekly_h4_children, weekly_font_style, \
        weekly_h4_1_children, weekly_font_style


@app.callback([Output('month_title_gov', 'style'),
               Output('month_h2_gov', 'children'),
               Output('month_h4_gov', 'children'),
               Output('month_h4_gov', 'style'),
               Output('month_h4_1_gov', 'children'),
               Output('month_h4_1_gov', 'style'), ],
              [Input('my-date-picker', 'date'), ]
              )
def month_func(date):
    month = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=30)
    month_sql = f'''
    select count(distinct pid) from per_gov_daily_info where date between '{month}' and '{date}'
    '''
    month = pd.read_sql(month_sql, cnn_root).loc[0][0]
    date_ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=31)
    date__ = datetime.datetime.strptime(date, "%Y-%m-%d").date() - datetime.timedelta(days=60)
    sql = f'''
        select count(distinct pid) from per_gov_daily_info where date between '{date__}' and '{date_}'
        '''
    last_week = pd.read_sql(sql, cnn_root).loc[0][0]
    month_ratio, month_title_style, month_font_style = compare_data(month, last_week)
    month_h2_children = f"当日月活为：{month}"
    month_h4_children = f"前一月月活为{last_week}，"
    month_h4_1_children = f"环比{month_ratio}"
    return month_title_style, month_h2_children, month_h4_children, month_font_style, \
        month_h4_1_children, month_font_style
