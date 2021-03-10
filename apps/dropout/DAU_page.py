import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from datetime import date
import pandas as pd
from pymongo import MongoClient
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go

from app import app

layout_index = html.Div([
    html.Div([
        # 时间筛选
        dcc.DatePickerRange(
            id='my-date-picker-range',
            min_date_allowed=date(2020, 9, 18),
            max_date_allowed=date(2021, 12, 31),
            display_format='Y-MM-DD',
            month_format='Y-MM',
            start_date=date(2021, 2, 1),
            end_date=date(2021, 2, 28)
        ),
        html.Div(id='output-container-date-picker-range')
    ]),
    html.P(),
    html.P(
        # 折线图
        dcc.Graph(id='graph-with-line',)
    )
]
)


# 数据获取
def data_get():
    client = MongoClient('localhost', 27017)
    col = client['xxz']['xxz_dailyLogin']
    df = pd.DataFrame(list(col.find())).drop('_id', axis=1)
    return df


# 【日活】从json中，返回给graph中
@app.callback(Output('graph-with-line', 'figure'),
              [Input('my-date-picker-range', 'start_date'),
               Input('my-date-picker-range', 'end_date')]
              )
def on_data_set_graph(start_date, end_date):
    if start_date and end_date is None:
        raise PreventUpdate
    data = data_get().query("date > '{start_date}' &date< '{end_date}' ".format(start_date=start_date, end_date=end_date
                                                                                ))
    data_ = data.groupby('date').agg({'pid': 'count', 'hanldNum': 'sum'})
    data_['avg'] = round(data_['hanldNum'] / data_['pid'], 2)
    data_ = data_.reset_index()
    trace1 = go.Bar(
        x=data_['date'],
        y=data_['pid'],
        name='每日登陆人数'
    )
    trace2 = go.Scatter(
        x=data_['date'],
        y=data_['hanldNum'],
        mode='lines',
        name='操作次数',
        yaxis="y2"
    )
    trace3 = go.Scatter(
        x=data_['date'],
        y=data_['avg'],
        name='平均操作次数',
        line={'dash': 'dot'}

    )
    data_1 = [trace1, trace2, trace3]

    layout = go.Layout(title="每日操作详情",
                       xaxis=dict(tickformat='%Y-%m-%d', showgrid=False, linewidth=0 ),
                       yaxis=dict(title="每日登陆人数", showgrid=False),
                       yaxis2=dict(title="次数", overlaying='y', side="right", showgrid=False),
                       # legend=dict(x=0, y=1, font=dict(size=12, color="black")),
                       height=600,
                       template='none',
                       plot_bgcolor="#E1FAF1",
                       )
    fig = go.Figure(data=data_1, layout=layout)
    return fig
