import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from app import app

from datetime import date
import pandas as pd
from pymongo import MongoClient
from dash.exceptions import PreventUpdate
import collections


layout_index = html.Div([
    dcc.Store(id='memory-output1'),
    html.Div([
        dcc.DatePickerRange(
            id='my-date-picker-range1',
            min_date_allowed=date(2020, 9, 18),
            max_date_allowed=date(2021, 12, 31),
            display_format='Y-MM-DD',
            month_format='Y-MM',
            start_date=date(2021, 2, 1),
            end_date=date(2021, 2, 28)
        ),
        html.Div(id='output-container-date-picker-range1')
    ]),
    html.P(),
    html.P(
        dcc.Graph(id='graph-with-date1', style={"margin-right": '6rem'},)
    )
]
)


# 数据获取
def data_get():
    client = MongoClient('localhost', 27017)
    col = client['xxz']['xxz_dailyLogin']
    df = pd.DataFrame(list(col.find())).drop('_id', axis=1)
    return df


# 【日活】数据读到json中
@app.callback(Output('memory-output1', 'data'),
              [Input('my-date-picker-range1', 'start_date'),
               Input('my-date-picker-range1', 'end_date')]
              )
def filter_date(start_date, end_date):
    filtered = data_get().query("date > '{start_date}' &date< '{end_date}' ".format(start_date=start_date,
                                                                                    end_date=end_date))
    return filtered.to_dict()


# 【日活】从json中，返回给graph中
@app.callback(Output('graph-with-date1', 'figure'),
              Input('memory-output1', 'data'))
def on_data_set_graph(data):
    if data is None:
        raise PreventUpdate

    aggregation = collections.defaultdict(
            lambda: collections.defaultdict(list)
        )
    data = pd.DataFrame(data)
    data = pd.DataFrame(data['date'].value_counts().sort_index()).reset_index()
    data.columns = ['date', 'num']
    data['uv'] = 'rihuo'
    data = data.to_dict('records')
    for row in data:
        a = aggregation[row['uv']]
        a['mode'] = 'lines+markers'
        a['x'].append(row['date'])
        a['y'].append(row['num'])
    return {
        'data': [x for x in aggregation.values()],
        'layout': {'xaxis': {'tickformat': '%Y-%m-%d'},
                   'title': '每日活跃用户'}
    }
