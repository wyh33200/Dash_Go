import logging
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output

from sqlalchemy import create_engine
import pandas as pd
import datetime
from app import app

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", )

cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")

layout_index = html.Div([
    # 时间筛选
    html.Div([
        dcc.DatePickerRange(
            id='my-date-picker-range',
            min_date_allowed=datetime.date(2021, 3, 24),
            max_date_allowed=datetime.date(2021, 12, 31),
            display_format='Y-MM-DD',
            month_format='Y-MM',
            start_date=datetime.date.today() - datetime.timedelta(days=7),
            end_date=datetime.date.today() - datetime.timedelta(days=1)
        ),
        html.Div(id='output-container-date-picker-range')
    ]),
    html.P(),
    html.H1("PASS")
])
