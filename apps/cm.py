import datetime
import logging

import dash_core_components as dcc
import dash_html_components as html
from sqlalchemy import create_engine

#
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", )

calendar = html.Div([
    dcc.DatePickerRange(
        id='my-date-picker-range',
        min_date_allowed=datetime.date(2020, 9, 18),
        max_date_allowed=datetime.date(2021, 12, 31),
        display_format='Y-MM-DD',
        month_format='Y-MM',
        start_date=datetime.date.today() - datetime.timedelta(days=7),
        end_date=datetime.date.today() - datetime.timedelta(days=1)
    ),
    html.Div(id='output-container-date-picker-range')
])

cnn_xxz_log = create_engine("mysql+mysqlconnector://xxzlog:xxz@log@192.168.2.6:3306/xxz_log")

cnn_xxz_base = create_engine("mysql+mysqlconnector://xxzdata:chitone@xxzdata@192.168.2.225:3306/xxz_base")

cnn_51job_base = create_engine("mysql+mysqlconnector://M5156BSQL:&,R}sH_F{g5!$w](+V8')-8gJ@192.168.0.51:3306/5156base")

cnn_51job_log = create_engine("mysql+mysqlconnector://db180:db180@job5156@192.168.0.98:3306/LogDB")

cnn_51job_action = create_engine("mysql+mysqlconnector://M5156ASQL:aPvM!)*^%~cdwc@7*^1@192.168.0.52:3306/5156action")

cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/wyh")
