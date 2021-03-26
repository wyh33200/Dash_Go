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


cnn_root = create_engine("mysql+mysqlconnector://root:root@localhost:3306/dash")
layout_index = html.Div([])
