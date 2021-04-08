import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from apps import daily, student_daily_report
from app import app

# 侧边栏的样式参数。使用位置:固定和固定宽度
SIDEBAR_STYLE = {
    "position": "fixed",
    "top": 0,
    "left": 0,
    "bottom": 0,
    "width": "16rem",
    "padding": "2rem 1rem",
    "background-color": "#f8f9fa",
}
# 主要内容的样式将其放置在侧边栏的右侧，然后添加一些填充
CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

header = html.Header(html.Title('校校招'))

sidebar = html.Div(
    [
        html.H2("校校招", className="display-4", title='校校招，连接高校与企业的平台'),
        html.P(
            "校校招轻量级分析平台", className="lead"
        ),
        html.Hr(),
        dbc.Nav([
            dbc.NavItem(dbc.NavLink("首页", href="/", active="exact")),
            html.H3('用户端数据概览', className='lead', style={}),
            dbc.NavItem(dbc.NavLink("每日数据概览", href="/per/report", active="exact", )),
            dbc.NavItem(dbc.NavLink("用户画像", href="/per/portrayal", active="exact", )),
            html.H3('三端日报', className='lead', style={}),
            dbc.NavItem(dbc.NavLink("学生端日报", href="/page-2", active="exact", )),
        ],
            vertical="md",
            style={}
        )
    ],
    style=SIDEBAR_STYLE,
)
content = html.Div(id="page-content", style=CONTENT_STYLE)

app.layout = html.Div([dcc.Location(id="url"), sidebar, content])


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname == "/":
        return html.P("pass")
    elif pathname == "/per/report":
        return daily.layout_index
    elif pathname == "/per/portrayal":
        return html.P("pass")
    elif pathname == "/page-2":
        return student_daily_report.layout_index
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True, threaded=True, port=8080)
