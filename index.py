import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
from apps import DAU_page, dalyLogin
from app import app

# 侧边栏的样式参数。我们使用位置:固定和固定宽度
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


sidebar = html.Div(
    [
        html.H2("校校招", className="display-4",title='校校招，连接高校与企业的平台'),
        html.P(
            "校校招轻量级分析平台", className="lead"
        ),
        html.Hr(),
        dbc.Nav([
            dbc.NavItem(dbc.NavLink("首页", href="/", active="exact")),
            html.H3('用户端数据概览', className='lead',style={}),
            dbc.NavItem(dbc.NavLink("每日数据概览", href="/page-1", active="exact", )),
            dbc.NavItem(dbc.NavLink("Page 2", href="/page-2", active="exact", )),
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
    elif pathname == "/page-1":
        return DAU_page.layout_index
    elif pathname == "/page-2":
        return dalyLogin.layout_index
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=True, threaded=True, port=808)
