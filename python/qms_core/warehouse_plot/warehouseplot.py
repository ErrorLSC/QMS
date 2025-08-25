import dash
import dash_bootstrap_components as dbc
from dash import dcc,html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from qms_core.warehouse_plot.datacleaning import read_db
import pandas as pd
# 仓库数据（多对多关系）
dblink = r"C:\Users\jpeqz\OneDrive - Epiroc\QMS\data\WHMaster.db"
data = read_db(dblink)
# Dash App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.title = "Qinglei Warehouse Management System"

# 📌 侧边栏
sidebar = dbc.Nav(
    [
        dbc.NavLink("📦 3D Stock View", href="/", active="exact"),
        dbc.NavLink("📊 Stock Trend", href="/trend", active="exact"),
        dbc.NavLink("📈 Picking Analysis", href="/picking", active="exact"),
        dbc.NavLink("📉 Order Analysis", href="/orders", active="exact"),
        dbc.NavLink("🔥 Heatmap", href="/heatmap", active="exact"),
    ],
    vertical=True,
    pills=True,
    className="bg-light p-3",
)

# 📌 页面内容区域
content = html.Div(id="page-content", style={"margin-left": "220px", "padding": "20px"})

app.layout = html.Div([
    dcc.Location(id="url", refresh=False),  # 监听 URL 变化
    dbc.Row([
        dbc.Col(sidebar, width=2, style={"height": "100vh"}),  # 侧边栏
        dbc.Col(content, width=10),
    ])
])

# 📌 3D 仓库可视化回调
@app.callback(
    Output("warehouse-plot", "figure"),
    Output("camera-store", "data"),
    [Input("search-mode", "value"),
     Input("search-input", "value"),
     Input("upload-data", "contents")],
    [State("camera-store", "data"),
     State("warehouse-plot", "relayoutData")]
)
def update_figure(mode, query,upload_contents, stored_camera, relayout_data):
    # 颜色逻辑：有库存=蓝色，无库存=灰色
    colors = ['blue' if not pd.isna(item) else 'gray' for item in data['ITEMNUM']]
    sizes = [5] * len(data)
    
    # 处理搜索高亮
    matching_indices = []
    if query:
        if mode == 'ItemNumber' and query in data['ITEMNUM'].values:
            matching_indices = data.index[data['ITEMNUM'] == query].tolist()
        elif mode == 'location' and query in data['WLOC'].values:
            matching_indices = data.index[data['WLOC'] == query].tolist()

    if upload_contents:
        content_type, content_string = upload_contents.split(',')
        decoded = pd.read_csv(pd.compat.StringIO(content_string), header=None)
        if 'location' in decoded.columns:
            csv_locations = decoded[20].dropna().tolist()
            matching_indices.extend(data.index[data['WLOC'].isin(csv_locations)])

    # 更新高亮颜色和大小
    for idx in matching_indices:
        colors[idx] = 'red'
        sizes[idx] = 10

    # 计算 3D 视图比例
    x_range = data['X'].max() - data['X'].min()
    y_range = data['Y'].max() - data['Y'].min()
    z_range = data['Z'].max() - data['Z'].min()

    # 读取相机视角
    camera = stored_camera.get("camera", None)
    if relayout_data and "scene.camera" in relayout_data:
        camera = relayout_data["scene.camera"]

    # 绘制 3D 仓库图
    fig = go.Figure(data=[go.Scatter3d(
        x=data['X'],
        y=data['Y'],
        z=data['Z'],
        mode='markers',
        marker=dict(size=sizes, color=colors),
        text=[f"Location: {loc}, ItemNumber: {ItemNumber}, QTY: {qty}"
              for loc, ItemNumber, qty in zip(data['WLOC'], data['ITEMNUM'], data['QTYOH'])],
        hoverinfo="text"
    )])
    
    fig.update_layout(
        title="IBARAKI 5CW",
        margin=dict(l=0, r=0, b=0, t=40),
        scene=dict(
            aspectmode="manual",
            aspectratio=dict(x=x_range, y=y_range, z=z_range),
            camera=camera
        )
    )
    return fig, {"camera": camera}

# 📌 页面切换回调
@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def render_page(pathname):
    if pathname == "/trend":
        return html.Div([
            html.H3("📊 Stock Trend"),
            dcc.Graph(figure={})  # 这里可以放库存趋势图
        ])
    elif pathname == "/picking":
        return html.Div([
            html.H3("📈 Picking Analysis"),
            dcc.Graph(figure={})  # 这里可以放提货分析图
        ])
    elif pathname == "/orders":
        return html.Div([
            html.H3("📉 Order Stastic"),
            dcc.Graph(figure={})  # 这里可以放订单统计图
        ])
    elif pathname == "/heatmap":
        return html.Div([
            html.H3("🔥 Heatmap"),
            dcc.Graph(figure={})  # 这里可以放热力图
        ])
    else:
        return html.Div([
            html.Div([
                dcc.Dropdown(
                    id="search-mode",
                    options=[
                        {'label': 'Search by ItemNumber', 'value': 'ItemNumber'},
                        {'label': 'Search by Location', 'value': 'location'}
                    ],
                    value='ItemNumber',
                    style={'width': '200px', 'display': 'inline-block'}
                ),
                dcc.Input(
                    id="search-input",
                    type="text",
                    placeholder="Input ItemNumber Or Location",
                    debounce=True,
                    style={'width': '200px', 'display': 'inline-block', 'marginLeft': '10px'}
                ),
                dcc.Upload(id='upload-data',children=html.Button('Upload CSV'),
                           style={'marginLeft': '10px', 'display': 'inline-block'})
            ], style={'marginBottom': '10px'}),
            dcc.Graph(id="warehouse-plot"),
            dcc.Store(id='camera-store', data={})  # 存储相机视角
        ])

if __name__ == '__main__':
    app.run(debug=True)