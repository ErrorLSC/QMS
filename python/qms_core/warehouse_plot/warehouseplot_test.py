import dash
import dash_bootstrap_components as dbc
from dash import dcc, html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
from plotly.express.colors import qualitative
from qms_core.warehouse_plot.datacleaning import read_db_warehouse,read_db_community,read_db_warehouselayout,read_db_itemheatmap,read_db_locheatmap
import pandas as pd
import io
import base64

# 仓库数据
dblink = r"C:\Users\jpeqz\OneDrive - Epiroc\QMS\data\WHMaster.db"
# Dash App
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP],suppress_callback_exceptions=True)
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
    dcc.Location(id="url", refresh=False),
    dbc.Row([
        dbc.Col(sidebar, width=2, style={"height": "100vh"}),
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
def update_3d_warehouse_view(mode, query, upload_contents, stored_camera, relayout_data):
    data = read_db_warehouse(dblink)
    # 默认尺寸和颜色
    no_stock_size =3 
    default_size = 5
    highlight_size = 15  # 放大尺寸
    no_stock_color = 'lightgray'
    csv_match_color = 'red'
    
    # 业务线颜色映射 (使用 Prism 色系)
    unique_lines = data['CXPPLC'].dropna().unique()
    color_map = {line: color for line, color in zip(unique_lines, qualitative.Prism)}
    
    matching_indices = []
    
    # 查询模式下的匹配
    if query:
        if mode == 'ItemNumber' and query in data['ITEMNUM'].values:
            matching_indices = data.index[data['ITEMNUM'] == query].tolist()
        elif mode == 'location' and query in data['WLOC'].values:
            matching_indices = data.index[data['WLOC'] == query].tolist()
    
    # CSV 文件上传时的匹配
    if upload_contents:
        content_type, content_string = upload_contents.split(',')
        decoded_string = base64.b64decode(content_string).decode('utf-8-sig')
        decoded = pd.read_csv(io.StringIO(decoded_string), encoding='utf-8-sig', header=None)
        if len(decoded.columns) >= 21:
            csv_locations = decoded[20].dropna().tolist()
            matching_indices.extend(data.index[data['WLOC'].isin(csv_locations)])
    
    # 确定相机视角
    camera = stored_camera.get("camera", None)
    if relayout_data and "scene.camera" in relayout_data:
        camera = relayout_data["scene.camera"]

    # 构建图形
    fig = go.Figure()

    # 为每条业务线创建单独的 Trace
    for line, color in color_map.items():
        line_data = data[data['CXPPLC'] == line]
        
        # 确定颜色和尺寸
        colors = [csv_match_color if idx in matching_indices else color 
                  for idx in line_data.index]
        sizes = [highlight_size if idx in matching_indices else default_size 
                 for idx in line_data.index]
        
        fig.add_trace(go.Scatter3d(
            x=line_data['X'],
            y=line_data['Y'],
            z=line_data['Z'],
            mode='markers',
            marker=dict(size=sizes, color=colors),
            name=line,
            text=[f"Location: {loc}, ItemNumber: {item}, QTY: {qty}, Line: {line}"
                  for loc, item, qty in zip(line_data['WLOC'], line_data['ITEMNUM'], line_data['QTYOH'])],
            hoverinfo="text"
        ))
    
    # 添加无库存的灰色点
    no_stock_data = data[data['QTYOH'].isna()]
    print(no_stock_data)
    fig.add_trace(go.Scatter3d(
        x=no_stock_data['X'],
        y=no_stock_data['Y'],
        z=no_stock_data['Z'],
        mode='markers',
        marker=dict(size=no_stock_size, color=no_stock_color, sizemode='diameter'),
        name='No Stock',
        legendgroup='',  # 确保在双击图例时不隐藏灰色点
        text=[f"Location: {loc}"
            for loc in zip(no_stock_data['WLOC'])],
        hoverinfo="text",
        showlegend=True
    ))

    fig.update_layout(
        title="IBARAKI 5CW",
        margin=dict(l=0, r=0, b=0, t=40),
        scene=dict(
            aspectmode="data",
            camera=camera
        ),
        legend=dict(title="Business Lines", itemsizing='constant')
    )

    return fig, {"camera": camera}

@app.callback(
    Output('3d-warehouse-view', 'figure'),
    Input('refresh-button', 'n_clicks')
)
def refresh_view(n_clicks):
    # 调用之前的 update_3d_warehouse_view 函数，保持参数默认或空
    fig, _ = update_3d_warehouse_view(
        mode=None, query=None, 
        upload_contents=None, 
        stored_camera={}, 
        relayout_data=None
    )
    return fig

@app.callback(
    Output("order-analysis-plot", "figure"),
    Output("order-camera-store", "data"),
    [Input("order-camera-store", "data"),
     Input("order-analysis-plot", "relayoutData")]
)
def update_order_analysis_figure(camera_data, relayout_data):
    data_plot = read_db_community(dblink,200)
    data_background = read_db_warehouselayout(dblink)
    unique_communities = data_plot['COMMUNITY_ID'].unique()
    color_map = {community: f"hsl({i * 360 / len(unique_communities)}, 100%, 50%)"
                 for i, community in enumerate(unique_communities)}
    colors = [color_map[cid] for cid in data_plot['COMMUNITY_ID']]

    x_range = data_background['X'].max() - data_background['X'].min()
    y_range = data_background['Y'].max() - data_background['Y'].min()
    z_range = data_background['Z'].max() - data_background['Z'].min()

    camera = camera_data.get("camera", None)
    if relayout_data and "scene.camera" in relayout_data:
        camera = relayout_data["scene.camera"]

    fig = go.Figure()

    # 背景图层 - 灰色点
    fig.add_trace(go.Scatter3d(
        x=data_background['X'],
        y=data_background['Y'],
        z=data_background['Z'],
        mode='markers',
        marker=dict(size=3, color='gray'),
        text=[f"Location: {wloc}" for wloc in data_background['WLOC']],
        hoverinfo="text",
        name = "Warehouse Layout"
    ))

    # 数据图层 - 彩色点
    fig.add_trace(go.Scatter3d(
        x=data_plot['X'],
        y=data_plot['Y'],
        z=data_plot['Z'],
        mode='markers',
        marker=dict(size=5, color=colors),
        text=[f"ITEMNUM: {item}, WLOC: {wloc}, COMMUNITY_ID: {cid}"
              for item, wloc, cid in zip(data_plot['ITEMNUM'], data_plot['WLOC'], data_plot['COMMUNITY_ID'])],
        hoverinfo="text",
        name = "Community Data"
    ))

    fig.update_layout(
        title="Order Analysis - COMMUNITY_ID Visualization",
        scene=dict(
            aspectmode="manual",
            aspectratio=dict(x=x_range, y=y_range, z=z_range),
            camera=camera
        )
    )
    return fig, {"camera": camera}

@app.callback(
    Output('heatmap-3d', 'figure'),
    [Input('heatmap-type', 'value'),
     Input('color-mode', 'value'),
     Input('refresh-button', 'n_clicks')]
)
def update_heatmap(heatmap_type, color_mode, n_clicks):
    ABC_COLORS = {
        'A': '#1f77b4',  # 蓝色
        'B': '#ff7f0e',  # 橙色
        'C': '#2ca02c'   # 绿色
    }   

    # 背景数据 (灰色)
    bg_data = read_db_warehouselayout(dblink)

    # 选择热力图数据源
    if heatmap_type == 'item':
        heatmap_data = read_db_itemheatmap(dblink)
    else:
        heatmap_data = read_db_locheatmap(dblink)

    # 背景点 (灰色)
    background_trace = go.Scatter3d(
        x=bg_data['X'],
        y=bg_data['Y'],
        z=bg_data['Z'],
        mode='markers',
        marker=dict(size=2, color='lightgrey'),
        name='All Locations',
        showlegend=True
    )

    traces = [background_trace]

    if color_mode == 'ABC_CLASS':
        # 为每个 ABC 类别单独创建一个 trace
        for cls, color in ABC_COLORS.items():
            class_data = heatmap_data[heatmap_data['ABC_CLASS'] == cls]
            traces.append(go.Scatter3d(
                x=class_data['X'],
                y=class_data['Y'],
                z=class_data['Z'],
                mode='markers',
                marker=dict(size=8, color=color),
                name=f'Class {cls}',
                text=[f"ABC Class: {cls}" for _ in class_data['ABC_CLASS']],
                hoverinfo='text',
                showlegend=True
            ))
    else:
        # PICK_NUM 模式
        traces.append(go.Scatter3d(
            x=heatmap_data['X'],
            y=heatmap_data['Y'],
            z=heatmap_data['Z'],
            mode='markers',
            marker=dict(
                size=[5 + (num / max(heatmap_data['PICK_NUM']) * 15) for num in heatmap_data['PICK_NUM']],
                color=heatmap_data['PICK_NUM'],
                colorscale='Viridis',
                showscale=True,
                colorbar=dict(title='Pick Number')
            ),
            text=[f"Pick Num: {p}, ABC Class: {c}" 
                  for p, c in zip(heatmap_data['PICK_NUM'], heatmap_data['ABC_CLASS'])],
            hoverinfo='text',
            name=f'{heatmap_type.capitalize()} Heatmap',
            showlegend=True
        ))

    # 绘制图形
    fig = go.Figure(data=traces)
    fig.update_layout(
        title='Warehouse 3D Heatmap',
        margin=dict(l=0, r=0, b=0, t=40),
        scene=dict(aspectmode='data')
    )
    return fig

@app.callback(
    Output("page-content", "children"),
    Input("url", "pathname")
)
def render_page(pathname):
    if pathname == "/trend":
        return html.Div([html.H3("📊 Stock Trend"), dcc.Graph(figure={})])
    elif pathname == "/picking":
        return html.Div([html.H3("📈 Picking Analysis"), dcc.Graph(figure={})])
    elif pathname == "/orders":
        return html.Div([
            html.H3("📉 Order Analysis"),
            dcc.Graph(id="order-analysis-plot"),
            dcc.Store(id='order-camera-store', data={})
        ])
    elif pathname == "/heatmap":
        return html.Div([html.H3("🔥 Heatmap"), 
        dcc.Dropdown(
            id='heatmap-type',
            options=[
                {'label': 'Item Heatmap', 'value': 'item'},
                {'label': 'Location Heatmap', 'value': 'location'}
            ],
            value='item',
            clearable=False
        ),
        dcc.Dropdown(
            id='color-mode',
            options=[
                {'label': 'ABC Class', 'value': 'ABC_CLASS'},
                {'label': 'Pick Number', 'value': 'PICK_NUM'}
            ],
            value='ABC_CLASS',
            clearable=False
        ),
        html.Button('Refresh View', id='refresh-button', n_clicks=0)
    ]),dcc.Graph(id='heatmap-3d', style={'height': '90vh'})
    else:
        return html.Div([
            html.H1("📦 3D Warehouse Visualization", style={'textAlign': 'center', 'marginBottom': '20px'}),
            html.Div([
                dcc.Dropdown(id="search-mode", options=[
                    {'label': 'Search by ItemNumber', 'value': 'ItemNumber'},
                    {'label': 'Search by Location', 'value': 'location'}
                ], value='ItemNumber', style={'width': '200px', 'display': 'inline-block'}),
                dcc.Input(id="search-input", type="text", placeholder="Input ItemNumber Or Location", debounce=True,
                          style={'width': '200px', 'display': 'inline-block', 'marginLeft': '10px'}),
                dcc.Upload(id='upload-data', children=html.Button('Upload Picking List CSV'),
                           style={'marginLeft': '500px', 'display': 'inline-block'}),
                html.Button('Refresh View', id='refresh-button', style={'marginLeft': '10px', 'display': 'inline-block'},n_clicks=0)
            ], style={'display': 'flex', 'alignItems': 'center', 'marginBottom': '10px'}),
            dcc.Graph(id="warehouse-plot"),
            dcc.Store(id='camera-store', data={})
        ])

if __name__ == '__main__':
    app.run(debug=True)