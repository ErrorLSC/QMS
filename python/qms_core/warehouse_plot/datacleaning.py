import pandas as pd
import plotly.express as px
import sqlite3
import networkx as nx
import community

def coordinate_convert_uniquexyz(file,to_csv = False,savepath = None):
    rawdata_df = pd.read_csv(file)
    rawdata_df['WLOC'] = rawdata_df['WLOC'].str.strip()
    rawdata_df[['X','Y','Z']] = rawdata_df['WDESC'].str.split(',', expand=True)
    convert_df = rawdata_df.groupby(['X', 'Y', 'Z'], as_index=False)['WLOC'].agg(lambda x: ','.join(x))
    print(convert_df.head(10))
    if to_csv == True:
        convert_df.to_csv(savepath,index=False)
        print("-----------Converted data successfully saved.-------------")
    return convert_df

def coordinate_convert_uniqueloc(file,to_csv = False,savepath = None):
    rawdata_df = pd.read_csv(file)
    rawdata_df['WLOC'] = rawdata_df['WLOC'].str.strip()
    rawdata_df[['X','Y','Z']] = rawdata_df['WDESC'].str.split(',', expand=True)
    convert_df = rawdata_df[['WWHS','WLOC','WDESC','X','Y','Z']]
    convert_df['WLTYP'] = 'NORMAL'
    convert_df['WZONE'] = None
    convert_df['VOLCAP'] = None
    convert_df['WEIGHTCAP'] = None
    duplicate = convert_df[convert_df['WLOC'].duplicated()]
    print(duplicate)
    if to_csv == True:
        convert_df.to_csv(savepath,index=False)
        print("-----------Converted data successfully saved.-------------")
    print(convert_df.head(10))
    return convert_df

def read_db_warehouselayout(dblink):
    conn = sqlite3.connect(dblink)
    query = "SELECT WLOC,X,Y,Z FROM ILM WHERE Warehouse='5'"
    df = pd.read_sql_query(query,conn)
    conn.close()
    # print(df)
    return df

def read_db_warehouse(dblink):
    conn = sqlite3.connect(dblink)
    query = "SELECT * FROM TO_PLOT"
    df = pd.read_sql_query(query,conn)
    conn.close()
    # print(df)
    return df

def read_db_community(dblink,pairs_num=100):
    conn = sqlite3.connect(dblink)
    query1 = "SELECT * FROM ITEMPAIR_5CW"
    query2 = "SELECT IWI.ITEMNUM,IWI.WLOC,ILM.X,ILM.Y,ILM.Z FROM IWI INNER JOIN ILM ON IWI.WLOC = ILM.WLOC"
    df = pd.read_sql_query(query1,conn)
    bindingloc = pd.read_sql_query(query2,conn)
    conn.close()
    # 限制显示前 100 组最高频的件号对
    df = df.nlargest(pairs_num, "CO_OCCURRENCE")  

    # 创建图
    G = nx.Graph()
    for _, row in df.iterrows():
        G.add_edge(row["ITEM_A"], row["ITEM_B"], weight=row["CO_OCCURRENCE"])

    # 计算社区（Louvain）
    partition = community.best_partition(G, weight='weight')  # {ITEMNUM: community_id}

    # 转换为 DataFrame
    community_df = pd.DataFrame(list(partition.items()), columns=["ITEMNUM", "COMMUNITY_ID"])
    community_df = community_df.merge(bindingloc,on='ITEMNUM',how='left')
    community_df = community_df.dropna(axis=0)
    # print(community_df)
    return community_df

def read_db_itemheatmap(dblink):
    conn = sqlite3.connect(dblink)
    query = "SELECT * FROM ABC_CLASS"
    df = pd.read_sql_query(query,conn)
    conn.close()
    return df

def read_db_locheatmap(dblink):
    conn = sqlite3.connect(dblink)
    query = "SELECT * FROM LOCATION_ABC_CLASS"
    df = pd.read_sql_query(query,conn)
    conn.close()
    return df


def warehouseplot(csv_data):
    fig_df = pd.read_csv(csv_data)
    x_range = fig_df['X'].max() - fig_df['X'].min()
    y_range = fig_df['Y'].max() - fig_df['Y'].min()
    z_range = fig_df['Z'].max() - fig_df['Z'].min()
    fig = px.scatter_3d(
        fig_df,
        x = 'X',
        y = 'Y',
        z = 'Z',
        title = 'Shimotsuma 5 CW'
    )
    fig.update_traces(marker=dict(size=5),hovertemplate="<b>Location:</b> %{text}<br>"
                  "<b>X:</b> %{x} m<br>"
                  "<b>Y:</b> %{y} m<br>"
                  "<b>Z:</b> %{z} m<extra></extra>",  # 只显示 XYZ,WLOC 信息
        text=fig_df['WLOC'])
    fig.update_layout(scene=dict(
        aspectmode="manual", 
        aspectratio = dict(x=x_range,y=y_range,z=z_range)))
    fig.show()

if __name__ == "__main__":
    rawdata_path = r"C:\Users\jpeqz\OneDrive - Epiroc\SCX\QRYs\Master File\Location_Coodinate.csv"
    converteddata_path = r"C:\Users\jpeqz\OneDrive - Epiroc\SCX\QRYs\Master File\Location_Coodinate_converted_uniqueloc.csv"
    dblink = r"C:\Users\jpeqz\OneDrive - Epiroc\Python\Warehouse\Database\WHMaster.db"
    # coordinate_convert_uniqueloc(rawdata_path,to_csv=True,savepath=converteddata_path)
    # warehouseplot(converteddata_path)
    # read_db_community(dblink,200)