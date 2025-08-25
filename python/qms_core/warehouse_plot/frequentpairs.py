import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import community  # pip install python-louvain

# 读取 SQL 导出的数据
df = pd.read_csv(r"C:\Users\jpeqz\OneDrive - Epiroc\SCX\QRYs\OUTBOUND\frequent_pairs.csv")  

# 限制显示前 100 组最高频的件号对
df = df.nlargest(100, "CO_OCCURRENCE")  

# 创建图
G = nx.Graph()
for _, row in df.iterrows():
    G.add_edge(row["ITEM_A"], row["ITEM_B"], weight=row["CO_OCCURRENCE"])

# 计算社区（Louvain）
partition = community.best_partition(G, weight='weight')  # {ITEMNUM: community_id}

# 转换为 DataFrame
community_df = pd.DataFrame(list(partition.items()), columns=["ITEMNUM", "COMMUNITY_ID"])

# 输出 CSV 方便后续分析
# community_df.to_csv("Warehouse\item_communities.csv", index=False)

# 打印前几行
print(community_df.head())

# 🎨 可视化社区网络
import random
community_colors = {c: f"#{random.randint(0, 0xFFFFFF):06x}" for c in set(partition.values())}

plt.figure(figsize=(12, 8))
pos = nx.spring_layout(G, k=0.8)  # 调整布局

# 画出节点，颜色按照社区分组
for node, community_id in partition.items():
    nx.draw_networkx_nodes(G, pos, nodelist=[node], node_color=community_colors[community_id], node_size=3000)

# 画出边
edges = G.edges(data=True)
weights = [d['weight'] for (_, _, d) in edges]
nx.draw_networkx_edges(G, pos, edge_color=weights, width=2, edge_cmap=plt.cm.Blues)

# 画出标签
nx.draw_networkx_labels(G, pos, font_size=10)

plt.title("Top 100 Co-Shipped Items Network (Community Detection)")
plt.show()
