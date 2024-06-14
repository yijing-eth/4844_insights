import clickhouse_connect
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

client = clickhouse_connect.get_client(
    host='127.0.0.1', username='default', password='', port=8123)


def get_query_res(columns, query):
    result = client.query(query).result_rows
    df = pd.DataFrame(
        result, columns=columns)
    return df


query1 = '''
with max_pos as (
    select
        block_root,
        max(position) as max_position
    from canonical_beacon_block_execution_transaction
    group by 1),
blob_pos as (
    select block_root, position
    from canonical_beacon_block_execution_transaction
    where type = 3
)
select block_root, position/max_position as position
from blob_pos bp
left join max_pos mp on bp.block_root = mp.block_root
'''

query2 = '''
        select block_root, position
        from canonical_beacon_block_execution_transaction
        where type = 3
    '''


def plot_histogram(df, column, title="Histogram", xlabel="X-axis", ylabel="Y-axis", bins=50, palette="Set2"):
    plt.figure(figsize=(10, 6))
    sns.histplot(df[column].dropna(), bins=bins, kde=False,
                 color=sns.color_palette(palette, 1)[0], edgecolor='black')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()


# df1 = get_query_res(['block_root', 'relative_position'], query1)
# plot_histogram(df1, 'relative_position', title="Relative Position of Blobs in Blocks",
            #    xlabel="Relative position", ylabel="Frequency", bins=50)
df2 = get_query_res(['block_root', 'position'], query2)
plot_histogram(df2, 'position', title="Actual Position of Blobs in Blocks", xlabel="Position",
                ylabel="Frequency", bins=50)
