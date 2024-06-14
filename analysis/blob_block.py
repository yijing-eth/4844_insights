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

def plot_box_plot(df, x_column, y_column, title, palette="Set2", xlabel="X-axis", ylabel="Y-axis"):
    plt.figure(figsize=(12, 6))
    sns.boxplot(x=x_column, y=y_column, data=df, palette=palette)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()

def plot_multiple_line_chart(dfs, labels, title="Line Chart", xlabel="X-axis", ylabel="Y-axis", figsize=(10, 6)):
    plt.figure(figsize=figsize)
    palette = sns.color_palette("viridis", len(dfs))

    for df, label, color in zip(dfs, labels, palette):
        sns.lineplot(data=df, x='num_of_blob_per_block', y='gas_used',
                     marker='o', linestyle='-', color=color, label=label)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.legend()
    plt.show()

query1 = '''
with num_blobs_per_block as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root
),
gas_used as (
    select 
        hash,
        `gasUsed` as gas_used
    from canonical_block_details
)
select
	num_of_blob_per_block,
	gas_used
from num_blobs_per_block b 
join root_hash r on b.block_root = r.block_root
join gas_used on gas_used.hash = r.block_hash
'''

query11 = '''
with num_blobs_per_block as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root
),
gas_used as (
    select 
        hash,
        `gasUsed` as gas_used
    from canonical_block_details
)
select
	num_of_blob_per_block,
	avg(gas_used) as gas_used
from num_blobs_per_block b 
join root_hash r on b.block_root = r.block_root
join gas_used on gas_used.hash = r.block_hash
group by 1
'''

query2 = '''
with num_blobs_per_block as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root
),
non_mev as (
	SELECT hash as non_mev_hash
	FROM canonical_block_details cbd 
	WHERE cbd.extraData NOT IN (
	    SELECT extraData
	    FROM canonical_block_details
	    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
),
gas_used as (
    select 
        hash,
        `gasUsed` as gas_used
    from canonical_block_details
)
select
	num_of_blob_per_block,
	gas_used
from num_blobs_per_block b 
join root_hash r on b.block_root = r.block_root
join gas_used on gas_used.hash = r.block_hash
where block_hash in (select * from non_mev)
'''

query22 = '''
with num_blobs_per_block as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root
),
non_mev as (
	SELECT hash as non_mev_hash
	FROM canonical_block_details cbd 
	WHERE cbd.extraData NOT IN (
	    SELECT extraData
	    FROM canonical_block_details
	    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
),
gas_used as (
    select 
        hash,
        `gasUsed` as gas_used
    from canonical_block_details
)
select
	num_of_blob_per_block,
	avg(gas_used) as gas_used
from num_blobs_per_block b 
join root_hash r on b.block_root = r.block_root
join gas_used on gas_used.hash = r.block_hash
where block_hash in (select * from non_mev)
group by 1
'''

query3 = '''
with num_blobs_per_block as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root
),
non_mev as (
	SELECT hash as non_mev_hash
	FROM canonical_block_details cbd 
	WHERE cbd.extraData NOT IN (
	    SELECT extraData
	    FROM canonical_block_details
	    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
),
gas_used as (
    select 
        hash,
        `gasUsed` as gas_used
    from canonical_block_details
)
select
	num_of_blob_per_block,
	gas_used
from num_blobs_per_block b 
join root_hash r on b.block_root = r.block_root
join gas_used on gas_used.hash = r.block_hash
where block_hash not in (select * from non_mev)
'''

query33 = '''
with num_blobs_per_block as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root
),
non_mev as (
	SELECT hash as non_mev_hash
	FROM canonical_block_details cbd 
	WHERE cbd.extraData NOT IN (
	    SELECT extraData
	    FROM canonical_block_details
	    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
),
gas_used as (
    select 
        hash,
        `gasUsed` as gas_used
    from canonical_block_details
)
select
	num_of_blob_per_block,
	median(gas_used) as gas_used
from num_blobs_per_block b 
join root_hash r on b.block_root = r.block_root
join gas_used on gas_used.hash = r.block_hash
where block_hash not in (select * from non_mev)
group by 1
'''

query4 = '''
with blob as (
	select
	    block_root,
	    max(position) + 1 as num_of_txns,
	    sum(blob_gas/131072) as num_of_blob_per_block
	from canonical_beacon_block_execution_transaction
	where type = 3 
	group by 1)
select 
	num_of_blob_per_block,
	size
from blob b
left join canonical_block_root r on r.block_root = b.block_root
left join canonical_block_details bd on r.block_hash = bd.hash
'''

columns = ['num_of_blob_per_block', 'gas_used']
df1 = get_query_res(columns, query1)
plot_box_plot(df1, 'num_of_blob_per_block', 'gas_used', 'Gas Used vs Number of Blobs per Block', palette="Set2", xlabel="Number of Blobs", ylabel="Gas Used (Gwei)")
df2 = get_query_res(columns, query2)
plot_box_plot(df2, 'num_of_blob_per_block', 'gas_used', 'Gas Used vs Number of Blobs per Block (Non-MEV)', palette="Set2", xlabel="Number of Blobs", ylabel="Gas Used (Gwei)")
df3 = get_query_res(columns, query3)
plot_box_plot(df3, 'num_of_blob_per_block', 'gas_used', 'Gas Used vs Number of Blobs per Block (MEV)', palette="Set2", xlabel="Number of Blobs", ylabel="Gas Used (Gwei)")
df11 = get_query_res(columns, query11)
df22 = get_query_res(columns, query22)
df33 = get_query_res(columns, query33)
plot_multiple_line_chart([df11, df22, df33], ['All', 'Non-MEV', 'MEV'], title="Median Gas Used vs Number of Blobs per Block", xlabel="Number of Blobs", ylabel="Gas Used (Gwei)", figsize=(12, 6))
df4 = get_query_res(['num_of_blob_per_block', 'size'], query4)
df4 = df4[df4['size'] <= df4['size'].quantile(0.99)]
plot_box_plot(df4, 'num_of_blob_per_block', 'size', 'Block size vs Number of Blobs per Block', palette="Set2", xlabel="Number of blobs", ylabel="Block size (bytes)")
