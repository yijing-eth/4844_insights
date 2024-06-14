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


def plot_single_line_chart(df, title="Line Chart", xlabel="X-axis", ylabel="Y-axis", figsize=(10, 6)):
    plt.figure(figsize=figsize)
    sns.lineplot(data=df, x='num_of_blobs', y='propagation_sec',
                 marker='o', color=sns.color_palette("viridis", as_cmap=True)(0.5))
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()


def plot_multiple_line_chart(dfs, labels, title="Line Chart", xlabel="X-axis", ylabel="Y-axis", figsize=(10, 6)):
    plt.figure(figsize=figsize)
    palette = sns.color_palette("viridis", len(dfs))

    for df, label, color in zip(dfs, labels, palette):
        sns.lineplot(data=df, x='num_of_blobs', y='propagation_sec',
                     marker='o', linestyle='-', color=color, label=label)

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.legend()
    plt.show()


query1 = '''
with blobs_prop as (
	select 
		block_root, 
		meta_client_id,
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
),
blocks_prop as (
	select 
		block,
		meta_client_id,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
)
select 
	COALESCE(bb.num_of_blobs, bk.num_of_blobs) as num_of_blobs,
	AVG(greatest(blob_prop_time, block_prop_time)) as propagation_sec
from blobs_prop bb
inner join blocks_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
where not (bk.block = '' and bb.block_root is not null)
group by 1
order by 1
'''

query2 = '''
with blobs_prop as (
	select 
		block_root, 
		meta_client_id,
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
),
blocks_prop as (
	select 
		block,
		meta_client_id,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
)
select 
	COALESCE(bb.num_of_blobs, bk.num_of_blobs) as num_of_blobs,
	AVG(greatest(blob_prop_time, block_prop_time)) as propagation_sec
from blobs_prop bb
inner join blocks_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
left join canonical_block_root r on bb.block_root = r.block_root
where not (bk.block = '' and bb.block_root is not null)
and r.block_hash not in (
SELECT hash as non_mev_hash
FROM canonical_block_details cbd 
WHERE cbd.extraData NOT IN (
    SELECT extraData
    FROM canonical_block_details
    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
)
group by 1
order by 1
'''

query3 = '''
with blobs_prop as (
	select 
		block_root, 
		meta_client_id,
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
),
blocks_prop as (
	select 
		block,
		meta_client_id,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
)
select 
	COALESCE(bb.num_of_blobs, bk.num_of_blobs) as num_of_blobs,
	AVG(greatest(blob_prop_time, block_prop_time)) as propagation_sec
from blobs_prop bb
inner join blocks_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
left join canonical_block_root r on bb.block_root = r.block_root
where not (bk.block = '' and bb.block_root is not null)
and r.block_hash in (
SELECT hash as non_mev_hash
FROM canonical_block_details cbd 
WHERE cbd.extraData NOT IN (
    SELECT extraData
    FROM canonical_block_details
    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
)
group by 1
order by 1
'''

query4 = '''
with blobs_prop as (
	select 
		block_root, 
		meta_client_id,
        meta_consensus_implementation,
        
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
),
blocks_prop as (
	select 
		block,
		meta_client_id,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	and meta_consensus_implementation = 'lighthouse'
	group by 1, 2
)
select 
	COALESCE(bb.num_of_blobs, bk.num_of_blobs) as num_of_blobs,
	AVG(greatest(blob_prop_time, block_prop_time)) as propagation_sec
from blobs_prop bb
inner join blocks_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
where not (bk.block = '' and bb.block_root is not null)
group by 1
order by 1
'''

columns = ['num_of_blobs', 'propagation_sec']
df1 = get_query_res(columns, query1)
df2 = get_query_res(columns, query2)
df3 = get_query_res(columns, query3)

# Plot single line chart for df1
plot_single_line_chart(df1, title="Number of Blobs vs. Propagation Time (All)", xlabel="Number of Blobs", ylabel="Propagation Time (sec)")

# Plot multiple line chart for df1, df2, and df3
dfs = [df1, df2, df3]
labels = ['All', 'MEV', 'Non-MEV']
colors = ['r', 'b', 'g']

plot_multiple_line_chart(dfs, labels, title="Number of Blobs vs. Propagation Time",
                         xlabel="Number of Blobs", ylabel="Propagation Time (sec)")
