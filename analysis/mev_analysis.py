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


def plot_scatter_chart(df, x_column, y_column, hue=None, title="Scatter Plot", xlabel="X-axis", ylabel="Y-axis", palette="Set2", marker_size=5):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x=x_column, y=y_column,
                    hue=hue, palette=palette, s=marker_size)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.legend(title=hue)
    plt.show()


query1 = '''
with non_mev as (
	SELECT hash as non_mev_hash
	FROM canonical_block_details cbd 
	WHERE cbd.extraData NOT IN (
	    SELECT extraData
	    FROM canonical_block_details
	    INNER JOIN mev_block_details ON mev_block_details.block_hash = canonical_block_details.hash
	)
),
blobs_prop as (
	select 
		block_root,
		meta_client_id,
		meta_consensus_implementation,
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	group by 1, 2, 3
),
blocks_only_prop as (
	select 
		block,
		meta_client_id,
		meta_consensus_implementation,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	group by 1, 2, 3
),
root_hash as (
	select
		block_root,
		block_hash
	from canonical_block_root r
),
blob_num as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
max_prop as (
	select
		block_root,
		avg(greatest(blob_prop_time, block_prop_time)) as propagation_sec
	from blobs_prop bb
	full outer join blocks_only_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
	where meta_consensus_implementation = 'lighthouse'
	group by 1
)
select
	propagation_sec,
	num_of_blob_per_block
from max_prop p
left join root_hash r on p.block_root = r.block_root
left join blob_num bn on bn.block_root = r.block_root
where block_hash in (select * from non_mev)
'''


query2 = '''
with blobs_prop as (
	select 
		block_root,
		meta_client_id,
		meta_consensus_implementation,
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	group by 1, 2, 3
),
blocks_only_prop as (
	select 
		block,
		meta_client_id,
		meta_consensus_implementation,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	group by 1, 2, 3
),
blob_num as (
    select
      	block_root,
        sum(blob_gas/131072) as num_of_blob_per_block
    from canonical_beacon_block_execution_transaction
    where type = 3 
    group by 1
),
max_prop as (
	select
		block_root,
		avg(greatest(blob_prop_time, block_prop_time)) as propagation_sec
	from blobs_prop bb
	full outer join blocks_only_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
	where meta_consensus_implementation = 'lighthouse'
	group by 1
)
select
	propagation_sec,
	num_of_blob_per_block
from max_prop p
left join blob_num bn on bn.block_root = p.block_root
'''

query3 = '''
with miner_tip_blob as (
	select
	    ctd.hash,
	    least(toUInt256(greatest(`maxFeePerGas` - toUInt256(`baseFeePerGas`), 0)), `maxPriorityFeePerGas`) AS miner_tip
	from canonical_transaction_details AS ctd
	left join canonical_block_details AS cbd on ctd.`blockHash` = cbd.hash
	),
num_blob_per_txn as (
    select
        hash,
        sum(blob_gas/131072) as num_of_blobs
    from canonical_beacon_block_execution_transaction
    where type = 3
    group by 1
)
select 
	num_of_blobs,
	miner_tip
from miner_tip_blob b
left join num_blob_per_txn t on b.hash = t.hash
'''


def main():
    # df = get_query_res(
    #     columns=['propagation_sec', 'num_of_blob_per_block'], query=query2)
    # df = df[df['propagation_sec'] <= df['propagation_sec'].quantile(0.99)]
    # plot_box_plot(df, x_column='num_of_blob_per_block', y_column='propagation_sec', title="Propagation Time vs Number of Blobs per Block")
    df1 = get_query_res(
        columns=['num_of_blobs', 'miner_tip'], query=query3)
    df1 = df1[df1['miner_tip'] <= df1['miner_tip'].quantile(0.95)]
    plot_box_plot(df1, 'num_of_blobs', 'miner_tip', title="Miner Tip vs Number of Blobs Per Transaction",
                       xlabel="Number of blobs per transaction", ylabel="Miner Tip")


if __name__ == '__main__':
    main()
