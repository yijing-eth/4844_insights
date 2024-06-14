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


def plot_box_plot(df, x_column, y_column, title, palette="Set2", xlabel=None, ylabel=None):
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
                sum(blob_gas/131072) as num_of_blobs_per_txn
            from canonical_beacon_block_execution_transaction
            where type = 3
            group by 1
        )
        select 
            num_of_blobs_per_txn,
            miner_tip
        from miner_tip_blob b
        left join num_blob_per_txn t on b.hash = t.hash
    '''

query2 = '''
        select
            block_root,
            max(position) + 1 as num_of_txns,
            sum(blob_gas/131072) as num_of_blob_per_block
        from canonical_beacon_block_execution_transaction
        where type = 3 
        group by 1
    '''

query3 = '''
        with blobs_prop as (
            select 
                slot,
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
                slot,
                meta_client_id,
                meta_consensus_implementation,
                max(propagation_slot_start_diff/1000) as block_prop_time,
                0 as num_of_blobs
            from beacon_api_eth_v1_events_block
            where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
            group by 1, 2, 3
        ),
        max_prop as (
            select
                bb.slot,
                avg(greatest(blob_prop_time, block_prop_time)) as propagation_sec
            from blobs_prop bb
            full outer join blocks_only_prop bk on bb.meta_client_id = bk.meta_client_id and bb.slot = bk.slot
            where meta_consensus_implementation = 'lighthouse'
            group by 1
        ),
        num_txns_per_block as (
            select
                slot,
                max(position) + 1 as num_of_txns_per_block,
                sum(blob_gas/131072) as num_of_blob_per_block
            from canonical_beacon_block_execution_transaction
            where type = 3 
            group by 1
        ),
        mev as (
            select 
                slot,
                value
            from mev_block_details
        )
        select
            p.slot,
            num_of_txns_per_block,
            value,
            propagation_sec
        from max_prop p
        join num_txns_per_block t on p.slot = t.slot
        join mev on mev.slot = t.slot
    '''

query4 = '''
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
        mev as (
            select 
                block_hash,
                value
            from mev_block_details
        )
        select
            num_of_blob_per_block,
            value as mev_extracted
        from num_blobs_per_block b 
        join root_hash r on b.block_root = r.block_root
        join mev on mev.block_hash = r.block_hash
'''

query5 = '''
         with blobs_prop as (
            select 
                block_root,
                meta_client_id,
                meta_consensus_implementation,
                max(propagation_slot_start_diff/1000) as blob_prop_time,
                max(blob_index)+1 as num_of_blobs
            from beacon_api_eth_v1_events_blob_sidecar
            where propagation_slot_start_diff <= 4000
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
            where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 4000
            group by 1, 2, 3
        ),
        root_hash as (
            select
                block_root,
                block_hash
            from canonical_block_root r
        ),
        max_prop as (
            select
                block_root,
                avg(greatest(blob_prop_time, block_prop_time)) as propagation_sec
            from blobs_prop bb
            full outer join blocks_only_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
            where meta_consensus_implementation = 'lighthouse'
            group by 1
        ),
        mev as (
            select 
                block_hash,
                value
            from mev_block_details
        )
        select
            propagation_sec,
            value
        from max_prop p
        join root_hash r on p.block_root = r.block_root
        join mev on mev.block_hash = r.block_hash
    '''


def plot_histogram_mev(df):
    grouped = df.groupby('propagation_sec')[
        'mev_extracted'].median().reset_index()
    grouped.columns = ['propagation_sec', 'median_mev_extracted']

    plt.figure(figsize=(14, 7))
    sns.histplot(grouped['median_mev_extracted'],
                 bins=50, kde=True, color='blue')
    plt.title('Histogram of Median MEV Value by Propagation Time')
    plt.xlabel('Median MEV Value')
    plt.ylabel('Frequency')
    plt.grid(True)
    plt.show()


def median_mev_by_bins(df, n_bins):
    # Create bins
    bins = np.linspace(0, 4, n_bins + 1)
    # Create a new column for the bins
    df['bin'] = pd.cut(df['propagation_sec'], bins, labels=range(n_bins))

    print(df)
    # Calculate median mev_extracted for each bin
    median_mev = df.groupby('bin')['mev_extracted'].median().to_list()

    print(median_mev)

    return median_mev


def plot_scatter_chart_with_regression(df, x_column, y_column, hue=None, title="Scatter Plot", xlabel="X-axis", ylabel="Y-axis", palette="Set2", marker_size=5):
    df = df.dropna(subset=[x_column, y_column])
    df[x_column] = pd.to_numeric(df[x_column], errors='coerce')
    df[y_column] = pd.to_numeric(df[y_column], errors='coerce')

    # Drop any rows with non-finite values
    df = df[np.isfinite(df[x_column]) & np.isfinite(df[y_column])]
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x=x_column, y=y_column,
                    hue=hue, palette=palette, s=marker_size)
    sns.regplot(data=df, x=x_column, y=y_column, scatter=False, color='red')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.legend(title=hue)
    plt.show()


def median_mev_by_bins(df, n_bins):
    # Create bins
    bins = np.linspace(0, 4, n_bins + 1)
    # Calculate the middle of each bin
    bin_centers = (bins[:-1] + bins[1:]) / 2
    # Create a new column for the bins
    df['bin'] = pd.cut(df['propagation_sec'], bins, labels=range(n_bins))
    # Calculate median mev_extracted for each bin
    median_mev = df.groupby('bin')['mev_extracted'].median().to_list()
    # Create the DataFrame with the bin centers and median mev values
    result_df = pd.DataFrame({
        'bin_center': bin_centers,
        'median_mev_extracted': median_mev
    })
    return result_df


def plot_histogram_mev2(df):
    print(df)
    bins = median_mev_by_bins(df, 100)
    print(bins)
    plot_scatter_chart_with_regression(bins, x_column='bin_center', y_column='median_mev_extracted',
                                       title="Median MEV Extracted by Propagation Time", xlabel="Propagation Time (sec)", ylabel="Median MEV Extracted")


def main():
    # df1 = get_query_res(columns=['num_of_blobs_per_txn', 'miner_tip'], query=query1)
    # df1 = df1[df1['miner_tip'] <= df1['miner_tip'].quantile(0.98)]
    # plot_box_plot(df1, 'num_of_blobs_per_txn', 'miner_tip', 'Miner Tip vs Number of Blobs')
    # df2 = get_query_res(columns=['block_root', 'num_of_txns_per_block', 'num_of_blob_per_block'], query=query2)
    # df2 = df2[df2['num_of_txns_per_block'] <= df2['num_of_txns_per_block'].quantile(0.99)]
    # plot_box_plot(df2[['num_of_txns_per_block', 'num_of_blob_per_block']], 'num_of_blob_per_block', 'num_of_txns_per_block', 'Total Transaction Number vs Number of Blobs Per Block')
    # df3 = get_query_res(columns=['block_root', 'num_of_txns_per_block',
    #                     'mev_value', 'propagation_sec'], query=query3)
    # plot_scatter_chart(df3[['num_of_txns_per_block', 'mev_value', 'propagation_sec']], 'num_of_txns_per_block', 'propagation_sec', hue='mev_value',
    #                            title='Propagation Time vs Number of Transactions Per Block', xlabel='Number of Transactions Per Block', ylabel='Propagation Time (s)', palette='Set2', marker_size=5)
    # plot_scatter_chart(df3[['num_of_blob_per_block', 'propagation_sec']], 'num_of_blob_per_block', 'propagation_sec', hue=None,
    #                            title='Propagation Time vs Number of Blobs Per Block', xlabel='Number of Blobs Per Block', ylabel='Propagation Time (s)', palette='Set2', marker_size=5)
    df4 = get_query_res(columns=['num_of_blob_per_block', 'mev_extracted'], query=query4)
    df4 = df4[df4['mev_extracted'] <= df4['mev_extracted'].quantile(0.95)]
    df4['mev_extracted'] = df4['mev_extracted']/1e18
    plot_box_plot(df4, 'num_of_blob_per_block', 'mev_extracted', 'MEV Extracted vs Number of Blobs Per Block', xlabel='Number of Blobs Per Block', ylabel='MEV Extracted (ETH)')
    # df5 = get_query_res(
    #     columns=['propagation_sec', 'mev_extracted'], query=query5)
    # df5 = df5[df5['mev_extracted'] <= df5['mev_extracted'].quantile(0.95)]
    # plot_scatter_chart_with_regression(df5, x_column='propagation_sec', y_column='mev_extracted',
    #                                    xlabel='Propagation Time (s)', ylabel='MEV Extracted', title='MEV Extracted by Propagation Time', marker_size=5)
    # plot_histogram_mev2(df5)


if __name__ == '__main__':
    main()
