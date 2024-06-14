import clickhouse_connect
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

MAX_SLOT_INC = 96

client = clickhouse_connect.get_client(
    host='127.0.0.1', username='default', password='', port=8123)

### Queries ###

def get_blob_behavior():
    query = f'''
         with top_10 as (
            select
                sum(blob_gas/131072) as num_of_blobs,
                to
            from canonical_beacon_block_execution_transaction
            where type = 3
            group by 2
            order by 1 desc
            limit 10
        ),
        t1 as (
            select
                slot,
                lower(to) as address,
                sum(blob_gas/131072) as num_of_blobs,
                count(*) as num_blob_txns
            from canonical_beacon_block_execution_transaction
            where type = 3
            and to in (
                select to from top_10
            )
            group by 1, 2
        )
        select
            address,
            label,
            median(num_of_blobs/num_blob_txns) as num_blob_per_txn
        from t1
        left join labels l on t1.address = l.address
        group by 1, 2
        '''
    result = client.query(query).result_rows
    df = pd.DataFrame(result, columns=['address', 'label', 'num_blob_per_txn'])
    df['address'] = df['address'].apply(lambda x: x.decode('utf-8').lower())
    return df

def get_blob_txns(address):
    query = f'''
         select
            slot,
            lower(to) as address,
            sum(blob_gas/131072) as num_of_blobs,
            count(*) as num_blob_txns
        from canonical_beacon_block_execution_transaction
        where type = 3
        and lower(to) = '{address}'
        group by 1, 2
        '''
    result = client.query(query).result_rows
    df = pd.DataFrame(
        result, columns=['slot', 'address', 'num_of_blobs', 'num_blob_txns'])
    df['address'] = df['address'].apply(lambda x: x.decode('utf-8').lower())
    return df

def get_blob_rel_position():
    query = f'''
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
    result = client.query(query).result_rows
    df = pd.DataFrame(
        result, columns=['block_root', 'rel_position'])
    return df

def get_blob_position():
    query = f'''
        select block_root, position
        from canonical_beacon_block_execution_transaction
        where type = 3
    '''
    result = client.query(query).result_rows
    df = pd.DataFrame(
        result, columns=['block_root', 'position'])
    return df

def get_blob_and_txn_num():
    query = '''
        select
            block_root,
            sum(size) as total_txn_size,
            max(position) + 1 as num_of_txns,
            sum(blob_gas/131072) as num_of_blob_per_block
        from canonical_beacon_block_execution_transaction
        where type = 3 
        group by 1
    '''
    result = client.query(query).result_rows
    df = pd.DataFrame(result, columns=['block_root', 'total_txn_size', 'num_of_txns', 'num_of_blob_per_block'])
    df = df[['num_of_blob_per_block', 'num_of_txns']]
    limit = df['num_of_txns'].quantile(0.99)
    df = df[df['num_of_txns'] <= limit]
    return df

def get_blob_per_block():
    query = '''
        select
            block_root,
            sum(blob_gas/131072) as num_of_blob_per_block
        from canonical_beacon_block_execution_transaction
        where type = 3 
        group by 1
    '''
    result = client.query(query).result_rows
    df = pd.DataFrame(result, columns=['block_root', 'num_of_blob_per_block'])
    return df

def get_blob_over_half_empty(address=None):
    if address:
        added_filter = f"and lower(to) = '{address}'"
    else:
        added_filter = ''
    query = f'''
        with blob_stats as (
            select 
                block_root, 
                sum(blob_gas/131072) as num_of_blobs,
                sum (
                    case 
                        when blob_sidecars_empty_size/blob_sidecars_size > 0.5 then 1
                        else 0 
                    end
                ) as num_of_more_than_half_empty_blobs
            from canonical_beacon_block_execution_transaction
            where type = 3 {added_filter}
            group by block_root
            )
        select num_of_blobs, num_of_more_than_half_empty_blobs
        from blob_stats
        order by 1
    '''
    result = client.query(query).result_rows
    df = pd.DataFrame(result, columns=['num_of_blobs', 'num_of_more_than_half_empty_blobs'])
    return df

def get_blob_empty_size(address=None):
    if address:
        added_filter = f"and lower(to) = '{address}'"
    else:
        added_filter = ''
    query = f'''
        select 
            block_root, 
            sum(blob_gas/131072) as num_of_blobs,
            blob_sidecars_empty_size/blob_sidecars_size as empty_size_ratio
        from canonical_beacon_block_execution_transaction
        where type = 3 {added_filter}
        group by 1, 3
        order by 1
    '''
    result = client.query(query).result_rows
    df = pd.DataFrame(result, columns=['block_root', 'num_of_blobs', 'empty_size_ratio'])
    df = df[['num_of_blobs', 'empty_size_ratio']]
    return df


### Plots ### 
def plot_num_blob_histograms(df_map):
    fig1, axes = plt.subplots(len(df_map.keys()), 1,
                             figsize=(10, 5 * len(df_map.keys())))
    if len(df_map.keys()) == 1:
        axes = [axes]

    for i, key in enumerate(address_affiliation):
        df = df_map[key]
        axes[i].hist(df['num_of_blobs'].dropna(), bins=[1, 2, 3, 4, 5, 6], edgecolor='black', align='left')
        axes[i].set_title(
            f'Histogram of number of blobs - {key}')
        axes[i].set_xlabel('Number of Blobs')
        axes[i].set_ylabel('Frequency')
        axes[i].grid(True)

    plt.tight_layout()
    plt.show()


def plot_freq_histograms(df_map):
    fig2, axes = plt.subplots(len(df_map.keys()), 1,
                             figsize=(10, 5 * len(df_map.keys())))

    if len(df_map.keys()) == 1:
        axes = [axes]

    for i, key in enumerate(address_affiliation):
        df = df_map[key]
        axes[i].hist(df['slot_increment'].dropna(), bins=range(1, MAX_SLOT_INC+1 ,1), edgecolor='black')
        axes[i].set_title(f'Histogram of Slot Increments - {key}')
        axes[i].set_xlabel('Slot Increment')
        axes[i].set_ylabel('Frequency')
        axes[i].grid(True)

    plt.tight_layout()
    plt.show()

def plot_histogram(df, column, title, xlabel, ylabel):
    plt.figure(figsize=(10, 6))
    plt.hist(df[column].dropna(), bins=50, edgecolor='black')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()

def plot_box_plot(df, x_column, y_column, title):
    plt.figure(figsize=(12, 6))
    sns.boxplot(x=x_column, y=y_column, data=df)
    plt.title(title)
    plt.xlabel(x_column)
    plt.ylabel(y_column)
    plt.grid(True)
    plt.show()

def plot_bar_plot(df, x_column, y_column, title, xlabel, ylabel):
    plt.figure(figsize=(12, 6))
    sns.barplot(x=x_column, y=y_column, data=df, palette='viridis')
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True)
    plt.show()

def main():
    
    df_rel_pos = get_blob_rel_position()
    df_pos = get_blob_position()
    # df_num_blob_per_block = get_blob_per_block()
    # df_blob_and_txn_num = get_blob_and_txn_num()
    # df_blob_empty_size = get_blob_empty_size()
    # df_blob_over_half_empty = get_blob_over_half_empty(address_affiliation['Optimism'])
    # df_blob_over_half_empty = get_blob_over_half_empty()

    plot_histogram(df_rel_pos, 'rel_position', 'Histogram of Relative Positions of Blobs within Blocks', 'Relative Position', 'Frequency')
    plot_histogram(df_pos, 'position', 'Histogram of Positions of Blobs within Blocks', 'Position', 'Frequency')
    # plot_histogram(df_num_blob_per_block, 'num_of_blob_per_block', 'Histogram of Number of Blobs per Block', 'Number of Blobs per Block', 'Frequency')
    
    # plot_box_plot(df_blob_and_block_size, 'num_of_blob_per_block', 'total_txn_size', 'Box Plot of Number of Blobs per Block')
    # plot_box_plot(df_blob_empty_size, 'num_of_blobs', 'empty_size_ratio', 'Box Plot of Empty Size Ratio of Blobs')
    # plot_box_plot(df_blob_over_half_empty, 'num_of_blobs', 'num_of_more_than_half_empty_blobs', 'Number of Blobs with More than Half Empty Sidecars')
    # plot_box_plot(df_blob_and_txn_num, 'num_of_blob_per_block', 'num_of_txns', 'Box Plot of Number of Blobs per Block')
    


if __name__ == '__main__':
    main()
