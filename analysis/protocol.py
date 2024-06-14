import clickhouse_connect
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

MAX_SLOT_INC = 96

client = clickhouse_connect.get_client(
    host='127.0.0.1', username='default', password='', port=8123)

address_affiliation = {
    "Base": "0xff00000000000000000000000000000000008453",
    "Arbitrum": "0x1c479675ad559dc151f6ec7ed3fbf8cee79582b6",
    "Optimism": "0xff00000000000000000000000000000000000010",
    "Linea": "0xd19d4b5d358258f05d7b411e21a1460d11b0876f",
    "ZKsync": "0xa8cb082a5a689e0d594d7da1e2d72a3d63adc1bd",
    "Scroll": "0xa13baf47339d63b743e7da8741db5456dac1e556",
    "Starknet": "0xc662c410c0ecf747543f5ba90660f6abebd9c8c4",
    "Zora": "0x6f54ca6f6ede96662024ffd61bfd18f3f4e34dff",
    "Mode": "0x24e59d9d3bd73ccc28dc54062af7ef7bff58bd67",
}
# "Paradex": "0xf338cad020d506e8e3d9b4854986e0ece6c23640",

### Queries ###
def get_blob_frequency(address):
    query = f'''
            select
                slot,
                lower(to) as address,
                sum(blob_gas/131072) as num_of_blobs
            from canonical_beacon_block_execution_transaction
            where type = 3
                and lower(to) = '{address}'
            group by 1, 2
            order by 1
        '''
    result = client.query(query).result_rows
    df = pd.DataFrame(result, columns=['slot', 'address', 'num_of_blobs'])
    df['address'] = df['address'].apply(lambda x: x.decode('utf-8').lower())
    df['slot_increment'] = df['slot'] - df['slot'].shift(1)
    df = df[df['slot_increment'] <= MAX_SLOT_INC]
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


### Graphs ###
def plot_num_blob_histograms(df_map):
    num_addresses = len(df_map.keys())
    rows = (num_addresses + 2) // 3  # Calculate the number of rows needed
    fig, axes = plt.subplots(rows, 3, figsize=(20, 5 * rows))
    axes = axes.flatten()

    for i, key in enumerate(df_map.keys()):
        df = df_map[key]
        sns.histplot(df['num_of_blobs'].dropna(), bins=range(1, 7), kde=False, ax=axes[i], color=sns.color_palette("viridis", num_addresses)[i])
        axes[i].set_title(f'Number of Blobs Per Transaction - {key}')
        axes[i].set_xlabel('Number of Blobs Per Transaction')
        axes[i].set_ylabel('Frequency')
        axes[i].grid(True)

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.show()

def plot_freq_histograms(df_map):
    num_addresses = len(df_map.keys())
    rows = (num_addresses + 2) // 3  # Calculate the number of rows needed
    fig, axes = plt.subplots(rows, 3, figsize=(20, 5 * rows))
    axes = axes.flatten()

    for i, key in enumerate(df_map.keys()):
        df = df_map[key]
        sns.histplot(df['slot_increment'].dropna(), bins=range(1, MAX_SLOT_INC + 1), kde=False, ax=axes[i], color=sns.color_palette("viridis", num_addresses)[i])
        axes[i].set_title(f'Blob Transaction Frequency - {key}')
        axes[i].set_xlabel('Time Between Posting in Slots')
        axes[i].set_ylabel('Number of Blob Transactions')
        axes[i].grid(True)

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    plt.tight_layout()
    plt.show()

df_map1 = {}
df_map2 = {}
for label, address in address_affiliation.items():
    df_map1[label] = get_blob_frequency(address)
    df_map2[label] = get_blob_txns(address)

plot_freq_histograms(df_map1)
plot_num_blob_histograms(df_map2)
