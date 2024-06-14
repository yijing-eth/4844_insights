import clickhouse_connect
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

client = clickhouse_connect.get_client(
    host='127.0.0.1', username='default', password='', port=8123)

### Queries ###


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


def plot_line_chart(df_list, x_column, y_column, hue=None, titles=None, xlabel="X-axis", ylabel="Y-axis", palette="Set2", vline_date=None, subplots=False, subplot_rows=1, subplot_cols=1):
    if subplots:
        fig, axes = plt.subplots(
            subplot_rows, subplot_cols, figsize=(15, 5 * subplot_rows))
        axes = axes.flatten()

        for i, ax in enumerate(axes):
            if i < len(df_list):
                sns.lineplot(data=df_list[i], x=x_column, y=y_column, hue=hue,
                             palette=palette, markers=True, dashes=False, ax=ax)
                if vline_date:
                    ax.axvline(pd.to_datetime(vline_date),
                               color='blue', linewidth=3, label=vline_date)
                title = titles[i] if titles and i < len(
                    titles) else f'Subplot {i + 1}'
                ax.set_title(title)
                ax.set_xlabel(xlabel)
                ax.set_ylabel(ylabel)
                ax.grid(True)
                if hue:
                    ax.legend(title=hue)
            else:
                fig.delaxes(ax)

        plt.tight_layout()
        plt.show()
    else:
        plt.figure(figsize=(10, 6))
        sns.lineplot(data=df_list, x=x_column, y=y_column, hue=hue,
                     palette=palette, markers=True, dashes=False)
        if vline_date:
            plt.axvline(pd.to_datetime(vline_date), color='blue',
                        linewidth=3, label=vline_date)
        plt.title(titles[0] if titles else title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.grid(True)
        if hue:
            plt.legend(title=hue)
        plt.show()


# Query 1: number of reorg events over time by consensus implementation
# number of events is averaged over all countries except Finland and India
# Finland and India are excluded because there is a lack of data prefork
query1 = f'''
        with reorg_by_region as (
        select
            toDate(slot_start_date_time) as date,
            meta_consensus_implementation,
            meta_client_geo_country_code,
            count(*) as num_of_reorg_events
        from beacon_api_eth_v1_events_chain_reorg
        where meta_client_geo_country_code not in ('FI', 'IN')
        group by 1, 2, 3)
        select
            date,
            meta_consensus_implementation as consensus_implementation,
            avg(num_of_reorg_events) as num_of_reorg_events
        from reorg_by_region
        group by 1, 2
        '''
# Query 2: number of reorg events by consensus implementation and country
# India is excluded due a lack of consistent data
query2 = f'''
        select
            toDate(slot_start_date_time) as date,
            meta_consensus_implementation,
            meta_client_geo_country_code,
            count(*) as num_of_reorg_events
        from beacon_api_eth_v1_events_chain_reorg
        where slot_start_date_time >= '2024-03-13'
        and meta_client_geo_country_code != 'IN'
        group by 1, 2, 3
        '''

query3 = '''
with reorg_events as (
	select
		slot,
		meta_client_geo_country_code,
		count(*) as num_of_reorg_events
	from beacon_api_eth_v1_events_chain_reorg
	where meta_consensus_implementation = 'lighthouse'
	group by 1, 2
),
num_of_blobs as (
	select
		slot,
		sum(blob_gas/131072) as num_of_blobs
	from
		canonical_beacon_block_execution_transaction
	where
		type = 3
	group by 1
)
select 
	b.slot,
	b.num_of_blobs,
	COALESCE(num_of_reorg_events, 0) as num_of_reorg_events
from num_of_blobs b
left join reorg_events r on b.slot = r.slot
'''


def main():
    # df1 = get_query_res(
    #     columns=['date', 'consensus_implementation', 'num_of_reorg_events'], query=query1)
    # plot_line_chart(df1, x_column='date', y_column='num_of_reorg_events', hue='consensus_implementation', titles=["Reorg Events over time"],
    #                 xlabel="Date", ylabel="Number of Reorg Events", palette="Set2", vline_date='2024-03-13')
    # df2 = get_query_res(
    #     columns=['date', 'consensus_implementation', 'country', 'num_of_reorg_events'], query=query2)
    # df2_lighthouse = df2[df2['consensus_implementation'] == 'lighthouse']
    # df2_teku = df2[df2['consensus_implementation'] == 'teku']
    # df2_prysm = df2[df2['consensus_implementation'] == 'prysm']
    # df2_lodestar = df2[df2['consensus_implementation'] == 'lodestar']
    # df_list = [df2_lighthouse, df2_lodestar, df2_prysm, df2_teku]
    # titles = ["Number of Reorg Events by Country - Lighthouse",
    #           "Number of Reorg Events by Country - Lodestar", "Number of Reorg Events by Country - Prysm", "Number of Reorg Events by Country - Teku"]
    # plot_line_chart(df_list, x_column='date', y_column='num_of_reorg_events', hue='country', titles=titles,
    #                 xlabel="Date", ylabel="Number of Reorg Events", subplots=True, subplot_rows=2, subplot_cols=2)
    df3 = get_query_res(
        columns=['slot', 'num_of_blobs', 'num_of_reorg_events'], query=query3)
    plot_box_plot(df3, x_column='num_of_blobs', y_column='num_of_reorg_events', title="Number of Reorg Events vs Number of Blobs per Block",
                  palette="Set2", xlabel="Number of Blobs", ylabel="Number of Reorg Events")


if __name__ == '__main__':
    main()
