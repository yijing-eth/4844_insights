import pandas as pd
import calendar
import textwrap
import clickhouse_connect

client = clickhouse_connect.get_client(host='127.0.0.1', username='default', password='', port=8123)

# Set the date range
start_date = '2024-03-01'
end_date = '2024-05-20'

month_ls = pd.date_range(pd.to_datetime(start_date).replace(day=1), end_date, freq='MS').strftime("%m-%Y").tolist()
# print(month_ls)
table_name='mempool_transaction'
base_url='https://data.ethpandaops.io/xatu/mainnet/databases/default'
delete_query = f'''truncate table default.{table_name} on cluster '{{cluster}}';'''
file_path = f'insert_queries_{table_name}.txt'
inc = 1

def get_query(month, year, start, end):
    if start == end:
        query = textwrap.dedent(f'''
            INSERT INTO `default`.{table_name}
            SELECT *
            FROM url('{base_url}/{table_name}/{year}/{month}/{end}.parquet', 'Parquet');'''
        )
    else: 
        query = textwrap.dedent(f'''
            INSERT INTO `default`.{table_name}
            SELECT *
            FROM url('{base_url}/{table_name}/{year}/{month}/{{{start}..{end}}}.parquet', 'Parquet');'''
        )
    return query

client.command(delete_query)
with open(file_path, 'w') as file:
    for item in month_ls:
        print("processing", item)
        month, year = item.split('-')
        month = int(month)
        year = int(year)
        last_day_of_month = calendar.monthrange(year, month)[1]
        if month == pd.to_datetime(start_date).month and year == pd.to_datetime(start_date).year:
            start = pd.to_datetime(start_date).day
            end = last_day_of_month
        elif month == pd.to_datetime(end_date).month and year == pd.to_datetime(end_date).year:
            start = 1
            end = pd.to_datetime(end_date).day
        else: 
            start = 1
            end = last_day_of_month
        for i in range(start, end+1, inc):
            query = get_query(month, year, i, min(i+inc-1, end))
            client.command(query)
            file.write(query)
