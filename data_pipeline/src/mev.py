#!/usr/bin/env python3
import requests
import json
import clickhouse_connect
import pandas as pd
import time

INTERVAL = 1000
method = "/relay/v1/data/bidtraces/proposer_payload_delivered"
url_ls = {
    "ultrasound": 'https://relay.ultrasound.money',
    "bloxroute-max-profit": 'https://bloxroute.max-profit.blxrbdn.com',
    "bloxroute-regulated": 'https://bloxroute.regulated.blxrbdn.com',
    "agnostic-relay": 'https://agnostic-relay.net',
    "boost-relay": 'https://boost-relay.flashbots.net',
    "titanrelay": 'https://titanrelay.xyz',
    "aestus": 'https://aestus.live',
    "mainnet-relay": 'https://mainnet-relay.securerpc.com',
}

def initialize_client():
    return clickhouse_connect.get_client(
        host='127.0.0.1', username='default', password='', port=8123)

# def initialize_cache():
#     requests_cache.install_cache('relay_cache', expire_after=1800)  # Cache expires after 1800 seconds (30 minutes)

def parse_lists(relay_name, start_slot):
    endpoint = url_ls[relay_name]
    url = "{}{}".format(endpoint, method)
    slot = start_slot
    limit = 100
    all_frames = []

    while slot > start_slot - INTERVAL - limit:
        response = requests.get(url, params={
            'cursor': slot,
            'limit': limit,
        })
        if response.status_code == 200:
            data = response.json()
            if not data:
                slot -= limit
                continue

            df = pd.DataFrame(data)
            # df['relay_name'] = relay_name
            all_frames.append(df)

            # Update 'slot' based on the smallest slot number in the returned data
            min_slot_received = int(df['slot'].min())
            slot = min_slot_received - 1  # Move to the next block before the current earliest
        else:
            print(f"Failed to fetch data: status[{response.status_code}]\n    {response.text}")
            slot -= limit
            continue

    if all_frames:
        final_df = pd.concat(all_frames, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()  # Return an empty DataFrame if no data was collected

def main():
    client = initialize_client()
    # initialize_cache()

    # Special slot 9058776
    START_SLOT = 9118797
    DENEB_SLOT = 8626176 - 216000

    current_slot = START_SLOT

    while current_slot > DENEB_SLOT:
        all_relay_data = []

        for relay, url in url_ls.items():
            print("Processing", relay)
            data = parse_lists(relay, current_slot)
            if not data.empty:
                all_relay_data.append(data)

        # Combine all dataframes into one and deduplicate based on block hash
        if all_relay_data:
            combined_df = pd.concat(all_relay_data, ignore_index=True)
            deduplicated_df = combined_df.drop_duplicates(subset=['block_hash'])
            # print(deduplicated_df)

            # Insert data into ClickHouse
            # client.insert('default.mev_block_details', deduplicated_df.to_dict('records'))
            client.insert_df('default.mev_block_details', deduplicated_df)

            # Update the current_slot based on the smallest slot number in the deduplicated data
            min_slot_received = int(deduplicated_df['slot'].min())
            current_slot = min_slot_received - 1
        else:
            print("No data collected from any relay.")
            current_slot -= INTERVAL  # Fallback in case no data is collected

        time.sleep(3)  # Optional: add a delay between iterations to avoid overwhelming the API

    client.close()

if __name__ == "__main__":
    main()


   