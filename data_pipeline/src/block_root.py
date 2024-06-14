#!/usr/bin/env python3
import requests
import json
import clickhouse_connect
import time

BEACON_RPC = "http://localhost:5052"


def initialize_client():
    return clickhouse_connect.get_client(
        host='127.0.0.1', username='default', password='', port=8123)


def get_block_roots_from_db(client):
    query = "select distinct block_root from canonical_beacon_block where slot >= 8410176 order by 1"
    result = client.query(query)
    block_roots_ls_bytes = [row[0] for row in result.result_rows]
    block_roots_ls = [tx_hash.decode('utf-8')
                      for tx_hash in block_roots_ls_bytes]
    return block_roots_ls


def get_execution_payload_from_root(beacon_block_root):
    url = f"{BEACON_RPC}/eth/v1/beacon/blinded_blocks/{beacon_block_root}"
    response = requests.get(url, headers={"Accept": "application/json"})
    if response.status_code == 200:
        data = response.json()
        payload = data['data']['message']['body']['execution_payload_header']
        # Add the block root to the payload
        payload['block_root'] = beacon_block_root
        if 'excess_blob_gas' not in payload:
            payload['excess_blob_gas'] = None
        if 'blob_gas_used' not in payload:
            payload['blob_gas_used'] = None
        return payload
    else:
        print(f"Failed to fetch data for block root {
              beacon_block_root}: {response.status_code}")
        print(json.dumps(response.json(), indent=2))
        return None


def insert_execution_payload(client, payload):
    # Provide default values for missing fields, setting Nullable fields to None
    payload_defaults = {
        'parent_hash': '',
        'fee_recipient': '',
        'state_root': '',
        'receipts_root': '',
        'logs_bloom': '',
        'prev_randao': '',
        'block_number': '0',
        'gas_limit': '0',
        'gas_used': '0',
        'timestamp': '0',
        'extra_data': '',
        'base_fee_per_gas': '0',
        'block_hash': '',
        'transactions_root': '',
        'withdrawals_root': '',
        'blob_gas_used': None,  # Nullable field
        'excess_blob_gas': None,  # Nullable field
        'block_root': ''
    }

    for key, default in payload_defaults.items():
        payload[key] = payload.get(key, default)

    # Convert None values to 'NULL' for SQL query
    def sql_value(value):
        return 'NULL' if value is None else f"{int(value)}" if isinstance(value, str) and value.isdigit() else f"'{value}'"

    query = """
    INSERT INTO canonical_block_root (
        parent_hash,
        fee_recipient,
        state_root,
        receipts_root,
        logs_bloom,
        prev_randao,
        block_number,
        gas_limit,
        gas_used,
        timestamp,
        extra_data,
        base_fee_per_gas,
        block_hash,
        transactions_root,
        withdrawals_root,
        blob_gas_used,
        excess_blob_gas,
        block_root
    ) VALUES
    """
    values = f"({sql_value(payload['parent_hash'])}, {sql_value(payload['fee_recipient'])}, {sql_value(payload['state_root'])}, {sql_value(payload['receipts_root'])}, " \
        f"{sql_value(payload['logs_bloom'])}, {sql_value(payload['prev_randao'])}, {int(payload['block_number'])}, {int(payload['gas_limit'])}, " \
        f"{int(payload['gas_used'])}, {int(payload['timestamp'])}, {sql_value(payload['extra_data'])}, {int(payload['base_fee_per_gas'])}, " \
        f"{sql_value(payload['block_hash'])}, {sql_value(payload['transactions_root'])}, {sql_value(payload['withdrawals_root'])}, " \
        f"{sql_value(payload['blob_gas_used'])}, {sql_value(payload['excess_blob_gas'])}, {
        sql_value(payload['block_root'])})"
    full_query = query + values
    client.command(full_query)


def main():
    client = initialize_client()
    try:
        block_roots = get_block_roots_from_db(client)
        for i, block_root in enumerate(block_roots):
            print(i)
            payload = get_execution_payload_from_root(block_root)
            print(json.dumps(payload, indent=2))
            if payload:
                insert_execution_payload(client, payload)
    finally:
        client.close()


if __name__ == "__main__":
    main()
