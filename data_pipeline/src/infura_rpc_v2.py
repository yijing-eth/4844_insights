import requests
import json
import clickhouse_connect
import pandas as pd
import numpy as np
import time

client = clickhouse_connect.get_client(
    host='127.0.0.1', username='default', password='', port=8123)

url = 'http://localhost:8545'

def insert_rpc_res(data):
    query = ''' 
        INSERT INTO `default`.canonical_block_details (
            baseFeePerGas, blobGasUsed, excessBlobGas, extraData, gasLimit, gasUsed, hash, miner, number, receiptsRoot, size, timestamp
        ) VALUES (  
            %(baseFeePerGas)s, %(blobGasUsed)s, %(excessBlobGas)s, %(extraData)s, %(gasLimit)s, %(gasUsed)s, %(hash)s, %(miner)s, %(number)s, %(receiptsRoot)s, %(size)s, %(timestamp)s
        )
    '''
    client.command(query, data)


def hex_to_int(value):
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.startswith('0x'):
            return int(value, 16)
        return int(value)  # In case value is already an integer
    except ValueError as e:
        print(f"Error converting value {value}: {e}")
        return None

def get_rpc_res(block):
    # Get the transaction receipt without logs
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBlockByNumber",
        "params": [hex(block), False],
        "id": 1
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(
        payload), headers=headers).json()

    if 'result' not in response:
        raise ValueError(f"Unexpected response format: {response}")

    result = response['result']

    data = {
        "baseFeePerGas": hex_to_int(result.get("baseFeePerGas")),
        "blobGasUsed": hex_to_int(result.get("blobGasUsed")),
        "excessBlobGas": hex_to_int(result.get("excessBlobGas")),
        "extraData": result.get("extraData"),
        "gasLimit": hex_to_int(result.get("gasLimit")),
        "gasUsed": hex_to_int(result.get("gasUsed")),
        "hash": result.get("hash"),
        "miner": result.get("miner"),
        "number": hex_to_int(result.get("number")),
        "receiptsRoot": result.get("receiptsRoot"),
        "size": hex_to_int(result.get("size")),
        "timestamp": hex_to_int(result.get("timestamp"))
    }

    return data


def main():
    for i in range(19422000, 20054917, 1):
        print(i)
        res = get_rpc_res(i)
        insert_rpc_res(res)

if __name__ == '__main__':
    main()
