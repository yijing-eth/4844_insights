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
        INSERT INTO `default`.canonical_beacon_transaction_receipts_blob_sidecars (
            blockHash, blockNumber, contractAddress, cumulativeGasUsed, effectiveGasPrice, from, gasUsed, status, to, transactionHash, transactionIndex, type
        ) VALUES (  
            %(blockHash)s, %(blockNumber)s, %(contractAddress)s, %(cumulativeGasUsed)s, %(effectiveGasPrice)s, %(from)s, %(gasUsed)s, %(status)s, %(to)s, %(transactionHash)s, %(transactionIndex)s, %(type)s
        )
    '''
    client.command(query, data)


def hex_to_int(value):
    try:
        if isinstance(value, str) and value.startswith('0x'):
            return int(value, 16)
        return int(value)  # In case value is already an integer
    except ValueError as e:
        print(f"Error converting value {value}: {e}")
        return None


def get_rpc_res(txn_hash):
    # Get the transaction receipt without logs
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getTransactionReceipt",
        "params": [txn_hash],
        "id": 1
    }
    headers = {'content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(
        payload), headers=headers).json()

    if 'result' not in response:
        raise ValueError(f"Unexpected response format: {response}")

    result = response['result']

    data = {
        'blockHash': result.get('blockHash'),
        'blockNumber': hex_to_int(result.get('blockNumber')),
        'contractAddress': result.get('contractAddress'),
        'cumulativeGasUsed': hex_to_int(result.get('cumulativeGasUsed')),
        'effectiveGasPrice': hex_to_int(result.get('effectiveGasPrice')),
        'from': result.get('from'),
        'gasUsed': hex_to_int(result.get('gasUsed')),
        'status': hex_to_int(result.get('status')),
        'to': result.get('to'),
        'transactionHash': result.get('transactionHash'),
        'transactionIndex': hex_to_int(result.get('transactionIndex')),
        'type': hex_to_int(result.get('type'))
    }

    # for key, value in data.items():
    #     print(f"Key: {key}, Value: {value}, Type: {type(value)}")

    return data


def get_txn_hash_ls():
    query = '''
        select hash from `default`.canonical_beacon_block_execution_transaction where epoch_start_date_time >= '2024-03-13 00:00:00' and type = 3 order by hash
    '''
    result = client.query(query)
    txn_hash_ls_bytes = [row[0] for row in result.result_rows]
    txn_hash_ls = [tx_hash.decode('utf-8') for tx_hash in txn_hash_ls_bytes]
    return txn_hash_ls


def main():
    txn_hash_ls = get_txn_hash_ls()
    print(f"Total number of transactions: {len(txn_hash_ls)}")
    for i in range(29226, len(txn_hash_ls), 1):
        j = 0
        while True:
            print(i)
            try:
                res = get_rpc_res(txn_hash_ls[i])
                # print(json.dumps(res, indent=4))
                insert_rpc_res(res)
                break  # Exit the while loop if successful
            except ValueError as e:
                if j == 5:
                    print(f"Error processing transaction {
                          txn_hash_ls[i]}: {e}")
                    break
                time.sleep(10)
                j += 1


if __name__ == '__main__':
    main()
