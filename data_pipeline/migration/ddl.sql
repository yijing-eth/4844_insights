ALTER TABLE `default`.beacon_api_eth_v1_events_blob_sidecar_local ON CLUSTER '{cluster}' ADD PROJECTION projection_slot_start_date_time (SELECT * ORDER BY slot_start_date_time);
ALTER TABLE `default`.beacon_api_eth_v1_events_blob_sidecar_local ON CLUSTER '{cluster}' ADD PROJECTION projection_meta_consensus_implementation (SELECT * ORDER BY meta_consensus_implementation);
ALTER TABLE `default`.beacon_api_eth_v1_events_blob_sidecar_local ON CLUSTER '{cluster}' MATERIALIZE PROJECTION projection_slot_start_date_time;
ALTER TABLE `default`.beacon_api_eth_v1_events_blob_sidecar_local ON CLUSTER '{cluster}' MATERIALIZE PROJECTION projection_meta_consensus_implementation;

ALTER TABLE `default`.beacon_api_eth_v1_events_block_local ON CLUSTER '{cluster}' ADD PROJECTION projection_slot_start_date_time (SELECT * ORDER BY slot_start_date_time);
ALTER TABLE `default`.beacon_api_eth_v1_events_block_local ON CLUSTER '{cluster}' ADD PROJECTION projection_meta_consensus_implementation (SELECT * ORDER BY meta_consensus_implementation);
ALTER TABLE `default`.beacon_api_eth_v1_events_block_local ON CLUSTER '{cluster}' MATERIALIZE PROJECTION projection_slot_start_date_time;
ALTER TABLE `default`.beacon_api_eth_v1_events_block_local ON CLUSTER '{cluster}' MATERIALIZE PROJECTION projection_meta_consensus_implementation;

CREATE TABLE `default`.relay_proposer_payload_delivered ON CLUSTER '{cluster}' (
	`slot` UInt128,
    `parent_hash` String,
    `block_hash` String,
    `builder_pubkey` String,
    `proposer_pubkey` String,
    `proposer_fee_recipient` String,
    `gas_limit` UInt128,
    `gas_used` UInt128,
    `value` UInt128,
    `block_number` UInt128,
    `num_tx` UInt32,
    `relay_name` String
) ENGINE = ReplicatedMergeTree()
ORDER BY (slot, block_hash);

CREATE TABLE IF NOT EXISTS `default`.canonical_beacon_transaction_receipts_blob_sidecars ON CLUSTER '{cluster}' (
    `blockHash` String,
    `blockNumber` UInt256,
    `contractAddress` Nullable(String),
    `cumulativeGasUsed` UInt128,
    `effectiveGasPrice` UInt128,
    `from` Nullable(String),
    `gasUsed` UInt128,
    `status` UInt8,
    `to` Nullable(String),
    `transactionHash` String,
    `transactionIndex` UInt32,
    `type` UInt8
) ENGINE = ReplicatedMergeTree()
ORDER BY (`blockHash`, `transactionHash`);

CREATE TABLE IF NOT EXISTS `default`.canonical_block_details ON CLUSTER '{cluster}' (
    `baseFeePerGas` UInt64,
    `blobGasUsed` UInt64,
    `excessBlobGas` UInt64,
    `extraData` String,
    `gasLimit` UInt64,
    `gasUsed` UInt64,
    `hash` String,
    `miner` String,
    `number` UInt64,
    `receiptsRoot` String,
    `size` UInt64,
    `timestamp` UInt64
) ENGINE = ReplicatedMergeTree()
ORDER BY (`number`, `hash`);

CREATE TABLE IF NOT EXISTS `default`.canonical_transaction_details ON CLUSTER '{cluster}' (
    `blockHash` String,
    `blockNumber` UInt128,
    `chainId` UInt256,
    from String,
    gas UInt256,
    `gasPrice` UInt256,
    hash String,
    `maxFeePerGas` UInt256,
    `maxPriorityFeePerGas` UInt256,
    nonce UInt32,
    to String,
    `transactionIndex` UInt32,
    type UInt32,
    value UInt256
) ENGINE = ReplicatedMergeTree()
ORDER BY (`blockHash`, hash);

CREATE TABLE IF NOT EXISTS mev_block_details ON CLUSTER '{cluster}' (
    slot UInt128,
    parent_hash String,
    block_hash String,
    builder_pubkey String,
    proposer_pubkey String,
    proposer_fee_recipient String,
    gas_limit UInt64,
    gas_used UInt64,
    value UInt256,
    num_tx UInt32,
    block_number UInt128
) ENGINE = ReplicatedMergeTree()
ORDER BY (slot, block_hash);

CREATE TABLE IF NOT EXISTS canonical_block_root ON CLUSTER '{cluster}' (
    parent_hash String,
    fee_recipient String,
    state_root String,
    receipts_root String,
    logs_bloom String,
    prev_randao String,
    block_number UInt128,
    gas_limit UInt256,
    gas_used UInt256,
    timestamp UInt128,
    extra_data String,
    base_fee_per_gas UInt256,
    block_hash String,
    transactions_root String,
    withdrawals_root String,
    blob_gas_used Nullable(UInt256),
    excess_blob_gas Nullable(UInt256),
    block_root String
) ENGINE = ReplicatedMergeTree()
ORDER BY (block_root);

