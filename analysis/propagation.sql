-- propagation time by slot_start_date_time 
-- by consensus client median
with blobs_prop as (
	select 
		block_root, 
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		meta_client_id,
		slot_start_date_time
	from beacon_api_eth_v1_events_blob_sidecar
	group by 1, 3, 4
),
blocks_prop as (
	select 
		block,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		meta_client_id
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13'
	group by 1, 3
)
select 
	bb.slot_start_date_time, 
	median(greatest(blob_prop_time, block_prop_time)) as propagation_sec
from blobs_prop bb
left join blocks_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
group by 1
order by 1
    union all
select 
	slot_start_date_time,
	avg(propagation_slot_start_diff/1000) as propagation_sec
from beacon_api_eth_v1_events_block
where slot_start_date_time < '2024-03-01'
group by 1
order by 1 final;

-- propagation time by number of blobs
with blobs_prop as (
	select 
		block_root, 
		meta_client_id,
		max(propagation_slot_start_diff/1000) as blob_prop_time,
		max(blob_index)+1 as num_of_blobs
	from beacon_api_eth_v1_events_blob_sidecar
	where propagation_slot_start_diff <= 24000
	group by 1, 2
),
blocks_prop as (
	select 
		block,
		meta_client_id,
		max(propagation_slot_start_diff/1000) as block_prop_time,
		0 as num_of_blobs
	from beacon_api_eth_v1_events_block
	where slot_start_date_time >= '2024-03-13' and propagation_slot_start_diff <= 24000
	group by 1, 2
)
select 
	COALESCE(bb.num_of_blobs, bk.num_of_blobs) as num_of_blobs,
	avg(greatest(blob_prop_time, block_prop_time)) as propagation_sec
from blobs_prop bb
full outer join blocks_prop bk on bb.meta_client_id = bk.meta_client_id and bb.block_root = bk.block
where not (bk.block = '' and bb.block_root is not null)
group by 1
order by 1 final;





