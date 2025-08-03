-- Script to delete duplicate records from etherscan_raw tables
-- Run this directly against your database to remove duplicates

-- Delete duplicate logs (keep the most recent _dlt_load_id)
DELETE FROM etherscan_raw.logs
WHERE _dlt_id NOT IN (
  SELECT _dlt_id
  FROM (
    SELECT _dlt_id,
           ROW_NUMBER() OVER (
             PARTITION BY 
               chainid,
               address,
               topics,
               data,
               block_number,
               block_hash,
               time_stamp,
               
               gas_price,
               gas_used,
               log_index,
               transaction_hash,
               transaction_index
             ORDER BY _dlt_load_id DESC
           ) as rn
    FROM etherscan_raw.logs
  ) ranked
  WHERE rn = 1
);

-- Check results
-- SELECT 'logs' as table_name, COUNT(*) as remaining_records
-- FROM etherscan_raw.logs
-- UNION ALL
-- SELECT 'transactions' as table_name, COUNT(*) as remaining_records
-- FROM etherscan_raw.transactions;