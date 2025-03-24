-- Flipside Query 1: Transactions for Arbitrum Airdrop (March 23, 2023)

WITH airdrop_txs AS (
  SELECT 
    CAST(block_timestamp AS DATE) AS Date,
    COUNT(*) AS airdrop_tx_count
  FROM arbitrum.core.fact_token_transfers
  WHERE CAST(block_timestamp AS DATE) BETWEEN '2023-03-15' AND '2023-04-11'
    AND from_address = '0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9'
    AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
  GROUP BY Date
),
total_txs AS (
  SELECT 
    CAST(block_timestamp AS DATE) AS Date,
    COUNT(*) AS total_tx_count
  FROM arbitrum.core.fact_transactions
  WHERE CAST(block_timestamp AS DATE) BETWEEN '2023-03-15' AND '2023-04-11'
  GROUP BY Date
)
SELECT 
  COALESCE(a.Date, t.Date) AS Date,
  COALESCE(a.airdrop_tx_count, 0) AS airdrop_tx_count,
  COALESCE(t.total_tx_count, 0) AS total_tx_count,
  (COALESCE(a.airdrop_tx_count, 0) * 100.0 / NULLIF(t.total_tx_count, 0)) AS pct_airdrop_txs
FROM airdrop_txs a
FULL OUTER JOIN total_txs t ON a.Date = t.Date
ORDER BY Date;

-- Flipside Query 2: Transactions for Optimism Airdrop (May 31, 2022)

WITH airdrop_txs AS (
  SELECT 
    CAST(block_timestamp AS DATE) AS Date,
    COUNT(*) AS airdrop_tx_count
  FROM optimism.core.fact_token_transfers
  WHERE CAST(block_timestamp AS DATE) BETWEEN '2022-05-15' AND '2022-07-01'
    AND from_address = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
    AND contract_address = '0x4200000000000000000000000000000000000042'
  GROUP BY Date
),
total_txs AS (
  SELECT 
    CAST(block_timestamp AS DATE) AS Date,
    COUNT(*) AS total_tx_count
  FROM optimism.core.fact_transactions
  WHERE CAST(block_timestamp AS DATE) BETWEEN '2022-05-15' AND '2022-07-01'
  GROUP BY Date
)
SELECT 
  COALESCE(a.Date, t.Date) AS Date,
  COALESCE(a.airdrop_tx_count, 0) AS airdrop_tx_count,
  COALESCE(t.total_tx_count, 0) AS total_tx_count,
  (COALESCE(a.airdrop_tx_count, 0) * 100.0 / NULLIF(t.total_tx_count, 0)) AS pct_airdrop_txs
FROM airdrop_txs a
FULL OUTER JOIN total_txs t ON a.Date = t.Date
ORDER BY Date;
