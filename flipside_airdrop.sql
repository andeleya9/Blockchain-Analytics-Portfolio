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

-- Flipside Query 3: Behaviour wallets 1 month Arbitrum

WITH airdrop_recipients AS (
  SELECT to_address AS wallet, SUM(raw_amount) / 1e18 AS initial_amount
  FROM arbitrum.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2023-03-23 00:00:00' AND '2023-04-11 23:59:59'
  AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
  AND from_address = '0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9'
  GROUP BY to_address
),
transfers_out AS (
  SELECT from_address AS wallet, SUM(raw_amount) / 1e18 AS sold_amount
  FROM arbitrum.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2023-03-23 00:00:00' AND '2023-04-23 23:59:59'
  AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
  AND from_address IN (SELECT wallet FROM airdrop_recipients)
  GROUP BY from_address
)
SELECT 
  COUNT(DISTINCT CASE WHEN COALESCE(t.sold_amount, 0) >= 1 * a.initial_amount THEN a.wallet END) AS sold_wallets,
  COUNT(DISTINCT CASE WHEN COALESCE(t.sold_amount, 0) <= 0.5 * a.initial_amount THEN a.wallet END) AS kept_wallets,
  COUNT(DISTINCT a.wallet) AS total_wallets,
  (sold_wallets::FLOAT / total_wallets) * 100 AS pct_sold,
  (kept_wallets::FLOAT / total_wallets) * 100 AS pct_kept
FROM airdrop_recipients a
LEFT JOIN transfers_out t ON a.wallet = t.wallet;

-- Flipside Query 4: Behaviour wallets 1 month Optimism

WITH airdrop_recipients AS (
  SELECT to_address AS wallet, SUM(raw_amount) / 1e18 AS initial_amount
  FROM optimism.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2022-05-31 00:00:00' AND '2022-06-01 23:59:59'
  AND contract_address = '0x4200000000000000000000000000000000000042'
  AND from_address = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
  GROUP BY to_address
),
transfers_out AS (
  SELECT from_address AS wallet, SUM(raw_amount) / 1e18 AS sold_amount
  FROM optimism.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2022-05-31 00:00:00' AND '2022-06-30 23:59:59'
  AND contract_address = '0x4200000000000000000000000000000000000042'
  AND from_address IN (SELECT wallet FROM airdrop_recipients)
  GROUP BY from_address
)
SELECT 
  COUNT(DISTINCT CASE WHEN COALESCE(t.sold_amount, 0) >= 1 * a.initial_amount THEN a.wallet END) AS sold_wallets,
  COUNT(DISTINCT CASE WHEN COALESCE(t.sold_amount, 0) <= 0.5 * a.initial_amount THEN a.wallet END) AS kept_wallets,
  COUNT(DISTINCT a.wallet) AS total_wallets,
  (sold_wallets::FLOAT / total_wallets) * 100 AS pct_sold,
  (kept_wallets::FLOAT / total_wallets) * 100 AS pct_kept
FROM airdrop_recipients a
LEFT JOIN transfers_out t ON a.wallet = t.wallet;

-- Flipside Query 5: Behaviour ARB airdropped 1 month

WITH airdrop_recipients AS (
  SELECT to_address AS wallet, 
         SUM(raw_amount) / 1e18 AS initial_amount
  FROM arbitrum.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2023-03-23 00:00:00' AND '2023-04-11 23:59:59'
    AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
    AND from_address IN (
      SELECT from_address
      FROM arbitrum.core.fact_token_transfers
      WHERE block_timestamp BETWEEN '2023-03-23 00:00:00' AND '2023-04-11 23:59:59'
        AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
        AND from_address = '0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9'
      GROUP BY from_address
    )
  GROUP BY to_address
),
transfers_out AS (
  SELECT from_address AS wallet, SUM(raw_amount) / 1e18 AS sold_amount
  FROM arbitrum.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2023-03-23 00:00:00' AND '2023-04-23 23:59:59'
    AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
    AND from_address IN (SELECT wallet FROM airdrop_recipients)
  GROUP BY from_address
),
arb_totals AS (
  SELECT 
    a.wallet,
    a.initial_amount,
    LEAST(COALESCE(t.sold_amount, 0), a.initial_amount) AS sold_amount  -- Limita sold_amount al máximo de initial_amount
  FROM airdrop_recipients a
  LEFT JOIN transfers_out t ON a.wallet = t.wallet
)
SELECT 
  SUM(initial_amount) AS total_arb_distributed,                        -- Total distribuido
  SUM(sold_amount) AS total_arb_sold,                                 -- Total vendido (limitado al airdrop)
  SUM(CASE WHEN sold_amount < initial_amount 
           THEN initial_amount - sold_amount 
           ELSE 0 END) AS total_arb_not_sold,                        -- Total no vendido
  (SUM(sold_amount) / SUM(initial_amount)) * 100 AS pct_sold,         -- Porcentaje vendido
  (SUM(CASE WHEN sold_amount < initial_amount 
            THEN initial_amount - sold_amount 
            ELSE 0 END) / SUM(initial_amount)) * 100 AS pct_not_sold  -- Porcentaje no vendido
FROM arb_totals;

-- Flipside Query 6: Behaviour Optimism airdropped 1 month

WITH airdrop_recipients AS (
  SELECT to_address AS wallet, 
         SUM(raw_amount) / 1e18 AS initial_amount
  FROM optimism.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2022-05-31 00:00:00' AND '2022-06-01 23:59:59'
    AND contract_address = '0x4200000000000000000000000000000000000042'
    AND from_address = '0xfedfaf1a10335448b7fa0268f56d2b44dbd357de'
  GROUP BY to_address
),
transfers_out AS (
  SELECT from_address AS wallet, SUM(raw_amount) / 1e18 AS sold_amount
  FROM optimism.core.fact_token_transfers
  WHERE block_timestamp BETWEEN '2022-05-31 00:00:00' AND '2022-06-30 23:59:59'
    AND contract_address = '0x4200000000000000000000000000000000000042'
    AND from_address IN (SELECT wallet FROM airdrop_recipients)
  GROUP BY from_address
),
op_totals AS (
  SELECT 
    a.wallet,
    a.initial_amount,
    LEAST(COALESCE(t.sold_amount, 0), a.initial_amount) AS sold_amount  -- Limita sold_amount al máximo de initial_amount
  FROM airdrop_recipients a
  LEFT JOIN transfers_out t ON a.wallet = t.wallet
)
SELECT 
  SUM(initial_amount) AS total_op_distributed,                        -- Total distribuido
  SUM(sold_amount) AS total_op_sold,                                 -- Total vendido (limitado al airdrop)
  SUM(CASE WHEN sold_amount < initial_amount 
           THEN initial_amount - sold_amount 
           ELSE 0 END) AS total_op_not_sold,                        -- Total no vendido
  (SUM(sold_amount) / SUM(initial_amount)) * 100 AS pct_sold,        -- Porcentaje vendido
  (SUM(CASE WHEN sold_amount < initial_amount 
            THEN initial_amount - sold_amount 
            ELSE 0 END) / SUM(initial_amount)) * 100 AS pct_not_sold -- Porcentaje no vendido
FROM op_totals;

-- Flipside Query 7: Evolution ARB price

SELECT 
  CAST(block_timestamp AS DATE) AS price_date,
  AVG(amount_usd / NULLIF(amount, 0)) AS avg_arb_price,
  MIN(amount_usd / NULLIF(amount, 0)) AS min_arb_price,
  MAX(amount_usd / NULLIF(amount, 0)) AS max_arb_price,
  COUNT(*) AS transfer_count
FROM arbitrum.core.ez_token_transfers
WHERE block_timestamp BETWEEN '2023-03-23 00:00:00' AND '2023-06-01 23:59:59'
  AND contract_address = '0x912ce59144191c1204e64559fe8253a0e49e6548'
GROUP BY price_date
ORDER BY price_date;

-- Flipside Query 8: Evolution OP price

SELECT 
  CAST(block_timestamp AS DATE) AS price_date,
  AVG(amount_usd / NULLIF(amount, 0)) AS avg_op_price,
  MIN(amount_usd / NULLIF(amount, 0)) AS min_op_price,
  MAX(amount_usd / NULLIF(amount, 0)) AS max_op_price,
  COUNT(*) AS transfer_count
FROM optimism.core.ez_token_transfers
WHERE block_timestamp BETWEEN '2022-05-31 00:00:00' AND '2022-08-01 23:59:59'
  AND contract_address = '0x4200000000000000000000000000000000000042'
GROUP BY price_date
ORDER BY price_date;

-- Flipside Query 9: Gas price evolution Arbitrum

SELECT 
  CAST(block_timestamp AS DATE) AS tx_date,
  COUNT(*) AS tx_count,
  SUM(gas_used * gas_price_paid) / 1e18 AS total_gas_cost_eth, -- Costo real en ETH
  AVG(gas_price_paid) / 1e9 AS avg_gas_price_gwei,            -- Precio promedio en Gwei
  COUNT(DISTINCT from_address) AS unique_wallets
FROM arbitrum.core.fact_transactions
WHERE block_timestamp BETWEEN '2023-03-01 00:00:00' AND '2023-04-23 23:59:59'
GROUP BY tx_date
ORDER BY tx_date;

-- Flipside Query 10: Gas price evolution Optimism

SELECT 
  CAST(block_timestamp AS DATE) AS tx_date,
  COUNT(*) AS tx_count,
  SUM(gas_used * EFFECTIVE_GAS_PRICE) / 1e18 AS total_gas_cost_eth, -- Costo real en ETH
  AVG(EFFECTIVE_GAS_PRICE) / 1e9 AS avg_gas_price_gwei,            -- Precio promedio en Gwei
  COUNT(DISTINCT from_address) AS unique_wallets
FROM optimism.core.fact_transactions
WHERE block_timestamp BETWEEN '2022-05-01 00:00:00' AND '2022-06-23 23:59:59'
GROUP BY tx_date
ORDER BY tx_date;
