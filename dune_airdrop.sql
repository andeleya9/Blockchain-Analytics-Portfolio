-- Dune Query 1: Transactions for Arbitrum Airdrop (March 23, 2023)

WITH airdrop_txs AS (
SELECT
    DATE_TRUNC('day', evt_block_time) AS Date,
    COUNT(*) AS airdrop_tx_count
FROM
    transfers_arbitrum.erc20
WHERE
    evt_block_time BETWEEN TRY_CAST('2023-03-15 00:00:00 UTC' AS TIMESTAMP) AND TRY_CAST('2023-04-11 00:00:00 UTC' AS TIMESTAMP)
    and token_address =  0x912ce59144191c1204e64559fe8253a0e49e6548 
    and wallet_address = 0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9
    and transfer_type = 'send'
GROUP BY
    1
),
 total_txs AS (
  SELECT 
    CAST(evt_block_time AS DATE) AS Date,
    COUNT(*) AS total_tx_count
  FROM     transfers_arbitrum.erc20
where evt_block_time BETWEEN TRY_CAST('2023-03-15 00:00:00 UTC' AS TIMESTAMP) AND TRY_CAST('2023-04-11 00:00:00 UTC' AS TIMESTAMP)
  GROUP BY 1
)
SELECT 
  COALESCE(a.Date, t.Date) AS Date,
  COALESCE(a.airdrop_tx_count, 0) AS airdrop_tx_count,
  COALESCE(t.total_tx_count, 0) AS total_tx_count,
  (COALESCE(a.airdrop_tx_count, 0) * 100.0 / NULLIF(t.total_tx_count, 0)) AS pct_airdrop_txs
FROM airdrop_txs a
FULL OUTER JOIN total_txs t ON a.Date = t.Date
ORDER BY Date;

-- Dune Query 2: Transactions for Optimism Airdrop (May 31, 2022)

WITH airdrop_txs AS (
  SELECT
    DATE_TRUNC('day', block_time) AS Date,
    COUNT(*) AS airdrop_tx_count
  FROM
    tokens.transfers
  WHERE
    block_time BETWEEN TRY_CAST('2022-05-15 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-06-11 00:00:00 UTC' AS TIMESTAMP) -- Período ajustado para Optimism
    AND blockchain = 'optimism'
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
    AND "from" = 0xfedfaf1a10335448b7fa0268f56d2b44dbd357de -- Dirección del distribuidor
  GROUP BY
    1
),
total_txs AS (
  SELECT 
    CAST(block_time AS DATE) AS Date,
    COUNT(*) AS total_tx_count
  FROM 
    tokens.transfers
  WHERE 
    block_time BETWEEN TRY_CAST('2022-05-15 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-06-11 00:00:00 UTC' AS TIMESTAMP)
    AND blockchain = 'optimism'
  GROUP BY 
    1
)
SELECT 
  COALESCE(a.Date, t.Date) AS Date,
  COALESCE(a.airdrop_tx_count, 0) AS airdrop_tx_count,
  COALESCE(t.total_tx_count, 0) AS total_tx_count,
  (COALESCE(a.airdrop_tx_count, 0) * 100.0 / NULLIF(t.total_tx_count, 0)) AS pct_airdrop_txs
FROM 
  airdrop_txs a
FULL OUTER JOIN 
  total_txs t 
  ON a.Date = t.Date
ORDER BY 
  Date;

-- Dune Query 3: Behaviour wallets 1 month Arbitrum

WITH airdrop_transactions AS (
  -- Identificar transacciones del airdrop conectando send y receive
  SELECT 
    r.evt_tx_hash AS tx_hash,
    r.wallet_address AS recipient_wallet,
    ABS(r.amount_raw) / 1e18 AS amount_received
  FROM 
    transfers_arbitrum.erc20 s
  JOIN 
    transfers_arbitrum.erc20 r 
    ON s.evt_tx_hash = r.evt_tx_hash
  WHERE 
    s.evt_block_time BETWEEN TRY_CAST('2023-03-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2023-04-11 23:59:59 UTC' AS TIMESTAMP) -- Período del airdrop
    AND s.token_address = 0x912ce59144191c1204e64559fe8253a0e49e6548 -- Token ARB
    AND s.wallet_address = 0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9 -- Dirección del distribuidor
    AND s.transfer_type = 'send'
    AND r.transfer_type = 'receive'
),
airdrop_recipients AS (
  -- Total recibido por cada wallet en el airdrop
  SELECT 
    recipient_wallet AS wallet,
    SUM(ABS(amount_received)) AS initial_amount
  FROM 
    airdrop_transactions
  GROUP BY 
    recipient_wallet
),
transfers_out AS (
  -- Total enviado por los receptores después del airdrop
  SELECT 
    wallet_address AS wallet,
    SUM(ABS(amount_raw)) / 1e18 AS sold_amount
  FROM 
    transfers_arbitrum.erc20
  WHERE 
    evt_block_time BETWEEN TRY_CAST('2023-03-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2023-04-23 23:59:59 UTC' AS TIMESTAMP) -- Período post-airdrop
    AND token_address = 0x912ce59144191c1204e64559fe8253a0e49e6548 -- Token ARB
    AND wallet_address IN (SELECT wallet FROM airdrop_recipients)
    AND transfer_type = 'send'
    AND NOT (wallet_address = 0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9)
  GROUP BY 
    wallet_address
),
wallet_behavior AS (
  -- Clasificar las billeteras según su comportamiento
  SELECT 
    a.wallet,
    a.initial_amount,
    COALESCE(t.sold_amount, 0) AS sold_amount
  FROM 
    airdrop_recipients a
  LEFT JOIN 
    transfers_out t ON a.wallet = t.wallet
),
metrics AS (
  -- Calcular métricas finales
  SELECT 
    COUNT(DISTINCT CASE WHEN sold_amount >= 1 * initial_amount THEN wallet END) AS sold_wallets,
    COUNT(DISTINCT CASE WHEN sold_amount <= 0.5 * initial_amount THEN wallet END) AS kept_wallets,
    COUNT(DISTINCT wallet) AS total_wallets
  FROM 
    wallet_behavior
)
SELECT 
  sold_wallets,
  kept_wallets,
  total_wallets,
  (
    TRY_CAST(sold_wallets AS REAL) / NULLIF(total_wallets, 0)
  ) * 100 AS pct_sold,
  (
    TRY_CAST(kept_wallets AS REAL) / NULLIF(total_wallets, 0)
  ) * 100 AS pct_kept
FROM 
  metrics;

-- Dune Query 4: Behaviour wallets 1 month Optimism

WITH airdrop_recipients AS (
  -- Total recibido por cada wallet en el airdrop
  SELECT 
    "to" AS wallet,
    SUM(amount) AS initial_amount
  FROM 
    tokens.transfers
  WHERE 
    block_time BETWEEN TRY_CAST('2022-05-31 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-06-01 23:59:59 UTC' AS TIMESTAMP)
    AND blockchain = 'optimism'
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
    AND "from" = 0xfedfaf1a10335448b7fa0268f56d2b44dbd357de -- Dirección del distribuidor
    AND "to" != 0xfedfaf1a10335448b7fa0268f56d2b44dbd357de -- Excluir el distribuidor
  GROUP BY 
    "to"
),
transfers_out AS (
  -- Total enviado por los receptores después del airdrop
  SELECT 
    "from" AS wallet,
    SUM(amount) AS sold_amount
  FROM 
    tokens.transfers
  WHERE 
    block_time BETWEEN TRY_CAST('2022-05-31 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-06-30 23:59:59 UTC' AS TIMESTAMP)
    AND blockchain = 'optimism'
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
    AND "from" IN (SELECT wallet FROM airdrop_recipients)
  GROUP BY 
    "from"
),
wallet_behavior AS (
  -- Clasificar las billeteras según su comportamiento
  SELECT 
    a.wallet,
    a.initial_amount,
    COALESCE(t.sold_amount, 0) AS sold_amount
  FROM 
    airdrop_recipients a
  LEFT JOIN 
    transfers_out t ON a.wallet = t.wallet
),
metrics AS (
  -- Calcular métricas finales
  SELECT 
    COUNT(DISTINCT CASE WHEN sold_amount >= 1 * initial_amount THEN wallet END) AS sold_wallets,
    COUNT(DISTINCT CASE WHEN sold_amount <= 0.5 * initial_amount THEN wallet END) AS kept_wallets,
    COUNT(DISTINCT wallet) AS total_wallets
  FROM 
    wallet_behavior
)
SELECT 
  sold_wallets,
  kept_wallets,
  total_wallets,
  (
    TRY_CAST(sold_wallets AS REAL) / NULLIF(total_wallets, 0)
  ) * 100 AS pct_sold,
  (
    TRY_CAST(kept_wallets AS REAL) / NULLIF(total_wallets, 0)
  ) * 100 AS pct_kept
FROM 
  metrics;

-- Dune Query 5: Behaviour ARB airdropped 1 month

WITH airdrop_transactions AS (
  -- Identificar transacciones del airdrop conectando send y receive
  SELECT 
    r.evt_tx_hash AS tx_hash,
    r.wallet_address AS recipient_wallet,
    ABS(r.amount_raw) / 1e18 AS amount_received
  FROM 
    transfers_arbitrum.erc20 s
  JOIN 
    transfers_arbitrum.erc20 r 
    ON s.evt_tx_hash = r.evt_tx_hash
  WHERE 
    s.evt_block_time BETWEEN TRY_CAST('2023-03-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2023-04-11 23:59:59 UTC' AS TIMESTAMP) -- Período del airdrop
    AND s.token_address = 0x912ce59144191c1204e64559fe8253a0e49e6548 -- Token ARB
    AND s.wallet_address = 0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9 -- Dirección del distribuidor
    AND s.transfer_type = 'send'
    AND r.transfer_type = 'receive'
),
airdrop_recipients AS (
  -- Total recibido por cada wallet en el airdrop
  SELECT 
    recipient_wallet AS wallet,
    SUM(ABS(amount_received)) AS total_amount_received
  FROM 
    airdrop_transactions
  GROUP BY 
    recipient_wallet
),
transfers_out AS (
  -- Total enviado por los receptores después del airdrop
  SELECT 
    wallet_address AS wallet,
    SUM(ABS(amount_raw)) / 1e18 AS total_amount_sent
  FROM 
    transfers_arbitrum.erc20
  WHERE 
    evt_block_time BETWEEN TRY_CAST('2023-03-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2023-04-23 23:59:59 UTC' AS TIMESTAMP) -- Período post-airdrop
    AND token_address = 0x912ce59144191c1204e64559fe8253a0e49e6548 -- Token ARB
    AND wallet_address IN (SELECT wallet FROM airdrop_recipients)
    AND transfer_type = 'send'
    AND NOT (wallet_address = 0x67a24ce4321ab3af51c2d0a4801c3e111d88c9d9)
  GROUP BY 
    wallet_address
),
adjusted_transfers AS (
  -- Limitar el monto enviado al monto recibido en el airdrop
  SELECT 
    a.wallet,
    a.total_amount_received,
    LEAST(COALESCE(t.total_amount_sent, 0), a.total_amount_received) AS total_amount_sent_limited
  FROM 
    airdrop_recipients a
  LEFT JOIN 
    transfers_out t ON a.wallet = t.wallet
),
debug_data AS (
  -- Añadir un paso de depuración para verificar los valores
  SELECT 
    COUNT(DISTINCT wallet) AS num_recipients,
    SUM(total_amount_received) AS total_received,
    SUM(total_amount_sent_limited) AS total_sent
  FROM 
    adjusted_transfers
)
SELECT 
  total_sent,
  total_received,
  (total_sent / NULLIF(total_received, 0) * 100) AS pct_sold,
  ((total_received - total_sent) / NULLIF(total_received, 0) * 100) AS pct_kept,
  num_recipients
FROM 
  debug_data;

-- Dune Query 6: Behaviour OP airdropped 1 month

WITH airdrop_recipients AS (
  -- Total recibido por cada wallet en el airdrop
  SELECT 
    "to" AS wallet,
    SUM(amount) AS total_amount_received
  FROM 
    tokens.transfers
  WHERE 
    block_time BETWEEN TRY_CAST('2022-05-31 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-06-01 23:59:59 UTC' AS TIMESTAMP) -- Período del airdrop de OP
    AND blockchain = 'optimism'
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
    AND "from" = 0xfedfaf1a10335448b7fa0268f56d2b44dbd357de -- Dirección del distribuidor
    AND "to" != 0xfedfaf1a10335448b7fa0268f56d2b44dbd357de -- Excluir el distribuidor
  GROUP BY 
    "to"
),
transfers_out AS (
  -- Total enviado por los receptores después del airdrop
  SELECT 
    "from" AS wallet,
    SUM(amount) AS total_amount_sent
  FROM 
    tokens.transfers
  WHERE 
    block_time BETWEEN TRY_CAST('2022-05-31 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-06-30 23:59:59 UTC' AS TIMESTAMP) -- Período post-airdrop
    AND blockchain = 'optimism'
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
    AND "from" IN (SELECT wallet FROM airdrop_recipients)
  GROUP BY 
    "from"
),
adjusted_transfers AS (
  -- Limitar el monto enviado al monto recibido en el airdrop
  SELECT 
    a.wallet,
    a.total_amount_received,
    LEAST(COALESCE(t.total_amount_sent, 0), a.total_amount_received) AS total_amount_sent_limited
  FROM 
    airdrop_recipients a
  LEFT JOIN 
    transfers_out t ON a.wallet = t.wallet
),
debug_data AS (
  -- Añadir un paso de depuración para verificar los valores
  SELECT 
    COUNT(DISTINCT wallet) AS num_recipients,
    SUM(total_amount_received) AS total_received,
    SUM(total_amount_sent_limited) AS total_sent
  FROM 
    adjusted_transfers
)
SELECT 
  total_sent,
  total_received,
  (total_sent / NULLIF(total_received, 0) * 100) AS pct_sold,
  ((total_received - total_sent) / NULLIF(total_received, 0) * 100) AS pct_kept,
  num_recipients
FROM 
  debug_data;

-- Dune Query 7: Evolution ARB price

WITH transfers AS (
  SELECT 
    t.evt_block_time AS block_timestamp,
    ABS(t.amount_raw) / 1e18 AS amount,
    t.token_address
  FROM 
    transfers_arbitrum.erc20 t
  WHERE 
    t.evt_block_time BETWEEN TRY_CAST('2023-01-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2023-04-23 23:59:59 UTC' AS TIMESTAMP)
    AND t.token_address = 0x912ce59144191c1204e64559fe8253a0e49e6548 -- Token ARB
),
prices AS (
  SELECT 
    p.minute,
    p.price
  FROM 
    prices.usd p
  WHERE 
    p.minute BETWEEN TRY_CAST('2023-03-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2023-06-01 23:59:59 UTC' AS TIMESTAMP)
    AND p.contract_address = 0x912ce59144191c1204e64559fe8253a0e49e6548 -- Token ARB
    AND p.blockchain = 'arbitrum'
),
combined AS (
  -- Unir transferencias con precios por minuto más cercano
  SELECT 
    t.block_timestamp,
    t.amount,
    p.price,
    t.amount * p.price AS amount_usd
  FROM 
    transfers t
  LEFT JOIN 
    prices p 
    ON DATE_TRUNC('minute', t.block_timestamp) = p.minute
)
SELECT 
  CAST(block_timestamp AS DATE) AS price_date,
  AVG(amount_usd / NULLIF(amount, 0)) AS avg_arb_price,
  MIN(amount_usd / NULLIF(amount, 0)) AS min_arb_price,
  MAX(amount_usd / NULLIF(amount, 0)) AS max_arb_price,
  COUNT(*) AS transfer_count
FROM 
  combined
GROUP BY 
  CAST(block_timestamp AS DATE)
ORDER BY 
  price_date;

-- Dune Query 8: Evolution OP price

WITH transfers AS (
  SELECT 
    block_time AS block_timestamp,
    amount,
    contract_address AS token_address
  FROM 
    tokens.transfers
  WHERE 
    block_time BETWEEN TRY_CAST('2022-03-23 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-07-23 23:59:59 UTC' AS TIMESTAMP)
    AND blockchain = 'optimism'
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
),
prices AS (
  SELECT 
    minute,
    price
  FROM 
    prices.usd
  WHERE 
    minute BETWEEN TRY_CAST('2022-05-31 00:00:00 UTC' AS TIMESTAMP) 
      AND TRY_CAST('2022-08-01 23:59:59 UTC' AS TIMESTAMP)
    AND contract_address = 0x4200000000000000000000000000000000000042 -- Token OP
    AND blockchain = 'optimism'
),
combined AS (
  -- Unir transferencias con precios por minuto más cercano
  SELECT 
    t.block_timestamp,
    t.amount,
    p.price,
    t.amount * p.price AS amount_usd
  FROM 
    transfers t
  LEFT JOIN 
    prices p 
    ON DATE_TRUNC('minute', t.block_timestamp) = p.minute
)
SELECT 
  CAST(block_timestamp AS DATE) AS price_date,
  AVG(amount_usd / NULLIF(amount, 0)) AS avg_op_price,
  MIN(amount_usd / NULLIF(amount, 0)) AS min_op_price,
  MAX(amount_usd / NULLIF(amount, 0)) AS max_op_price,
  COUNT(*) AS transfer_count
FROM 
  combined
GROUP BY 
  CAST(block_timestamp AS DATE)
ORDER BY 
  price_date;

-- Dune Query 9: Gas price and transactions Arbitrum

SELECT 
  CAST(block_time AS DATE) AS tx_date,
  COUNT(*) AS tx_count,
  SUM(gas_used * gas_price) / 1e18 AS total_gas_cost_eth, -- Costo real en ETH
  AVG(gas_price) / 1e9 AS avg_gas_price_gwei,            -- Precio promedio en Gwei
  COUNT(DISTINCT "from") AS unique_wallets
FROM 
  arbitrum.transactions
WHERE 
  block_time BETWEEN TRY_CAST('2023-03-01 00:00:00 UTC' AS TIMESTAMP) 
    AND TRY_CAST('2023-04-23 23:59:59 UTC' AS TIMESTAMP)
GROUP BY 
  CAST(block_time AS DATE)
ORDER BY 
  tx_date;

-- Dune Query 10: Gas price and transactions Optimism

SELECT 
  CAST(block_time AS DATE) AS tx_date,
  COUNT(*) AS tx_count,
  SUM(gas_used * gas_price) / 1e18 AS total_gas_cost_eth, -- Costo real en ETH
  AVG(gas_price) / 1e9 AS avg_gas_price_gwei,            -- Precio promedio en Gwei
  COUNT(DISTINCT "from") AS unique_wallets
FROM 
  optimism.transactions
WHERE 
  block_time BETWEEN TRY_CAST('2022-05-01 00:00:00 UTC' AS TIMESTAMP) 
    AND TRY_CAST('2022-06-23 23:59:59 UTC' AS TIMESTAMP)
GROUP BY 
  CAST(block_time AS DATE)
ORDER BY 
  tx_date;
