-- Params
DECLARE day DATE DEFAULT '2025-09-16';
DECLARE txhash STRING DEFAULT '0xff50ad2b40c409c308b49408335819780217525b6c8e335f41f3fbd9b1a27503';
DECLARE pool STRING  DEFAULT '0x4e68ccd3e89f51c3074ca5072bbac773960dfa36'; -- WETH/USDT v3 0.3%
DECLARE SWAP_SIG STRING DEFAULT '0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67';
DECLARE fee_ppm INT64 DEFAULT 3000;  -- 0.3% = 3000 ppm

-- UDFs
CREATE TEMP FUNCTION HEX_TO_BN(hex STRING) RETURNS BIGNUMERIC
LANGUAGE js AS r"""
if (hex === null) return null;
let s = hex.startsWith('0x') ? hex.slice(2) : hex;
if (s.length === 0) return '0';
const v = BigInt('0x' + s);
return v.toString();
""";

CREATE TEMP FUNCTION HEX_TO_INT256(hex STRING) RETURNS BIGNUMERIC
LANGUAGE js AS r"""
if (hex === null) return null;
let s = hex.startsWith('0x') ? hex.slice(2) : hex;
if (s.length === 0) return '0';
let v = BigInt('0x' + s);
const TWO256 = BigInt('0x1' + '0'.repeat(64));
const TWO255 = TWO256 >> BigInt(1);
if (v >= TWO255) v = v - TWO256;
return v.toString();
""";

CREATE TEMP FUNCTION WORD(data STRING, n INT64) AS (
  CONCAT('0x', SUBSTR(data, 3 + n*64, 64))
);

CREATE TEMP FUNCTION WORD_INT24(data STRING, n INT64) RETURNS INT64
LANGUAGE js AS r"""
if (data === null) return null;
const s = (data.startsWith('0x') ? data.slice(2) : data).toLowerCase();
const start = s.length - 6; // 24 bits
const chunk = '0x' + s.slice(start);
let v = BigInt(chunk);
if (v >= (BigInt(1) << BigInt(23))) v = v - (BigInt(1) << BigInt(24));
return Number(v);
""";

WITH swap_log AS (
  SELECT
    l.block_timestamp,
    l.block_number,
    l.transaction_hash,
    l.address AS pool_address,
    l.topics[OFFSET(1)] AS sender,
    l.topics[OFFSET(2)] AS recipient,
    WORD(l.data, 0) AS w_amount0,
    WORD(l.data, 1) AS w_amount1,
    WORD(l.data, 2) AS w_sqrtPriceX96,
    WORD(l.data, 3) AS w_liquidity,
    WORD(l.data, 4) AS w_tick
  FROM `bigquery-public-data.crypto_ethereum.logs` AS l
  WHERE
    l.block_timestamp >= TIMESTAMP(day)
    AND l.block_timestamp <  TIMESTAMP(DATE_ADD(day, INTERVAL 1 DAY))
    AND l.transaction_hash = txhash
    AND l.address = pool
    AND l.topics[OFFSET(0)] = SWAP_SIG
),
decoded AS (
  SELECT
    block_timestamp,
    block_number,
    transaction_hash,
    pool_address,
    sender,
    recipient,
    CAST(HEX_TO_INT256(w_amount0) AS BIGNUMERIC) AS amount0_raw,  -- token0 = WETH
    CAST(HEX_TO_INT256(w_amount1) AS BIGNUMERIC) AS amount1_raw,  -- token1 = USDT
    CAST(HEX_TO_BN(w_sqrtPriceX96) AS BIGNUMERIC) AS sqrtPriceX96,
    CAST(HEX_TO_BN(w_liquidity)    AS BIGNUMERIC) AS liquidity,
    WORD_INT24(w_tick, 4) AS tick
  FROM swap_log
),
with_fee AS (
  SELECT
    d.*,
    CASE
      WHEN d.amount0_raw > 0 THEN 'WETH'
      WHEN d.amount1_raw > 0 THEN 'USDT'
      ELSE NULL
    END AS input_token,
    CASE
      WHEN d.amount0_raw > 0 THEN d.amount0_raw
      WHEN d.amount1_raw > 0 THEN d.amount1_raw
      ELSE CAST(NULL AS BIGNUMERIC)
    END AS amount_in_raw,
    CASE
      WHEN d.amount0_raw > 0 OR d.amount1_raw > 0
      THEN (CASE WHEN d.amount0_raw > 0 THEN d.amount0_raw ELSE d.amount1_raw END) * fee_ppm / 1000000
      ELSE CAST(NULL AS BIGNUMERIC)
    END AS lp_fee_raw
  FROM decoded d
)

SELECT
  block_timestamp,
  block_number,
  transaction_hash,
  pool_address,
  sender, recipient,
  amount0_raw,                           -- signed WETH delta (raw)
  amount1_raw,                           -- signed USDT delta (raw)
  CAST(amount0_raw / 1e18 AS BIGNUMERIC) AS amount0_WETH,
  CAST(amount1_raw / 1e6  AS BIGNUMERIC) AS amount1_USDT,
  sqrtPriceX96, liquidity, tick,

  input_token,
  amount_in_raw,
  -- LP fee (raw)
  lp_fee_raw,
  CASE
    WHEN input_token = 'WETH' THEN CAST(lp_fee_raw / 1e18 AS BIGNUMERIC)
    WHEN input_token = 'USDT' THEN CAST(lp_fee_raw / 1e6  AS BIGNUMERIC)
    ELSE NULL
  END AS lp_fee_normalized
FROM with_fee;
