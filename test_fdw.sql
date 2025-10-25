-- NTP FDW Wrapper Test Suite
-- German NTP Energy Market API - WASM Foreign Data Wrapper
-- Generated: 2025-10-25
-- API: https://www.netztransparenz.de
-- Version: v0.2.0

-- ============================================
-- SETUP SECTION
-- ============================================

\timing on

-- Enable wrappers extension
CREATE EXTENSION IF NOT EXISTS wrappers;

-- Create WASM foreign data wrapper
CREATE FOREIGN DATA WRAPPER IF NOT EXISTS wasm_wrapper
  HANDLER wasm_fdw_handler
  VALIDATOR wasm_fdw_validator;

-- Create foreign server
-- NOTE: Replace YOUR_CLIENT_ID and YOUR_CLIENT_SECRET with actual OAuth2 credentials
-- Get credentials from: https://www.netztransparenz.de
CREATE SERVER IF NOT EXISTS ntp_server
  FOREIGN DATA WRAPPER wasm_wrapper
  OPTIONS (
    fdw_package_url 'https://github.com/powabase/supabase-fdw-ntp/releases/download/v0.2.0/supabase_fdw_ntp.wasm',
    fdw_package_name 'powabase:supabase-fdw-ntp',
    fdw_package_version '0.2.0',
    fdw_package_checksum '494038bc7b5ed52880a2d9e276bb85adb7c8b91794f6bbfbba9ec147467297f2',
    api_base_url 'https://ds.netztransparenz.de',
    oauth2_token_url 'https://identity.netztransparenz.de/users/connect/token',
    oauth2_client_id 'YOUR_CLIENT_ID',
    oauth2_client_secret 'YOUR_CLIENT_SECRET',
    oauth2_scope 'ntpStatistic.read_all_public'
  );

-- Create schema
CREATE SCHEMA IF NOT EXISTS ntp;

-- ============================================
-- TABLE 1: renewable_energy_timeseries
-- ============================================
-- Purpose: Consolidated renewable energy production data
-- Endpoints: 9 (prognose, hochrechnung, onlinehochrechnung × 3 products)
-- Granularity: 15-minute or 60-minute intervals
-- ============================================

CREATE FOREIGN TABLE IF NOT EXISTS ntp.renewable_energy_timeseries (
  -- TEMPORAL DIMENSIONS
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,
  interval_minutes SMALLINT NOT NULL,

  -- CATEGORICAL DIMENSIONS
  product_type TEXT NOT NULL
    CHECK (product_type IN ('solar', 'wind_onshore', 'wind_offshore')),
  data_category TEXT NOT NULL
    CHECK (data_category IN ('forecast', 'extrapolation', 'online_actual')),

  -- TSO ZONE BREAKDOWN (German Transmission System Operators)
  tso_50hertz_mw NUMERIC(10,3),      -- Eastern Germany
  tso_amprion_mw NUMERIC(10,3),      -- Western Germany
  tso_tennet_mw NUMERIC(10,3),       -- Northern Germany
  tso_transnetbw_mw NUMERIC(10,3),   -- Southern Germany

  -- COMPUTED AGGREGATES
  total_germany_mw NUMERIC(10,3) GENERATED ALWAYS AS (
    COALESCE(tso_50hertz_mw, 0) +
    COALESCE(tso_amprion_mw, 0) +
    COALESCE(tso_tennet_mw, 0) +
    COALESCE(tso_transnetbw_mw, 0)
  ) STORED,

  has_missing_data BOOLEAN GENERATED ALWAYS AS (
    tso_50hertz_mw IS NULL OR
    tso_amprion_mw IS NULL OR
    tso_tennet_mw IS NULL OR
    tso_transnetbw_mw IS NULL
  ) STORED,

  -- METADATA
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ DEFAULT NOW()

) SERVER ntp_server
OPTIONS (table 'renewable_energy_timeseries');

-- ============================================
-- TABLE 2: electricity_market_prices
-- ============================================
-- Purpose: Consolidated electricity pricing data
-- Endpoints: 4 (Spotmarktpreise, NegativePreise, marktpraemie, Jahresmarktpraemie)
-- Granularity: Hourly, monthly, annual
-- ============================================

CREATE FOREIGN TABLE IF NOT EXISTS ntp.electricity_market_prices (
  -- TEMPORAL DIMENSIONS
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,
  granularity TEXT NOT NULL
    CHECK (granularity IN ('hourly', 'monthly', 'annual')),

  -- PRICE DIMENSIONS
  price_type TEXT NOT NULL
    CHECK (price_type IN (
      'spot_market',
      'market_premium',
      'annual_market_value',
      'negative_flag'
    )),
  price_eur_mwh NUMERIC(10,3),       -- Can be negative during oversupply

  -- COMPUTED PRICE CONVERSIONS
  price_ct_kwh NUMERIC(10,4) GENERATED ALWAYS AS (
    price_eur_mwh / 10
  ) STORED,

  is_negative BOOLEAN GENERATED ALWAYS AS (
    price_eur_mwh < 0
  ) STORED,

  -- PRODUCT CATEGORIES
  product_category TEXT,             -- For premiums: 'solar', 'wind_onshore', etc.

  -- NEGATIVE PRICE FLAGS
  negative_logic_hours TEXT
    CHECK (negative_logic_hours IN ('1h', '3h', '4h', '6h') OR negative_logic_hours IS NULL),
  negative_flag_value BOOLEAN,

  -- METADATA
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ DEFAULT NOW()

) SERVER ntp_server
OPTIONS (table 'electricity_market_prices');

-- ============================================
-- TABLE 3: redispatch_events
-- ============================================
-- Purpose: Grid intervention events (TSO power plant adjustments)
-- Endpoint: 1 (Redispatch)
-- Granularity: Variable duration events
-- ============================================

CREATE FOREIGN TABLE IF NOT EXISTS ntp.redispatch_events (
  -- TEMPORAL DIMENSIONS
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,

  -- EVENT DETAILS
  reason TEXT NOT NULL,              -- German text: "Probestart (NetzRes)", etc.
  direction TEXT NOT NULL,           -- 'increase_generation' | 'reduce_generation'
  avg_power_mw NUMERIC,
  max_power_mw NUMERIC,
  total_energy_mwh NUMERIC,

  -- TSO & FACILITY
  requesting_tso TEXT NOT NULL,      -- '50Hertz' | 'Amprion' | 'TenneT' | 'TransnetBW'
  instructing_tso TEXT,
  affected_facility TEXT,
  energy_type TEXT,                  -- 'Konventionell' | 'Erneuerbar' | 'Sonstiges'

  -- METADATA
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ DEFAULT NOW()

) SERVER ntp_server
OPTIONS (table 'redispatch_events');

-- ============================================
-- TABLE 4: grid_status_timeseries
-- ============================================
-- Purpose: Minute-by-minute grid stability status (traffic light system)
-- Endpoint: 1 (TrafficLight - JSON)
-- Granularity: 1-minute intervals (1440 rows per day)
-- ============================================

CREATE FOREIGN TABLE IF NOT EXISTS ntp.grid_status_timeseries (
  -- TEMPORAL DIMENSIONS
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,

  -- STATUS
  grid_status TEXT NOT NULL          -- 'GREEN' | 'YELLOW' | 'RED'
    CHECK (grid_status IN ('GREEN', 'YELLOW', 'RED')),

  -- METADATA
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ DEFAULT NOW()

) SERVER ntp_server
OPTIONS (table 'grid_status_timeseries');

-- ============================================
-- PERMISSIONS
-- ============================================

GRANT USAGE ON SCHEMA ntp TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA ntp TO postgres;

-- ============================================
-- TABLE COMMENTS
-- ============================================

COMMENT ON FOREIGN TABLE ntp.renewable_energy_timeseries IS
  'Consolidated renewable energy production data (forecasts, actuals, real-time). Consolidates 9 NTP API endpoints.';

COMMENT ON FOREIGN TABLE ntp.electricity_market_prices IS
  'Consolidated electricity pricing data (spot, premiums, annual values, negative price flags). Consolidates 4 NTP API endpoints.';

COMMENT ON FOREIGN TABLE ntp.redispatch_events IS
  'Grid intervention events where TSOs adjust power plant output to maintain grid stability.';

COMMENT ON FOREIGN TABLE ntp.grid_status_timeseries IS
  'Minute-by-minute grid stability status using traffic light system (GREEN/YELLOW/RED).';

-- ============================================
-- TEST QUERIES - Renewable Energy
-- ============================================

\echo ''
\echo '=== Test 1: Basic Solar Forecast Query ==='
-- Expected: 96 rows (4 per hour x 24 hours for 15-minute intervals)
-- Expected: All TSO zones populated, reasonable MW values
SELECT
  timestamp_utc,
  total_germany_mw,
  tso_50hertz_mw,
  tso_amprion_mw,
  interval_minutes
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'forecast'
  AND timestamp_utc >= CURRENT_DATE
  AND timestamp_utc < CURRENT_DATE + INTERVAL '1 day'
ORDER BY timestamp_utc
LIMIT 10;
-- Expected: 10 rows, 15-minute intervals, solar production values 0-5000 MW

\echo ''
\echo '=== Test 2: Wind Onshore Extrapolation (Hourly) ==='
-- Expected: 24 rows (hourly data for online_actual)
-- Expected: Higher MW values than solar (typical wind production)
SELECT
  timestamp_utc,
  total_germany_mw,
  interval_minutes,
  has_missing_data
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'wind_onshore'
  AND data_category = 'online_actual'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '1 day'
  AND timestamp_utc < CURRENT_DATE
ORDER BY timestamp_utc
LIMIT 10;
-- Expected: 10 rows, 60-minute intervals, wind production 1000-15000 MW

\echo ''
\echo '=== Test 3: Multi-Product Comparison ==='
-- Expected: 2-3 rows (solar, wind_onshore, wind_offshore if available)
-- Expected: Wind onshore > Solar in most cases
SELECT
  product_type,
  COUNT(*) as row_count,
  ROUND(AVG(total_germany_mw)::numeric, 2) as avg_production_mw,
  MIN(total_germany_mw) as min_mw,
  MAX(total_germany_mw) as max_mw
FROM ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE
GROUP BY product_type
ORDER BY avg_production_mw DESC;
-- Expected: Wind onshore typically highest avg, solar shows day/night variation

\echo ''
\echo '=== Test 4: TSO Zone Distribution Analysis ==='
-- Expected: 4 rows (one per TSO zone)
-- Expected: TenneT typically highest (Northern Germany, good wind resources)
SELECT
  'tso_50hertz' as tso_zone,
  ROUND(AVG(tso_50hertz_mw)::numeric, 2) as avg_mw,
  ROUND(STDDEV(tso_50hertz_mw)::numeric, 2) as volatility_mw,
  MIN(tso_50hertz_mw) as min_mw,
  MAX(tso_50hertz_mw) as max_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE
UNION ALL
SELECT
  'tso_amprion',
  ROUND(AVG(tso_amprion_mw)::numeric, 2),
  ROUND(STDDEV(tso_amprion_mw)::numeric, 2),
  MIN(tso_amprion_mw),
  MAX(tso_amprion_mw)
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE
UNION ALL
SELECT
  'tso_tennet',
  ROUND(AVG(tso_tennet_mw)::numeric, 2),
  ROUND(STDDEV(tso_tennet_mw)::numeric, 2),
  MIN(tso_tennet_mw),
  MAX(tso_tennet_mw)
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE
UNION ALL
SELECT
  'tso_transnetbw',
  ROUND(AVG(tso_transnetbw_mw)::numeric, 2),
  ROUND(STDDEV(tso_transnetbw_mw)::numeric, 2),
  MIN(tso_transnetbw_mw),
  MAX(tso_transnetbw_mw)
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE
ORDER BY tso_zone;
-- Expected: Regional differences visible (TenneT highest for wind, varied for solar)

-- ============================================
-- TEST QUERIES - Electricity Prices
-- ============================================

\echo ''
\echo '=== Test 5: Spot Market Prices (Last 24 Hours) ==='
-- Expected: 24 rows (hourly prices)
-- Expected: Price range typically -5 to 250 EUR/MWh
SELECT
  timestamp_utc,
  price_eur_mwh,
  price_ct_kwh,
  is_negative
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND granularity = 'hourly'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '1 day'
  AND timestamp_utc < CURRENT_DATE
ORDER BY timestamp_utc
LIMIT 10;
-- Expected: 10 rows, hourly prices, some may be negative during high renewable periods

\echo ''
\echo '=== Test 6: Negative Price Detection ==='
-- Expected: 0-10 rows (negative prices during renewable overproduction)
-- Expected: Primarily during midday (solar peak) or high wind periods
SELECT
  timestamp_utc,
  price_eur_mwh,
  CASE
    WHEN EXTRACT(HOUR FROM timestamp_utc) BETWEEN 10 AND 14 THEN 'Solar Peak'
    WHEN EXTRACT(HOUR FROM timestamp_utc) BETWEEN 0 AND 6 THEN 'Night (Wind)'
    ELSE 'Other'
  END as time_category
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND is_negative = true
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE
ORDER BY price_eur_mwh ASC
LIMIT 10;
-- Expected: Negative prices correlate with high renewable production periods

\echo ''
\echo '=== Test 7: Price Statistics Summary ==='
-- Expected: 1 row with aggregated statistics
-- Expected: Some negative hours (4-10%), average price 50-150 EUR/MWh
SELECT
  COUNT(*) as total_hours,
  COUNT(*) FILTER (WHERE is_negative) as negative_hours,
  ROUND((COUNT(*) FILTER (WHERE is_negative)::numeric / COUNT(*) * 100)::numeric, 2) as negative_pct,
  ROUND(MIN(price_eur_mwh)::numeric, 2) as min_price,
  ROUND(MAX(price_eur_mwh)::numeric, 2) as max_price,
  ROUND(AVG(price_eur_mwh)::numeric, 2) as avg_price
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND granularity = 'hourly'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND timestamp_utc < CURRENT_DATE;
-- Expected: ~168 hours (7 days), 5-10% negative, avg 50-150 EUR/MWh

-- ============================================
-- TEST QUERIES - Redispatch Events
-- ============================================

\echo ''
\echo '=== Test 8: Recent Redispatch Events ==='
-- Expected: Variable row count (depends on grid congestion)
-- Expected: TSO zones and power adjustments
SELECT
  timestamp_utc,
  reason,
  direction,
  requesting_tso,
  avg_power_mw
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY timestamp_utc DESC
LIMIT 10;
-- Expected: Redispatch events show grid management actions

\echo ''
\echo '=== Test 9: Redispatch Summary by TSO ==='
-- Expected: 1-4 rows (one per affected TSO zone)
-- Expected: Aggregated power adjustments
SELECT
  requesting_tso,
  COUNT(*) as event_count,
  ROUND(SUM(avg_power_mw)::numeric, 2) as total_adjustment_mw,
  ROUND(AVG(avg_power_mw)::numeric, 2) as avg_adjustment_mw
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY requesting_tso
ORDER BY event_count DESC;
-- Expected: Shows which TSO zones have most congestion/redispatch needs

-- ============================================
-- TEST QUERIES - Grid Status
-- ============================================

\echo ''
\echo '=== Test 10: Current Grid Status (TrafficLight) ==='
-- Expected: Recent grid status entries
-- Expected: Status values: GREEN/YELLOW/RED
SELECT
  timestamp_utc,
  grid_status,
  source_endpoint
FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp_utc DESC
LIMIT 10;
-- Expected: Real-time grid stability indicators

\echo ''
\echo '=== Test 11: Grid Status Distribution ==='
-- Expected: 3 rows showing status counts
-- Expected: Mostly GREEN under normal conditions
SELECT
  grid_status,
  COUNT(*) as occurrences,
  ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER() * 100)::numeric, 2) as percentage
FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY grid_status
ORDER BY grid_status;
-- Expected: GREEN should dominate, RED indicates critical periods

-- ============================================
-- ADVANCED QUERIES
-- ============================================

\echo ''
\echo '=== Test 12: Daily Price vs Solar Production Correlation ==='
-- Expected: 7 rows (one per day)
-- Expected: Inverse correlation (high solar = lower/negative prices)
SELECT
  DATE(p.timestamp_utc) as date,
  ROUND(AVG(p.price_eur_mwh)::numeric, 2) as avg_price_eur_mwh,
  ROUND(AVG(r.total_germany_mw)::numeric, 2) as avg_solar_mw,
  COUNT(*) FILTER (WHERE p.is_negative) as negative_hours
FROM ntp.electricity_market_prices p
LEFT JOIN ntp.renewable_energy_timeseries r
  ON DATE_TRUNC('hour', p.timestamp_utc) = DATE_TRUNC('hour', r.timestamp_utc)
  AND r.product_type = 'solar'
  AND r.data_category = 'extrapolation'
WHERE p.price_type = 'spot_market'
  AND p.granularity = 'hourly'
  AND p.timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND p.timestamp_utc < CURRENT_DATE
GROUP BY DATE(p.timestamp_utc)
ORDER BY date;
-- Expected: Days with high solar production show lower average prices

\echo ''
\echo '=== Test 13: Forecast vs Actual Accuracy ==='
-- Expected: 1 row per day showing forecast error
-- Expected: Forecast error typically within +/- 10-20%
SELECT
  DATE(r1.timestamp_utc) as date,
  ROUND(AVG(r1.total_germany_mw)::numeric, 2) as avg_forecast_mw,
  ROUND(AVG(r2.total_germany_mw)::numeric, 2) as avg_actual_mw,
  ROUND(((AVG(r2.total_germany_mw) - AVG(r1.total_germany_mw)) /
         NULLIF(AVG(r1.total_germany_mw), 0) * 100)::numeric, 2) as forecast_error_pct
FROM ntp.renewable_energy_timeseries r1
JOIN ntp.renewable_energy_timeseries r2
  ON DATE_TRUNC('hour', r1.timestamp_utc) = DATE_TRUNC('hour', r2.timestamp_utc)
WHERE r1.product_type = 'solar'
  AND r1.data_category = 'forecast'
  AND r2.product_type = 'solar'
  AND r2.data_category = 'extrapolation'
  AND r1.timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND r1.timestamp_utc < CURRENT_DATE
GROUP BY DATE(r1.timestamp_utc)
ORDER BY date;
-- Expected: Forecast accuracy metrics, small error percentage indicates good predictions

\echo ''
\echo '=== Test 14: Peak Production Hours Analysis ==='
-- Expected: 24 rows (one per hour of day)
-- Expected: Solar peaks around 11:00-13:00, wind more distributed
SELECT
  EXTRACT(HOUR FROM timestamp_utc) as hour_of_day,
  ROUND(AVG(CASE WHEN product_type = 'solar' THEN total_germany_mw END)::numeric, 2) as avg_solar_mw,
  ROUND(AVG(CASE WHEN product_type = 'wind_onshore' THEN total_germany_mw END)::numeric, 2) as avg_wind_mw
FROM ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
  AND timestamp_utc < CURRENT_DATE
GROUP BY EXTRACT(HOUR FROM timestamp_utc)
ORDER BY hour_of_day;
-- Expected: Clear solar diurnal pattern (0 at night, peak midday), wind more variable

\timing off

\echo ''
\echo '=== Test Suite Complete ==='
\echo 'Total Tests: 14 queries across 4 foreign tables'
\echo 'Coverage: Renewable energy, prices, redispatch, grid status'
\echo 'Review results above for any failures or unexpected values'
\echo ''
\echo 'Notes:'
\echo '- Dates use CURRENT_DATE for flexibility (no hardcoded 2024 dates)'
\echo '- Some queries may return 0 rows if data not available for date range'
\echo '- JOIN queries validate re_scan() implementation'
\echo '- Replace YOUR_CLIENT_ID and YOUR_CLIENT_SECRET before running'
\echo '- Foreign tables do NOT support indexes (queries use API directly)'
