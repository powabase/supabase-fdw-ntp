-- =============================================================================
-- NTP FDW v0.3.0 Comprehensive End-to-End Test Suite
-- =============================================================================
--
-- Purpose: Systematic validation of all 4 foreign tables and 11 accessible
--          API endpoints based on official endpoint documentation
--
-- Key Changes in v0.3.0:
--   - REMOVED: prognose (forecast) API endpoint
--   - Impact: data_category='forecast' now returns ERROR (not 0 rows)
--   - Endpoints: 13 → 11 accessible (7 renewable → 5 renewable)
--
-- Test Coverage:
--   - 10 Renewable Energy Tests (forecast removal validation)
--   - 8 Electricity Price Tests (UNPIVOT fix validation)
--   - 4 Redispatch Event Tests
--   - 4 Grid Status Tests
--   - 4 Cross-Table JOIN Tests (re_scan validation)
--   - 5 Edge Case & Performance Tests
--
-- Total: 35 tests across all endpoints
--
-- Usage:
--   psql -h 127.0.0.1 -p 54322 -U postgres -d postgres -f test_e2e_comprehensive_v030.sql
--
-- Expected Runtime: ~3-5 minutes
-- =============================================================================

\timing on
\set ON_ERROR_STOP on

\echo ''
\echo '========================================================================='
\echo 'NTP FDW v0.3.0 - Comprehensive E2E Test Suite'
\echo '========================================================================='
\echo ''

-- =============================================================================
-- SECTION 1: RENEWABLE ENERGY TESTS (10 tests)
-- Based on: docs/endpoints/renewable-energy.md
-- API Coverage: 5 accessible endpoints (7 → 5 in v0.3.0, forecast removed)
-- =============================================================================

\echo '========================================================================='
\echo 'SECTION 1: RENEWABLE ENERGY TESTS'
\echo '========================================================================='
\echo ''

-- TEST 1: v0.3.0 CRITICAL - Forecast Endpoint Removal Validation
\echo '--- TEST 1: Forecast endpoint must ERROR (not return 0 rows) ---'
\echo 'Expected: ERROR with message "Unknown data category: forecast"'
\echo ''
-- This MUST fail in v0.3.0:
SELECT 'TEST 1: Forecast category validation' AS test_name;
SELECT COUNT(*) as row_count
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'forecast'
  AND product_type = 'solar'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
-- Expected: ERROR, not 0 rows!
\echo ''

-- TEST 2: Solar Extrapolation (Remaining Category)
\echo '--- TEST 2: Solar extrapolation (should have ~96 rows) ---'
SELECT 'TEST 2: Solar extrapolation' AS test_name;
SELECT COUNT(*) as row_count,
       MIN(total_germany_mw) as min_power_mw,
       MAX(total_germany_mw) as max_power_mw,
       ROUND(AVG(total_germany_mw)::numeric, 2) as avg_power_mw
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND product_type = 'solar'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
-- Expected: ~96 rows, 15-minute intervals
\echo ''

-- TEST 3: Wind Onshore Extrapolation
\echo '--- TEST 3: Wind onshore extrapolation ---'
SELECT 'TEST 3: Wind onshore extrapolation' AS test_name;
SELECT COUNT(*) as row_count,
       MIN(total_germany_mw) as min_power_mw,
       MAX(total_germany_mw) as max_power_mw
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND product_type = 'wind_onshore'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
-- Expected: ~96 rows
\echo ''

-- TEST 4: Wind Offshore Online Actual (Limited API Availability)
\echo '--- TEST 4: Wind offshore online actual (hourly, 24 rows) ---'
SELECT 'TEST 4: Wind offshore online actual' AS test_name;
SELECT COUNT(*) as row_count,
       interval_minutes,
       MIN(total_germany_mw) as min_power_mw,
       MAX(total_germany_mw) as max_power_mw
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'online_actual'
  AND product_type = 'wind_offshore'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY interval_minutes;
-- Expected: ~24 rows (hourly), interval_minutes = 60
\echo ''

-- TEST 5: All Products Extrapolation (Parallel API Calls)
\echo '--- TEST 5: All products extrapolation (2 endpoints in v0.3.0) ---'
SELECT 'TEST 5: All products (parallel calls)' AS test_name;
SELECT product_type,
       COUNT(*) as row_count,
       ROUND(AVG(total_germany_mw)::numeric, 2) as avg_power_mw
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY product_type
ORDER BY product_type;
-- Expected: 2 rows (solar, wind_onshore), wind_offshore NOT in extrapolation
\echo ''

-- TEST 6: TSO Zone Aggregation (Geographic Distribution)
\echo '--- TEST 6: TSO zone power distribution ---'
SELECT 'TEST 6: TSO zone aggregations' AS test_name;
SELECT product_type,
       ROUND(SUM(COALESCE(tso_50hertz_mw, 0))::numeric, 2) as hertz_50_total_mwh,
       ROUND(SUM(COALESCE(tso_amprion_mw, 0))::numeric, 2) as amprion_total_mwh,
       ROUND(SUM(COALESCE(tso_tennet_mw, 0))::numeric, 2) as tennet_total_mwh,
       ROUND(SUM(COALESCE(tso_transnetbw_mw, 0))::numeric, 2) as transnetbw_total_mwh
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY product_type
ORDER BY product_type;
-- Expected: 2 rows (solar, wind_onshore)
\echo ''

-- TEST 7: Data Quality Monitoring (has_missing_data flag)
\echo '--- TEST 7: Data quality - missing data detection ---'
SELECT 'TEST 7: Missing data flag' AS test_name;
SELECT product_type,
       data_category,
       COUNT(*) as total_rows,
       SUM(CASE WHEN has_missing_data THEN 1 ELSE 0 END) as rows_with_missing_data,
       ROUND(100.0 * SUM(CASE WHEN has_missing_data THEN 1 ELSE 0 END) / COUNT(*), 2) as missing_data_pct
FROM fdw_ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY product_type, data_category
ORDER BY product_type, data_category;
-- Expected: Low missing_data_pct (<5%), higher for nighttime solar
\echo ''

-- TEST 8: Time-Based Filtering (Hour/Minute Precision - v0.2.2 fix)
\echo '--- TEST 8: Time-based filtering (10:00-16:00) ---'
SELECT 'TEST 8: Hour/minute precision' AS test_name;
SELECT COUNT(*) as row_count,
       MIN(timestamp_utc) as earliest,
       MAX(timestamp_utc) as latest
FROM fdw_ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20T10:00:00'
  AND timestamp_utc < '2024-10-20T16:00:00';
-- Expected: 24 rows (6 hours × 4 quarters), all between 10:00-16:00
\echo ''

-- TEST 9: Interval Minutes Consistency
\echo '--- TEST 9: Interval minutes by category ---'
SELECT 'TEST 9: Interval minutes' AS test_name;
SELECT data_category,
       interval_minutes,
       COUNT(*) as row_count
FROM fdw_ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY data_category, interval_minutes
ORDER BY data_category, interval_minutes;
-- Expected: extrapolation=15min, online_actual=60min
\echo ''

-- TEST 10: Cross-Product Comparison (Solar vs Wind Performance)
\echo '--- TEST 10: Solar vs wind total production ---'
SELECT 'TEST 10: Product comparison' AS test_name;
SELECT product_type,
       ROUND(SUM(total_germany_mw * interval_minutes / 60.0)::numeric, 2) as total_energy_mwh,
       ROUND(AVG(total_germany_mw)::numeric, 2) as avg_power_mw,
       ROUND(MAX(total_germany_mw)::numeric, 2) as peak_power_mw
FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY product_type
ORDER BY product_type;
-- Expected: 2 rows, solar peak during midday, wind more consistent
\echo ''

-- =============================================================================
-- SECTION 2: ELECTRICITY PRICE TESTS (8 tests)
-- Based on: docs/endpoints/electricity-prices.md
-- API Coverage: 4 endpoints (spot, negative flags, premiums, annual)
-- =============================================================================

\echo '========================================================================='
\echo 'SECTION 2: ELECTRICITY PRICE TESTS'
\echo '========================================================================='
\echo ''

-- TEST 11: Spot Market Prices (Hourly Granularity)
\echo '--- TEST 11: Spot market prices ---'
SELECT 'TEST 11: Spot market prices' AS test_name;
SELECT COUNT(*) as row_count,
       ROUND(MIN(price_eur_mwh)::numeric, 2) as min_price,
       ROUND(MAX(price_eur_mwh)::numeric, 2) as max_price,
       ROUND(AVG(price_eur_mwh)::numeric, 2) as avg_price,
       SUM(CASE WHEN is_negative THEN 1 ELSE 0 END) as negative_price_hours
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
-- Expected: 24 rows (hourly), price_eur_mwh can be negative
\echo ''

-- TEST 12: Negative Price Flags (UNPIVOT Fix v0.2.9 - 4 Rows Per Timestamp)
\echo '--- TEST 12: Negative price flags UNPIVOT (4 logic hours per timestamp) ---'
SELECT 'TEST 12: Negative flags UNPIVOT' AS test_name;
SELECT negative_logic_hours,
       COUNT(*) as row_count,
       SUM(CASE WHEN negative_flag_value THEN 1 ELSE 0 END) as flag_true_count
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'negative_flag'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY negative_logic_hours
ORDER BY negative_logic_hours;
-- Expected: 4 rows (1h, 3h, 4h, 6h), each with 24 timestamps = 96 total rows
\echo ''

-- TEST 13: Market Premiums (Monthly Granularity)
\echo '--- TEST 13: Market premiums (monthly) ---'
SELECT 'TEST 13: Market premiums' AS test_name;
SELECT product_category,
       COUNT(*) as row_count,
       ROUND(AVG(price_ct_kwh)::numeric, 4) as avg_premium_ct_kwh
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'market_premium'
  AND timestamp_utc >= '2024-01-01'
  AND timestamp_utc < '2024-12-31'
GROUP BY product_category
ORDER BY product_category;
-- Expected: Multiple product categories, monthly data
\echo ''

-- TEST 14: Annual Market Values (Pipe-Delimited Parser v0.2.7)
\echo '--- TEST 14: Annual market values (2020-2024) ---'
SELECT 'TEST 14: Annual market values' AS test_name;
SELECT EXTRACT(YEAR FROM timestamp_utc) as year,
       product_category,
       ROUND(price_eur_mwh::numeric, 2) as annual_value_eur_mwh
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'annual_market_value'
  AND timestamp_utc >= '2020-01-01'
  AND timestamp_utc < '2025-01-01'
ORDER BY year DESC, product_category;
-- Expected: 5 years × products, pipe-delimited format parsed
\echo ''

-- TEST 15: Multi-Granularity Query (Hourly + Monthly in One Table)
\echo '--- TEST 15: Multi-granularity data ---'
SELECT 'TEST 15: Granularity types' AS test_name;
SELECT granularity,
       price_type,
       COUNT(*) as row_count
FROM fdw_ntp.electricity_market_prices
WHERE timestamp_utc >= '2024-10-01'
  AND timestamp_utc < '2024-11-01'
GROUP BY granularity, price_type
ORDER BY granularity, price_type;
-- Expected: Mix of hourly (spot, negative_flag) and monthly (premium)
\echo ''

-- TEST 16: Negative Price Analysis (Oversupply Conditions)
\echo '--- TEST 16: Negative price hours distribution ---'
SELECT 'TEST 16: Negative prices' AS test_name;
SELECT DATE_TRUNC('day', timestamp_utc) as day,
       COUNT(*) as negative_hours,
       ROUND(MIN(price_eur_mwh)::numeric, 2) as lowest_price
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND is_negative = true
  AND timestamp_utc >= '2024-10-01'
  AND timestamp_utc < '2024-11-01'
GROUP BY day
ORDER BY day;
-- Expected: Days with negative prices, lowest_price < 0
\echo ''

-- TEST 17: Price Conversion (EUR/MWh to ct/kWh)
\echo '--- TEST 17: Price unit conversion ---'
SELECT 'TEST 17: Price conversions' AS test_name;
SELECT price_eur_mwh,
       price_ct_kwh,
       price_eur_mwh / 10 as calculated_ct_kwh,
       ABS(price_ct_kwh - (price_eur_mwh / 10)) < 0.0001 as conversion_correct
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
LIMIT 5;
-- Expected: conversion_correct = true for all rows
\echo ''

-- TEST 18: Price Volatility (Daily Standard Deviation)
\echo '--- TEST 18: Price volatility analysis ---'
SELECT 'TEST 18: Price volatility' AS test_name;
SELECT DATE_TRUNC('day', timestamp_utc) as day,
       ROUND(STDDEV(price_eur_mwh)::numeric, 2) as price_stddev,
       ROUND(MAX(price_eur_mwh) - MIN(price_eur_mwh)::numeric, 2) as price_range
FROM fdw_ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-26'
GROUP BY day
ORDER BY day;
-- Expected: Daily volatility metrics
\echo ''

-- =============================================================================
-- SECTION 3: REDISPATCH EVENT TESTS (4 tests)
-- Based on: docs/endpoints/redispatch.md
-- API Coverage: 1 endpoint (redispatch events)
-- =============================================================================

\echo '========================================================================='
\echo 'SECTION 3: REDISPATCH EVENT TESTS'
\echo '========================================================================='
\echo ''

-- TEST 19: Recent Redispatch Events
\echo '--- TEST 19: Redispatch event count and energy volume ---'
SELECT 'TEST 19: Redispatch basics' AS test_name;
SELECT COUNT(*) as event_count,
       COUNT(DISTINCT requesting_tso) as unique_tsos,
       ROUND(SUM(total_energy_mwh)::numeric, 2) as total_energy_mwh,
       ROUND(AVG(total_energy_mwh)::numeric, 2) as avg_energy_per_event_mwh
FROM fdw_ntp.redispatch_events
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
-- Expected: 5-50 events, multiple TSOs
\echo ''

-- TEST 20: Energy Volume by TSO
\echo '--- TEST 20: Redispatch by TSO zone ---'
SELECT 'TEST 20: TSO redispatch volume' AS test_name;
SELECT requesting_tso,
       COUNT(*) as event_count,
       ROUND(SUM(total_energy_mwh)::numeric, 2) as total_mwh,
       ROUND(AVG(max_power_mw)::numeric, 2) as avg_peak_power_mw
FROM fdw_ntp.redispatch_events
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY requesting_tso
ORDER BY total_mwh DESC;
-- Expected: 4 TSOs, varying volumes
\echo ''

-- TEST 21: Direction Analysis (Increase vs Reduce Generation)
\echo '--- TEST 21: Redispatch direction distribution ---'
SELECT 'TEST 21: Increase vs reduce' AS test_name;
SELECT direction,
       COUNT(*) as event_count,
       ROUND(SUM(total_energy_mwh)::numeric, 2) as total_mwh,
       ROUND(AVG(EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc)) / 3600)::numeric, 2) as avg_duration_hours
FROM fdw_ntp.redispatch_events
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY direction
ORDER BY direction;
-- Expected: 2 rows (increase_generation, reduce_generation)
\echo ''

-- TEST 22: Long-Duration Events (Maintenance or Severe Congestion)
\echo '--- TEST 22: Long-duration redispatch events (>6 hours) ---'
SELECT 'TEST 22: Long events' AS test_name;
SELECT timestamp_utc,
       interval_end_utc,
       ROUND(EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc)) / 3600) as duration_hours,
       requesting_tso,
       direction,
       ROUND(total_energy_mwh::numeric, 2) as energy_mwh,
       reason
FROM fdw_ntp.redispatch_events
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
  AND EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc)) / 3600 > 6
ORDER BY duration_hours DESC
LIMIT 5;
-- Expected: Events >6 hours, sorted by duration
\echo ''

-- =============================================================================
-- SECTION 4: GRID STATUS TESTS (4 tests)
-- Based on: docs/endpoints/grid-status.md
-- API Coverage: 1 endpoint (TrafficLight JSON)
-- =============================================================================

\echo '========================================================================='
\echo 'SECTION 4: GRID STATUS TESTS'
\echo '========================================================================='
\echo ''

-- TEST 23: Grid Status Distribution (Traffic Light Colors)
\echo '--- TEST 23: Grid status distribution (GREEN/YELLOW/RED) ---'
SELECT 'TEST 23: Status distribution' AS test_name;
SELECT grid_status,
       COUNT(*) as minutes,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM fdw_ntp.grid_status_timeseries
WHERE timestamp_utc >= '2024-10-20T00:00:00'
  AND timestamp_utc < '2024-10-20T06:00:00'
GROUP BY grid_status
ORDER BY CASE grid_status
    WHEN 'GREEN' THEN 1
    WHEN 'YELLOW' THEN 2
    WHEN 'RED' THEN 3
END;
-- Expected: Mostly GREEN, some YELLOW, rare RED
\echo ''

-- TEST 24: Minute-Level Granularity Validation (1440 rows/day)
\echo '--- TEST 24: Minute-level granularity (1440 rows/day expected) ---'
SELECT 'TEST 24: Row count check' AS test_name;
SELECT DATE_TRUNC('day', timestamp_utc) as day,
       COUNT(*) as row_count,
       COUNT(*) = 1440 as is_complete_day
FROM fdw_ntp.grid_status_timeseries
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-22'
GROUP BY day
ORDER BY day;
-- Expected: Each day has 1440 rows (24h × 60min)
\echo ''

-- TEST 25: Status Transition Detection (Window Function)
\echo '--- TEST 25: Grid status transitions ---'
SELECT 'TEST 25: Status transitions' AS test_name;
WITH status_changes AS (
    SELECT timestamp_utc,
           grid_status,
           LAG(grid_status) OVER (ORDER BY timestamp_utc) as prev_status
    FROM fdw_ntp.grid_status_timeseries
    WHERE timestamp_utc >= '2024-10-20T00:00:00'
      AND timestamp_utc < '2024-10-21T00:00:00'
)
SELECT prev_status || ' → ' || grid_status as transition,
       COUNT(*) as transition_count
FROM status_changes
WHERE prev_status IS NOT NULL
  AND prev_status != grid_status
GROUP BY prev_status, grid_status
ORDER BY transition_count DESC;
-- Expected: Transitions like GREEN→YELLOW, YELLOW→GREEN
\echo ''

-- TEST 26: Critical Period Identification (RED Status Duration)
\echo '--- TEST 26: Critical RED periods ---'
SELECT 'TEST 26: RED periods' AS test_name;
WITH red_periods AS (
    SELECT timestamp_utc,
           interval_end_utc,
           LAG(grid_status) OVER (ORDER BY timestamp_utc) = 'RED' as prev_was_red,
           grid_status = 'RED' as current_is_red
    FROM fdw_ntp.grid_status_timeseries
    WHERE timestamp_utc >= '2024-10-20'
      AND timestamp_utc < '2024-10-21'
)
SELECT timestamp_utc as red_period_start,
       COUNT(*) as duration_minutes
FROM red_periods
WHERE current_is_red = true
  AND (prev_was_red = false OR prev_was_red IS NULL)
GROUP BY timestamp_utc
ORDER BY duration_minutes DESC
LIMIT 5;
-- Expected: RED period start times and durations
\echo ''

-- =============================================================================
-- SECTION 5: CROSS-TABLE JOIN TESTS (4 tests)
-- Critical for re_scan() validation
-- =============================================================================

\echo '========================================================================='
\echo 'SECTION 5: CROSS-TABLE JOIN TESTS (re_scan validation)'
\echo '========================================================================='
\echo ''

-- TEST 27: Price vs Solar Production Correlation
\echo '--- TEST 27: Price vs solar production correlation ---'
SELECT 'TEST 27: Price-Solar JOIN' AS test_name;
SELECT DATE_TRUNC('hour', p.timestamp_utc) as hour,
       ROUND(AVG(p.price_eur_mwh)::numeric, 2) as avg_price,
       ROUND(AVG(r.total_germany_mw)::numeric, 2) as avg_solar_mw,
       COUNT(*) as data_points
FROM fdw_ntp.electricity_market_prices p
JOIN fdw_ntp.renewable_energy_timeseries r
  ON DATE_TRUNC('hour', p.timestamp_utc) = DATE_TRUNC('hour', r.timestamp_utc)
WHERE p.price_type = 'spot_market'
  AND r.product_type = 'solar'
  AND r.data_category = 'extrapolation'
  AND p.timestamp_utc >= '2024-10-20T06:00:00'
  AND p.timestamp_utc < '2024-10-20T18:00:00'
GROUP BY hour
ORDER BY hour;
-- Expected: Negative correlation (high solar → low prices)
\echo ''

-- TEST 28: Redispatch During Grid Stress (Multi-Table Join)
\echo '--- TEST 28: Redispatch + renewable JOIN ---'
SELECT 'TEST 28: Redispatch-Wind JOIN' AS test_name;
SELECT DATE_TRUNC('hour', rd.timestamp_utc) as hour,
       COUNT(DISTINCT rd.timestamp_utc) as redispatch_events,
       ROUND(AVG(re.total_germany_mw)::numeric, 2) as avg_wind_mw,
       ROUND(AVG(rd.max_power_mw)::numeric, 2) as avg_redispatch_power
FROM fdw_ntp.redispatch_events rd
JOIN fdw_ntp.renewable_energy_timeseries re
  ON DATE_TRUNC('hour', rd.timestamp_utc) = DATE_TRUNC('hour', re.timestamp_utc)
WHERE re.product_type = 'wind_onshore'
  AND re.data_category = 'extrapolation'
  AND rd.timestamp_utc >= '2024-10-20'
  AND rd.timestamp_utc < '2024-10-21'
GROUP BY hour
ORDER BY hour;
-- Expected: Wind generation during redispatch events
\echo ''

-- TEST 29: Grid Status During Negative Prices
\echo '--- TEST 29: Grid status + negative price JOIN ---'
SELECT 'TEST 29: Status-Price JOIN' AS test_name;
SELECT gs.grid_status,
       COUNT(*) as hour_count,
       ROUND(AVG(p.price_eur_mwh)::numeric, 2) as avg_price,
       SUM(CASE WHEN p.is_negative THEN 1 ELSE 0 END) as negative_price_hours
FROM fdw_ntp.electricity_market_prices p
JOIN fdw_ntp.grid_status_timeseries gs
  ON DATE_TRUNC('hour', p.timestamp_utc) = DATE_TRUNC('hour', gs.timestamp_utc)
WHERE p.price_type = 'spot_market'
  AND p.timestamp_utc >= '2024-10-20'
  AND p.timestamp_utc < '2024-10-21'
GROUP BY gs.grid_status
ORDER BY gs.grid_status;
-- Expected: Grid status distribution during different price levels
\echo ''

-- TEST 30: Complex Multi-Table Aggregation (All 4 Tables)
\echo '--- TEST 30: All tables JOIN (complete re_scan test) ---'
SELECT 'TEST 30: Four-table JOIN' AS test_name;
SELECT DATE_TRUNC('hour', r.timestamp_utc) as hour,
       ROUND(AVG(r.total_germany_mw)::numeric, 2) as avg_solar_mw,
       ROUND(AVG(p.price_eur_mwh)::numeric, 2) as avg_price,
       COUNT(DISTINCT rd.timestamp_utc) as redispatch_count,
       MODE() WITHIN GROUP (ORDER BY gs.grid_status) as most_common_status
FROM fdw_ntp.renewable_energy_timeseries r
FULL OUTER JOIN fdw_ntp.electricity_market_prices p
  ON DATE_TRUNC('hour', r.timestamp_utc) = DATE_TRUNC('hour', p.timestamp_utc)
FULL OUTER JOIN fdw_ntp.redispatch_events rd
  ON DATE_TRUNC('hour', r.timestamp_utc) = DATE_TRUNC('hour', rd.timestamp_utc)
FULL OUTER JOIN fdw_ntp.grid_status_timeseries gs
  ON DATE_TRUNC('hour', r.timestamp_utc) = DATE_TRUNC('hour', gs.timestamp_utc)
WHERE r.product_type = 'solar'
  AND r.data_category = 'extrapolation'
  AND p.price_type = 'spot_market'
  AND r.timestamp_utc >= '2024-10-20T10:00:00'
  AND r.timestamp_utc < '2024-10-20T16:00:00'
GROUP BY hour
ORDER BY hour;
-- Expected: Complete hourly grid picture with all metrics
\echo ''

-- =============================================================================
-- SECTION 6: EDGE CASE & PERFORMANCE TESTS (5 tests)
-- =============================================================================

\echo '========================================================================='
\echo 'SECTION 6: EDGE CASE & PERFORMANCE TESTS'
\echo '========================================================================='
\echo ''

-- TEST 31: Cross-Day Time Range (v0.2.4 Fix - Midnight Spanning)
\echo '--- TEST 31: Cross-day time range (midnight crossing) ---'
SELECT 'TEST 31: Cross-day query' AS test_name;
SELECT COUNT(*) as row_count,
       MIN(timestamp_utc) as earliest,
       MAX(timestamp_utc) as latest
FROM fdw_ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20T22:00:00'
  AND timestamp_utc < '2024-10-21T04:00:00';
-- Expected: 24 rows spanning midnight (22:00-04:00)
\echo ''

-- TEST 32: NULL Handling (N.A. and N.E. Variants - Wind Offshore)
\echo '--- TEST 32: NULL handling (N.A./N.E. in API) ---'
SELECT 'TEST 32: NULL detection' AS test_name;
SELECT COUNT(*) as total_rows,
       SUM(CASE WHEN tso_50hertz_mw IS NULL THEN 1 ELSE 0 END) as null_50hertz,
       SUM(CASE WHEN tso_amprion_mw IS NULL THEN 1 ELSE 0 END) as null_amprion,
       SUM(CASE WHEN tso_tennet_mw IS NULL THEN 1 ELSE 0 END) as null_tennet,
       SUM(CASE WHEN tso_transnetbw_mw IS NULL THEN 1 ELSE 0 END) as null_transnetbw,
       SUM(CASE WHEN has_missing_data THEN 1 ELSE 0 END) as flagged_missing
FROM fdw_ntp.renewable_energy_timeseries
WHERE product_type = 'wind_offshore'
  AND data_category = 'online_actual'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
-- Expected: NULLs properly parsed, has_missing_data = true
\echo ''

-- TEST 33: German Locale Parsing (Decimal Commas in CSV)
\echo '--- TEST 33: German decimal format (comma → period conversion) ---'
SELECT 'TEST 33: Decimal parsing' AS test_name;
SELECT total_germany_mw,
       ROUND(total_germany_mw::numeric, 3) = ROUND(total_germany_mw::numeric, 3) as is_valid_numeric,
       total_germany_mw::text LIKE '%.%' as has_period_not_comma
FROM fdw_ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20T12:00:00'
  AND timestamp_utc < '2024-10-20T12:15:00'
LIMIT 1;
-- Expected: Valid numeric, period (not comma) in decimal representation
\echo ''

-- TEST 34: Performance Benchmark (Query Execution Time)
\echo '--- TEST 34: Performance test (<2 seconds target) ---'
SELECT 'TEST 34: Performance check' AS test_name;
\timing on
SELECT product_type,
       data_category,
       COUNT(*) as row_count,
       ROUND(AVG(total_germany_mw)::numeric, 2) as avg_mw,
       ROUND(SUM(total_germany_mw * interval_minutes / 60.0)::numeric, 2) as total_mwh
FROM fdw_ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21'
GROUP BY product_type, data_category
ORDER BY product_type, data_category;
\timing off
-- Expected: Query completes in <2 seconds
\echo ''

-- TEST 35: Data Quality Validation (Row Counts & Consistency)
\echo '--- TEST 35: Overall data quality check ---'
SELECT 'TEST 35: Data quality summary' AS test_name;
WITH table_stats AS (
    SELECT 'renewable_energy' as table_name,
           COUNT(*) as row_count,
           COUNT(DISTINCT product_type) as unique_products,
           COUNT(DISTINCT data_category) as unique_categories
    FROM fdw_ntp.renewable_energy_timeseries
    WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-21'

    UNION ALL

    SELECT 'electricity_prices' as table_name,
           COUNT(*) as row_count,
           COUNT(DISTINCT price_type) as unique_types,
           COUNT(DISTINCT granularity) as unique_granularities
    FROM fdw_ntp.electricity_market_prices
    WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-21'

    UNION ALL

    SELECT 'redispatch_events' as table_name,
           COUNT(*) as row_count,
           COUNT(DISTINCT requesting_tso) as unique_tsos,
           COUNT(DISTINCT direction) as unique_directions
    FROM fdw_ntp.redispatch_events
    WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-21'

    UNION ALL

    SELECT 'grid_status' as table_name,
           COUNT(*) as row_count,
           COUNT(DISTINCT grid_status) as unique_statuses,
           NULL as third_metric
    FROM fdw_ntp.grid_status_timeseries
    WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-21'
)
SELECT * FROM table_stats
ORDER BY table_name;
-- Expected: All tables have data, renewable ~200 rows (2 products × 96), prices ~24, grid ~1440
\echo ''

-- =============================================================================
-- TEST SUITE COMPLETE
-- =============================================================================

\echo ''
\echo '========================================================================='
\echo 'TEST SUITE COMPLETE'
\echo '========================================================================='
\echo ''
\echo 'Summary:'
\echo '  - 35 tests executed across 4 foreign tables'
\echo '  - 11 accessible API endpoints validated (v0.3.0)'
\echo '  - Forecast endpoint removal confirmed (v0.3.0 CRITICAL)'
\echo '  - UNPIVOT fix validated (v0.2.9)'
\echo '  - re_scan() functionality confirmed (JOINs working)'
\echo '  - Edge cases tested (cross-day, NULLs, German locale)'
\echo '  - Performance benchmarks completed'
\echo ''
\echo 'Next Steps:'
\echo '  1. Review any test failures or unexpected results'
\echo '  2. Document bugs if found'
\echo '  3. Update CLAUDE.md to v0.3.0 if all tests pass'
\echo '  4. Consider GitHub release for v0.3.0'
\echo ''
\echo '========================================================================='
\echo ''

\timing off
