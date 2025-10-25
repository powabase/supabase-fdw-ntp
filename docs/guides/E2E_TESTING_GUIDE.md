# End-to-End Testing Guide - NTP FDW v0.2.0

**Date**: 2025-10-25
**Status**: Ready for E2E Testing
**WASM Binary**: 301 KB (validated)
**SHA256**: See [GitHub Releases](https://github.com/powabase/supabase-fdw-ntp/releases/latest)

---

## Overview

This guide provides step-by-step instructions for end-to-end testing of the NTP FDW with a local Supabase instance. This validates all Phase 1 security fixes and re_scan() JOIN functionality.

**Estimated Time**: 1-2 hours (including setup)

---

## Prerequisites

### 1. Install Supabase CLI

**macOS:**
```bash
brew install supabase/tap/supabase
```

**Other platforms:**
See https://supabase.com/docs/guides/cli

**Verify installation:**
```bash
supabase --version
```

### 2. Docker Desktop

Docker Desktop must be running (required for Supabase local development).

**Download**: https://www.docker.com/products/docker-desktop

**Verify:**
```bash
docker --version
docker ps  # Should list running containers
```

### 3. OAuth2 Credentials (REQUIRED)

**Contact**: NTP API Provider (Netztransparenz.de)
**Request**: OAuth2 client credentials for API access

**Required Information:**
- `client_id` (OAuth2 Client ID)
- `client_secret` (OAuth2 Client Secret)
- `token_url` (typically: `https://identity.netztransparenz.de/users/connect/token`)
- `scope` (typically: `ntpStatistic.read_all_public`)

**Note**: Without valid credentials, you cannot fetch data from the NTP API. The FDW will fail with authentication errors.

---

## Setup Local Supabase Environment

### Step 1: Initialize Supabase Project

```bash
cd <project-root>  # Navigate to your supabase-fdw-ntp directory

# Initialize Supabase (if not already initialized)
supabase init

# Start Supabase services (PostgreSQL, Storage, etc.)
supabase start
```

**Output**: Note the database credentials:
```
API URL: http://localhost:54321
DB URL: postgresql://postgres:postgres@localhost:54322/postgres
Studio URL: http://localhost:54323
```

**Common Issues:**
- Docker not running → Start Docker Desktop
- Port 54322 in use → Stop other PostgreSQL instances
- Timeout → Increase Docker memory (Settings → Resources)

---

### Step 2: Verify WASM Binary

```bash
# Binary should already be built from Phase 2
ls -lh target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm
```

**Expected Output:**
```
-rw------- 1 user staff 282K Oct 25 20:24 target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm
```

**If binary doesn't exist**, rebuild:
```bash
cargo component build --release --target wasm32-unknown-unknown
```

**Validation:**
```bash
# Validate WASM structure
wasm-tools validate target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm

# Verify zero WASI CLI imports (CRITICAL!)
wasm-tools component wit target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm | grep -i "wasi:cli"
# Expected: No output (zero matches)
```

---

### Step 3: Load WASM FDW into Supabase

#### 3.1 Connect to Local PostgreSQL

```bash
psql postgresql://postgres:postgres@localhost:54322/postgres
```

#### 3.2 Load Supabase Wrappers Extension

```sql
-- Load Supabase Wrappers extension
CREATE EXTENSION IF NOT EXISTS wrappers;
```

**Expected Output:**
```
CREATE EXTENSION
```

#### 3.3 Create WASM FDW

**Note**: Supabase Wrappers loads WASM files from a specific location. You need to copy the WASM binary to the Supabase data directory or use the Supabase CLI to deploy it.

**Option A: Use Supabase CLI (Recommended)**
```bash
# Exit psql (Ctrl+D)

# Deploy WASM wrapper (this uploads the binary to Supabase)
supabase db push
```

**Option B: Manual Load (Development)**
```sql
-- This depends on Supabase Wrappers configuration
-- Check Supabase documentation for latest instructions
-- https://github.com/supabase/wrappers
```

---

### Step 4: Create Foreign Server & Tables

#### 4.1 Create Foreign Server

**⚠️ IMPORTANT**: Replace placeholders with your actual OAuth2 credentials.

```sql
CREATE SERVER ntp_server
  FOREIGN DATA WRAPPER wasm_fdw_handler
  OPTIONS (
    wasm_file 'supabase_fdw_ntp.wasm',
    api_base_url 'https://ds.netztransparenz.de',
    oauth2_token_url 'https://identity.netztransparenz.de/users/connect/token',
    oauth2_client_id 'YOUR_CLIENT_ID_HERE',
    oauth2_client_secret 'YOUR_CLIENT_SECRET_HERE',
    oauth2_scope 'ntpStatistic.read_all_public'
  );
```

**Expected Output:**
```
CREATE SERVER
```

**Troubleshooting:**
```sql
-- Verify server created
SELECT srvname, srvoptions
FROM pg_foreign_server
WHERE srvname = 'ntp_server';
```

#### 4.2 Create Schema for Foreign Tables

```sql
CREATE SCHEMA IF NOT EXISTS ntp;
```

#### 4.3 Create Foreign Tables

**Option A: Load from SQL file**
```bash
# Exit psql (Ctrl+D)
psql postgresql://postgres:postgres@localhost:54322/postgres \
  -f schema.sql
```

**Option B: Manual creation**
```sql
-- Renewable Energy Time Series
CREATE FOREIGN TABLE ntp.renewable_energy_timeseries (
    timestamp_utc TIMESTAMPTZ NOT NULL,
    interval_end_utc TIMESTAMPTZ NOT NULL,
    interval_minutes INT2 NOT NULL,
    product_type TEXT NOT NULL,
    data_category TEXT NOT NULL,
    tso_50hertz_mw NUMERIC,
    tso_amprion_mw NUMERIC,
    tso_tennet_mw NUMERIC,
    tso_transnetbw_mw NUMERIC,
    source_endpoint TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    total_germany_mw NUMERIC GENERATED ALWAYS AS (
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
    ) STORED
) SERVER ntp_server;

-- Electricity Market Prices
CREATE FOREIGN TABLE ntp.electricity_market_prices (
    timestamp_utc TIMESTAMPTZ NOT NULL,
    interval_end_utc TIMESTAMPTZ NOT NULL,
    granularity TEXT NOT NULL,
    price_type TEXT NOT NULL,
    price_eur_mwh NUMERIC,
    product_category TEXT,
    negative_logic_hours TEXT,
    negative_flag_value BOOLEAN,
    source_endpoint TEXT NOT NULL,
    fetched_at TIMESTAMPTZ DEFAULT NOW(),
    price_ct_kwh NUMERIC GENERATED ALWAYS AS (price_eur_mwh / 10.0) STORED,
    is_negative BOOLEAN GENERATED ALWAYS AS (price_eur_mwh < 0) STORED
) SERVER ntp_server;
```

**Verify tables created:**
```sql
\dt ntp.*
```

**Expected Output:**
```
                    List of relations
 Schema |             Name              | Type  |  Owner
--------+-------------------------------+-------+----------
 ntp    | electricity_market_prices     | foreign table | postgres
 ntp    | renewable_energy_timeseries   | foreign table | postgres
(2 rows)
```

---

## Validation Test Queries

Connect to the database and run these validation queries:

```bash
psql postgresql://postgres:postgres@localhost:54322/postgres
```

### Test 1: Basic Query - Renewable Energy

**Purpose**: Validate OAuth2 authentication, CSV parsing, and data retrieval.

```sql
\timing on

SELECT
    product_type,
    data_category,
    COUNT(*) as row_count,
    ROUND(AVG(total_germany_mw)::numeric, 2) as avg_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
    AND data_category = 'forecast'
    AND timestamp_utc >= '2024-10-24'
    AND timestamp_utc < '2024-10-25'
GROUP BY product_type, data_category;
```

**Expected Result:**
```
 product_type | data_category | row_count |  avg_mw
--------------+---------------+-----------+----------
 solar        | forecast      |        96 | 2500.00
(1 row)

Time: 1200.456 ms (00:01.200)
```

**Validation Checklist:**
- ✅ 1 row returned (solar forecast data)
- ✅ row_count = 96 (4 per hour × 24 hours)
- ✅ avg_mw reasonable (1000-4000 MW)
- ✅ Query completes in <5 seconds
- ❌ Error → Check OAuth2 credentials or API connectivity

**What This Tests:**
- OAuth2 token fetching ✅
- API endpoint routing ✅
- CSV parsing ✅
- German decimal conversion (`,` → `.`) ✅
- Timestamp parsing (ISO 8601) ✅

---

### Test 2: Price Query with Negative Prices

**Purpose**: Validate price parsing and negative price handling.

```sql
SELECT
    timestamp_utc,
    price_eur_mwh,
    price_ct_kwh,
    is_negative
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
    AND timestamp_utc >= '2024-10-24'
    AND timestamp_utc < '2024-10-25'
ORDER BY price_eur_mwh ASC
LIMIT 10;
```

**Expected Result:**
```
     timestamp_utc      | price_eur_mwh | price_ct_kwh | is_negative
------------------------+---------------+--------------+-------------
 2024-10-24 13:00:00+00 |         -5.50 |        -0.55 | t
 2024-10-24 14:00:00+00 |         -2.30 |        -0.23 | t
 2024-10-24 12:00:00+00 |         10.25 |         1.02 | f
 ...
(10 rows)

Time: 800.123 ms
```

**Validation Checklist:**
- ✅ 10 rows returned
- ✅ Negative prices handled correctly (is_negative = true)
- ✅ price_ct_kwh calculated correctly (EUR/MWh ÷ 10)
- ✅ Timestamps in chronological order
- ❌ Error → Check price endpoint mapping

**What This Tests:**
- Price parsing ✅
- Negative price handling (C-4 fix) ✅
- Generated columns (price_ct_kwh, is_negative) ✅

---

### Test 3: JOIN Query (re_scan() Validation) 🔴 **CRITICAL**

**Purpose**: Validate re_scan() position reset for JOIN operations.

```sql
-- Create temporary outer table
CREATE TEMP TABLE date_filter (test_date DATE PRIMARY KEY);
INSERT INTO date_filter VALUES ('2024-10-24'), ('2024-10-25');

-- JOIN renewable energy with date filter
SELECT
    df.test_date,
    COUNT(*) as renewable_row_count,
    ROUND(AVG(r.total_germany_mw)::numeric, 2) as avg_solar_mw
FROM date_filter df
JOIN ntp.renewable_energy_timeseries r
    ON DATE(r.timestamp_utc) = df.test_date
WHERE r.product_type = 'solar'
    AND r.data_category = 'forecast'
    AND r.timestamp_utc >= '2024-10-24'
    AND r.timestamp_utc < '2024-10-26'
GROUP BY df.test_date
ORDER BY df.test_date;
```

**Expected Result:**
```
 test_date  | renewable_row_count | avg_solar_mw
------------+---------------------+--------------
 2024-10-24 |                  96 |      2500.00
 2024-10-25 |                  96 |      2600.00
(2 rows)

Time: 2400.789 ms (00:02.401)
```

**Validation Checklist:**
- ✅ 2 rows returned (one per date) → **re_scan() working**
- ✅ Each date has 96 rows → Position tracking correct
- ✅ avg_solar_mw reasonable (1000-4000 MW)
- ❌ Only 1 row → **re_scan() NOT resetting** (critical bug!)
- ❌ Zero rows → JOIN condition broken
- ❌ Wrong row counts → Position tracking broken

**What This Tests:**
- **re_scan() position reset** (CRITICAL!) ✅
- JOIN functionality ✅
- Position tracking (C-1 fix) ✅

**If this test fails, re_scan() is broken!** Check src/lib.rs:847-855.

---

### Test 4: Two-FDW JOIN (Complex JOIN)

**Purpose**: Validate JOIN between two foreign tables.

```sql
SELECT
    DATE(r.timestamp_utc) as date,
    ROUND(AVG(r.total_germany_mw)::numeric, 2) as avg_solar_mw,
    ROUND(AVG(p.price_eur_mwh)::numeric, 2) as avg_price_eur_mwh,
    COUNT(DISTINCT DATE_TRUNC('hour', r.timestamp_utc)) as hours_with_data
FROM ntp.renewable_energy_timeseries r
JOIN ntp.electricity_market_prices p
    ON DATE_TRUNC('hour', r.timestamp_utc) = p.timestamp_utc
WHERE r.product_type = 'solar'
    AND r.data_category = 'forecast'
    AND r.timestamp_utc >= '2024-10-24'
    AND r.timestamp_utc < '2024-10-25'
    AND p.price_type = 'spot_market'
GROUP BY DATE(r.timestamp_utc)
ORDER BY date;
```

**Expected Result:**
```
    date    | avg_solar_mw | avg_price_eur_mwh | hours_with_data
------------+--------------+-------------------+-----------------
 2024-10-24 |      2500.00 |             50.00 |              24
(1 row)

Time: 3200.456 ms (00:03.200)
```

**Validation Checklist:**
- ✅ 1 row returned
- ✅ hours_with_data = 24 (complete day)
- ✅ Data correlation correct (avg values reasonable)
- ❌ Zero rows → JOIN condition not matching

**What This Tests:**
- Two-FDW JOIN ✅
- Timestamp alignment ✅
- Data correlation ✅

---

### Test 5: Multiple Product Types

**Purpose**: Validate multi-product queries and API endpoint multiplexing.

```sql
SELECT
    product_type,
    SUM(total_germany_mw) as total_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type IN ('solar', 'wind_onshore')
    AND data_category = 'extrapolation'
    AND timestamp_utc >= '2024-10-24T00:00:00Z'
    AND timestamp_utc < '2024-10-24T02:00:00Z'
GROUP BY product_type
ORDER BY total_mw DESC;
```

**Expected Result:**
```
 product_type | total_mw
--------------+----------
 wind_onshore |  8000.00
 solar        |   500.00
(2 rows)

Time: 1800.234 ms (00:01.800)
```

**Validation Checklist:**
- ✅ 2 rows (solar + wind_onshore)
- ✅ wind_onshore > solar (nighttime data)
- ❌ Missing rows → Multi-product routing broken

**What This Tests:**
- Multiple API endpoint calls ✅
- Product type mapping ✅
- Data aggregation ✅

---

## Security Fixes Validation

After running all tests, verify the 6 Phase 1 security fixes:

### C-1: Array Indexing (Bounds Checking)
- **Location**: src/lib.rs:797-808
- **Test**: Run JOIN queries (Test 3)
- **Expected**: No panics, graceful termination at end of data
- **Status**: ✅ Pass / ❌ Fail

### C-2: Mutex Poisoning (Error Recovery)
- **Location**: src/oauth2.rs:152, 268, 301
- **Test**: OAuth2 continues working even if errors occur
- **Expected**: Token refresh works after cache errors
- **Status**: ✅ Pass / ❌ Fail

### C-3: Timestamp Epoch Fallback (Explicit Error)
- **Location**: src/lib.rs:246-258
- **Test**: Invalid dates return clear errors (not 1970-01-01)
- **Expected**: Error message with invalid timestamp
- **Status**: ✅ Pass / ❌ Fail

### C-4: Case-Sensitive NULL (Normalization)
- **Location**: src/transformations.rs:128-137
- **Test**: Both "N.A." and "N.E." parsed correctly
- **Expected**: NULL values handled for both variants
- **Status**: ✅ Pass / ❌ Fail

### C-6: Integer Overflow (Safe Conversion)
- **Location**: src/transformations.rs:256-275
- **Test**: No data corruption in interval_minutes column
- **Expected**: Error for intervals >32,767 minutes
- **Status**: ✅ Pass / ❌ Fail

### C-8: Silent Parse Failure (Explicit Error)
- **Location**: src/lib.rs:477-490
- **Test**: Malformed timestamps fail with clear errors
- **Expected**: Error message with ISO 8601 format hint
- **Status**: ✅ Pass / ❌ Fail

---

## Troubleshooting

### Issue: "Foreign table not found"

**Solution**: Verify schema and table creation
```sql
\dt ntp.*
-- Should show: renewable_energy_timeseries, electricity_market_prices
```

### Issue: "OAuth2 authentication failed"

**Solution**: Verify credentials
```sql
-- Check server options
SELECT srvname, srvoptions
FROM pg_foreign_server
WHERE srvname = 'ntp_server';
```

**Test credentials manually**:
```bash
curl -X POST https://identity.netztransparenz.de/users/connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=YOUR_ID&client_secret=YOUR_SECRET&scope=ntpStatistic.read_all_public"
```

**Expected**: JSON response with `access_token` field.

### Issue: "WASM import errors"

**Cause**: Built with wrong target (wasm32-wasip1)

**Solution**: Rebuild with correct target
```bash
cargo component build --release --target wasm32-unknown-unknown
# NOT: wasm32-wasip1 (includes WASI CLI which Supabase doesn't support)
```

**Verify**:
```bash
wasm-tools component wit target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm | grep -i "wasi:cli"
# Expected: No output
```

### Issue: "JOIN returns zero rows"

**This is likely re_scan() not working!**

**Debug Steps**:

1. Check both tables individually:
```sql
SELECT COUNT(*) FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
    AND timestamp_utc >= '2024-10-24'
    AND timestamp_utc < '2024-10-25';

SELECT COUNT(*) FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
    AND timestamp_utc >= '2024-10-24'
    AND timestamp_utc < '2024-10-25';
```

2. If both return data but JOIN returns zero:
   - ❌ **re_scan() is not resetting positions** → Critical bug!
   - Check PostgreSQL logs for panic/error messages
   - Verify WASM binary is the latest build
   - Run unit tests: `cargo test --lib test_re_scan`

### Issue: Query timeout

**Cause**: Large date range or slow API

**Solution**:
- Reduce date range (e.g., 1 day instead of 7 days)
- Check PostgreSQL logs for API errors
- Verify network connectivity to NTP API

---

## Viewing Logs

### PostgreSQL Logs (Supabase)

```bash
# Find Supabase container ID
docker ps | grep supabase

# Tail PostgreSQL logs
docker logs -f <container_id>
```

**Look for**:
- OAuth2 token fetches
- API HTTP requests
- Error messages from FDW
- Panic stack traces (should be none!)

---

## Success Criteria

After completing all tests, verify:

### Functional Validation
- ✅ Basic SELECT queries return data
- ✅ JOIN queries work correctly (re_scan() functioning)
- ✅ Negative prices handled correctly
- ✅ Generated columns calculated correctly
- ✅ OAuth2 token caching works
- ✅ No PostgreSQL backend crashes

### Security Fixes Validation
- ✅ C-1: No panics during JOIN queries
- ✅ C-2: OAuth2 continues working after errors
- ✅ C-3: Invalid dates return clear errors
- ✅ C-4: Both "N.A." and "N.E." parsed correctly
- ✅ C-6: No data corruption in interval_minutes
- ✅ C-8: Malformed timestamps fail with clear errors

### Performance Validation
- ✅ Queries complete in <5 seconds (1-day range)
- ✅ No excessive API calls
- ✅ Memory usage stable

---

## Next Steps

### If All Tests Pass ✅

**Phase 2 is complete!** The NTP FDW is ready for production use.

1. Document test results
2. Update HANDOVER.md with completion status
3. Create release tag: `v0.2.0`
4. Deploy to production Supabase instance

### If Any Test Fails ❌

1. Check PostgreSQL logs for error messages
2. Run unit tests: `cargo test --lib`
3. Verify WASM binary is latest build
4. Debug specific failure:
   - JOIN failures → Check re_scan() implementation (src/lib.rs:847-855)
   - Auth failures → Verify OAuth2 credentials
   - Parse failures → Check CSV parser (src/csv_parser.rs)

---

## Additional Resources

- **HANDOVER.md**: Complete security fixes documentation
- **schema.sql**: Table DDL with example queries
- **test_fdw.sql**: SQL integration tests and examples
- **Supabase Wrappers**: https://github.com/supabase/wrappers
- **WASM FDW Guide**: https://fdw.dev/guides/create-wasm-wrapper/

---

**End of E2E Testing Guide**
