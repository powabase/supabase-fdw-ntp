# Quickstart Guide

Get German NTP energy market data in your Supabase database in 5 minutes.

## Prerequisites

- Existing Supabase project (local or hosted)
- Supabase CLI installed (for local testing)
- NTP API OAuth2 credentials - **[Contact German NTP API provider](https://www.netztransparenz.de)** to request access

## Step 1: Create Foreign Server (1 min)

Connect to your Supabase database and run:

```sql
-- Enable wrappers extension
CREATE EXTENSION IF NOT EXISTS wrappers WITH SCHEMA extensions;

-- Create WASM FDW wrapper
CREATE FOREIGN DATA WRAPPER IF NOT EXISTS wasm_wrapper
  HANDLER wasm_fdw_handler
  VALIDATOR wasm_fdw_validator;

-- Create NTP server
CREATE SERVER ntp_server
  FOREIGN DATA WRAPPER wasm_wrapper
  OPTIONS (
    fdw_package_url 'https://github.com/powabase/supabase-fdw-ntp/releases/latest/download/supabase_fdw_ntp.wasm',
    fdw_package_name 'supabase-fdw-ntp',
    fdw_package_version '<version>',  -- Get from: https://github.com/powabase/supabase-fdw-ntp/releases/latest
    fdw_package_checksum '<checksum>',  -- Get from: https://github.com/powabase/supabase-fdw-ntp/releases/latest
    api_base_url 'https://ds.netztransparenz.de/api/v1/data',
    oauth2_token_url 'https://identity.netztransparenz.de/users/connect/token',
    oauth2_client_id 'YOUR_CLIENT_ID',           -- Replace with your credentials
    oauth2_client_secret 'YOUR_CLIENT_SECRET',   -- Replace with your credentials
    oauth2_scope 'ntpStatistic.read_all_public'
  );
```

### OAuth2 Setup

**How to get credentials:**

1. Visit [netztransparenz.de](https://www.netztransparenz.de)
2. Contact API provider to request OAuth2 credentials for programmatic access
3. You will receive:
   - `client_id` - Your application identifier
   - `client_secret` - Your authentication secret (keep secure!)
   - `scope` - Use `ntpStatistic.read_all_public` for read-only access

**Security note:** Never commit credentials to version control. Store them securely in your Supabase project settings.

## Step 2: Create Foreign Tables (2 min)

Create all 4 foreign tables for comprehensive energy market access:

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS ntp;

-- Table 1: Renewable Energy Timeseries
CREATE FOREIGN TABLE ntp.renewable_energy_timeseries (
  -- Timestamps
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,
  interval_minutes SMALLINT NOT NULL,

  -- Classification
  product_type TEXT NOT NULL,                -- 'solar', 'wind_onshore', 'wind_offshore'
  data_category TEXT NOT NULL,               -- 'forecast', 'extrapolation', 'online_actual'

  -- TSO Zone Production (MW)
  tso_50hertz_mw NUMERIC(10,3),              -- Eastern Germany
  tso_amprion_mw NUMERIC(10,3),              -- Western Germany
  tso_tennet_mw NUMERIC(10,3),               -- Northern Germany
  tso_transnetbw_mw NUMERIC(10,3),           -- Southern Germany

  -- Computed Totals
  total_germany_mw NUMERIC(10,3) NOT NULL,   -- Sum of all TSO zones
  has_missing_data BOOLEAN NOT NULL,         -- TRUE if any TSO zone is NULL

  -- Metadata
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ
)
SERVER ntp_server
OPTIONS (object 'renewable_energy_timeseries');

-- Table 2: Electricity Market Prices
CREATE FOREIGN TABLE ntp.electricity_market_prices (
  -- Timestamps (multi-granularity)
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,
  granularity TEXT NOT NULL,                 -- 'hourly', 'monthly', 'annual'

  -- Price Classification
  price_type TEXT NOT NULL,                  -- 'spot_market', 'market_premium', 'annual_market_value', 'negative_flag'

  -- Price Values
  price_eur_mwh NUMERIC(10,3),               -- EUR per MWh (can be negative!)
  price_ct_kwh NUMERIC(10,4),                -- Euro cents per kWh (computed)
  is_negative BOOLEAN,                       -- TRUE if price < 0 (oversupply)

  -- Product Context
  product_category TEXT,                     -- 'epex', 'wind_onshore', 'wind_offshore', 'solar', 'annual'

  -- Negative Price Details
  negative_logic_hours TEXT,                 -- '1h', '3h', '4h', '6h' (consecutive negative hours)
  negative_flag_value BOOLEAN,               -- TRUE if negative condition met

  -- Metadata
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ
)
SERVER ntp_server
OPTIONS (object 'electricity_market_prices');

-- Table 3: Redispatch Events
CREATE FOREIGN TABLE ntp.redispatch_events (
  -- Event Time
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,

  -- Event Details
  reason TEXT NOT NULL,                      -- German: 'Netzengpass', 'Probestart (NetzRes)', etc.
  direction TEXT NOT NULL,                   -- 'increase_generation', 'reduce_generation'

  -- Power & Energy
  avg_power_mw NUMERIC,
  max_power_mw NUMERIC,
  total_energy_mwh NUMERIC,

  -- Affected Entities
  requesting_tso TEXT NOT NULL,              -- '50Hertz', 'Amprion', 'TenneT TSO', 'TransnetBW'
  instructing_tso TEXT,
  affected_facility TEXT,                    -- German facility names or 'Börse' (exchange)
  energy_type TEXT,                          -- 'Konventionell', 'Erneuerbar', 'Sonstiges'

  -- Metadata
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ
)
SERVER ntp_server
OPTIONS (object 'redispatch_events');

-- Table 4: Grid Status Timeseries
CREATE FOREIGN TABLE ntp.grid_status_timeseries (
  -- Timestamps (minute-level granularity)
  timestamp_utc TIMESTAMPTZ NOT NULL,
  interval_end_utc TIMESTAMPTZ NOT NULL,

  -- Status
  grid_status TEXT NOT NULL,                 -- 'GREEN', 'YELLOW', 'RED'

  -- Metadata
  source_endpoint TEXT NOT NULL,
  fetched_at TIMESTAMPTZ
)
SERVER ntp_server
OPTIONS (object 'grid_status_timeseries');

-- Grant permissions
GRANT USAGE ON SCHEMA ntp TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA ntp TO postgres;
```

## Step 3: Query Data (1 min)

Run your first query! Get solar production for a recent date:

```sql
SELECT
  timestamp_utc,
  total_germany_mw,
  tso_50hertz_mw,
  tso_amprion_mw,
  tso_tennet_mw,
  tso_transnetbw_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-23'
  AND timestamp_utc < '2024-10-24'
ORDER BY timestamp_utc
LIMIT 10;
```

**Expected output:**

| timestamp_utc       | total_germany_mw | tso_50hertz_mw | tso_amprion_mw | tso_tennet_mw | tso_transnetbw_mw |
|---------------------|------------------|----------------|----------------|---------------|-------------------|
| 2024-10-23 06:00:00 | 1164.602         | 245.123        | 312.456        | 428.789       | 178.234           |
| 2024-10-23 06:15:00 | 2450.890         | 520.340        | 650.120        | 890.230       | 390.200           |
| 2024-10-23 06:30:00 | 3892.450         | 830.560        | 1020.340       | 1450.670      | 590.880           |
| ...                 | ...              | ...            | ...            | ...           | ...               |

**Query time:** ~540ms

**Rows returned:** 96 (15-minute intervals for full day)

---

## Common Use Cases

### Find Negative Electricity Prices

Identify oversupply periods with negative prices (ideal for energy-intensive operations):

```sql
SELECT
  timestamp_utc,
  price_eur_mwh,
  price_ct_kwh
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND is_negative = true
  AND timestamp_utc >= '2024-10-01'
  AND timestamp_utc < '2024-11-01'
ORDER BY price_eur_mwh ASC
LIMIT 10;
```

**Insight:** Negative prices indicate renewable overproduction. These are optimal times for battery charging or flexible industrial loads.

### Compare Solar vs Wind Production

Analyze renewable energy mix over a date range:

```sql
SELECT
  product_type,
  COUNT(*) as intervals,
  ROUND(AVG(total_germany_mw)::numeric, 2) as avg_mw,
  ROUND(MAX(total_germany_mw)::numeric, 2) as peak_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type IN ('solar', 'wind_onshore')
  AND data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-24'
GROUP BY product_type;
```

**Insight:** Compare average and peak production across renewable sources. Solar shows higher daytime peaks; wind is more consistent 24/7.

### Correlate Prices with Solar Production

Analyze how solar production affects electricity prices:

```sql
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
  AND p.timestamp_utc >= '2024-10-20'
  AND p.timestamp_utc < '2024-10-24'
GROUP BY DATE(p.timestamp_utc)
ORDER BY date;
```

**Insight:** Demonstrates inverse correlation - higher solar production typically correlates with lower (sometimes negative) electricity prices.

### Monitor Grid Stability

Check recent grid status distribution:

```sql
SELECT
  grid_status,
  COUNT(*) as minutes,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= '2024-10-24'
  AND timestamp_utc < '2024-10-25'
GROUP BY grid_status
ORDER BY grid_status;
```

**Insight:** Typical distribution: GREEN 95-98%, YELLOW 2-5%, RED <1%. Higher YELLOW/RED percentages indicate grid stress.

---

## Troubleshooting

### NULL values in results?

**Cause:** Expected behavior for certain conditions (solar at night, missing TSO data, etc.)

**Solution:**
- Solar production at night returns NULL (not measured). Filter to daytime hours:
  ```sql
  WHERE timestamp_utc::time >= '06:00:00'
    AND timestamp_utc::time <= '20:00:00'
  ```
- Use `COALESCE()` for aggregations:
  ```sql
  SELECT COALESCE(tso_50hertz_mw, 0) as production_mw
  ```

### Missing WHERE parameter error?

**Error:** `date_from and date_to parameters required for renewable energy queries`

**Solution:** Always include timestamp filters:

```sql
-- Bad - will fail
SELECT * FROM ntp.renewable_energy_timeseries LIMIT 5;

-- Good - includes date range
SELECT * FROM ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-23' AND timestamp_utc < '2024-10-24'
LIMIT 5;
```

### Authentication errors?

**Cause:** Invalid or expired OAuth2 credentials

**Solution:**
1. Verify credentials are correct:
   ```sql
   SELECT * FROM pg_foreign_server WHERE srvname = 'ntp_server';
   ```
2. Update credentials if needed:
   ```sql
   ALTER SERVER ntp_server
   OPTIONS (
     SET oauth2_client_id 'YOUR_NEW_CLIENT_ID',
     SET oauth2_client_secret 'YOUR_NEW_CLIENT_SECRET'
   );
   ```
3. Tokens are cached for 1 hour and refresh automatically on 401 errors

### Slow queries?

**Expected:**
- Simple queries (1 product, 1 category, 1 day): 500-700ms
- Multi-product queries: 1-3 seconds
- Cross-table JOINs: 1.5-2.5 seconds

**Optimization tips:**

1. Always specify filters to reduce API calls:
   ```sql
   -- Optimal - 1 API call
   WHERE product_type = 'solar' AND data_category = 'forecast'

   -- Slow - 9 API calls (3 products × 3 categories)
   WHERE timestamp_utc >= '2024-10-23'
   ```

2. Use appropriate date ranges:
   ```sql
   -- Efficient - returns 96 rows
   WHERE timestamp_utc >= '2024-10-23' AND timestamp_utc < '2024-10-24'

   -- Slower - returns 2,880 rows
   WHERE timestamp_utc >= '2024-10-01' AND timestamp_utc < '2024-11-01'
   ```

3. Create materialized views for frequently accessed data:
   ```sql
   CREATE MATERIALIZED VIEW daily_solar AS
   SELECT * FROM ntp.renewable_energy_timeseries
   WHERE product_type = 'solar'
     AND data_category = 'extrapolation'
     AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days';

   -- Refresh hourly
   REFRESH MATERIALIZED VIEW daily_solar;
   ```

### No data returned?

**Possible causes:**

1. **Using 2025 dates** - API hasn't published 2025 data yet (as of October 2025)
   - **Solution:** Use 2024 dates for testing:
     ```sql
     WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-25'
     ```

2. **Invalid product_type or data_category** - Typos in filter values
   - **Solution:** Valid values:
     - `product_type`: 'solar', 'wind_onshore', 'wind_offshore'
     - `data_category`: 'forecast', 'extrapolation', 'online_actual'

3. **Wind offshore with old version** - v0.1.x had 'N.E.' parsing bug
   - **Solution:** Ensure using v0.2.0+ (verify checksum from [releases](https://github.com/powabase/supabase-fdw-ntp/releases))

---

## Testing Notes

**CRITICAL:** Use 2024 dates for testing, not 2025 dates!

The NTP API hasn't published 2025 data yet (as of October 2025). All examples in this guide use 2024 dates.

**Recommended test date ranges:**
- Single day: `'2024-10-23'` to `'2024-10-24'`
- Week: `'2024-10-20'` to `'2024-10-27'`
- Month: `'2024-10-01'` to `'2024-11-01'`

**Data categories:**
- `forecast` - Future predictions
- `extrapolation` - Historical actuals (best for testing)
- `online_actual` - Near real-time (hourly granularity)

---

## Local Development

For local Supabase testing:

### Start Supabase

```bash
# Start local instance
supabase start

# Connect to database (default credentials)
psql postgresql://postgres:postgres@127.0.0.1:54322/postgres
```

### Build and Serve WASM Locally

```bash
# Build WASM binary (use wasm32-unknown-unknown target!)
cargo component build --release --target wasm32-unknown-unknown

# Calculate checksum
shasum -a 256 target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm

# Serve via HTTP
cd target/wasm32-unknown-unknown/release
python3 -m http.server 8000 &

# Verify accessibility
curl -I http://localhost:8000/supabase_fdw_ntp.wasm
```

### Create Server with Local URL

```sql
CREATE SERVER ntp_server_local
  FOREIGN DATA WRAPPER wasm_wrapper
  OPTIONS (
    fdw_package_url 'http://host.docker.internal:8000/supabase_fdw_ntp.wasm',
    fdw_package_name 'supabase-fdw-ntp',
    fdw_package_version '<current-version>',  -- Match your build version
    fdw_package_checksum '<YOUR_CHECKSUM_HERE>',
    api_base_url 'https://ds.netztransparenz.de/api/v1/data',
    oauth2_token_url 'https://identity.netztransparenz.de/users/connect/token',
    oauth2_client_id 'YOUR_CLIENT_ID',
    oauth2_client_secret 'YOUR_CLIENT_SECRET',
    oauth2_scope 'ntpStatistic.read_all_public'
  );
```

**Note:** Use `host.docker.internal` to access localhost from Docker containers (Supabase runs in Docker).

---

## Performance Tips

### Use WHERE Clause Pushdown

Filters are pushed to the API level for optimal performance:

```sql
-- Excellent - 1 API call (all filters pushed down)
SELECT * FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'forecast'
  AND timestamp_utc >= '2024-10-23'
  AND timestamp_utc < '2024-10-24';

-- Inefficient - 9 API calls (no product/category filter)
SELECT * FROM ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-23'
  AND timestamp_utc < '2024-10-24';
```

### Create Materialized Views

Cache frequently accessed data to avoid repeated API calls:

```sql
-- Create materialized view
CREATE MATERIALIZED VIEW ntp_daily_solar AS
SELECT
  DATE(timestamp_utc) as date,
  COUNT(*) as intervals,
  ROUND(AVG(total_germany_mw)::numeric, 2) as avg_mw,
  ROUND(MAX(total_germany_mw)::numeric, 2) as peak_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(timestamp_utc);

-- Create index
CREATE INDEX ON ntp_daily_solar (date);

-- Refresh periodically (use pg_cron or external scheduler)
REFRESH MATERIALIZED VIEW ntp_daily_solar;
```

### Aggregate to Hourly for Long Ranges

Reduce data volume for large date ranges:

```sql
-- Good for 1-7 days
SELECT * FROM ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-27';

-- Better for 30+ days - aggregate first
SELECT
  DATE_TRUNC('hour', timestamp_utc) as hour,
  AVG(total_germany_mw) as avg_mw
FROM ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-01' AND timestamp_utc < '2024-11-01'
GROUP BY DATE_TRUNC('hour', timestamp_utc);
```

---

## Next Steps

- **More examples:** See [README.md](README.md) for comprehensive usage examples
- **Endpoint reference:** See detailed endpoint documentation:
  - [Renewable Energy](docs/endpoints/renewable-energy.md)
  - [Electricity Prices](docs/endpoints/electricity-prices.md)
  - [Redispatch Events](docs/endpoints/redispatch.md)
  - [Grid Status](docs/endpoints/grid-status.md)
- **Complete test suite:** See tests/ directory for validation queries
- **API documentation:** [NTP Netztransparenz.de](https://www.netztransparenz.de)

---

## Version Info

**WASM Size:** ~301 KB
**Tables:** 4 (renewable energy, electricity prices, redispatch events, grid status)
**Endpoints:** 15 (9 renewable + 4 prices + 2 grid operations)
**Supabase Wrappers:** v0.2.0+

**Latest Version:** See [GitHub Releases](https://github.com/powabase/supabase-fdw-ntp/releases/latest) for current version and changelog
- 6 critical security fixes applied
- 155 tests passing (100% success rate)

---

**Need help?** Check [GitHub Issues](https://github.com/powabase/supabase-fdw-ntp/issues) or see [full documentation](README.md).

**Ready to explore?** Try the queries above with 2024 dates and start analyzing German energy market data!
