# Renewable Energy Timeseries Endpoint

## Purpose

The `renewable_energy_timeseries` endpoint consolidates all renewable energy production data (forecasts, actuals, real-time) into a single queryable time-series for solar, wind onshore, and wind offshore generation across 4 German TSO zones (50Hertz, Amprion, TenneT, TransnetBW).

**Use Cases:**
- Renewable energy forecasting and production monitoring
- TSO zone capacity planning and grid balancing analysis
- Forecast accuracy evaluation (comparing forecast vs actual production)
- Energy trading and market analysis (correlate renewable production with electricity prices)
- Multi-product renewable energy comparison (solar vs wind performance)
- Data quality monitoring (identify gaps in TSO coverage)

**Data Characteristics:**
- 96 rows per product per day (15-minute intervals) or 24 rows (hourly intervals, for online_actual)
- Real-time and historical extrapolation data (forecast removed in v0.3.0)
- Geographic scope: Germany (4 TSO control zones covering entire country)
- Query time: ~500ms - 2 seconds (depending on filters)
- API coverage: 5 accessible endpoints (wind_offshore limited to online_actual only)

---

## Parameters

### Required Parameters

None - all parameters are optional. If no filters are provided, defaults to last 7 days across all products and categories (9 API calls).

### Optional Parameters

| Parameter | Type | Description | Default | Example | Notes |
|-----------|------|-------------|---------|---------|-------|
| `product_type` | TEXT | Filter by renewable energy type | All products | `'solar'` | Values: `'solar'`, `'wind_onshore'`, `'wind_offshore'`. **Highly recommended** to specify to avoid 9 API calls. |
| `data_category` | TEXT | Filter by data category | All categories | `'extrapolation'` | Values: `'extrapolation'`, `'online_actual'` (forecast removed in v0.3.0). **Highly recommended** to specify to reduce API calls. |
| `timestamp_utc` | TIMESTAMPTZ | Date/time range filter | Last 7 days | `>= '2024-10-24'` | Pushed to API as YYYY-MM-DD format. Hour/minute filters applied locally after fetch. |

---

## Return Columns

### Timestamp Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `timestamp_utc` | TIMESTAMPTZ | Start time of measurement interval | UTC timestamp | `2024-10-24 06:00:00+00` | Normalized from API 'Datum' + 'von' columns. Always UTC timezone, microsecond precision. Used for time-range queries and JOINs. |
| `interval_end_utc` | TIMESTAMPTZ | End time of measurement interval | UTC timestamp | `2024-10-24 06:15:00+00` | Normalized from API 'Datum' + 'bis' columns. Used for duration calculations. |
| `interval_minutes` | SMALLINT | Duration of measurement interval | minutes | `15` | 15 for prognose/hochrechnung (quarter-hourly), 60 for onlinehochrechnung (hourly). Computed from timestamps. |

### Product and Category Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `product_type` | TEXT | Type of renewable energy source | categorical | `solar` | CHECK constraint: (`'solar'`, `'wind_onshore'`, `'wind_offshore'`). Standardized from API names: 'Solar'→'solar', 'Wind'/'Windonshore'→'wind_onshore', 'Windoffshore'→'wind_offshore'. |
| `data_category` | TEXT | Category of data | categorical | `extrapolation` | CHECK constraint: (`'extrapolation'`, `'online_actual'`). Mapped from API endpoints: 'hochrechnung'→'extrapolation', 'onlinehochrechnung'→'online_actual'. |

### TSO Zone Power Generation Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `tso_50hertz_mw` | NUMERIC(10,3) | Power in 50Hertz TSO zone (Eastern Germany) | MW (Megawatts) | `245.123` | NULL for missing data (nighttime solar, forecast unavailable). Source CSV column: '50Hertz (MW)'. Covers Berlin, Brandenburg, Saxony. |
| `tso_amprion_mw` | NUMERIC(10,3) | Power in Amprion TSO zone (Western Germany) | MW (Megawatts) | `312.456` | Source CSV column: 'Amprion (MW)'. Covers NRW, Rhineland-Palatinate, Saarland. |
| `tso_tennet_mw` | NUMERIC(10,3) | Power in TenneT TSO zone (Northern Germany) | MW (Megawatts) | `428.789` | Source CSV column: 'TenneT TSO (MW)'. Covers Lower Saxony, Schleswig-Holstein. Typically highest solar capacity. |
| `tso_transnetbw_mw` | NUMERIC(10,3) | Power in TransnetBW TSO zone (Southern Germany) | MW (Megawatts) | `178.234` | Source CSV column: 'TransnetBW (MW)'. Covers Baden-Württemberg. |

### Computed and Metadata Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `total_germany_mw` | NUMERIC(10,3) | Total German production/forecast across all 4 TSO zones | MW (Megawatts) | `1164.602` | GENERATED ALWAYS AS (COALESCE(tso_50hertz_mw,0) + COALESCE(tso_amprion_mw,0) + COALESCE(tso_tennet_mw,0) + COALESCE(tso_transnetbw_mw,0)) STORED. Queryable and indexable. |
| `has_missing_data` | BOOLEAN | Data quality indicator - TRUE if any TSO zone is missing data | boolean | `false` | GENERATED ALWAYS AS (tso_50hertz_mw IS NULL OR tso_amprion_mw IS NULL OR tso_tennet_mw IS NULL OR tso_transnetbw_mw IS NULL) STORED. Used for data quality monitoring. |
| `source_endpoint` | TEXT | Original API endpoint path for data lineage | text | `prognose/Solar/2024-10-24/2024-10-25` | Useful for debugging and tracking data provenance. |
| `fetched_at` | TIMESTAMPTZ | When this data was retrieved from API | UTC timestamp | `2024-10-25 10:30:45+00` | DEFAULT NOW(). Useful for cache invalidation and data freshness checks. |

**Notes:**
- NULL values in TSO columns are normal for nighttime solar production (no generation)
- `total_germany_mw` is a stored generated column - automatically computed, queryable, and indexable
- `has_missing_data` flags incomplete records for data quality monitoring

---

## Examples

### Example 1: Basic Solar Extrapolation Retrieval

**Purpose:** Get yesterday's actual solar production for all TSO zones

```sql
SELECT
  timestamp_utc,
  tso_50hertz_mw,
  tso_amprion_mw,
  tso_tennet_mw,
  tso_transnetbw_mw,
  total_germany_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= '2024-10-24'
  AND timestamp_utc < '2024-10-25'
ORDER BY timestamp_utc
LIMIT 10;
```

**Expected Output:**

| timestamp_utc | tso_50hertz_mw | tso_amprion_mw | tso_tennet_mw | tso_transnetbw_mw | total_germany_mw |
|---------------|----------------|----------------|---------------|-------------------|------------------|
| 2024-10-24 00:00:00+00 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| 2024-10-24 00:15:00+00 | 0.000 | 0.000 | 0.000 | 0.000 | 0.000 |
| 2024-10-24 06:00:00+00 | 245.123 | 312.456 | 428.789 | 178.234 | 1164.602 |

**Insights:**
- 10 rows shown from 96 total for full day (15-minute intervals)
- 0.000 for nighttime hours (no solar generation), positive MW values during daylight
- Use this pattern for single-product, single-category queries (most efficient)

**Performance:** ~500ms (1 API call: hochrechnung/Solar/YYYY-MM-DD/YYYY-MM-DD)

---

### Example 2: Extrapolation vs Online Actual Comparison

**Purpose:** Compare historical actuals with real-time data for yesterday

```sql
WITH historical AS (
  SELECT
    DATE_TRUNC('hour', timestamp_utc) as hour,
    AVG(total_germany_mw) as extrapolation_mw
  FROM ntp.renewable_energy_timeseries
  WHERE product_type = 'solar'
    AND data_category = 'extrapolation'
    AND timestamp_utc >= '2024-10-24'
    AND timestamp_utc < '2024-10-25'
  GROUP BY DATE_TRUNC('hour', timestamp_utc)
),
realtime AS (
  SELECT
    DATE_TRUNC('hour', timestamp_utc) as hour,
    AVG(total_germany_mw) as online_actual_mw
  FROM ntp.renewable_energy_timeseries
  WHERE product_type = 'solar'
    AND data_category = 'online_actual'
    AND timestamp_utc >= '2024-10-24'
    AND timestamp_utc < '2024-10-25'
  GROUP BY DATE_TRUNC('hour', timestamp_utc)
)
SELECT
  h.hour,
  h.extrapolation_mw,
  r.online_actual_mw,
  ABS(h.extrapolation_mw - r.online_actual_mw) as diff_mw
FROM historical h
JOIN realtime r ON h.hour = r.hour
ORDER BY h.hour;
```

**Expected Output:**

| hour | extrapolation_mw | online_actual_mw | diff_mw |
|------|------------------|------------------|---------|
| 2024-10-24 07:00:00+00 | 2912.3 | 2908.5 | 3.8 |
| 2024-10-24 12:00:00+00 | 14985.7 | 14992.1 | 6.4 |
| 2024-10-24 18:00:00+00 | 4198.2 | 4195.8 | 2.4 |

**Insights:**
- 24 rows for full day (hourly aggregation)
- Small differences between extrapolation and online_actual (typically <1%)
- Both data sources provide actual generation (not forecasts)
- Self-JOIN pattern requires re_scan() support (v0.2.0+)

**Performance:** ~1-2 seconds (2 API calls: hochrechnung/Solar + onlinehochrechnung/Solar)

---

### Example 3: Multi-Product Comparison

**Purpose:** Compare solar vs wind production for current week

```sql
SELECT
  product_type,
  DATE(timestamp_utc) as day,
  SUM(total_germany_mw * interval_minutes / 60.0) as total_energy_mwh,
  AVG(total_germany_mw) as avg_power_mw,
  MAX(total_germany_mw) as peak_power_mw
FROM ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'
  AND timestamp_utc >= DATE_TRUNC('week', CURRENT_DATE)
  AND timestamp_utc < CURRENT_DATE
GROUP BY product_type, DATE(timestamp_utc)
ORDER BY day, product_type;
```

**Expected Output:**

| product_type | day | total_energy_mwh | avg_power_mw | peak_power_mw |
|--------------|-----|------------------|--------------|---------------|
| solar | 2024-10-21 | 182345.5 | 7597.7 | 28450.2 |
| wind_onshore | 2024-10-21 | 245678.3 | 10236.6 | 18234.5 |
| wind_offshore | 2024-10-21 | 98234.2 | 4093.1 | 7856.3 |

**Insights:**
- N rows (days × products)
- Solar: peak ~10-15 GW midday, 0 MW at night
- Wind: more consistent 24/7, avg 8-12 GW
- Energy calculation: power × duration (divide by 60 to convert minutes to hours)

**Performance:** ~1.5-2 seconds (3 API calls: hochrechnung for Solar, Wind; onlinehochrechnung for Windoffshore)

---

### Example 4: TSO Zone Volatility Analysis

**Purpose:** Identify which TSO zone has most volatile solar production

```sql
SELECT
  'tso_50hertz' as tso_zone,
  STDDEV(tso_50hertz_mw) as volatility_mw,
  AVG(tso_50hertz_mw) as avg_mw
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
  AND tso_50hertz_mw IS NOT NULL
UNION ALL
SELECT 'tso_amprion', STDDEV(tso_amprion_mw), AVG(tso_amprion_mw)
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar' AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days' AND tso_amprion_mw IS NOT NULL
UNION ALL
SELECT 'tso_tennet', STDDEV(tso_tennet_mw), AVG(tso_tennet_mw)
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar' AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days' AND tso_tennet_mw IS NOT NULL
UNION ALL
SELECT 'tso_transnetbw', STDDEV(tso_transnetbw_mw), AVG(tso_transnetbw_mw)
FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar' AND data_category = 'extrapolation'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days' AND tso_transnetbw_mw IS NOT NULL
ORDER BY volatility_mw DESC;
```

**Expected Output:**

| tso_zone | volatility_mw | avg_mw |
|----------|---------------|--------|
| tso_tennet | 1677.2 | 1091.5 |
| tso_amprion | 892.3 | 687.4 |
| tso_50hertz | 678.9 | 456.2 |
| tso_transnetbw | 623.1 | 445.8 |

**Insights:**
- 4 rows (one per TSO zone)
- TenneT typically highest avg (1091 MW) and volatility (1677 MW) due to largest solar capacity
- TransnetBW/50Hertz lower (~450 MW avg)

**Performance:** ~2.6 seconds (4 API calls - same endpoint called 4x due to UNION ALL pattern)

**Note:** Inefficient pattern (same API called multiple times). Consider caching optimization in future versions.

---

### Example 5: Real-Time Production Monitoring

**Purpose:** Get latest near-real-time production data (last hour)

```sql
SELECT
  product_type,
  timestamp_utc,
  total_germany_mw,
  has_missing_data
FROM ntp.renewable_energy_timeseries
WHERE data_category = 'online_actual'
  AND timestamp_utc >= NOW() - INTERVAL '1 hour'
ORDER BY timestamp_utc DESC;
```

**Expected Output:**

| product_type | timestamp_utc | total_germany_mw | has_missing_data |
|--------------|---------------|------------------|------------------|
| solar | 2024-10-25 13:00:00+00 | 12345.5 | false |
| wind_onshore | 2024-10-25 13:00:00+00 | 8234.2 | false |
| wind_offshore | 2024-10-25 13:00:00+00 | 3456.8 | false |

**Insights:**
- 3-6 rows (solar, wind_onshore, wind_offshore if available)
- Hourly granularity (60-minute intervals) for online_actual data
- Wind offshore may fail if 'N.E.' values present (v0.2.0 fixed this bug)

**Performance:** ~500-800ms (3 API calls: onlinehochrechnung for each product)

---

### Example 6: Data Quality Check

**Purpose:** Find periods with incomplete TSO coverage

```sql
SELECT
  DATE(timestamp_utc) as date,
  product_type,
  data_category,
  COUNT(*) as total_rows,
  COUNT(*) FILTER (WHERE has_missing_data = true) as incomplete_rows,
  ROUND(100.0 * COUNT(*) FILTER (WHERE has_missing_data = true) / COUNT(*), 2) as incomplete_pct
FROM ntp.renewable_energy_timeseries
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(timestamp_utc), product_type, data_category
HAVING COUNT(*) FILTER (WHERE has_missing_data = true) > 0
ORDER BY date DESC, incomplete_pct DESC;
```

**Expected Output:**

| date | product_type | data_category | total_rows | incomplete_rows | incomplete_pct |
|------|--------------|---------------|------------|-----------------|----------------|
| 2024-10-24 | solar | forecast | 96 | 40 | 41.67 |
| 2024-10-24 | solar | extrapolation | 96 | 38 | 39.58 |

**Insights:**
- Shows rows with missing TSO data
- Solar at night: 100% incomplete (expected - no nighttime generation)
- Forecast data: occasional gaps (valid API behavior)
- Use to identify API data quality issues

**Performance:** ~2-3 seconds (9 API calls if no filters)

---

## Performance Notes

### Query Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **API Latency** | 300-800ms | NTP API response time per endpoint |
| **WASM Overhead** | 100-200ms | CSV parsing, timestamp normalization, row conversion |
| **Total Query Time** | 500ms - 3 seconds | End-to-end execution time |
| **Single endpoint** | ~500ms | 1 product + 1 category |
| **Multi-endpoint** | 2-3 seconds | Multiple products/categories (sequential API calls) |

### Response Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Response size** | 20-100 KB per API call | CSV payload from NTP API |
| **Rows returned** | 96 rows per product per day (15-min intervals) | 24 rows for hourly (online_actual) |
| **Scaling** | Linear with date range | 7-day query: 672 rows (7×96). 30-day query: 2,880 rows |
| **API calls** | 1-9 per query | Depends on product_type and data_category filters |

### Optimization Tips

1. **Always specify product_type to avoid 9 API calls:**
   ```sql
   WHERE product_type = 'solar'  -- Reduces from 9 to 3 API calls
   ```

2. **Always specify data_category to avoid 3 API calls per product:**
   ```sql
   WHERE product_type = 'solar' AND data_category = 'forecast'  -- Only 1 API call
   ```

3. **Use date filters to minimize data transfer:**
   ```sql
   WHERE timestamp_utc >= '2024-10-24' AND timestamp_utc < '2024-10-25'  -- Single day
   ```

4. **For hourly aggregations, use DATE_TRUNC:**
   ```sql
   SELECT DATE_TRUNC('hour', timestamp_utc) as hour, AVG(total_germany_mw)
   ```

5. **For energy calculations (power × time):**
   ```sql
   SUM(total_germany_mw * interval_minutes / 60.0) as energy_mwh
   ```

6. **Wind offshore requires v0.2.0+ for 'N.E.' NULL handling:**
   - Ensure WASM binary version is v0.2.0 or later

---

## Troubleshooting

### Issue: Query returns NULL for all TSO zones during nighttime (solar)

**Symptoms:** All `tso_*_mw` columns are NULL between ~20:00 and ~06:00 UTC

**Cause:** Solar production at night is zero/not measured. API returns 'N.A.' which maps to NULL.

**Solution:** This is **expected behavior**. Use one of these approaches:
```sql
-- Filter to daytime hours (06:00-20:00 UTC)
WHERE product_type = 'solar'
  AND DATE_PART('hour', timestamp_utc) BETWEEN 6 AND 20

-- OR use COALESCE for aggregations
SELECT SUM(COALESCE(tso_50hertz_mw, 0)) as total_50hertz
```

---

### Issue: Error: 'Invalid decimal format: N.E.'

**Symptoms:** Query fails with parsing error on wind offshore data

**Cause:** Wind offshore API returns 'N.E.' (Nicht Erfasst = Not Recorded). Fixed in v0.2.0.

**Solution:** Ensure using v0.2.0+ WASM binary. Rebuild if needed:
```bash
cargo component build --release --target wasm32-unknown-unknown
```

---

### Issue: JOIN returns zero rows

**Symptoms:** Self-JOIN or JOIN with other tables produces empty result set

**Cause:** re_scan() not implemented or not resetting positions. Fixed in v0.2.0.

**Solution:**
1. Ensure using v0.2.0+ with re_scan() implementation
2. Run validation test:
```sql
-- Run tests/test_rescan_join.sql to validate
-- Should return matching rows, not zero rows
```

---

### Issue: Query very slow (>10 seconds)

**Symptoms:** Long wait time, multiple API calls visible in logs

**Cause:** Missing product_type or data_category filter causing 9 API calls.

**Solution:** Add filters to query:
```sql
-- ❌ Slow (9 API calls)
SELECT * FROM ntp.renewable_energy_timeseries
WHERE timestamp_utc >= '2024-10-24'

-- ✅ Fast (1 API call)
SELECT * FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'forecast'
  AND timestamp_utc >= '2024-10-24'
```

Check query plan:
```sql
EXPLAIN ANALYZE
SELECT * FROM ntp.renewable_energy_timeseries WHERE ...;
```

---

### Issue: has_missing_data always TRUE

**Symptoms:** All rows show `has_missing_data = true`

**Cause:** NULL values in TSO zones. Check if querying nighttime solar or forecast data with gaps.

**Solution:**
```sql
-- Filter to only complete records
WHERE has_missing_data = false

-- Or query daytime hours only (for solar)
WHERE product_type = 'solar'
  AND DATE_PART('hour', timestamp_utc) BETWEEN 6 AND 20
```

This is normal for:
- Nighttime solar (no generation = NULLs)
- Forecast data with unavailable TSO zones
- API data quality issues (occasional)

---

## API Constraints

### Rate Limiting

- No documented rate limits for NTP API
- OAuth2 token valid for 1 hour (automatic refresh)
- Recommend caching results for repeated queries

### Data Availability

- **Forecast data**: Next 48-72 hours available
- **Extrapolation data**: Historical data available (past ~90 days typical)
- **Online actual**: Last 24-48 hours of hourly data
- **Granularity**: 15-minute intervals (forecast/extrapolation), 60-minute (online_actual)

### Geographic Scope

- **Germany only**: All 4 TSO zones (50Hertz, Amprion, TenneT TSO, TransnetBW)
- **Products**: Solar, Wind Onshore, Wind Offshore
- **Coverage**: Entire German power grid

### Product Availability Limitations

**Wind Offshore Restrictions:**
- ✅ **Available:** `online_actual` (hourly actual generation)
- ❌ **Not Available:** `forecast` and `extrapolation` (not provided by NTP API)
- **Reason:** NTP API does not publish forecast/extrapolation data for offshore wind farms

**All Other Product × Category Combinations:** Fully supported

---

## Related Documentation

- **[QUICKSTART.md](../../QUICKSTART.md)** - 5-minute setup guide
- **[README.md](../../README.md)** - Project overview
- **[electricity-prices.md](electricity-prices.md)** - Electricity market prices endpoint
- **[redispatch.md](redispatch.md)** - Grid redispatch events endpoint
- **[grid-status.md](grid-status.md)** - Grid stability status endpoint
- **[ARCHITECTURE.md](../reference/ARCHITECTURE.md)** - Complete design reference (15 ADRs)
- **[ETL_LOGIC.md](../reference/ETL_LOGIC.md)** - Data transformation specifications

---

**Built with NTP API** • **Powered by Supabase WASM FDW v0.3.0**
