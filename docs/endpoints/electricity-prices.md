# Electricity Market Prices Endpoint

## Purpose

The `electricity_market_prices` endpoint consolidates all electricity pricing data (spot market prices, market premiums, annual values, negative price flags) for market analysis and trading decisions. This table provides comprehensive pricing data across multiple granularities (hourly, monthly, annual) for German electricity markets.

**Use Cases:**
- Electricity trading and spot price analysis
- Negative price detection for load shifting optimization
- Renewable energy market premium tracking
- Price vs renewable production correlation analysis
- Daily price volatility assessment for risk management
- Long-term renewable energy project valuation (annual market values)

**Data Characteristics:**
- 24 rows per day (hourly spot prices), 96 rows per day (negative flags: 24h × 4 logic types, v0.2.9+), 1 row per month (premiums), 1 row per year (annual values)
- Real-time and historical pricing data
- Geographic scope: Germany (national electricity market)
- Query time: ~200ms - 2 seconds (depending on granularity and date range)
- API coverage: 4 endpoints consolidated (spot, negative flags, premiums, annual)

---

## Parameters

### Required Parameters

None - all parameters are optional. If no filters are provided, defaults to last 7 days across all price types.

### Optional Parameters

| Parameter | Type | Description | Default | Example | Notes |
|-----------|------|-------------|---------|---------|-------|
| `price_type` | TEXT | Filter by price type | All types | `'spot_market'` | Values: `'spot_market'`, `'market_premium'`, `'annual_market_value'`, `'negative_flag'`. **Highly recommended** to specify to avoid querying all 4 endpoints. |
| `granularity` | TEXT | Filter by time granularity | All granularities | `'hourly'` | Values: `'hourly'`, `'monthly'`, `'annual'`. Use to separate different data types. |
| `timestamp_utc` | TIMESTAMPTZ | Date/time range filter | Last 7 days | `>= '2024-10-24'` | Format varies by granularity. Hourly: YYYY-MM-DD, Monthly: YYYY-MM, Annual: YYYY. API format conversion handled by FDW. |
| `is_negative` | BOOLEAN | Filter for negative prices | N/A | `= true` | Efficient with partial index. Use for analyzing oversupply conditions. |

---

## Return Columns

### Timestamp Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `timestamp_utc` | TIMESTAMPTZ | Start time of price period | UTC timestamp | `2024-10-24 14:00:00+00` (hourly), `2024-10-01 00:00:00+00` (monthly), `2024-01-01 00:00:00+00` (annual) | Granularity depends on price_type. Hourly for spot, monthly for premiums, annual for market values. |
| `interval_end_utc` | TIMESTAMPTZ | End time of price period | UTC timestamp | `2024-10-24 15:00:00+00` (hourly +1h), `2024-11-01 00:00:00+00` (monthly), `2025-01-01 00:00:00+00` (annual) | Duration varies by granularity: +1 hour, +1 month, or +1 year. |

### Price Type and Granularity Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `granularity` | TEXT | Time granularity of price record | categorical | `hourly` | CHECK constraint: (`'hourly'`, `'monthly'`, `'annual'`). Determines interval duration. |
| `price_type` | TEXT | Type of price data | categorical | `spot_market` | CHECK constraint: (`'spot_market'`, `'market_premium'`, `'annual_market_value'`, `'negative_flag'`). Maps to API endpoints. |

### Price Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `price_eur_mwh` | NUMERIC(10,3) | Price in EUR per MWh | EUR/MWh | `82.73` or `-45.23` | NULL for negative_flag records. **Can be negative** during oversupply. Source: API ct/kWh × 10. |
| `price_ct_kwh` | NUMERIC(10,4) | Price in ct/kWh (German standard unit) | ct/kWh | `8.273` | GENERATED ALWAYS AS (price_eur_mwh / 10) STORED. Convenience field. 1 EUR/MWh = 0.1 ct/kWh. |
| `is_negative` | BOOLEAN | TRUE if price is negative (oversupply) | boolean | `true` | GENERATED ALWAYS AS (price_eur_mwh < 0) STORED. Quick filter for negative price periods. NULL if price_eur_mwh is NULL. |

### Product and Flag Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `product_category` | TEXT | Product category for market premiums/annual values | categorical | `wind_onshore` | Values: `'epex'` (MW-EPEX), `'wind_onshore'`, `'wind_offshore'`, `'solar'`, `'annual'`. NULL for spot_market. Indicates renewable type for premium pricing. |
| `negative_logic_hours` | TEXT | Duration threshold for negative price detection (UNPIVOT: 4 rows per timestamp) | categorical | `3h` | CHECK constraint: (`'1h'`, `'3h'`, `'4h'`, `'6h'`). Each timestamp returns 4 rows (v0.2.9+), one for each threshold. '1h'=at least 1 hour negative, '3h'=3+ consecutive hours. NULL for non-negative-flag records. |
| `negative_flag_value` | BOOLEAN | TRUE if negative price condition met for specific logic_hours threshold | boolean | `true` | NULL for non-negative-flag records. Combined with negative_logic_hours to identify threshold-specific negative price periods (v0.2.9+ returns all thresholds). |

### Metadata Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `source_endpoint` | TEXT | Original API endpoint path | text | `Spotmarktpreise/2024-10-24/2024-10-24` | Data lineage tracking. Useful for debugging. |
| `fetched_at` | TIMESTAMPTZ | When this data was retrieved from API | UTC timestamp | `2024-10-25 10:30:45+00` | DEFAULT NOW(). Cache invalidation and freshness tracking. |

**Notes:**
- Negative prices indicate electricity oversupply (high renewable generation + low demand)
- `price_ct_kwh` is a stored generated column - automatically computed from `price_eur_mwh`
- `is_negative` is indexed for efficient filtering of oversupply events
- Multi-granularity table: mix of hourly, monthly, and annual data (use `granularity` filter to separate)

---

## Examples

### Example 1: Negative Price Analysis

**Purpose:** Find all negative price periods in last month with duration context

```sql
SELECT
  timestamp_utc,
  timestamp_utc + INTERVAL '1 hour' as interval_end,
  price_eur_mwh,
  price_ct_kwh,
  negative_logic_hours,
  CASE
    WHEN negative_logic_hours = '1h' THEN 'Single hour'
    WHEN negative_logic_hours >= '3h' THEN negative_logic_hours || ' consecutive hours'
    ELSE 'Unknown'
  END as duration_description
FROM ntp.electricity_market_prices
WHERE is_negative = true
  AND price_type = 'spot_market'
  AND timestamp_utc >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
  AND timestamp_utc < DATE_TRUNC('month', CURRENT_DATE)
ORDER BY price_eur_mwh ASC
LIMIT 20;
```

**Expected Output:**

| timestamp_utc | interval_end | price_eur_mwh | price_ct_kwh | negative_logic_hours | duration_description |
|---------------|--------------|---------------|--------------|----------------------|----------------------|
| 2024-09-15 12:00:00+00 | 2024-09-15 13:00:00+00 | -48.23 | -4.823 | 3h | 3h consecutive hours |
| 2024-09-22 13:00:00+00 | 2024-09-22 14:00:00+00 | -35.67 | -3.567 | 1h | Single hour |
| 2024-09-28 11:00:00+00 | 2024-09-28 12:00:00+00 | -22.15 | -2.215 | 4h | 4h consecutive hours |

**Insights:**
- 0-20 rows of most negative prices (typically -10 to -50 EUR/MWh)
- More common in spring/summer (high solar production)
- Negative prices indicate oversupply - often correlates with high renewable generation + low demand

**Performance:** ~200-500ms (1 API call: Spotmarktpreise)

---

### Example 2: Price vs Solar Production Correlation

**Purpose:** Show correlation between high solar production and low/negative prices

```sql
SELECT
  DATE(p.timestamp_utc) as date,
  AVG(p.price_eur_mwh) as avg_price_eur_mwh,
  MIN(p.price_eur_mwh) as min_price_eur_mwh,
  AVG(r.total_germany_mw) as avg_solar_production_mw,
  MAX(r.total_germany_mw) as peak_solar_production_mw,
  COUNT(*) FILTER (WHERE p.is_negative) as negative_hours
FROM ntp.electricity_market_prices p
JOIN ntp.renewable_energy_timeseries r
  ON DATE_TRUNC('hour', p.timestamp_utc) = DATE_TRUNC('hour', r.timestamp_utc)
  AND r.product_type = 'solar'
  AND r.data_category = 'extrapolation'
WHERE p.price_type = 'spot_market'
  AND p.timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
  AND r.total_germany_mw > 0
GROUP BY DATE(p.timestamp_utc)
HAVING MAX(r.total_germany_mw) > 20000
ORDER BY avg_price_eur_mwh ASC
LIMIT 15;
```

**Expected Output:**

| date | avg_price_eur_mwh | min_price_eur_mwh | avg_solar_production_mw | peak_solar_production_mw | negative_hours |
|------|-------------------|-------------------|-------------------------|--------------------------|----------------|
| 2024-09-15 | 18.45 | -42.30 | 15234.5 | 28450.2 | 4 |
| 2024-09-22 | 22.83 | -28.15 | 14872.3 | 27123.8 | 3 |
| 2024-09-28 | 25.12 | -15.67 | 13456.7 | 25678.1 | 2 |

**Insights:**
- 15 rows showing inverse correlation
- Days with 28+ GW peak solar often have lowest avg prices (12-25 EUR/MWh) and 2-4 negative hours
- Demonstrates merit order effect: high renewable generation pushes prices down
- Critical for trading strategies and load shifting optimization

**Performance:** ~1-2 seconds (2 API calls + JOIN processing)

---

### Example 3: Monthly Market Premium Trends

**Purpose:** Track renewable energy market premiums over past year

```sql
SELECT
  DATE_TRUNC('month', timestamp_utc) as month,
  product_category,
  AVG(price_ct_kwh) as avg_premium_ct_kwh,
  COUNT(*) as data_points
FROM ntp.electricity_market_prices
WHERE price_type = 'market_premium'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '12 months'
  AND product_category IN ('wind_onshore', 'wind_offshore', 'solar')
GROUP BY DATE_TRUNC('month', timestamp_utc), product_category
ORDER BY month DESC, product_category;
```

**Expected Output:**

| month | product_category | avg_premium_ct_kwh | data_points |
|-------|------------------|-------------------|-------------|
| 2024-10-01 00:00:00+00 | solar | 7.234 | 1 |
| 2024-10-01 00:00:00+00 | wind_offshore | 8.456 | 1 |
| 2024-10-01 00:00:00+00 | wind_onshore | 6.789 | 1 |

**Insights:**
- 36 rows (12 months × 3 products)
- Premiums typically 6-8 ct/kWh
- Wind offshore usually highest premium
- Market premiums compensate renewable generators for difference between spot price and feed-in tariff

**Performance:** ~500ms (1 API call: marktpraemie)

---

### Example 4: Spot Price Volatility Analysis

**Purpose:** Calculate daily price volatility (standard deviation)

```sql
SELECT
  DATE(timestamp_utc) as date,
  ROUND(AVG(price_eur_mwh)::numeric, 2) as avg_price,
  ROUND(STDDEV(price_eur_mwh)::numeric, 2) as price_volatility,
  ROUND(MIN(price_eur_mwh)::numeric, 2) as min_price,
  ROUND(MAX(price_eur_mwh)::numeric, 2) as max_price,
  COUNT(*) as hours
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND granularity = 'hourly'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE(timestamp_utc)
ORDER BY price_volatility DESC;
```

**Expected Output:**

| date | avg_price | price_volatility | min_price | max_price | hours |
|------|-----------|------------------|-----------|-----------|-------|
| 2024-10-18 | 45.23 | 38.67 | -35.20 | 112.45 | 24 |
| 2024-10-20 | 52.18 | 28.34 | 8.50 | 95.30 | 24 |
| 2024-10-24 | 38.90 | 12.45 | 22.15 | 58.75 | 24 |

**Insights:**
- 7 rows (one per day)
- High volatility days (20-40 EUR/MWh stddev) often have negative prices
- Low volatility (5-10) indicates stable demand/supply balance
- Volatility indicates market uncertainty - useful for risk management and trading strategies

**Performance:** ~500ms (1 API call: Spotmarktpreise/7-day range)

---

### Example 5: Annual Market Value Comparison

**Purpose:** Compare annual market values across renewable types

```sql
SELECT
  DATE_PART('year', timestamp_utc) as year,
  product_category,
  price_ct_kwh as annual_value_ct_kwh
FROM ntp.electricity_market_prices
WHERE price_type = 'annual_market_value'
  AND timestamp_utc >= '2020-01-01'
ORDER BY year DESC, product_category;
```

**Expected Output:**

| year | product_category | annual_value_ct_kwh |
|------|------------------|---------------------|
| 2024 | solar | 7.845 |
| 2024 | wind_offshore | 8.234 |
| 2024 | wind_onshore | 7.123 |
| 2023 | solar | 8.012 |

**Insights:**
- N rows (years × products)
- Shows long-term trends in renewable energy value
- Typically 7-9 ct/kWh
- Annual values used for long-term renewable energy project valuation and policy analysis

**Performance:** ~200ms (1 API call: Jahresmarktpraemie)

---

### Example 6: Negative Price Frequency by Hour of Day

**Purpose:** Identify which hours of day have most negative prices (solar midday effect)

```sql
SELECT
  DATE_PART('hour', timestamp_utc) as hour_of_day,
  COUNT(*) as total_hours,
  COUNT(*) FILTER (WHERE is_negative = true) as negative_hours,
  ROUND(100.0 * COUNT(*) FILTER (WHERE is_negative = true) / COUNT(*), 2) as negative_pct,
  ROUND(AVG(price_eur_mwh)::numeric, 2) as avg_price
FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_PART('hour', timestamp_utc)
ORDER BY negative_pct DESC;
```

**Expected Output:**

| hour_of_day | total_hours | negative_hours | negative_pct | avg_price |
|-------------|-------------|----------------|--------------|-----------|
| 12 | 90 | 14 | 15.56 | 18.45 |
| 13 | 90 | 12 | 13.33 | 20.12 |
| 11 | 90 | 8 | 8.89 | 22.78 |
| 14 | 90 | 7 | 7.78 | 25.34 |

**Insights:**
- 24 rows (one per hour of day)
- Hours 11-14 (midday) typically highest negative_pct (5-15%) due to solar peak
- Night hours (0-5) rarely negative
- Reveals diurnal patterns in negative pricing
- Critical for battery storage optimization (charge during negative prices)

**Performance:** ~500ms (1 API call: Spotmarktpreise/90-day range)

---

## Performance Notes

### Query Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **API Latency** | 150-500ms | NTP API response time per endpoint |
| **WASM Overhead** | 50-100ms | CSV parsing, price conversion, row conversion |
| **Total Query Time** | 200ms - 2 seconds | End-to-end execution time |
| **Single endpoint** | ~200-500ms | 1 price_type (e.g., spot_market only) |
| **Multi-endpoint** | 1-2 seconds | Multiple price types (sequential API calls) |

### Response Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Response size** | 10-50 KB per API call | CSV payload from NTP API |
| **Rows returned (hourly)** | 24 rows per day | Spot market prices |
| **Rows returned (monthly)** | 1 row per month | Market premiums |
| **Rows returned (annual)** | 1 row per year | Annual market values |
| **Scaling** | Linear with date range | Hourly: 168 rows/week, 720 rows/month. Monthly: 12 rows/year. |

### Optimization Tips

1. **Always specify price_type to avoid querying all 4 endpoints:**
   ```sql
   WHERE price_type = 'spot_market'  -- Only 1 API call instead of 4
   ```

2. **Use is_negative = true filter (has partial index) for negative price analysis:**
   ```sql
   WHERE is_negative = true  -- Efficient index scan
   ```

3. **For JOINs with renewable data, use DATE_TRUNC for alignment:**
   ```sql
   JOIN renewable_energy_timeseries r
     ON DATE_TRUNC('hour', r.timestamp_utc) = p.timestamp_utc
   ```

4. **Monthly premiums: query by month boundaries:**
   ```sql
   WHERE timestamp_utc >= '2024-10-01' AND timestamp_utc < '2024-11-01'
   ```

5. **Spot price queries: date range filter essential to avoid large data transfers:**
   ```sql
   WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'  -- Limit scope
   ```

6. **Use granularity filter to separate hourly/monthly/annual data:**
   ```sql
   WHERE granularity = 'hourly'  -- Avoids mixing different data types
   ```

---

## Troubleshooting

### Issue: No negative prices found in recent data

**Symptoms:** Query with `WHERE is_negative = true` returns zero rows

**Cause:** Negative prices are rare events (5-10% of hours in high solar months, <1% in winter).

**Solution:** Extend date range and check seasonal patterns:
```sql
-- Try 30-90 day range
WHERE is_negative = true
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '90 days'

-- More common in March-May (high solar, moderate demand)
-- Check if is_negative index exists
```

Verify index exists:
```sql
SELECT indexname FROM pg_indexes
WHERE tablename = 'electricity_market_prices'
  AND indexname LIKE '%negative%';
```

---

### Issue: JOIN with renewable data returns zero rows

**Symptoms:** JOIN between electricity_market_prices and renewable_energy_timeseries produces empty result set

**Cause:** Timestamp alignment issue or re_scan() not working. Hourly prices vs 15-min renewable data.

**Solution:** Use DATE_TRUNC for hour alignment:
```sql
-- ❌ Won't match (different granularities)
JOIN renewable_energy_timeseries r ON r.timestamp_utc = p.timestamp_utc

-- ✅ Correct alignment
JOIN renewable_energy_timeseries r
  ON DATE_TRUNC('hour', r.timestamp_utc) = p.timestamp_utc
```

Ensure v0.2.0+ with re_scan() support. Rebuild if needed:
```bash
cargo component build --release --target wasm32-unknown-unknown
```

---

### Issue: price_eur_mwh is NULL for some rows

**Symptoms:** NULL values in price columns despite successful query

**Cause:** NegativePreise endpoint returns flags, not actual prices.

**Solution:** Use `price_type = 'spot_market'` for actual prices:
```sql
-- ❌ Returns NULLs (negative_flag records have no prices)
SELECT price_eur_mwh FROM ntp.electricity_market_prices
WHERE price_type = 'negative_flag'

-- ✅ Returns actual prices
SELECT price_eur_mwh FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
```

For negative_flag records:
- `negative_logic_hours` contains duration info ('1h', '3h', etc.)
- `negative_flag_value` contains boolean flag
- `price_eur_mwh` is NULL (use spot_market for actual prices)

---

### Issue: Query returns mixed hourly/monthly data

**Symptoms:** Result set contains both hourly and monthly rows with different timestamp formats

**Cause:** No granularity filter. Table contains multi-granularity data.

**Solution:** Add granularity filter to separate data types:
```sql
-- ❌ Mixed granularities
SELECT * FROM ntp.electricity_market_prices
WHERE price_type = 'market_premium'

-- ✅ Only monthly data
SELECT * FROM ntp.electricity_market_prices
WHERE price_type = 'market_premium'
  AND granularity = 'monthly'

-- ✅ Only hourly data
SELECT * FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND granularity = 'hourly'
```

---

### Issue: Market premium query returns no data

**Symptoms:** Empty result set when querying market premiums

**Cause:** Wrong date format or product_category filter. Monthly data has different structure.

**Solution:** Use correct date format and filters:
```sql
-- ❌ Wrong (daily format for monthly data)
WHERE price_type = 'market_premium'
  AND timestamp_utc = '2024-10-24'

-- ✅ Correct (monthly format)
WHERE price_type = 'market_premium'
  AND timestamp_utc >= '2024-10-01'
  AND timestamp_utc < '2024-11-01'

-- Filter by product category
WHERE product_category IN ('wind_onshore', 'solar', 'wind_offshore')
```

---

## API Constraints

### Rate Limiting

- No documented rate limits for NTP API
- OAuth2 token valid for 1 hour (automatic refresh)
- Recommend caching results for repeated queries

### Data Availability

- **Spot market prices**: Historical data available (past ~90 days typical)
- **Market premiums**: Monthly data from 2018 onwards
- **Annual market values**: Yearly data from 2018 onwards
- **Negative price flags**: Historical data aligned with spot market availability
- **Granularity**: Hourly (spot), monthly (premiums), annual (market values)

### Pricing Ranges

- **Typical spot prices**: 20-80 EUR/MWh (2-8 ct/kWh)
- **Negative prices**: -10 to -50 EUR/MWh (rare, oversupply events)
- **Positive extremes**: 100-200+ EUR/MWh (demand peaks, supply constraints)
- **Market premiums**: 6-8 ct/kWh typical
- **Annual market values**: 7-9 ct/kWh typical

---

## Known Limitations

### Negative Price Logic Types (FIXED in v0.2.9) ✅

**Previous Issue (v0.2.8):**
- `negative_logic_hours` returned only `'6h'` values
- Logic types `'1h'`, `'3h'`, `'4h'` were not accessible
- Users could not query specific negative price duration thresholds

**Fixed in v0.2.9:**
- ✅ Full UNPIVOT implementation now provides all 4 logic types per timestamp
- ✅ Each timestamp returns 4 rows (one for each duration threshold: 1h, 3h, 4h, 6h)
- ✅ Users can filter by specific `negative_logic_hours` values
- ✅ All flag values (true/false) are preserved for each logic type

**New Query Capability (v0.2.9+):**
```sql
-- Query specific negative price logic threshold
SELECT timestamp_utc, negative_logic_hours, negative_flag_value
FROM ntp.electricity_market_prices
WHERE price_type = 'negative_flag'
  AND negative_logic_hours = '1h'  -- Filter for 1-hour threshold
  AND timestamp_utc >= '2024-10-01'
  AND timestamp_utc < '2024-11-01';
```

**Impact:**
- 4× more rows returned per query (96 rows/day instead of 24 rows/day)
- Complete information about negative price duration thresholds
- Better analysis of market conditions and threshold sensitivity

**Details:** See `BUG_INVESTIGATION_REPORT.md` for full analysis and fix validation

---

## Related Documentation

- **[QUICKSTART.md](../../QUICKSTART.md)** - 5-minute setup guide
- **[README.md](../../README.md)** - Project overview
- **[renewable-energy.md](renewable-energy.md)** - Renewable energy timeseries endpoint
- **[redispatch.md](redispatch.md)** - Grid redispatch events endpoint
- **[grid-status.md](grid-status.md)** - Grid stability status endpoint
- **[ARCHITECTURE.md](../reference/ARCHITECTURE.md)** - Complete design reference (15 ADRs)
- **[ETL_LOGIC.md](../reference/ETL_LOGIC.md)** - Data transformation specifications

---

**Built with NTP API** • **Powered by Supabase WASM FDW v0.2.9**
