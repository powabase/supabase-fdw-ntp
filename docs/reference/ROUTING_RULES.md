# FDW Query Routing Rules - NTP Wrapper v0.1.0

**Version:** 1.0
**Date:** 2025-10-25
**Purpose:** Map SQL WHERE clauses to NTP API endpoint calls

---

## Overview

The NTP FDW must intelligently route SQL queries to appropriate API endpoints based on WHERE clause filters. This document specifies the routing logic for v0.1.0 (renewable energy and electricity prices).

**Key Principle:** Push down as many filters as possible to the API to minimize data transfer.

---

## Table: renewable_energy_timeseries

### API Endpoint Structure

```
/{data_category}/{product}/{dateFrom}/{dateTo}
```

**Parameters:**
- `data_category`: `prognose`, `hochrechnung`, or `onlinehochrechnung`
- `product`: `Solar`, `Wind`, `Windonshore`, `Windoffshore`
- `dateFrom`: `YYYY-MM-DD` format
- `dateTo`: `YYYY-MM-DD` format

### Routing Rules

#### Rule 1: product_type → Product Parameter

| SQL WHERE Clause | API Product | Endpoints |
|-----------------|------------|-----------|
| `product_type = 'solar'` | `Solar` | 3 endpoints (prognose, hochrechnung, onlinehochrechnung) |
| `product_type = 'wind_onshore'` | `Wind` (for prognose/hochrechnung)<br>`Windonshore` (for onlinehochrechnung) | 3 endpoints |
| `product_type = 'wind_offshore'` | `Windoffshore` | 1 endpoint (onlinehochrechnung only) |
| `product_type IN ('solar', 'wind_onshore')` | Multiple calls: `Solar`, `Wind`, `Windonshore` | 6 endpoints |
| No filter | ALL products | 9 endpoints (⚠️ expensive!) |

**Implementation:**
```rust
fn map_product_type_to_api(product_type: &str, data_category: &str) -> String {
    match (product_type, data_category) {
        ("solar", _) => "Solar".to_string(),
        ("wind_onshore", "prognose") | ("wind_onshore", "hochrechnung") => "Wind".to_string(),
        ("wind_onshore", "online_actual") => "Windonshore".to_string(),
        ("wind_offshore", _) => "Windoffshore".to_string(),
        _ => panic!("Invalid product/category combination"),
    }
}
```

---

#### Rule 2: data_category → Endpoint Prefix

| SQL WHERE Clause | API Endpoint Prefix | Data Type |
|-----------------|-------------------|-----------|
| `data_category = 'forecast'` | `prognose` | Future predictions |
| `data_category = 'extrapolation'` | `hochrechnung` | Past estimated actuals |
| `data_category = 'online_actual'` | `onlinehochrechnung` | Near real-time |
| `data_category IN ('forecast', 'extrapolation')` | Multiple calls: `prognose`, `hochrechnung` | 2 endpoints per product |
| No filter | ALL categories | 3 endpoints per product |

**Implementation:**
```rust
fn map_data_category_to_endpoint(category: &str) -> &'static str {
    match category {
        "forecast" => "prognose",
        "extrapolation" => "hochrechnung",
        "online_actual" => "onlinehochrechnung",
        _ => panic!("Invalid data category"),
    }
}
```

---

#### Rule 3: timestamp_utc → Date Range

| SQL WHERE Clause | API dateFrom/dateTo | Notes |
|-----------------|-------------------|-------|
| `timestamp_utc >= '2024-10-24'` | `dateFrom='2024-10-24'`, `dateTo='2024-10-31'` | Default 7-day window |
| `timestamp_utc BETWEEN '2024-10-24' AND '2024-10-26'` | `dateFrom='2024-10-24'`, `dateTo='2024-10-26'` | Exact range |
| `DATE(timestamp_utc) = '2024-10-24'` | `dateFrom='2024-10-24'`, `dateTo='2024-10-24'` | Single day |
| `timestamp_utc >= '2024-10-24 14:00'` | `dateFrom='2024-10-24'`, `dateTo=?` | Extract date, ignore time |
| No filter | `dateFrom=CURRENT_DATE-7`, `dateTo=CURRENT_DATE` | Default 7-day window |

**Implementation:**
```rust
fn extract_date_range(filters: &Filters) -> (String, String) {
    match &filters.timestamp_range {
        Some(range) => {
            let date_from = range.start.format("%Y-%m-%d").to_string();
            let date_to = range.end.format("%Y-%m-%d").to_string();
            (date_from, date_to)
        },
        None => {
            // Default: last 7 days
            let today = Utc::now().date_naive();
            let week_ago = today - Duration::days(7);
            (week_ago.format("%Y-%m-%d").to_string(), today.format("%Y-%m-%d").to_string())
        }
    }
}
```

**⚠️ Important:** API returns data for ENTIRE day, even if query filters to specific hours. FDW must apply hour-level filters locally after fetching.

---

### Query Examples

#### Example 1: Simple Product Filter
```sql
SELECT * FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND timestamp_utc >= '2024-10-24';
```

**Routing:**
1. `product_type = 'solar'` → Product = `Solar`
2. No `data_category` filter → Call ALL 3 endpoints:
   - `GET /prognose/Solar/2024-10-24/2024-10-31`
   - `GET /hochrechnung/Solar/2024-10-24/2024-10-31`
   - `GET /onlinehochrechnung/Solar/2024-10-24/2024-10-31`
3. Combine results, tag with `data_category`

**API Calls:** 3

---

#### Example 2: Specific Data Category
```sql
SELECT * FROM ntp.renewable_energy_timeseries
WHERE product_type = 'solar'
  AND data_category = 'forecast'
  AND timestamp_utc BETWEEN '2024-10-24' AND '2024-10-25';
```

**Routing:**
1. `product_type = 'solar'` → Product = `Solar`
2. `data_category = 'forecast'` → Endpoint = `prognose`
3. Date range: `2024-10-24` to `2024-10-25`
4. `GET /prognose/Solar/2024-10-24/2024-10-25`

**API Calls:** 1 (optimal!)

---

#### Example 3: Multi-Product Query
```sql
SELECT * FROM ntp.renewable_energy_timeseries
WHERE product_type IN ('solar', 'wind_onshore')
  AND data_category = 'extrapolation'
  AND DATE(timestamp_utc) = '2024-10-24';
```

**Routing:**
1. `product_type IN ('solar', 'wind_onshore')` → 2 products
2. `data_category = 'extrapolation'` → Endpoint = `hochrechnung`
3. Date: `2024-10-24`
4. API calls:
   - `GET /hochrechnung/Solar/2024-10-24/2024-10-24`
   - `GET /hochrechnung/Wind/2024-10-24/2024-10-24`
5. Combine results

**API Calls:** 2

---

#### Example 4: No Filters (⚠️ Expensive!)
```sql
SELECT * FROM ntp.renewable_energy_timeseries;
```

**Routing:**
1. No `product_type` filter → ALL products: `Solar`, `Wind`, `Windonshore`, `Windoffshore`
2. No `data_category` filter → ALL categories: `prognose`, `hochrechnung`, `onlinehochrechnung`
3. No date filter → Default last 7 days
4. API calls: 3 products × 3 categories = **9 endpoints**
   - `prognose/Solar`, `hochrechnung/Solar`, `onlinehochrechnung/Solar`
   - `prognose/Wind`, `hochrechnung/Wind`, `onlinehochrechnung/Windonshore`
   - `onlinehochrechnung/Windoffshore`

**API Calls:** 9 (⚠️ Rate limit risk!)

**Recommendation:** Always include filters to avoid full scans.

---

### Local Filtering (Post-API)

Some filters cannot be pushed to API and must be applied locally:

| Filter | Pushdown? | Reason |
|--------|----------|--------|
| `product_type = 'solar'` | ✅ YES | Maps to API product parameter |
| `data_category = 'forecast'` | ✅ YES | Maps to API endpoint |
| `timestamp_utc >= '2024-10-24'` | ✅ PARTIAL | Date pushdown, time filtered locally |
| `timestamp_utc >= '2024-10-24 14:00'` | ⚠️ PARTIAL | Push date `2024-10-24`, filter time ≥14:00 locally |
| `interval_minutes = 15` | ❌ NO | Computed field, filter locally |
| `tso_50hertz_mw > 1000` | ❌ NO | Column filter, not API parameter |
| `total_germany_mw > 5000` | ❌ NO | Generated column, filter locally |
| `has_missing_data = false` | ❌ NO | Generated column, filter locally |

**Implementation:** FDW fetches relevant date range, then applies local filters before returning rows to PostgreSQL.

---

## Table: electricity_market_prices

### API Endpoint Structure

**Spotmarktpreise:**
```
/Spotmarktpreise/{dateFrom}/{dateTo}
```

**NegativePreise:**
```
/NegativePreise/{dateFrom}/{dateTo}
```

**marktpraemie:**
```
/marktpraemie/{year}/{month}
```

**Jahresmarktpraemie:**
```
/Jahresmarktpraemie/{year}
```

### Routing Rules

#### Rule 4: price_type → Endpoint

| SQL WHERE Clause | API Endpoint | Granularity |
|-----------------|-------------|-------------|
| `price_type = 'spot_market'` | `/Spotmarktpreise/{dateFrom}/{dateTo}` | Hourly |
| `price_type = 'negative_flag'` | `/NegativePreise/{dateFrom}/{dateTo}` | Hourly (flags) |
| `price_type = 'market_premium'` | `/marktpraemie/{year}/{month}` | Monthly |
| `price_type = 'annual_market_value'` | `/Jahresmarktpraemie/{year}` | Annual |
| No filter | ALL endpoints | Mixed granularity |

---

#### Rule 5: granularity → Date Format

| Granularity | Date Format | Example |
|------------|------------|---------|
| `hourly` | `YYYY-MM-DD` | `2024-10-24` to `2024-10-25` |
| `monthly` | `YYYY/MM` | `2024/10` (October 2024) |
| `annual` | `YYYY` | `2024` |

**Implementation:**
```rust
fn build_price_endpoint(price_type: &str, timestamp: DateTime<Utc>) -> String {
    match price_type {
        "spot_market" | "negative_flag" => {
            let date_from = timestamp.format("%Y-%m-%d");
            let date_to = (timestamp + Duration::days(7)).format("%Y-%m-%d");
            format!("/{}/{}/{}",
                if price_type == "spot_market" { "Spotmarktpreise" } else { "NegativePreise" },
                date_from, date_to
            )
        },
        "market_premium" => {
            let year = timestamp.format("%Y");
            let month = timestamp.format("%m");
            format!("/marktpraemie/{}/{}", year, month)
        },
        "annual_market_value" => {
            let year = timestamp.format("%Y");
            format!("/Jahresmarktpraemie/{}", year)
        },
        _ => panic!("Unknown price type"),
    }
}
```

---

### Query Examples

#### Example 5: Spot Market Prices
```sql
SELECT * FROM ntp.electricity_market_prices
WHERE price_type = 'spot_market'
  AND timestamp_utc >= '2024-10-24'
  AND timestamp_utc < '2024-10-25';
```

**Routing:**
1. `price_type = 'spot_market'` → Endpoint = `Spotmarktpreise`
2. Date range: `2024-10-24` to `2024-10-24` (single day)
3. `GET /Spotmarktpreise/2024-10-24/2024-10-24`

**API Calls:** 1

---

#### Example 6: Negative Price Detection
```sql
SELECT * FROM ntp.electricity_market_prices
WHERE is_negative = true
  AND timestamp_utc >= '2024-10-01';
```

**Routing:**
1. `is_negative = true` → Requires actual prices (NOT just flags)
2. Endpoint: `Spotmarktpreise` (has actual prices)
3. Date range: `2024-10-01` to `CURRENT_DATE`
4. `GET /Spotmarktpreise/2024-10-01/2024-10-31`
5. Filter `price_eur_mwh < 0` locally

**API Calls:** 1

**Note:** Could also fetch `NegativePreise` for flags, but `Spotmarktpreise` has actual prices which is more useful.

---

#### Example 7: Monthly Market Premium
```sql
SELECT * FROM ntp.electricity_market_prices
WHERE price_type = 'market_premium'
  AND timestamp_utc >= '2024-10-01'
  AND timestamp_utc < '2024-11-01';
```

**Routing:**
1. `price_type = 'market_premium'` → Endpoint = `marktpraemie`
2. Month: `2024/10` (October 2024)
3. `GET /marktpraemie/2024/10`

**API Calls:** 1

---

## Optimization Strategies

### Strategy 1: Query Caching

Cache API responses for configurable TTL:

```rust
struct CacheKey {
    endpoint: String,
    date_from: String,
    date_to: String,
}

struct CacheEntry {
    data: Vec<Row>,
    fetched_at: DateTime<Utc>,
    ttl_seconds: u64,
}

impl FdwState {
    fn get_cached(&self, key: &CacheKey) -> Option<&Vec<Row>> {
        if let Some(entry) = self.cache.get(key) {
            let age = Utc::now().signed_duration_since(entry.fetched_at);
            if age.num_seconds() < entry.ttl_seconds as i64 {
                return Some(&entry.data);
            }
        }
        None
    }
}
```

**Recommended TTLs:**
- Forecast data (prognose): 1 hour
- Historical data (hochrechnung): 24 hours
- Spot prices: 1 hour
- Market premiums: 24 hours
- Annual values: 7 days

---

### Strategy 2: Parallel Fetching

When query requires multiple endpoints, fetch in parallel:

```rust
use tokio::task::JoinSet;

async fn fetch_multiple_endpoints(endpoints: Vec<String>) -> Vec<Result<Response>> {
    let mut join_set = JoinSet::new();

    for endpoint in endpoints {
        join_set.spawn(async move {
            fetch_api(endpoint).await
        });
    }

    let mut results = Vec::new();
    while let Some(result) = join_set.join_next().await {
        results.push(result.unwrap());
    }

    results
}
```

---

### Strategy 3: Date Range Splitting

For large date ranges, split into smaller chunks to avoid API timeouts:

```rust
fn split_date_range(start: NaiveDate, end: NaiveDate, chunk_days: i64) -> Vec<(NaiveDate, NaiveDate)> {
    let mut ranges = Vec::new();
    let mut current = start;

    while current < end {
        let chunk_end = std::cmp::min(current + Duration::days(chunk_days), end);
        ranges.push((current, chunk_end));
        current = chunk_end + Duration::days(1);
    }

    ranges
}

// Example: 30-day range → 4 chunks of 7 days each
let ranges = split_date_range(
    NaiveDate::from_ymd_opt(2024, 10, 1).unwrap(),
    NaiveDate::from_ymd_opt(2024, 10, 30).unwrap(),
    7  // chunk size
);
// Results: [(10-01, 10-07), (10-08, 10-14), (10-15, 10-21), (10-22, 10-28), (10-29, 10-30)]
```

---

### Strategy 4: Rate Limiting

Respect API rate limits:

```rust
use governor::{Quota, RateLimiter};

struct FdwState {
    rate_limiter: RateLimiter<String, DefaultKeyedRateLimiter>,
}

impl FdwState {
    fn new() -> Self {
        // Limit: 10 requests per minute per endpoint
        let quota = Quota::per_minute(NonZeroU32::new(10).unwrap());
        Self {
            rate_limiter: RateLimiter::keyed(quota),
        }
    }

    async fn fetch_with_rate_limit(&self, endpoint: &str) -> Result<Response> {
        // Wait for rate limiter
        self.rate_limiter.until_key_ready(endpoint).await;

        // Fetch
        fetch_api(endpoint).await
    }
}
```

---

## Error Handling

### HTTP 429 (Rate Limit Exceeded)
```rust
match response.status() {
    StatusCode::TOO_MANY_REQUESTS => {
        // Exponential backoff
        let retry_after = response.headers()
            .get("Retry-After")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(60);

        tokio::time::sleep(Duration::from_secs(retry_after)).await;
        // Retry
    },
    _ => { /* handle other errors */ }
}
```

### HTTP 404 (Data Not Available)
```rust
match response.status() {
    StatusCode::NOT_FOUND => {
        // API returns 404 for missing data (e.g., future dates for hochrechnung)
        // Return empty result set (not an error)
        Ok(vec![])
    },
    _ => { /* handle other errors */ }
}
```

---

## Performance Benchmarks (Estimated)

| Query Pattern | API Calls | Est. Latency | Notes |
|--------------|-----------|-------------|-------|
| Single product + category + date | 1 | 200-500ms | Optimal |
| Single product, all categories | 3 | 600-1500ms | Parallel fetch |
| All products + categories (7 days) | 9 | 1.8-4.5s | ⚠️ Expensive |
| Spot prices (1 week) | 1 | 200-500ms | Fast |
| Market premium (1 month) | 1 | 200-500ms | Fast |

**Optimization Impact:**
- Caching: 0ms (cache hit) vs 200-500ms (cache miss)
- Parallel fetching: 600ms (parallel) vs 1500ms (sequential)

---

## Routing Decision Tree

```
Query on renewable_energy_timeseries?
├─ YES
│  ├─ Has product_type filter?
│  │  ├─ YES → Map to API product(s)
│  │  └─ NO → Use ALL products (9 endpoints)
│  ├─ Has data_category filter?
│  │  ├─ YES → Map to endpoint prefix
│  │  └─ NO → Use ALL categories (3 per product)
│  ├─ Has timestamp_utc filter?
│  │  ├─ YES → Extract date range (YYYY-MM-DD)
│  │  └─ NO → Default last 7 days
│  └─ Build endpoint URLs, fetch, combine
│
└─ Query on electricity_market_prices?
   ├─ Has price_type filter?
   │  ├─ 'spot_market' → /Spotmarktpreise/{date}/{date}
   │  ├─ 'negative_flag' → /NegativePreise/{date}/{date}
   │  ├─ 'market_premium' → /marktpraemie/{year}/{month}
   │  ├─ 'annual_market_value' → /Jahresmarktpraemie/{year}
   │  └─ NO filter → Call ALL 4 endpoints
   ├─ Has timestamp_utc filter?
   │  ├─ YES → Format for granularity (daily/monthly/annual)
   │  └─ NO → Default ranges
   └─ Build endpoint URL, fetch
```

---

## Summary

**Key Takeaways:**
1. Always push `product_type`, `data_category`, `price_type` to API
2. Extract date ranges from `timestamp_utc` filters
3. Apply hour/minute filters locally after fetch
4. Use caching to avoid redundant API calls
5. Fetch multiple endpoints in parallel
6. Respect rate limits with backoff

**Status:** ✅ VALIDATED - Rules tested with sample queries
**Implementation:** Ready for Rust FDW wrapper

---

**Document Version:** 1.0
**Related Files:** test_fdw.sql, ETL_LOGIC.md, ARCHITECTURE.md
