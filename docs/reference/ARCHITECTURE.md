# NTP FDW Architecture

**Version:** v0.3.0
**Date:** 2025-10-26
**Status:** Production Ready

---

## Overview

The NTP FDW provides SQL access to German energy market data via a WebAssembly Foreign Data Wrapper for PostgreSQL. Instead of creating 35+ separate tables (1:1 API mapping), we consolidate related endpoints into **4 domain-driven tables**.

### Design Philosophy

**Consolidate, Don't Replicate**
- 11 API endpoints → 4 tables (not 1:1 mapping)
- Simpler for users (fewer tables to understand)
- Richer context (related data in single query)
- Better performance (fewer JOINs required)

**Key Metrics**
- Tables: 4 (renewable energy, prices, redispatch, grid status)
- Endpoints: 11 accessible (5 renewable + 4 price + 2 grid)
- Binary Size: 327 KB
- Tests: 190 passing (100%)
- Query Time: 200ms-2s (depending on complexity)

---

## Table Design

### 1. renewable_energy_timeseries

**Purpose:** Consolidate all renewable generation data (solar, wind onshore/offshore)

**Endpoints Covered:** 5
- `hochrechnung/Solar` → extrapolation/solar
- `hochrechnung/Wind` → extrapolation/wind_onshore
- `onlinehochrechnung/Solar` → online_actual/solar
- `onlinehochrechnung/Windonshore` → online_actual/wind_onshore
- `onlinehochrechnung/Windoffshore` → online_actual/wind_offshore

**Key Columns:**
- `product_type` (solar, wind_onshore, wind_offshore)
- `data_category` (extrapolation, online_actual)
- `tso_*_mw` (4 TSO zones: 50Hertz, Amprion, TenneT, TransnetBW)
- `total_germany_mw` (GENERATED column)

**Query Pattern:**
```sql
-- Single query for multiple products
WHERE product_type IN ('solar', 'wind_onshore')
  AND data_category = 'extrapolation'
```

---

### 2. electricity_market_prices

**Purpose:** Consolidate all pricing data (spot, premiums, flags)

**Endpoints Covered:** 4
- `Spotmarktpreise` → spot_market
- `NegativePreise` → negative_flag (UNPIVOT: 4 rows per timestamp)
- `marktpraemie` → market_premium (monthly)
- `Jahresmarktpraemie` → annual_market_value (pipe-delimited)

**Key Columns:**
- `price_type` (spot_market, market_premium, annual_market_value, negative_flag)
- `granularity` (hourly, monthly, annual)
- `price_eur_mwh` (can be negative!)
- `is_negative` (GENERATED column)

**Multi-Granularity:**
- Hourly: spot prices (24 rows/day)
- Monthly: market premiums
- Annual: market values (2020-2024)

---

### 3. redispatch_events

**Purpose:** Grid congestion management interventions

**Endpoints Covered:** 1
- `redispatch` → Event-based data (variable duration)

**Key Columns:**
- `direction` (increase_generation, reduce_generation)
- `reason` (German: "Netzengpass", "Probestart")
- `total_energy_mwh`, `max_power_mw`
- `requesting_tso`, `affected_facility`

---

### 4. grid_status_timeseries

**Purpose:** Real-time grid monitoring (minute-by-minute)

**Endpoints Covered:** 1
- `TrafficLight` → Minute-level status (JSON endpoint)

**Key Columns:**
- `grid_status` (GREEN, YELLOW, RED)
- `timestamp_utc` (1440 rows/day)

**Note:** First JSON endpoint (all others are CSV)

---

## Key Architecture Decisions

### ADR-001: Consolidated Tables

**Decision:** 11 endpoints → 4 tables

**Rationale:**
- Fewer tables = simpler mental model
- Related data accessible in single query
- Reduces JOIN complexity

**Example:**
```sql
-- Instead of: UNION ALL across 5 separate tables
-- We use: Single table with product_type filter
SELECT * FROM renewable_energy_timeseries
WHERE product_type IN ('solar', 'wind_onshore');
```

---

### ADR-002: English Schema, German Data

**Decision:** Column names in English, data values in German (when applicable)

**Schema (English):**
- Column: `timestamp_utc`, `price_eur_mwh`, `direction`

**Data (German where appropriate):**
- Reason: "Strombedingter Redispatch"
- Facility: "Grosskraftwerk Mannheim Block 8"

**Normalized Enums (English):**
- Direction: `increase_generation` (from German "Wirkleistungseinspeisung erhöhen")
- Data category: `extrapolation` (from German "hochrechnung")

---

### ADR-003: Wide Tables with TSO Zones as Columns

**Decision:** 4 TSO columns (50Hertz, Amprion, TenneT, TransnetBW) instead of normalized rows

**Schema:**
```sql
tso_50hertz_mw NUMERIC(10,3),
tso_amprion_mw NUMERIC(10,3),
tso_tennet_mw NUMERIC(10,3),
tso_transnetbw_mw NUMERIC(10,3)
```

**Benefits:**
- Direct aggregation: `SUM(tso_*_mw)`
- Fewer rows (4x reduction)
- Simpler queries: `WHERE tso_50hertz_mw > 1000`

**Trade-off:** Less normalized, but more queryable

---

### ADR-004: GENERATED Columns for Common Calculations

**Decision:** Use PostgreSQL GENERATED columns for frequently computed values

**Examples:**
```sql
total_germany_mw GENERATED ALWAYS AS (
  COALESCE(tso_50hertz_mw, 0) +
  COALESCE(tso_amprion_mw, 0) +
  COALESCE(tso_tennet_mw, 0) +
  COALESCE(tso_transnetbw_mw, 0)
) STORED

is_negative GENERATED ALWAYS AS (price_eur_mwh < 0) STORED
```

**Benefits:**
- Pre-computed for performance
- Consistent calculation across queries
- Index-able for filtering

---

### ADR-005: NULL for Missing Data

**Decision:** Use SQL NULL (not zero) for missing/unavailable data

**Rationale:**
- NULL = data unavailable (forecast not generated, measurement missing)
- 0.000 = actual zero (nighttime solar generation)
- Semantic distinction critical for data quality

**API Mappings:**
- `"N.A."` → NULL
- `"N.E."` (Nicht Erfasst) → NULL
- `"0,000"` → 0.000

---

### ADR-006: Table Detection via OPTIONS

**Decision:** Detect table type via OPTIONS, not column-based detection

**Critical Fix (v0.2.5):**
```sql
CREATE FOREIGN TABLE renewable_energy_timeseries (...)
SERVER ntp_server
OPTIONS (object 'renewable_energy_timeseries');  -- Required!
```

**Why:**
- `ctx.get_columns()` returns only **projected** columns from SELECT
- Column-based detection fails for `SELECT * FROM redispatch_events`
- OPTIONS provide explicit table identity

---

### ADR-007: Two-Phase Timestamp Filtering

**Decision:** Split timestamp filtering into API routing (date) + local filtering (time)

**Implementation:**

**Phase 1 - API Routing:** Extract DATE for efficient API calls
```rust
let date_str = micros_to_date_string(micros)?;  // "2024-10-20"
```

**Phase 2 - Local Filtering:** Apply hour/minute precision after fetch
```rust
let filtered_rows = filter_by_timestamp_bounds(all_rows, &bounds);
```

**Benefits:**
- Time-based queries work: `WHERE timestamp_utc >= '2024-10-20T10:00:00'`
- API efficiency preserved (only fetch needed dates)
- Performance: Local filtering is fast (in-memory)

---

### ADR-008: OAuth2 Token Caching

**Decision:** Cache OAuth2 token with 1-hour lifetime + proactive refresh

**Implementation:**
- Token cached in `Arc<Mutex<Option<CachedToken>>>`
- Proactive refresh at 30 minutes (50% lifetime)
- Reactive refresh on 401 errors
- Graceful mutex poisoning recovery

**Benefits:**
- Reduces API calls (one token for many queries)
- Improves performance (~200ms saved per query)
- Handles expiry gracefully

---

## Critical Implementation Patterns

### 1. Build Target

**✅ ALWAYS use wasm32-unknown-unknown:**
```bash
cargo component build --release --target wasm32-unknown-unknown
```

**❌ NEVER use wasm32-wasip1** - adds WASI CLI imports that Supabase doesn't provide

---

### 2. German Locale Parsing

**CSV Format:**
- Delimiter: `;` (semicolon)
- Decimal: `,` → must convert to `.`
- NULL: `"N.A."`, `"N.E."` (case-insensitive)

**Parsing:**
```rust
fn parse_german_decimal(value: &str) -> Result<Option<f64>> {
    let trimmed = value.trim().to_uppercase();
    if trimmed == "N.A." || trimmed == "N.E." || trimmed.is_empty() {
        return Ok(None);
    }
    value.replace(',', '.').parse::<f64>().map(Some)
}
```

---

### 3. Bounds Checking (Prevents Panics!)

**✅ Safe:**
```rust
let row = match rows.get(position) {
    Some(r) => r,
    None => return Ok(None),  // Graceful termination
};
```

**❌ Panics if out of bounds:**
```rust
let row = &rows[position];  // Don't do this!
```

**Why:** PostgreSQL calls `re_scan()` during JOINs, resetting positions. Direct indexing crashes backend.

---

### 4. Safe Integer Conversions

**✅ Safe:**
```rust
i16::try_from(minutes).map_err(|_| {
    ParseError::InvalidTimestamp(format!(
        "Interval too large: {} minutes", minutes
    ))
})
```

**❌ Silent overflow:**
```rust
Ok(minutes as i16)  // 40000 becomes -25536
```

---

## v0.3.0 Breaking Changes

### Forecast Endpoint Removed

**What Changed:**
- REMOVED: All `forecast` (prognose) endpoints
- Endpoints: 13 → 11 accessible
- Renewable: 7 → 5 endpoints

**Rationale:**
- Unreliable forecast data quality from NTP API
- Only historical actuals (`extrapolation`) and real-time (`online_actual`) remain

**Migration:**
```sql
-- Before (v0.2.x):
WHERE data_category = 'forecast'  -- Returned 0 rows

-- After (v0.3.0):
WHERE data_category = 'forecast'  -- Returns ERROR

-- Fix: Use extrapolation for historical data
WHERE data_category = 'extrapolation'
```

---

## Known Limitations

### Not Yet Implemented
- `import_foreign_schema()` - Manual table creation required
- Binary size optimization - Current: 327 KB, Target: <200 KB
- Rate limit handling - No retry logic for 429 errors
- Response caching - All queries hit API

### API Constraints
- Geographic scope: Germany only (4 TSO zones)
- Data availability: Historical data varies by endpoint
- Rate limiting: ~60 requests/minute observed
- Timezones: UTC only

---

## Production Metrics

**WASM Binary:**
- Size: 327 KB
- Zero WASI CLI imports ✅
- Validated with wasm-tools ✅

**Query Performance:**
- Single endpoint: ~200-500ms
- Multiple endpoints: ~600-1500ms
- API latency: ~200-700ms per call
- OAuth2 caching: 1-hour token lifetime

**Data Quality:**
- 190 tests passing (100%)
- German locale parsing ✅
- NULL handling (N.A./N.E.) ✅
- JOIN support ✅
- Time-based filtering ✅

---

## Related Documentation

- **[CLAUDE.md](../../CLAUDE.md)** - Development guide
- **[ETL_LOGIC.md](ETL_LOGIC.md)** - Data transformations
- **[ROUTING_RULES.md](ROUTING_RULES.md)** - Query routing logic
- **[Endpoint Docs](../endpoints/)** - Table reference (4 tables)

---

**Last Updated:** 2025-10-26
**Status:** Production Ready
