# CLAUDE.md

This file provides guidance to Claude Code when working with the NTP FDW wrapper for German energy market data.

## Project Overview

**supabase-fdw-ntp** is a WebAssembly (WASM) Foreign Data Wrapper for PostgreSQL that enables querying the German NTP (Netztransparenz) energy market API (https://www.netztransparenz.de) as if it were native PostgreSQL tables.

This wrapper follows the WASM FDW architecture required for hosted Supabase instances and can be used with any Supabase project.

## Project Status

**✅ v0.2.10 - Production Ready (100% Accessible Endpoints)**

- **Current Version:** v0.2.10
- **Status:** Production-ready, 13/13 accessible endpoints working (100% coverage) 🎉
- **Tables:** 4 (renewable energy, electricity prices, redispatch events, grid status) - ALL WORKING ✅
- **API Endpoints:** 13/13 accessible endpoints functional (100% completion) ✅
- **Note:** NTP API provides 13 accessible endpoints (2 wind_offshore variants unavailable per API design)
- **WASM Binary:** ~326 KB, validated, zero WASI CLI imports ✅
- **Tests:** 198 unit tests passing ✅
- **Query Performance:** Single endpoint ~200-500ms, 3 parallel ~600-1500ms ✅
- **New in v0.2.10:** Fixed TrafficLight endpoint datetime format (requires `T00:00:00` suffix per API spec) and timezone-less timestamp parsing ✅
- **New in v0.2.9:** Fixed NegativePreise UNPIVOT bug (4 rows per timestamp), updated documentation to reflect 13 accessible endpoints ✅
- **New in v0.2.8:** Marktpraemie monthly premium parser with UNPIVOT logic (100% endpoint coverage achieved) ✅
- **New in v0.2.7:** Jahresmarktpraemie pipe-delimited parser (annual market value 2020-2024 accessible) ✅
- **Fixed in v0.2.6:** 2 production bug fixes (YELLOW_NEG grid status variants, Jahresmarktpraemie URL construction) ✅
- **Fixed in v0.2.5:** 5 critical bug fixes (redispatch aggregation, GENERATED columns, midnight crossing, NegativePreise parser, table detection) ✅
- **Fixed in v0.2.4:** Cross-day time range auto-adjustment (complete time filtering) ✅
- **Fixed in v0.2.3:** Same-date query auto-adjustment (exclusive end date fix) ✅
- **Fixed in v0.2.2:** String timestamp parsing (time-based filtering fully functional) ✅

## Technology Stack

- **Language:** Rust 1.70+ (stable channel)
- **Target:** wasm32-unknown-unknown (WebAssembly - NO wasip1!)
- **Framework:** Supabase Wrappers v0.2.0
- **Build Tool:** cargo-component 0.21.1
- **API:** German NTP Energy Market API
- **Authentication:** OAuth2 client credentials flow
- **Deployment:** GitHub releases with WASM binaries

## Available Tables

| Table | Purpose | API Coverage | Data Type |
|-------|---------|--------------|-----------|
| **renewable_energy_timeseries** | Solar and wind generation | 7 endpoints | CSV |
| **electricity_market_prices** | Spot market, premiums, flags | 4 endpoints | CSV |
| **redispatch_events** | Grid intervention events | 1 endpoint | CSV |
| **grid_status_timeseries** | Minute-by-minute grid monitoring | 1 endpoint | JSON |

### API Endpoint Limitations

The NTP API does not provide all product × category combinations. Specifically:

**Available Endpoints (13 total):**
- **Renewable Energy (7):** All combinations EXCEPT wind_offshore forecast/extrapolation
  - ✅ Solar: forecast, extrapolation, online_actual
  - ✅ Wind Onshore: forecast, extrapolation, online_actual
  - ✅ Wind Offshore: online_actual only
- **Electricity Prices (4):** Spotmarktpreise, marktpraemie, Jahresmarktpraemie, NegativePreise
- **Grid Operations (2):** redispatch, TrafficLight

**Unavailable Combinations (per API design):**
- ❌ `wind_offshore` + `forecast` (prognose/Windoffshore) - Not supported by NTP API
- ❌ `wind_offshore` + `extrapolation` (hochrechnung/Windoffshore) - Not supported by NTP API

**Rationale:** The NTP API only provides online actual data for offshore wind farms. Forecast and extrapolation data are not published, likely due to technical complexity of marine weather forecasting or regulatory restrictions.

**Implementation:** The wrapper handles these missing endpoints gracefully - queries return 0 rows without errors.

## Quick Reference

### Build Commands

```bash
# Development build
cargo component build --target wasm32-unknown-unknown

# Production build (optimized for size)
# ⚠️ CRITICAL: Must use wasm32-unknown-unknown (NOT wasm32-wasip1)
cargo component build --release --target wasm32-unknown-unknown

# Verify output
ls -lh target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm
# Expected: ~301 KB (v0.2.0 with 4 tables, 13 accessible endpoints)
```

### Validation Commands

```bash
# Validate WASM structure
wasm-tools validate target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm

# Check for WASI CLI imports (should be ZERO)
wasm-tools component wit target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm | grep wasi:cli
# Expected: (no output)

# Calculate checksum
shasum -a 256 target/wasm32-unknown-unknown/release/supabase_fdw_ntp.wasm
```

## Key Architecture Decisions

### Pattern: Consolidated Tables for Similar Endpoints

**Decision:** Consolidate 9 renewable energy endpoints into 1 table, 4 price endpoints into 1 table, while keeping 2 grid operation endpoints as separate tables.

**Implementation:**
- `renewable_energy_timeseries`: Includes `product_type` (solar, wind_onshore, wind_offshore) and `data_category` (forecast, extrapolation, online_actual) columns
- `electricity_market_prices`: Includes `price_type` column (spot_market, market_premium, annual_market_value, negative_flag)
- `redispatch_events` and `grid_status_timeseries`: Standalone tables (different schemas, different query patterns)

**Benefit:** Reduces table count while maintaining query flexibility via WHERE clause routing.

### Pattern: Table Detection via OPTIONS

**Decision:** Detect table type via OPTIONS passed to `begin_scan()` instead of parsing table names.

**Implementation:**
```rust
fn detect_table(opts: &HashMap<String, String>) -> Result<TableType> {
    if opts.contains_key("product_type") {
        return Ok(TableType::RenewableEnergy);
    }
    if opts.contains_key("price_type") {
        return Ok(TableType::ElectricityPrices);
    }
    // ... other table types
}
```

**Benefit:** Table name parsing is brittle. OPTIONS provide explicit discriminator columns.

### Pattern: OAuth2 Token Caching with Proactive Refresh

**Decision:** Cache OAuth2 token in memory with 1-hour lifetime, proactive refresh at 30 minutes, reactive refresh on 401.

**Implementation:**
- Token cached in `Arc<Mutex<Option<CachedToken>>>`
- `is_near_expiry()` checks if >50% lifetime elapsed
- Mutex poisoning handled gracefully (returns error, triggers refresh)

**Benefit:** Reduces API calls, improves performance, handles expiry gracefully.

## Critical Implementation Patterns

### 1. Build Target (Most Common Error!)

**✅ ALWAYS use wasm32-unknown-unknown:**
```bash
cargo component build --release --target wasm32-unknown-unknown
```

**❌ NEVER use wasm32-wasip1:**
- Adds WASI CLI interfaces (stdin/stdout/env)
- Supabase doesn't provide these interfaces
- Causes: `component imports instance 'wasi:cli/environment@0.2.0'`

### 2. Bounds Checking on Array Access (Prevents Panics!)

**✅ Safe:**
```rust
let row_data = match this.renewable_rows.get(this.renewable_row_position) {
    Some(row) => row,
    None => return Ok(None),  // Graceful termination
};
```

**❌ Panics if out of bounds:**
```rust
let row_data = &this.renewable_rows[this.renewable_row_position];  // Don't do this!
```

**Why Critical:** PostgreSQL calls `re_scan()` during JOINs, which resets position counters. Direct indexing causes backend crashes.

### 3. German Decimal Format (CSV Parsing)

The NTP API returns CSV with German locale:
- Delimiter: `;` (semicolon)
- Decimal separator: `,` → must convert to `.`
- NULL indicators: `"N.A."`, `"N.E."` (case-insensitive)

**✅ Correct parsing:**
```rust
fn parse_german_decimal(value: &str) -> Result<Option<f64>, ParseError> {
    let trimmed = value.trim().to_uppercase();
    if trimmed == "N.A." || trimmed == "NA" || trimmed == "N.E." || trimmed == "NE" || trimmed.is_empty() {
        return Ok(None);
    }
    value.replace(',', '.').parse::<f64>()
        .map(Some)
        .map_err(|_| ParseError::InvalidDecimal(value.to_string()))
}
```

### 4. Safe Integer Conversions (Prevents Overflow)

**✅ Safe:**
```rust
i16::try_from(minutes).map_err(|_| {
    ParseError::InvalidTimestamp(format!(
        "Interval too large: {} minutes (max: {} minutes)",
        minutes, i16::MAX
    ))
})
```

**❌ Silent overflow:**
```rust
Ok(minutes as i16)  // 40000 becomes -25536
```

### 5. Two-Phase Timestamp Filtering (v0.2.1 Fix)

**Problem:** Time-based filters like `WHERE timestamp_utc >= '2024-10-20T10:00:00'` were failing because time components were stripped during qual parsing.

**Solution:** Two-phase filtering approach:

**Phase 1 (API Routing):** Extract DATE for efficient API calls
```rust
// In parse_quals(): Extract both date AND full timestamp
let date_str = micros_to_date_string(micros)?;  // "2024-10-20"
let timestamp_micros = micros;                   // Full precision preserved
```

**Phase 2 (Local Filtering):** Apply hour/minute filtering after fetching
```rust
// In begin_scan(): Filter by full timestamp bounds after API fetch
let filtered_rows = filter_renewable_rows(all_rows, &filters.timestamp_bounds);
```

**Benefit:**
- ✅ Time-based queries work: `WHERE timestamp_utc >= '2024-10-20T10:00:00' AND timestamp_utc < '2024-10-20T16:00:00'`
- ✅ Date-only queries still work: `WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-21'`
- ✅ API efficiency preserved: Only fetch needed dates
- ✅ Performance: Local filtering is fast (in-memory, already fetched data)

**Key Implementation:**
```rust
fn matches_timestamp_bounds(timestamp_str: &str, bounds: &TimestampBounds) -> bool {
    // Parse ISO 8601 timestamp to microseconds
    // Compare using original SQL operators (>=, >, <, <=, =)
    // Returns true if row passes all bounds
}
```

## Production Metrics

**WASM Binary:**
- Size: 301 KB (under 320 KB warning threshold ✅)
- Checksum: See [GitHub Releases](https://github.com/powabase/supabase-fdw-ntp/releases/latest)
- Validation: Zero WASI CLI imports ✅
- Host version: ^0.1.0 (critical requirement)

**Query Performance:**
- Single endpoint: ~200-500ms
- 3 endpoints parallel: ~600-1500ms
- API latency: ~200-700ms per call
- OAuth2 caching: 1-hour token lifetime

**Data Quality:**
- 164 unit tests passing (100%) - Updated v0.2.4
- All 6 security fixes validated
- German locale parsing working (CSV)
- NULL handling robust (N.A./N.E. variants)
- JOIN support validated
- Cross-day time range queries working (v0.2.4 - FIXED)
- Same-date query auto-adjustment working (v0.2.3 - FIXED)
- Time-based timestamp filtering working (v0.2.2 - FIXED)

## Known Limitations & Edge Cases

**Handled in v0.2.4:**
- ✅ Cross-day time range queries (midnight-spanning queries fully working)
- ✅ Multi-day time queries fetch all required dates
- ✅ Three-case date adjustment logic (same-date, cross-day time, date-only)
- ✅ Complete time filtering across all endpoints

**Handled in v0.2.3:**
- ✅ Same-date query auto-adjustment (exclusive end date behavior - FULLY WORKING)
- ✅ Single-day queries now return full day of data (2024-10-20 to 2024-10-20 → auto-adjusted to 2024-10-21)
- ✅ Works across all endpoints (renewable, prices, grid status, redispatch)

**Handled in v0.2.2:**
- ✅ String timestamp parsing (PostgreSQL passes timestamps as strings)
- ✅ Time-based timestamp filtering (hour/minute precision - FULLY WORKING)
- ✅ Two-phase filtering (API routing + local filtering)

**Handled in v0.2.0:**
- ✅ Array bounds checking (no panic on JOINs)
- ✅ Mutex poisoning recovery (OAuth2)
- ✅ Invalid timestamps (explicit errors, no silent epoch 0)
- ✅ NULL variants (case-insensitive N.A./N.E.)
- ✅ Integer overflow prevention (safe conversions)
- ✅ CSV parsing (German locale with semicolons)
- ✅ JSON parsing (safe `.get()` access for TrafficLight)

**Not Yet Implemented:**
- ⚠️ `import_foreign_schema()` - Returns empty vec (manual table creation required)
- ⚠️ Binary size optimization - Deferred to v0.3.0+ (current: 301 KB, target: <150 KB)
- ⚠️ Rate limit handling - No retry logic for 429 errors
- ⚠️ Response caching - All queries hit API

**API Constraints:**
- Geographic scope: Germany only (German TSO zones)
- Data availability: Historical data limited (varies by endpoint)
- Rate limiting: ~60 requests/minute observed
- Timezones: UTC only (no local timezone support)

## Documentation

**User Documentation:**
- **README.md** - Project overview and features
- **QUICKSTART.md** - 5-minute setup guide
- **test_fdw.sql** - Complete setup + 14 test queries
- **docs/endpoints/** - 4 table reference docs (renewable-energy.md, electricity-prices.md, redispatch.md, grid-status.md)

**Developer Documentation:**
- **docs/reference/ARCHITECTURE.md** - 15 ADRs and complete design decisions
- **docs/reference/ETL_LOGIC.md** - 11 transformation details
- **docs/reference/ROUTING_RULES.md** - Query routing and parameter pushdown logic
- **docs/reference/API_SPECIFICATION.md** - NTP API reference
- **docs/guides/E2E_TESTING_GUIDE.md** - End-to-end testing instructions

## Version Coordination

**Important:** Keep versions synchronized across:
- `Cargo.toml` - version = "0.2.10"
- `wit/world.wit` - package powabase:supabase-fdw-ntp@0.2.10
- `CLAUDE.md` - Current Version section (this file)

All three must match for successful builds and releases.

## Repository

- **GitHub:** https://github.com/powabase/supabase-fdw-ntp
- **Package:** powabase:supabase-fdw-ntp
- **License:** Apache-2.0

---

**Version:** v0.2.10
**Last Updated:** 2025-10-26
**Status:** Production Ready - 100% Accessible Endpoint Coverage (13/13)
