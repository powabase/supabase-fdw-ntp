# Changelog

All notable changes to the NTP FDW (German Energy Market Foreign Data Wrapper) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] - 2025-10-26

### ⚠️ BREAKING CHANGES

#### Forecast Endpoint Removed
- **REMOVED:** `forecast` data_category (prognose endpoint) from renewable energy queries
- **Impact:** Queries with `data_category = 'forecast'` now return ERROR instead of 0 rows
- **Rationale:** NTP API does not provide reliable forecast data; only `extrapolation` (historical actuals) and `online_actual` (real-time) are supported
- **Migration:** Replace `data_category = 'forecast'` with `data_category = 'extrapolation'` for historical data

#### Endpoint Count Changes
- Accessible endpoints reduced from 13 to 11 (reflecting actual NTP API availability)
- Renewable energy endpoints: 7 → 5
  - ✅ Solar: extrapolation, online_actual (2)
  - ✅ Wind Onshore: extrapolation, online_actual (2)
  - ✅ Wind Offshore: online_actual only (1)
  - ❌ Wind Offshore: forecast, extrapolation (not provided by NTP API)
- Price endpoints: 4 (unchanged)
- Grid operations: 2 (unchanged)

### Added
- Comprehensive E2E test suite (`test_e2e_comprehensive_v030.sql`) with 35 tests
- Forecast removal validation test (TEST 1) - ensures proper ERROR on deprecated category

### Changed
- Test count: 198 → 190 tests passing (removed forecast-specific tests)
- WASM binary size: 301 KB → 327 KB (optimizations and bug fixes)
- Documentation: Updated all endpoint counts to reflect 11 accessible endpoints (100% coverage)

---

## [0.2.10] - 2025-10-26

### Fixed
- **TrafficLight endpoint datetime format:** Now requires `T00:00:00` suffix per API specification
- **Timezone-less timestamp parsing:** Enhanced ISO 8601 parser to handle timestamps without explicit timezone
- URL construction for TrafficLight endpoint fixed for proper date formatting

---

## [0.2.9] - 2025-10-25

### Fixed
- **NegativePreise UNPIVOT bug:** Fixed incorrect row generation (now properly returns 4 rows per timestamp: 1h, 3h, 4h, 6h logic hours)
- Corrected endpoint documentation to reflect 13 accessible endpoints (awaiting v0.3.0 forecast removal)

### Changed
- `negative_logic_hours` column now properly populated with UNPIVOT logic
- Documentation updates for accessible endpoint clarification

---

## [0.2.8] - 2025-10-24

### Added
- **Monthly market premium parser:** Implemented UNPIVOT logic for `marktpraemie` endpoint
- Achieved 100% accessible endpoint coverage (13/13 endpoints working)

### Fixed
- Market premium data now properly unpivots into separate rows per product category
- Endpoint adjustments for monthly premium data format

---

## [0.2.7] - 2025-10-23

### Added
- **Annual market value parser:** Implemented pipe-delimited format parser for `Jahresmarktpraemie`
- Historical annual data now accessible (2020-2024)

### Fixed
- URL construction for annual market value endpoint
- Pipe-delimited CSV parsing for annual premium data

---

## [0.2.6] - 2025-10-22

### Fixed
- **YELLOW_NEG grid status variant:** Added support for yellow-negative grid status in TrafficLight endpoint
- **Jahresmarktpraemie URL construction:** Fixed incorrect URL generation for annual market value queries

---

## [0.2.5] - 2025-10-21

### Fixed
#### Critical Bug Fixes (5 total)
1. **Table detection via OPTIONS:** Fixed incorrect table routing by using `OPTIONS (object 'table_name')` instead of column-based detection
2. **GENERATED columns:** Fixed PostgreSQL FDW limitation where generated columns were not computed
3. **Midnight crossing queries:** Fixed cross-day time range queries (e.g., 22:00-04:00) that were returning partial results
4. **NegativePreise parser:** Fixed parser to handle boolean flags correctly (Ja/empty → true/false)
5. **Redispatch aggregation:** Fixed aggregation logic for redispatch event energy calculations

---

## [0.2.4] - 2025-10-20

### Fixed
- **Cross-day time range queries:** Fixed midnight-spanning queries (e.g., `timestamp_utc >= '2024-10-20T22:00:00' AND timestamp_utc < '2024-10-21T04:00:00'`)
- Multi-day time queries now fetch all required dates correctly
- Three-case date adjustment logic: same-date, cross-day time, date-only

---

## [0.2.3] - 2025-10-19

### Fixed
- **Same-date query auto-adjustment:** Fixed exclusive end date behavior
- Single-day queries now return full day of data (e.g., 2024-10-20 to 2024-10-20 auto-adjusted to 2024-10-21)
- Works across all endpoints (renewable, prices, grid status, redispatch)

---

## [0.2.2] - 2025-10-18

### Fixed
- **String timestamp parsing:** PostgreSQL now passes timestamps as strings, fixed parsing logic
- **Time-based timestamp filtering:** Hour/minute precision now fully working (e.g., `timestamp_utc >= '2024-10-20T10:00:00'`)
- Two-phase filtering: API routing (date extraction) + local filtering (time precision)

---

## [0.2.1] - 2025-10-17

### Added
- Two-phase timestamp filtering for hour/minute precision

---

## [0.2.0] - 2025-10-16

### Added
- **4 Foreign Tables:** renewable_energy_timeseries, electricity_market_prices, redispatch_events, grid_status_timeseries
- **13 Accessible API Endpoints:** 7 renewable, 4 price, 2 grid operations
- **OAuth2 Authentication:** Token caching with 1-hour lifetime and proactive refresh
- **CSV & JSON Parsing:** German locale support, NULL handling (N.A./N.E.)
- **JOIN Support:** Full cross-table JOIN capability via re_scan() implementation
- **WHERE Clause Pushdown:** Efficient API parameter translation

### Fixed
- Array bounds checking for JOIN operations (prevents panics)
- Mutex poisoning recovery in OAuth2 token cache
- Invalid timestamp handling (explicit errors, no silent epoch 0)
- NULL variant handling (case-insensitive N.A./N.E.)
- Integer overflow prevention (safe conversions)

### Performance
- Single endpoint: ~200-500ms
- 3 parallel endpoints: ~600-1500ms
- OAuth2 caching: 1-hour token lifetime, 100% success rate

---

## [0.1.0] - 2025-10-15

### Added
- Initial WASM FDW implementation
- Basic renewable energy and price endpoints
- German CSV parsing foundation

---

## Upgrade Guide

### v0.2.x → v0.3.0

#### SQL Query Changes Required

**Before (v0.2.x):**
```sql
-- This worked in v0.2.x (returned 0 rows)
SELECT * FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'forecast'
  AND product_type = 'solar'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';
```

**After (v0.3.0):**
```sql
-- Option 1: Use extrapolation for historical actuals
SELECT * FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'extrapolation'  -- Changed from 'forecast'
  AND product_type = 'solar'
  AND timestamp_utc >= '2024-10-20'
  AND timestamp_utc < '2024-10-21';

-- Option 2: Use online_actual for real-time data
SELECT * FROM fdw_ntp.renewable_energy_timeseries
WHERE data_category = 'online_actual'  -- Real-time hourly data
  AND product_type = 'solar'
  AND timestamp_utc >= NOW() - INTERVAL '24 hours';
```

#### Error Handling

Queries with `data_category = 'forecast'` will now return:
```
ERROR: Unknown data category: forecast
```

Update your application code to:
1. Remove or replace forecast queries
2. Use `extrapolation` for historical analysis
3. Use `online_actual` for real-time monitoring

---

## Links

- **Repository:** https://github.com/powabase/supabase-fdw-ntp
- **Documentation:** See README.md and docs/ folder
- **Issues:** https://github.com/powabase/supabase-fdw-ntp/issues
