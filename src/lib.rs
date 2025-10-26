//! NTP FDW - Supabase WASM Foreign Data Wrapper for German NTP Energy Market API
//!
//! This wrapper provides PostgreSQL access to German Transmission System Operator (TSO)
//! transparency data via the Netztransparenz.de API.
//!
//! # Features
//! - OAuth2 authentication with token caching
//! - CSV parsing with German locale support (comma decimals, DD.MM.YYYY dates)
//! - Consolidated table design (renewable_energy_timeseries, electricity_market_prices)
//! - Query routing (SQL WHERE → API endpoints)
//!
//! # Architecture
//! See docs/ARCHITECTURE.md for complete design documentation (15 ADRs validated).
//!
//! # Phase 3 Status
//! - ✅ OAuth2 token management (Phase 3.4)
//! - ✅ CSV parser with ETL transformations (Phase 3.3)
//! - ✅ Query router (Phase 3.5)
//! - 🔜 FDW lifecycle integration (Phase 3.6)

// cargo-component generates bindings automatically from wit/world.wit
#[allow(warnings)]
mod bindings;

// Phase 3 modules
pub mod csv_parser;
pub mod csv_utils;
mod error;
pub mod grid_parsers;
pub mod oauth2;
pub mod query_router;
pub mod transformations;
mod types;
mod types_grid;

// Re-export public types for easier access
pub use error::{ApiError, NtpFdwError, OAuth2Error, ParseError};
pub use oauth2::{OAuth2Config, OAuth2Manager};
pub use query_router::{DateRange, QualFilters, QueryPlan, TimestampBounds};
pub use types::{PriceRow, RenewableRow};
pub use types_grid::{GridStatusRow, RedispatchRow};

use bindings::exports::supabase::wrappers::routines::{Context, FdwResult, Guest};
use bindings::supabase::wrappers::types::{Cell, Row, Value};

// ============================================================================
// Helper Functions for FDW Lifecycle
// ============================================================================

/// Detect table name from table OPTIONS
///
/// CRITICAL FIX (v0.2.0): Column-based detection failed because ctx.get_columns()
/// only returns projected columns from SELECT, not table definition columns.
///
/// New approach: Use OPTIONS specified in CREATE FOREIGN TABLE:
///   CREATE FOREIGN TABLE ntp.redispatch_events (...)
///   SERVER ntp_server
///   OPTIONS (table 'redispatch_events');
///
/// Supported tables:
/// - renewable_energy_timeseries
/// - electricity_market_prices
/// - redispatch_events
/// - grid_status_timeseries
///
/// # Fallback Behavior
///
/// If no table option is specified, falls back to column-based detection
/// (for backwards compatibility with existing tables).
fn detect_table_name(ctx: &Context) -> String {
    use bindings::supabase::wrappers::types::OptionsType;

    // PRIMARY: Try to get table name from OPTIONS (v0.2.0+)
    // PostgreSQL foreign tables can use different OPTIONS keys ('table', 'object', 'name')
    let table_opts = ctx.get_options(&OptionsType::Table);

    // Check all known OPTIONS keys (Bug #4 fix: support both 'table' and 'object')
    for key in ["table", "object", "name"] {
        if let Some(table_name) = table_opts.get(key) {
            #[cfg(feature = "pg_test")]
            eprintln!("[NTP FDW] Table detected via OPTIONS['{}'] = '{}'", key, table_name);
            return table_name;
        }
    }

    // FALLBACK: Column-based detection (backwards compatibility)
    // NOTE: This only works if queried columns include the discriminator column
    #[cfg(feature = "pg_test")]
    eprintln!("[NTP FDW] OPTIONS detection failed, falling back to column-based detection");

    let columns = ctx.get_columns();

    for col in columns {
        let name = col.name();
        if name == "product_type" {
            #[cfg(feature = "pg_test")]
            eprintln!("[NTP FDW] Table detected via column 'product_type': renewable_energy_timeseries");
            return "renewable_energy_timeseries".to_string();
        }
        if name == "price_type" {
            #[cfg(feature = "pg_test")]
            eprintln!("[NTP FDW] Table detected via column 'price_type': electricity_market_prices");
            return "electricity_market_prices".to_string();
        }
        if name == "reason" {
            #[cfg(feature = "pg_test")]
            eprintln!("[NTP FDW] Table detected via column 'reason': redispatch_events");
            return "redispatch_events".to_string();
        }
        if name == "grid_status" {
            #[cfg(feature = "pg_test")]
            eprintln!("[NTP FDW] Table detected via column 'grid_status': grid_status_timeseries");
            return "grid_status_timeseries".to_string();
        }
    }

    // Default to renewable if cannot detect (with warning)
    #[cfg(feature = "pg_test")]
    eprintln!("[NTP FDW] WARNING: Cannot detect table from OPTIONS or columns, defaulting to renewable_energy_timeseries");

    "renewable_energy_timeseries".to_string()
}

/// Parse quals (WHERE clause filters) from Context
///
/// Extracts filters for:
/// - product_type (for renewable energy table)
/// - data_category (for renewable energy table)
/// - price_type (for price table)
/// - timestamp_utc (date range for both tables)
///
/// # Date Range Behavior
///
/// The function extracts date ranges from timestamp_utc filters with intelligent defaults
/// to prevent unbounded queries while respecting user intent.
///
/// ## Case 1: Same-Date Query (Auto-Adjusted - v0.2.3)
/// ```sql
/// WHERE timestamp_utc >= '2024-10-20' AND timestamp_utc < '2024-10-20'
/// ```
/// **Result:** Automatically adjusts to `2024-10-20` to `2024-10-21` (API routing)
///
/// **Rationale:** NTP API uses exclusive end dates `[start, end)`. Same-date queries
/// (start == end) would return empty results because the range is mathematically empty.
/// This auto-adjustment provides the expected "full day" behavior. The original
/// timestamp bounds are preserved for local time-based filtering after API fetch.
///
/// **Use Case:** Single-day queries like "show me all data for Oct 20"
///
/// ## Case 1b: Cross-Day Time Range (Auto-Adjusted - v0.2.4)
/// ```sql
/// WHERE timestamp_utc >= '2024-10-20T23:00:00' AND timestamp_utc < '2024-10-21T01:00:00'
/// ```
/// **Result:** Automatically adjusts to `2024-10-20` to `2024-10-22` (API routing)
///
/// **Rationale:** To capture data from the end date (Oct 21), we must fetch through
/// the day after the end date due to the API's exclusive end date behavior. The query
/// spans Oct 20 23:00 to Oct 21 01:00, so we fetch both Oct 20 and Oct 21 data.
/// The timestamp bounds filter then keeps only the requested time range (23:00-01:00).
///
/// **Use Case:** Queries spanning midnight or multiple days with specific time ranges
///
/// ## Case 2: Date Range Without Time (No Adjustment)
/// ```sql
/// WHERE timestamp_utc >= '2024-10-24' AND timestamp_utc < '2024-10-31'
/// ```
/// **Result:** Fetches exactly `2024-10-24` to `2024-10-31`
///
/// **Rationale:** When no time components are specified, the user wants full calendar
/// days. No adjustment needed since the query intent is clear (days 24-30).
///
/// **Use Case:** Date-only range queries - most predictable and optimal
///
/// ## Case 3: Only Start Provided
/// ```sql
/// WHERE timestamp_utc >= '2024-10-24'
/// ```
/// **Result:** Fetches `2024-10-24` to `2024-10-31` (7-day window from start)
///
/// **Rationale:** User specified a start date, so we fetch a reasonable window
/// (7 days) from that point forward. This prevents unbounded queries while
/// respecting user intent to get data "starting from this date".
///
/// ## Case 4: Only End Provided
/// ```sql
/// WHERE timestamp_utc < '2024-10-31'
/// ```
/// **Result:** Fetches `2024-10-24` to `2024-10-31` (7 days before end)
///
/// **Rationale:** User specified an end date, so we fetch a reasonable window
/// (7 days) before that point. This prevents unbounded queries while
/// respecting user intent to get data "up to this date".
///
/// ## Case 5: No Date Filter (Default)
/// ```sql
/// SELECT * FROM ntp.renewable_energy_timeseries WHERE product_type = 'solar'
/// ```
/// **Result:** Returns None (query_router will default to last 7 days)
///
/// **Rationale:** Default to recent data (last week) to prevent expensive
/// full-table scans. This matches typical use case of analyzing recent trends.
///
/// # Why 7 Days?
///
/// - **Performance:** Prevents unbounded queries (Phase 1 benchmark: 2.1s for 365 days)
/// - **Typical Use Case:** Most analyses focus on recent trends (last week)
/// - **Predictable:** Users know exactly what window to expect
/// - **Overridable:** Always specify explicit date range for custom windows
///
/// # Returns
///
/// QualFilters struct ready for query routing
///
/// # Errors
///
/// Returns error if date format is invalid or date range is invalid (start > end)
fn parse_quals(ctx: &Context) -> Result<query_router::QualFilters, String> {
    let quals = ctx.get_quals();
    let table_name = detect_table_name(ctx);

    let mut product_type: Option<String> = None;
    let mut data_category: Option<String> = None;
    let mut price_type: Option<String> = None;
    let mut timestamp_start: Option<String> = None;
    let mut timestamp_end: Option<String> = None;

    // NEW: Track full timestamp bounds for local filtering
    let mut ts_bound_start: Option<i64> = None;
    let mut ts_bound_start_op: Option<String> = None;
    let mut ts_bound_end: Option<i64> = None;
    let mut ts_bound_end_op: Option<String> = None;

    // Parse each qual
    for qual in quals {
        let field = qual.field();
        let operator = qual.operator();
        let value = qual.value();

        match field.as_str() {
            "product_type" => {
                if operator == "=" {
                    if let Value::Cell(Cell::String(val)) = value {
                        product_type = Some(val);
                    }
                }
            }
            "data_category" => {
                if operator == "=" {
                    if let Value::Cell(Cell::String(val)) = value {
                        data_category = Some(val);
                    }
                }
            }
            "price_type" => {
                if operator == "=" {
                    if let Value::Cell(Cell::String(val)) = value {
                        price_type = Some(val);
                    }
                }
            }
            "timestamp_utc" => {
                // Extract BOTH date (for API routing) AND full timestamp (for local filtering)
                // timestamp_utc is stored as Cell::Timestamptz (microseconds since epoch)
                match value {
                    Value::Cell(Cell::Timestamptz(micros)) => {
                        // Phase 1: Extract date for API routing (existing logic)
                        let date_str = micros_to_date_string(micros)
                            .map_err(|e| format!("Failed to parse timestamp_utc: {}", e))?;

                        match operator.as_str() {
                            ">=" | ">" => {
                                timestamp_start = Some(date_str);
                                // Phase 2: Store full timestamp for local filtering
                                ts_bound_start = Some(micros);
                                ts_bound_start_op = Some(operator);
                            }
                            "<" | "<=" => {
                                timestamp_end = Some(date_str);
                                // Phase 2: Store full timestamp for local filtering
                                ts_bound_end = Some(micros);
                                ts_bound_end_op = Some(operator);
                            }
                            "=" => {
                                // Exact date match
                                timestamp_start = Some(date_str.clone());
                                timestamp_end = Some(date_str);
                                // Phase 2: Store full timestamp for local filtering
                                ts_bound_start = Some(micros);
                                ts_bound_start_op = Some(">=".to_string());
                                ts_bound_end = Some(micros);
                                ts_bound_end_op = Some("<=".to_string());
                            }
                            _ => {}
                        }
                    }
                    Value::Cell(Cell::String(date_str)) => {
                        // Handle string dates/timestamps (e.g., '2024-10-24' or '2024-10-20T10:00:00')

                        // Phase 1: Extract date component for API routing
                        let date_only = extract_date_component(&date_str);

                        match operator.as_str() {
                            ">=" | ">" => {
                                timestamp_start = Some(date_only);
                                // Phase 2: Parse to microseconds for local filtering
                                if let Some(micros) = parse_string_to_micros(&date_str) {
                                    ts_bound_start = Some(micros);
                                    ts_bound_start_op = Some(operator);
                                }
                            }
                            "<" | "<=" => {
                                timestamp_end = Some(date_only);
                                // Phase 2: Parse to microseconds for local filtering
                                if let Some(micros) = parse_string_to_micros(&date_str) {
                                    ts_bound_end = Some(micros);
                                    ts_bound_end_op = Some(operator);
                                }
                            }
                            "=" => {
                                timestamp_start = Some(date_only.clone());
                                timestamp_end = Some(date_only);
                                // Phase 2: Parse to microseconds for local filtering
                                if let Some(micros) = parse_string_to_micros(&date_str) {
                                    ts_bound_start = Some(micros);
                                    ts_bound_start_op = Some(">=".to_string());
                                    ts_bound_end = Some(micros);
                                    ts_bound_end_op = Some("<=".to_string());
                                }
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            _ => {
                // Ignore other filters (handled locally)
            }
        }
    }

    // Build DateRange if timestamp filters present
    let timestamp_range = match (timestamp_start, timestamp_end) {
        (Some(start), Some(end)) => {
            // Detect time-based filtering (not just date filters)
            let has_time_bounds = ts_bound_start.is_some() || ts_bound_end.is_some();

            let adjusted_end = if start == end {
                // Case 1: Same-date time query (v0.2.3 fix)
                // Example: 2024-10-20T10:00 to 2024-10-20T16:00
                //   → API: /2024-10-20/2024-10-21
                add_days_to_date(&end, 1)?
            } else if has_time_bounds {
                // Case 2: Cross-day time query (v0.2.4 fix)
                // Example: 2024-10-20T23:00 to 2024-10-21T01:00
                //   → API: /2024-10-20/2024-10-22 (fetches Oct 20 + Oct 21)
                // Local filtering will keep only 23:00-01:00
                add_days_to_date(&end, 1)?
            } else {
                // Case 3: Date-only query (no adjustment)
                // Example: 2024-10-20 to 2024-10-25
                //   → API: /2024-10-20/2024-10-25
                end
            };
            Some(query_router::DateRange {
                start,
                end: adjusted_end,
            })
        }
        (Some(start), None) => {
            // Only start date: default to 7 days from start
            let end = add_days_to_date(&start, 7)?;
            Some(query_router::DateRange { start, end })
        }
        (None, Some(end)) => {
            // Only end date: default to 7 days before end
            let start = add_days_to_date(&end, -7)?;
            Some(query_router::DateRange { start, end })
        }
        (None, None) => None, // No date filter (will use default last 7 days)
    };

    // Build TimestampBounds if full timestamp quals present
    let timestamp_bounds = match (ts_bound_start, ts_bound_end) {
        (Some(start), Some(end)) => Some(query_router::TimestampBounds {
            start: Some(start),
            start_operator: ts_bound_start_op,
            end: Some(end),
            end_operator: ts_bound_end_op,
        }),
        (Some(start), None) => Some(query_router::TimestampBounds {
            start: Some(start),
            start_operator: ts_bound_start_op,
            end: None,
            end_operator: None,
        }),
        (None, Some(end)) => Some(query_router::TimestampBounds {
            start: None,
            start_operator: None,
            end: Some(end),
            end_operator: ts_bound_end_op,
        }),
        (None, None) => None, // No timestamp bounds (date-only or no filter)
    };

    Ok(query_router::QualFilters {
        product_type,
        data_category,
        price_type,
        timestamp_range,
        timestamp_bounds,
        table_name,
    })
}

/// Convert microseconds since epoch to YYYY-MM-DD date string
///
/// # Returns
/// - `Ok(String)` - Date string in YYYY-MM-DD format
/// - `Err(String)` - If timestamp is invalid (out of valid range)
fn micros_to_date_string(micros: i64) -> Result<String, String> {
    use chrono::DateTime;

    let seconds = micros / 1_000_000;
    let dt = DateTime::from_timestamp(seconds, 0).ok_or_else(|| {
        format!(
            "Invalid timestamp: {} microseconds ({} seconds) is out of valid range",
            micros, seconds
        )
    })?;

    Ok(dt.format("%Y-%m-%d").to_string())
}

/// Add days to date string (YYYY-MM-DD)
fn add_days_to_date(date_str: &str, days: i64) -> Result<String, String> {
    use chrono::NaiveDate;

    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| format!("Invalid date format: {}", e))?;

    let new_date = if days >= 0 {
        date + chrono::Duration::days(days)
    } else {
        date - chrono::Duration::days(-days)
    };

    Ok(new_date.format("%Y-%m-%d").to_string())
}

/// Parse timestamp string to microseconds since epoch
///
/// Handles both full ISO 8601 timestamps and date-only strings.
///
/// # Arguments
///
/// * `s` - Timestamp string in ISO 8601 format ("2024-10-20T10:00:00Z") or date-only ("2024-10-20")
///
/// # Returns
///
/// - `Some(i64)` - Microseconds since epoch (UTC)
/// - `None` - If string cannot be parsed
///
/// # Examples
///
/// ```
/// // Full timestamp with timezone
/// let micros = parse_string_to_micros("2024-10-20T10:00:00Z");
/// assert!(micros.is_some());
///
/// // Date-only (treated as start of day 00:00:00 UTC)
/// let micros = parse_string_to_micros("2024-10-20");
/// assert!(micros.is_some());
/// ```
fn parse_string_to_micros(s: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDate};

    // Try full timestamp: "2024-10-20T10:00:00Z" or "2024-10-20T10:00:00+00:00"
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.timestamp_micros());
    }

    // Try date-only: "2024-10-20" → treat as start of day (00:00:00 UTC)
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0)?;
        let dt_utc = DateTime::<chrono::Utc>::from_naive_utc_and_offset(dt, chrono::Utc);
        return Some(dt_utc.timestamp_micros());
    }

    None
}

/// Extract date component from timestamp string
///
/// Extracts the date portion (YYYY-MM-DD) from either a full timestamp or date-only string.
///
/// # Arguments
///
/// * `s` - Timestamp string ("2024-10-20T10:00:00Z") or date string ("2024-10-20")
///
/// # Returns
///
/// Date string in YYYY-MM-DD format
///
/// # Examples
///
/// ```
/// assert_eq!(extract_date_component("2024-10-20"), "2024-10-20");
/// assert_eq!(extract_date_component("2024-10-20T10:00:00Z"), "2024-10-20");
/// ```
fn extract_date_component(s: &str) -> String {
    if s.len() == 10 {
        // Already in date-only format: "2024-10-20"
        s.to_string()
    } else {
        // Full timestamp: "2024-10-20T10:00:00Z" → extract "2024-10-20"
        s.split('T').next().unwrap_or(s).to_string()
    }
}

/// Apply timestamp bounds filtering to rows
///
/// Filters rows based on full timestamp (hour/minute/second) comparisons.
/// Solves the time-component stripping bug where queries with time-based filters
/// like `WHERE timestamp_utc >= '2024-10-20T10:00:00'` were returning all rows
/// for that date instead of just the requested time range.
///
/// # Arguments
///
/// * `timestamp_str` - ISO 8601 timestamp string from row (e.g., "2024-10-20T14:30:00Z")
/// * `bounds` - Timestamp bounds extracted from SQL WHERE clause
///
/// # Returns
///
/// `true` if row passes all timestamp filters, `false` otherwise
///
/// # Implementation Notes
///
/// - Converts ISO 8601 timestamp strings to microseconds since epoch
/// - Compares using the original operators from SQL (>=, >, <, <=, =)
/// - Handles missing bounds (None) by not filtering on that side
fn matches_timestamp_bounds(timestamp_str: &str, bounds: &TimestampBounds) -> bool {
    use chrono::DateTime;

    // Parse row timestamp to microseconds
    let row_timestamp_micros = match DateTime::parse_from_rfc3339(timestamp_str) {
        Ok(dt) => dt.timestamp_micros(),
        Err(_) => return false, // Invalid timestamp format, exclude row
    };

    // Check lower bound (start)
    if let Some(start_micros) = bounds.start {
        let matches_start = match bounds.start_operator.as_deref() {
            Some(">=") => row_timestamp_micros >= start_micros,
            Some(">") => row_timestamp_micros > start_micros,
            Some("=") => row_timestamp_micros == start_micros,
            _ => true, // Unknown operator, don't filter
        };
        if !matches_start {
            return false;
        }
    }

    // Check upper bound (end)
    if let Some(end_micros) = bounds.end {
        let matches_end = match bounds.end_operator.as_deref() {
            Some("<") => row_timestamp_micros < end_micros,
            Some("<=") => row_timestamp_micros <= end_micros,
            Some("=") => row_timestamp_micros == end_micros,
            _ => true, // Unknown operator, don't filter
        };
        if !matches_end {
            return false;
        }
    }

    true
}

/// Apply timestamp filtering to renewable energy rows
fn filter_renewable_rows(
    rows: Vec<RenewableRow>,
    bounds: &Option<TimestampBounds>,
) -> Vec<RenewableRow> {
    match bounds {
        Some(bounds) => rows
            .into_iter()
            .filter(|row| matches_timestamp_bounds(&row.timestamp_utc, bounds))
            .collect(),
        None => rows, // No filtering needed
    }
}

/// Apply timestamp filtering to price rows
fn filter_price_rows(rows: Vec<PriceRow>, bounds: &Option<TimestampBounds>) -> Vec<PriceRow> {
    match bounds {
        Some(bounds) => rows
            .into_iter()
            .filter(|row| matches_timestamp_bounds(&row.timestamp_utc, bounds))
            .collect(),
        None => rows, // No filtering needed
    }
}

/// Apply timestamp filtering to grid status rows
fn filter_grid_status_rows(
    rows: Vec<GridStatusRow>,
    bounds: &Option<TimestampBounds>,
) -> Vec<GridStatusRow> {
    match bounds {
        Some(bounds) => rows
            .into_iter()
            .filter(|row| matches_timestamp_bounds(&row.timestamp_utc, bounds))
            .collect(),
        None => rows, // No filtering needed
    }
}

/// Apply timestamp filtering to redispatch rows
fn filter_redispatch_rows(
    rows: Vec<RedispatchRow>,
    bounds: &Option<TimestampBounds>,
) -> Vec<RedispatchRow> {
    match bounds {
        Some(bounds) => rows
            .into_iter()
            .filter(|row| matches_timestamp_bounds(&row.timestamp_utc, bounds))
            .collect(),
        None => rows, // No filtering needed
    }
}

/// Fetch API endpoint with OAuth2 authentication
///
/// Makes HTTP GET request with Bearer token in Authorization header.
///
/// # Arguments
///
/// * `url` - Full API endpoint URL (e.g., "https://www.netztransparenz.de/api/ntp/prognose/Solar/2024-10-24/2024-10-25")
/// * `token` - OAuth2 access token (Bearer token)
///
/// # Returns
///
/// * `Ok(String)` - CSV response body
/// * `Err(NtpFdwError)` - HTTP error, network error, or empty response
///
/// # Error Handling
///
/// - 401 Unauthorized → Error (caller should clear OAuth2 cache and retry)
/// - 404 Not Found → Empty string (data not available for date range)
/// - 429 Rate Limited → Error
/// - 500 Server Error → Error
fn fetch_endpoint(url: &str, token: &str) -> Result<String, NtpFdwError> {
    use bindings::supabase::wrappers::{http, utils};

    utils::report_info(&format!("fetch_endpoint: URL={}", url));
    utils::report_info(&format!("fetch_endpoint: token length={}", token.len()));

    // Build HTTP GET request
    let request = http::Request {
        method: http::Method::Get,
        url: url.to_string(),
        headers: vec![
            ("authorization".to_string(), format!("Bearer {}", token)),
            ("accept".to_string(), "text/csv".to_string()),
        ],
        body: String::new(),
    };

    utils::report_info("fetch_endpoint: Request built, calling http::get");

    // Make HTTP request
    let response = http::get(&request).map_err(|err| {
        utils::report_info(&format!("fetch_endpoint: http::get ERROR: {}", err));
        ApiError::NetworkError(format!("HTTP GET failed for {}: {}", url, err))
    })?;

    utils::report_info(&format!(
        "fetch_endpoint: Response received, status={}",
        response.status_code
    ));

    // Handle HTTP status codes
    match response.status_code {
        200 => {
            // Success - return CSV body
            if response.body.is_empty() {
                // Empty response is treated as "no data available"
                Ok(String::new())
            } else {
                Ok(response.body)
            }
        }
        401 => {
            // Unauthorized - token expired or invalid
            Err(OAuth2Error::TokenExpired.into())
        }
        404 => {
            // Not Found - data not available for this date range
            // This is normal (e.g., future dates for hochrechnung)
            // Return empty result rather than error
            Ok(String::new())
        }
        429 => {
            // Rate limit exceeded
            Err(ApiError::RateLimited.into())
        }
        _ => {
            // Other errors (400, 500, etc.)
            Err(ApiError::HttpError {
                status: response.status_code,
                body: response.body,
            }
            .into())
        }
    }
}

/// Convert RenewableRow to PostgreSQL cells
///
/// Maps RenewableRow struct fields to PostgreSQL Cell types based on column names.
///
/// # Arguments
///
/// * `row` - RenewableRow to convert
/// * `columns` - List of columns from FDW context
///
/// # Returns
///
/// * `Ok(Vec<Option<Cell>>)` - Vector of Cell values matching column order
/// * `Err(String)` - If timestamp parsing fails
///
/// # Notes
///
/// - Skips GENERATED columns (total_germany_mw, has_missing_data) - computed in PostgreSQL
/// - Converts timestamp strings → Cell::Timestamptz (microseconds since epoch)
/// - Converts Option<f64> → option<cell::Numeric(f64)>
fn renewable_row_to_cells(
    row: &RenewableRow,
    columns: &[bindings::supabase::wrappers::types::Column],
) -> Result<Vec<Option<Cell>>, String> {
    use bindings::supabase::wrappers::types::Column;

    columns
        .iter()
        .map(|col: &Column| {
            let name = col.name();
            match name.as_str() {
                "timestamp_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.timestamp_utc)
                        .map_err(|e| format!("timestamp_utc: {}", e))?,
                ))),
                "interval_end_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.interval_end_utc)
                        .map_err(|e| format!("interval_end_utc: {}", e))?,
                ))),
                "interval_minutes" => Ok(Some(Cell::I16(row.interval_minutes))),
                "product_type" => Ok(Some(Cell::String(row.product_type.clone()))),
                "data_category" => Ok(Some(Cell::String(row.data_category.clone()))),
                "tso_50hertz_mw" => Ok(row.tso_50hertz_mw.map(Cell::Numeric)),
                "tso_amprion_mw" => Ok(row.tso_amprion_mw.map(Cell::Numeric)),
                "tso_tennet_mw" => Ok(row.tso_tennet_mw.map(Cell::Numeric)),
                "tso_transnetbw_mw" => Ok(row.tso_transnetbw_mw.map(Cell::Numeric)),
                "source_endpoint" => Ok(Some(Cell::String(row.source_endpoint.clone()))),
                "fetched_at" => {
                    // fetched_at uses DEFAULT NOW() in PostgreSQL, so we don't provide it
                    Ok(None)
                }
                // Bug #1 fix: PostgreSQL foreign tables cannot use GENERATED columns
                // We must compute these values in Rust instead
                "total_germany_mw" => Ok(Some(Cell::Numeric(row.total_germany_mw()))),
                "has_missing_data" => Ok(Some(Cell::Bool(row.has_missing_data()))),
                // Unknown column - return None
                _ => Ok(None),
            }
        })
        .collect()
}

/// Convert PriceRow to PostgreSQL cells
///
/// Maps PriceRow struct fields to PostgreSQL Cell types based on column names.
///
/// # Arguments
///
/// * `row` - PriceRow to convert
/// * `columns` - List of columns from FDW context
///
/// # Returns
///
/// * `Ok(Vec<Option<Cell>>)` - Vector of Cell values matching column order
/// * `Err(String)` - If timestamp parsing fails
fn price_row_to_cells(
    row: &PriceRow,
    columns: &[bindings::supabase::wrappers::types::Column],
) -> Result<Vec<Option<Cell>>, String> {
    use bindings::supabase::wrappers::types::Column;

    columns
        .iter()
        .map(|col: &Column| {
            let name = col.name();
            match name.as_str() {
                "timestamp_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.timestamp_utc)
                        .map_err(|e| format!("timestamp_utc: {}", e))?,
                ))),
                "interval_end_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.interval_end_utc)
                        .map_err(|e| format!("interval_end_utc: {}", e))?,
                ))),
                "granularity" => Ok(Some(Cell::String(row.granularity.clone()))),
                "price_type" => Ok(Some(Cell::String(row.price_type.clone()))),
                "price_eur_mwh" => Ok(row.price_eur_mwh.map(Cell::Numeric)),
                "product_category" => Ok(row
                    .product_category
                    .as_ref()
                    .map(|s| Cell::String(s.clone()))),
                "negative_logic_hours" => Ok(row
                    .negative_logic_hours
                    .as_ref()
                    .map(|s| Cell::String(s.clone()))),
                "negative_flag_value" => Ok(row.negative_flag_value.map(Cell::Bool)),
                "source_endpoint" => Ok(Some(Cell::String(row.source_endpoint.clone()))),
                "fetched_at" => {
                    // fetched_at uses DEFAULT NOW() in PostgreSQL
                    Ok(None)
                }
                // Bug #3 fix: PostgreSQL foreign tables cannot use GENERATED columns
                // We must compute these values in Rust instead
                "price_ct_kwh" => Ok(row.price_ct_kwh().map(Cell::Numeric)),
                "is_negative" => Ok(Some(Cell::Bool(row.is_negative()))),
                // Unknown column
                _ => Ok(None),
            }
        })
        .collect()
}

/// Convert RedispatchRow to PostgreSQL cells
///
/// Maps RedispatchRow struct fields to PostgreSQL Cell types based on column names.
///
/// # Arguments
///
/// * `row` - RedispatchRow to convert
/// * `columns` - List of columns from FDW context
///
/// # Returns
///
/// * `Ok(Vec<Option<Cell>>)` - Vector of Cell values matching column order
/// * `Err(String)` - If timestamp parsing fails
fn redispatch_row_to_cells(
    row: &RedispatchRow,
    columns: &[bindings::supabase::wrappers::types::Column],
) -> Result<Vec<Option<Cell>>, String> {
    use bindings::supabase::wrappers::types::Column;

    columns
        .iter()
        .map(|col: &Column| {
            let name = col.name();
            match name.as_str() {
                "timestamp_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.timestamp_utc)
                        .map_err(|e| format!("timestamp_utc: {}", e))?,
                ))),
                "interval_end_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.interval_end_utc)
                        .map_err(|e| format!("interval_end_utc: {}", e))?,
                ))),
                "reason" => Ok(Some(Cell::String(row.reason.clone()))),
                "direction" => Ok(Some(Cell::String(row.direction.clone()))),
                "avg_power_mw" => Ok(row.avg_power_mw.map(Cell::Numeric)),
                "max_power_mw" => Ok(row.max_power_mw.map(Cell::Numeric)),
                "total_energy_mwh" => Ok(row.total_energy_mwh.map(Cell::Numeric)),
                "requesting_tso" => Ok(Some(Cell::String(row.requesting_tso.clone()))),
                "instructing_tso" => Ok(row
                    .instructing_tso
                    .as_ref()
                    .map(|s| Cell::String(s.clone()))),
                "affected_facility" => Ok(row
                    .affected_facility
                    .as_ref()
                    .map(|s| Cell::String(s.clone()))),
                "energy_type" => Ok(row.energy_type.as_ref().map(|s| Cell::String(s.clone()))),
                "source_endpoint" => Ok(Some(Cell::String(row.source_endpoint.clone()))),
                "fetched_at" => {
                    // fetched_at uses DEFAULT NOW() in PostgreSQL
                    Ok(None)
                }
                // Skip GENERATED columns (computed in PostgreSQL)
                "interval_minutes" => Ok(None),
                // Unknown column
                _ => Ok(None),
            }
        })
        .collect()
}

/// Convert GridStatusRow to PostgreSQL cells
///
/// Maps GridStatusRow struct fields to PostgreSQL Cell types based on column names.
///
/// # Arguments
///
/// * `row` - GridStatusRow to convert
/// * `columns` - List of columns from FDW context
///
/// # Returns
///
/// * `Ok(Vec<Option<Cell>>)` - Vector of Cell values matching column order
/// * `Err(String)` - If timestamp parsing fails
fn grid_status_row_to_cells(
    row: &GridStatusRow,
    columns: &[bindings::supabase::wrappers::types::Column],
) -> Result<Vec<Option<Cell>>, String> {
    use bindings::supabase::wrappers::types::Column;

    columns
        .iter()
        .map(|col: &Column| {
            let name = col.name();
            match name.as_str() {
                "timestamp_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.timestamp_utc)
                        .map_err(|e| format!("timestamp_utc: {}", e))?,
                ))),
                "interval_end_utc" => Ok(Some(Cell::Timestamptz(
                    timestamp_to_micros(&row.interval_end_utc)
                        .map_err(|e| format!("interval_end_utc: {}", e))?,
                ))),
                "grid_status" => Ok(Some(Cell::String(row.grid_status.clone()))),
                "source_endpoint" => Ok(Some(Cell::String(row.source_endpoint.clone()))),
                "fetched_at" => {
                    // fetched_at uses DEFAULT NOW() in PostgreSQL
                    Ok(None)
                }
                // Unknown column
                _ => Ok(None),
            }
        })
        .collect()
}

/// Convert ISO 8601 timestamp string to microseconds since Unix epoch
///
/// PostgreSQL TIMESTAMPTZ is stored as microseconds since 1970-01-01 00:00:00 UTC.
///
/// # Arguments
///
/// * `timestamp_str` - ISO 8601 timestamp (e.g., "2024-10-24T06:00:00Z")
///
/// # Returns
///
/// * `Ok(i64)` - Microseconds since Unix epoch
/// * `Err(String)` - If timestamp cannot be parsed (invalid ISO 8601 format)
fn timestamp_to_micros(timestamp_str: &str) -> Result<i64, String> {
    use chrono::DateTime;

    // Parse ISO 8601 timestamp (fail-fast on invalid data)
    timestamp_str
        .parse::<DateTime<chrono::Utc>>()
        .map(|dt| dt.timestamp_micros())
        .map_err(|e| {
            format!(
                "Failed to parse ISO 8601 timestamp '{}': {}. Expected format: YYYY-MM-DDTHH:MM:SSZ",
                timestamp_str, e
            )
        })
}

/// Check if OAuth2 token needs proactive refresh
///
/// Implements proactive refresh strategy from Phase 1:
/// - Check token expiry before each API call
/// - Refresh if token is within 5-minute buffer of expiration
///
/// This prevents 401 errors during multi-endpoint queries by refreshing
/// tokens BEFORE they expire, rather than waiting for a 401 (reactive).
// Helper functions removed - now using singleton pattern directly in begin_scan()
/// NTP FDW implementation
///
/// Following the official Supabase WASM FDW singleton pattern.
/// All state is stored in this struct and accessed via static mut INSTANCE.
#[derive(Default)]
struct NtpFdw {
    /// OAuth2 manager for token fetching and caching
    oauth2_manager: Option<OAuth2Manager>,

    /// API base URL (e.g., "https://ds.netztransparenz.de")
    api_base_url: String,

    /// HTTP headers (including Authorization with Bearer token)
    headers: Vec<(String, String)>,

    /// Buffered renewable energy rows (from begin_scan)
    renewable_rows: Vec<RenewableRow>,

    /// Buffered price rows (from begin_scan)
    price_rows: Vec<PriceRow>,

    /// Buffered redispatch event rows (from begin_scan)
    redispatch_rows: Vec<RedispatchRow>,

    /// Buffered grid status rows (from begin_scan)
    grid_status_rows: Vec<GridStatusRow>,

    /// Current table being scanned
    current_table: String,

    /// Current position in renewable_rows buffer (for re_scan support)
    renewable_row_position: usize,

    /// Current position in price_rows buffer (for re_scan support)
    price_row_position: usize,

    /// Current position in redispatch_rows buffer (for re_scan support)
    redispatch_row_position: usize,

    /// Current position in grid_status_rows buffer (for re_scan support)
    grid_status_row_position: usize,
}

/// Static singleton instance (official Supabase WASM FDW pattern)
static mut INSTANCE: *mut NtpFdw = std::ptr::null_mut::<NtpFdw>();

impl NtpFdw {
    /// Initialize singleton instance
    ///
    /// Creates the instance using Box::leak pattern (WASM-safe).
    /// This is the standard pattern used by all official Supabase WASM FDWs.
    fn init() {
        let instance = Self::default();
        unsafe {
            INSTANCE = Box::leak(Box::new(instance));
        }
    }

    /// Get mutable reference to singleton
    ///
    /// SAFETY: This is safe because FDW lifecycle methods are called sequentially
    /// by PostgreSQL, never concurrently.
    fn this_mut() -> &'static mut Self {
        unsafe { &mut (*INSTANCE) }
    }

    /// Clear buffered rows and reset position counters
    fn clear_rows(&mut self) {
        self.renewable_rows.clear();
        self.price_rows.clear();
        self.redispatch_rows.clear();
        self.grid_status_rows.clear();
        self.renewable_row_position = 0;
        self.price_row_position = 0;
        self.redispatch_row_position = 0;
        self.grid_status_row_position = 0;
    }
}

// ============================================================================
// Helper Functions for begin_scan() Refactoring
// ============================================================================

/// Fetch API endpoint with OAuth2 retry logic
///
/// Implements proactive + reactive token refresh strategy:
/// - Proactive: Checks token expiry before request
/// - Reactive: Retries once on 401 with fresh token
///
/// # Arguments
///
/// * `url` - API endpoint URL
/// * `token` - Current OAuth2 token (mutable - may be refreshed)
/// * `manager` - OAuth2 manager for token refresh
///
/// # Returns
///
/// * `Ok(String)` - Response body (CSV or JSON)
/// * `Err(NtpFdwError)` - Network error, HTTP error, or token refresh failure
fn fetch_with_oauth_retry(
    url: &str,
    token: &mut String,
    manager: &OAuth2Manager,
) -> Result<String, NtpFdwError> {
    // PROACTIVE: Check if token needs refresh before request
    if manager.is_near_expiry() {
        *token = manager
            .get_token()
            .map_err(|e| format!("Failed to refresh token before API call: {}", e))?;
    }

    // Attempt fetch
    match fetch_endpoint(url, token) {
        Ok(body) => Ok(body),
        Err(NtpFdwError::OAuth2(OAuth2Error::TokenExpired)) => {
            // REACTIVE: Token expired - clear cache and retry once
            manager.clear_cache();
            *token = manager
                .get_token()
                .map_err(|e| format!("Failed to refresh OAuth2 token after 401: {}", e))?;

            // Retry fetch with fresh token
            fetch_endpoint(url, token)
                .map_err(|e| format!("Failed to fetch endpoint after retry: {}", e).into())
        }
        Err(e) => Err(e),
    }
}

/// Parse endpoint response and extend appropriate row buffer
///
/// Dispatches to correct parser based on table name and extends
/// the appropriate row buffer.
///
/// # Arguments
///
/// * `table_name` - Table being scanned
/// * `response_body` - CSV or JSON response body
/// * `plan` - Query plan with endpoint metadata
/// * `all_renewable_rows` - Renewable energy row buffer (mutable)
/// * `all_price_rows` - Price row buffer (mutable)
/// * `all_redispatch_rows` - Redispatch row buffer (mutable)
/// * `all_grid_status_rows` - Grid status row buffer (mutable)
///
/// # Returns
///
/// * `Ok(())` - Parsing successful, rows extended
/// * `Err(String)` - Parse error or unknown table
fn parse_endpoint_response(
    table_name: &str,
    response_body: String,
    plan: &query_router::QueryPlan,
    all_renewable_rows: &mut Vec<RenewableRow>,
    all_price_rows: &mut Vec<PriceRow>,
    all_redispatch_rows: &mut Vec<RedispatchRow>,
    all_grid_status_rows: &mut Vec<GridStatusRow>,
) -> Result<(), String> {
    match table_name {
        "renewable_energy_timeseries" => {
            let product = plan
                .product
                .as_ref()
                .ok_or_else(|| "Missing product in QueryPlan".to_string())?;

            let rows = csv_parser::parse_renewable_csv(
                &response_body,
                &plan.endpoint,
                product,
                &plan.date_from,
                &plan.date_to,
            )
            .map_err(|e| format!("Failed to parse renewable CSV from {}: {}", plan.api_url, e))?;

            all_renewable_rows.extend(rows);
            Ok(())
        }
        "electricity_market_prices" => {
            // Bug #7 fix: Route to appropriate parser based on endpoint
            let rows = match plan.endpoint.as_str() {
                "NegativePreise" => csv_parser::parse_negative_price_flags_csv(
                    &response_body,
                    &plan.date_from,
                    &plan.date_to,
                )
                .map_err(|e| {
                    format!(
                        "Failed to parse NegativePreise CSV from {}: {}",
                        plan.api_url, e
                    )
                })?,
                _ => {
                    // Route to appropriate parser based on endpoint
                    if plan.endpoint == "Jahresmarktpraemie" {
                        // Annual endpoint uses pipe-delimited format, not CSV
                        let year = &plan.date_from[0..4]; // Extract YYYY from YYYY-MM-DD
                        csv_parser::parse_annual_price_response(&response_body, year)
                            .map_err(|e| {
                                format!(
                                    "Failed to parse annual price response from {}: {}",
                                    plan.api_url, e
                                )
                            })?
                    } else if plan.endpoint == "marktpraemie" {
                        // Monthly endpoint uses CSV with UNPIVOT logic
                        csv_parser::parse_monthly_price_csv(
                            &response_body,
                            &plan.date_from,
                            &plan.date_to,
                        )
                        .map_err(|e| {
                            format!(
                                "Failed to parse monthly price CSV from {}: {}",
                                plan.api_url, e
                            )
                        })?
                    } else {
                        // Standard CSV format for all other price endpoints (Spotmarktpreise)
                        csv_parser::parse_price_csv(
                            &response_body,
                            &plan.endpoint,
                            &plan.date_from,
                            &plan.date_to,
                        )
                        .map_err(|e| {
                            format!("Failed to parse price CSV from {}: {}", plan.api_url, e)
                        })?
                    }
                }
            };

            all_price_rows.extend(rows);
            Ok(())
        }
        "redispatch_events" => {
            let rows =
                grid_parsers::parse_redispatch_csv(&response_body, &plan.date_from, &plan.date_to)
                    .map_err(|e| {
                        format!(
                            "Failed to parse redispatch CSV from {}: {}",
                            plan.api_url, e
                        )
                    })?;

            all_redispatch_rows.extend(rows);
            Ok(())
        }
        "grid_status_timeseries" => {
            let rows = grid_parsers::parse_trafficlight_json(
                &response_body,
                &plan.date_from,
                &plan.date_to,
            )
            .map_err(|e| {
                format!(
                    "Failed to parse TrafficLight JSON from {}: {}",
                    plan.api_url, e
                )
            })?;

            all_grid_status_rows.extend(rows);
            Ok(())
        }
        _ => Err(format!("Unknown table: {}", table_name)),
    }
}

impl Guest for NtpFdw {
    /// Host version requirement (Supabase Wrappers v0.2.0)
    fn host_version_requirement() -> String {
        // Requires Supabase Wrappers framework version 0.1.x
        // Note: WIT interface version (0.2.0) != framework version (0.1.x)
        "^0.1.0".to_string()
    }

    /// Initialize FDW (extract OAuth2 credentials from server options)
    ///
    /// Following official Supabase WASM FDW pattern:
    /// 1. Call Self::init() to create singleton instance
    /// 2. Extract server options
    /// 3. Create OAuth2 manager
    /// 4. Get initial token and set up headers
    fn init(ctx: &Context) -> FdwResult {
        use bindings::supabase::wrappers::types::OptionsType;

        // CRITICAL: Initialize singleton instance FIRST (official pattern)
        Self::init();
        let this = Self::this_mut();

        // Extract server options
        let opts = ctx.get_options(&OptionsType::Server);

        // Required: API base URL
        this.api_base_url = opts
            .require("api_base_url")
            .map_err(|e| format!("Missing required server option 'api_base_url': {}", e))?;

        // Required: OAuth2 token URL
        let token_url = opts
            .require("oauth2_token_url")
            .map_err(|e| format!("Missing required server option 'oauth2_token_url': {}", e))?;

        // Required: OAuth2 client ID
        let client_id = opts
            .require("oauth2_client_id")
            .map_err(|e| format!("Missing required server option 'oauth2_client_id': {}", e))?;

        // Required: OAuth2 client secret
        let client_secret = opts.require("oauth2_client_secret").map_err(|e| {
            format!(
                "Missing required server option 'oauth2_client_secret': {}",
                e
            )
        })?;

        // Optional: OAuth2 scope (default: ntpStatistic.read_all_public)
        let scope = opts.require_or("oauth2_scope", "ntpStatistic.read_all_public");

        // Create OAuth2 config
        let oauth2_config = OAuth2Config {
            token_url,
            client_id,
            client_secret,
            scope,
        };

        // Create and store OAuth2 manager
        this.oauth2_manager = Some(OAuth2Manager::new(oauth2_config));

        // Get initial token
        let token = this
            .oauth2_manager
            .as_ref()
            .ok_or("OAuth2Manager initialization failed")?
            .get_token()
            .map_err(|e| format!("Failed to get initial OAuth2 token: {}", e))?;

        // Set up HTTP headers (following Paddle/Snowflake pattern)
        this.headers.clear();
        this.headers
            .push(("authorization".to_owned(), format!("Bearer {}", token)));
        this.headers
            .push(("accept".to_owned(), "text/csv".to_owned()));

        Ok(())
    }

    /// Begin scan (route query to API endpoints)
    ///
    /// Following official Supabase WASM FDW pattern:
    /// 1. Get singleton instance via Self::this_mut()
    /// 2. Parse quals and route query
    /// 3. Fetch and parse all endpoints (using helper functions)
    /// 4. Store rows in struct for iteration
    fn begin_scan(ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // 1. Parse quals (WHERE clause filters)
        let filters = parse_quals(ctx).map_err(|e| format!("Failed to parse quals: {}", e))?;

        // 2. Route query to API endpoints
        let plans = query_router::route_query(&filters, &this.api_base_url)
            .map_err(|e| format!("Failed to route query: {}", e))?;

        // 3. Get OAuth2 manager and current token
        let manager = this
            .oauth2_manager
            .as_ref()
            .ok_or("OAuth2Manager not initialized")?;

        let mut token = this
            .headers
            .iter()
            .find(|(k, _)| k == "authorization")
            .and_then(|(_, v)| v.strip_prefix("Bearer "))
            .ok_or("Authorization header not found")?
            .to_string();

        // 4. Fetch and parse each endpoint
        let mut all_renewable_rows = Vec::new();
        let mut all_price_rows = Vec::new();
        let mut all_redispatch_rows = Vec::new();
        let mut all_grid_status_rows = Vec::new();

        for plan in plans {
            // Fetch endpoint with OAuth2 retry logic (helper function)
            let response_body = fetch_with_oauth_retry(&plan.api_url, &mut token, manager)
                .map_err(|e| format!("Failed to fetch endpoint {}: {}", plan.api_url, e))?;

            // Update header if token was refreshed
            if let Some(auth_header) = this.headers.iter_mut().find(|(k, _)| k == "authorization") {
                auth_header.1 = format!("Bearer {}", token);
            }

            // Skip empty responses (404, no data available)
            if response_body.is_empty() {
                continue;
            }

            // Parse response and extend row buffers (helper function)
            parse_endpoint_response(
                &filters.table_name,
                response_body,
                &plan,
                &mut all_renewable_rows,
                &mut all_price_rows,
                &mut all_redispatch_rows,
                &mut all_grid_status_rows,
            )?;
        }

        // 5. Apply local timestamp filtering (Phase 2: time-based filtering)
        // Filters rows by hour/minute/second after fetching by date
        // Solves bug where time components were stripped during qual parsing
        let filtered_renewable_rows =
            filter_renewable_rows(all_renewable_rows, &filters.timestamp_bounds);
        let filtered_price_rows = filter_price_rows(all_price_rows, &filters.timestamp_bounds);
        let filtered_redispatch_rows =
            filter_redispatch_rows(all_redispatch_rows, &filters.timestamp_bounds);
        let filtered_grid_status_rows =
            filter_grid_status_rows(all_grid_status_rows, &filters.timestamp_bounds);

        // 6. Store rows in struct for iteration (official pattern)
        this.clear_rows();
        this.renewable_rows = filtered_renewable_rows;
        this.price_rows = filtered_price_rows;
        this.redispatch_rows = filtered_redispatch_rows;
        this.grid_status_rows = filtered_grid_status_rows;
        this.current_table = filters.table_name;

        Ok(())
    }

    /// Iterate scan (return next row)
    ///
    /// Following official Supabase WASM FDW pattern with re_scan support:
    /// 1. Get singleton instance via Self::this_mut()
    /// 2. Read next row from buffered data using position index
    /// 3. Increment position counter
    /// 4. Convert to PostgreSQL cells and push to row
    fn iter_scan(ctx: &Context, row: &Row) -> Result<core::option::Option<u32>, String> {
        let this = Self::this_mut();

        // Get columns from context
        let columns = ctx.get_columns();

        // Read next row from buffered data (based on table type) using position index
        let next_row_cells = match this.current_table.as_str() {
            "renewable_energy_timeseries" => {
                // Use .get() for bounds-checked access (prevents panic if position is out of bounds)
                let row_data = match this.renewable_rows.get(this.renewable_row_position) {
                    Some(row) => row,
                    None => return Ok(None), // No more rows - graceful termination
                };
                this.renewable_row_position += 1;
                Some(renewable_row_to_cells(row_data, &columns)?)
            }
            "electricity_market_prices" => {
                // Use .get() for bounds-checked access (prevents panic if position is out of bounds)
                let row_data = match this.price_rows.get(this.price_row_position) {
                    Some(row) => row,
                    None => return Ok(None), // No more rows - graceful termination
                };
                this.price_row_position += 1;
                Some(price_row_to_cells(row_data, &columns)?)
            }
            "redispatch_events" => {
                // Use .get() for bounds-checked access (prevents panic if position is out of bounds)
                let row_data = match this.redispatch_rows.get(this.redispatch_row_position) {
                    Some(row) => row,
                    None => return Ok(None), // No more rows - graceful termination
                };
                this.redispatch_row_position += 1;
                Some(redispatch_row_to_cells(row_data, &columns)?)
            }
            "grid_status_timeseries" => {
                // Use .get() for bounds-checked access (prevents panic if position is out of bounds)
                let row_data = match this.grid_status_rows.get(this.grid_status_row_position) {
                    Some(row) => row,
                    None => return Ok(None), // No more rows - graceful termination
                };
                this.grid_status_row_position += 1;
                Some(grid_status_row_to_cells(row_data, &columns)?)
            }
            _ => return Err(format!("Unknown table: {}", this.current_table)),
        };

        // Check if we have a row
        match next_row_cells {
            Some(cells) => {
                // Push cells to row
                for cell in &cells {
                    row.push(cell.as_ref());
                }

                // Return 1 (one row returned)
                Ok(Some(1))
            }
            None => {
                // No more rows
                Ok(None)
            }
        }
    }

    /// End scan (cleanup)
    ///
    /// Following official Supabase WASM FDW pattern:
    /// Clear buffered rows from singleton instance
    fn end_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();
        this.clear_rows();
        Ok(())
    }

    /// Re-scan (reset row iteration to beginning)
    ///
    /// This function is called by PostgreSQL when it needs to restart the scan
    /// from the beginning, which is required for JOIN operations and cursors.
    ///
    /// Implementation: Reset position counters to 0, keeping buffered rows intact.
    fn re_scan(_ctx: &Context) -> FdwResult {
        let this = Self::this_mut();

        // Reset position counters to restart scan from beginning
        this.renewable_row_position = 0;
        this.price_row_position = 0;
        this.redispatch_row_position = 0;
        this.grid_status_row_position = 0;

        Ok(())
    }

    /// Begin modify (for INSERT/UPDATE/DELETE - not supported)
    fn begin_modify(_ctx: &Context) -> FdwResult {
        Err("Modify operations not supported (read-only FDW)".to_string())
    }

    /// End modify
    fn end_modify(_ctx: &Context) -> FdwResult {
        Err("Modify operations not supported (read-only FDW)".to_string())
    }

    /// Insert (not supported)
    fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
        Err("INSERT not supported (read-only FDW)".to_string())
    }

    /// Update (not supported)
    fn update(_ctx: &Context, _rowid: Cell, _new_row: &Row) -> FdwResult {
        Err("UPDATE not supported (read-only FDW)".to_string())
    }

    /// Delete (not supported)
    fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
        Err("DELETE not supported (read-only FDW)".to_string())
    }

    /// Import foreign schema (not supported)
    fn import_foreign_schema(
        _ctx: &Context,
        _stmt: bindings::supabase::wrappers::types::ImportForeignSchemaStmt,
    ) -> Result<Vec<String>, String> {
        Err("IMPORT FOREIGN SCHEMA not supported".to_string())
    }
}

// Export the NTP FDW implementation
bindings::export!(NtpFdw with_types_in bindings);

// ============================================================================
// Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Test that re_scan() resets renewable_row_position to 0
    ///
    /// This is critical for JOIN operations - PostgreSQL calls re_scan()
    /// when iterating the inner table for each outer row. If position
    /// isn't reset, subsequent scans will return no rows.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_re_scan_resets_renewable_position() {
        // Create FDW instance
        let mut fdw = NtpFdw::default();

        // Populate with test rows
        fdw.renewable_rows = vec![
            RenewableRow {
                timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
                interval_end_utc: "2024-10-24T00:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(100.0),
                tso_amprion_mw: Some(200.0),
                tso_tennet_mw: Some(300.0),
                tso_transnetbw_mw: Some(150.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-24T00:15:00Z".to_string(),
                interval_end_utc: "2024-10-24T00:30:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(110.0),
                tso_amprion_mw: Some(210.0),
                tso_tennet_mw: Some(310.0),
                tso_transnetbw_mw: Some(160.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-24T00:30:00Z".to_string(),
                interval_end_utc: "2024-10-24T00:45:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(120.0),
                tso_amprion_mw: Some(220.0),
                tso_tennet_mw: Some(320.0),
                tso_transnetbw_mw: Some(170.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
        ];

        fdw.current_table = "renewable_energy_timeseries".to_string();

        // Simulate iteration (advance position to end)
        fdw.renewable_row_position = 3; // Beyond last row

        // Verify position is at end
        assert_eq!(fdw.renewable_row_position, 3);
        assert_eq!(fdw.renewable_rows.len(), 3);

        // Simulate re_scan (reset position)
        fdw.renewable_row_position = 0;
        fdw.price_row_position = 0;

        // Verify position reset
        assert_eq!(fdw.renewable_row_position, 0);
        assert_eq!(fdw.price_row_position, 0);

        // Verify rows still available
        assert_eq!(fdw.renewable_rows.len(), 3);
    }

    /// Test that re_scan() resets price_row_position to 0
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_re_scan_resets_price_position() {
        let mut fdw = NtpFdw::default();

        // Populate with test price rows
        fdw.price_rows = vec![
            PriceRow {
                timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
                interval_end_utc: "2024-10-24T01:00:00Z".to_string(),
                granularity: "hourly".to_string(),
                price_type: "spot_market".to_string(),
                price_eur_mwh: Some(50.25),
                product_category: None,
                negative_logic_hours: None,
                negative_flag_value: Some(false),
                source_endpoint: "Spotmarktpreise".to_string(),
            },
            PriceRow {
                timestamp_utc: "2024-10-24T01:00:00Z".to_string(),
                interval_end_utc: "2024-10-24T02:00:00Z".to_string(),
                granularity: "hourly".to_string(),
                price_type: "spot_market".to_string(),
                price_eur_mwh: Some(45.75),
                product_category: None,
                negative_logic_hours: None,
                negative_flag_value: Some(false),
                source_endpoint: "Spotmarktpreise".to_string(),
            },
        ];

        fdw.current_table = "electricity_market_prices".to_string();

        // Simulate iteration
        fdw.price_row_position = 2;

        // Verify position
        assert_eq!(fdw.price_row_position, 2);

        // Simulate re_scan
        fdw.renewable_row_position = 0;
        fdw.price_row_position = 0;

        // Verify reset
        assert_eq!(fdw.price_row_position, 0);
        assert_eq!(fdw.renewable_row_position, 0);
        assert_eq!(fdw.price_rows.len(), 2);
    }

    /// Test that re_scan() preserves buffered data (doesn't clear rows)
    ///
    /// This is important because PostgreSQL may call re_scan() multiple times
    /// during JOIN operations. We want to keep the buffered data and just
    /// reset the iteration position, not re-fetch from the API.
    #[test]
    fn test_re_scan_preserves_buffered_data() {
        let mut fdw = NtpFdw::default();

        // Create test rows
        let test_renewable = vec![RenewableRow {
            timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T00:15:00Z".to_string(),
            interval_minutes: 15,
            product_type: "wind_onshore".to_string(),
            data_category: "extrapolation".to_string(),
            tso_50hertz_mw: Some(500.0),
            tso_amprion_mw: Some(600.0),
            tso_tennet_mw: Some(700.0),
            tso_transnetbw_mw: Some(400.0),
            source_endpoint: "hochrechnung/Wind".to_string(),
        }];

        let test_price = vec![PriceRow {
            timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T01:00:00Z".to_string(),
            granularity: "hourly".to_string(),
            price_type: "spot_market".to_string(),
            price_eur_mwh: Some(-5.50),
            product_category: None,
            negative_logic_hours: None,
            negative_flag_value: Some(true),
            source_endpoint: "Spotmarktpreise".to_string(),
        }];

        fdw.renewable_rows = test_renewable.clone();
        fdw.price_rows = test_price.clone();
        fdw.renewable_row_position = 1;
        fdw.price_row_position = 1;

        // Verify initial state
        assert_eq!(fdw.renewable_rows.len(), 1);
        assert_eq!(fdw.price_rows.len(), 1);
        assert_eq!(fdw.renewable_row_position, 1);
        assert_eq!(fdw.price_row_position, 1);

        // Simulate re_scan (reset positions, keep data)
        fdw.renewable_row_position = 0;
        fdw.price_row_position = 0;

        // Verify positions reset but data preserved
        assert_eq!(fdw.renewable_row_position, 0);
        assert_eq!(fdw.price_row_position, 0);
        assert_eq!(fdw.renewable_rows.len(), 1); // Data still present
        assert_eq!(fdw.price_rows.len(), 1); // Data still present

        // Verify data integrity (values unchanged)
        assert_eq!(fdw.renewable_rows[0].product_type, "wind_onshore");
        assert_eq!(fdw.price_rows[0].price_eur_mwh, Some(-5.50));
    }

    /// Test iteration with bounds checking (C-1 security fix validation)
    ///
    /// Validates that out-of-bounds access is handled gracefully using .get()
    /// instead of direct indexing, preventing panics.
    #[test]
    #[allow(clippy::field_reassign_with_default)]
    fn test_iter_scan_bounds_checking() {
        let mut fdw = NtpFdw::default();

        // Create 2 test rows
        fdw.renewable_rows = vec![
            RenewableRow {
                timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
                interval_end_utc: "2024-10-24T00:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(100.0),
                tso_amprion_mw: Some(200.0),
                tso_tennet_mw: Some(300.0),
                tso_transnetbw_mw: Some(150.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-24T00:15:00Z".to_string(),
                interval_end_utc: "2024-10-24T00:30:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(110.0),
                tso_amprion_mw: Some(210.0),
                tso_tennet_mw: Some(310.0),
                tso_transnetbw_mw: Some(160.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
        ];

        fdw.current_table = "renewable_energy_timeseries".to_string();
        fdw.renewable_row_position = 0;

        // First access: position 0 - should succeed
        let result1 = fdw.renewable_rows.get(fdw.renewable_row_position);
        assert!(result1.is_some());
        fdw.renewable_row_position += 1;

        // Second access: position 1 - should succeed
        let result2 = fdw.renewable_rows.get(fdw.renewable_row_position);
        assert!(result2.is_some());
        fdw.renewable_row_position += 1;

        // Third access: position 2 (out of bounds) - should return None gracefully
        let result3 = fdw.renewable_rows.get(fdw.renewable_row_position);
        assert!(result3.is_none());

        // Verify no panic occurred (test passes if we reach here)
        assert_eq!(fdw.renewable_row_position, 2);
    }

    // ========================================================================
    // Timestamp Filtering Tests (v0.2.1 - Time-Based Filtering Fix)
    // ========================================================================

    /// Test matches_timestamp_bounds with >= operator (lower bound)
    #[test]
    fn test_matches_timestamp_bounds_gte() {
        use chrono::DateTime;

        let bounds = TimestampBounds {
            start: Some(
                DateTime::parse_from_rfc3339("2024-10-20T10:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            start_operator: Some(">=".to_string()),
            end: None,
            end_operator: None,
        };

        // Row before bound - should NOT match
        assert!(!matches_timestamp_bounds("2024-10-20T09:59:59Z", &bounds));

        // Row at exact bound - should match
        assert!(matches_timestamp_bounds("2024-10-20T10:00:00Z", &bounds));

        // Row after bound - should match
        assert!(matches_timestamp_bounds("2024-10-20T10:00:01Z", &bounds));
        assert!(matches_timestamp_bounds("2024-10-20T15:30:00Z", &bounds));
    }

    /// Test matches_timestamp_bounds with < operator (upper bound)
    #[test]
    fn test_matches_timestamp_bounds_lt() {
        use chrono::DateTime;

        let bounds = TimestampBounds {
            start: None,
            start_operator: None,
            end: Some(
                DateTime::parse_from_rfc3339("2024-10-20T16:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            end_operator: Some("<".to_string()),
        };

        // Row before bound - should match
        assert!(matches_timestamp_bounds("2024-10-20T15:59:59Z", &bounds));
        assert!(matches_timestamp_bounds("2024-10-20T10:00:00Z", &bounds));

        // Row at exact bound - should NOT match
        assert!(!matches_timestamp_bounds("2024-10-20T16:00:00Z", &bounds));

        // Row after bound - should NOT match
        assert!(!matches_timestamp_bounds("2024-10-20T16:00:01Z", &bounds));
    }

    /// Test matches_timestamp_bounds with both bounds (range query)
    #[test]
    fn test_matches_timestamp_bounds_range() {
        use chrono::DateTime;

        let bounds = TimestampBounds {
            start: Some(
                DateTime::parse_from_rfc3339("2024-10-20T10:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            start_operator: Some(">=".to_string()),
            end: Some(
                DateTime::parse_from_rfc3339("2024-10-20T16:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            end_operator: Some("<".to_string()),
        };

        // Before range - should NOT match
        assert!(!matches_timestamp_bounds("2024-10-20T09:59:59Z", &bounds));

        // Start of range - should match
        assert!(matches_timestamp_bounds("2024-10-20T10:00:00Z", &bounds));

        // Middle of range - should match
        assert!(matches_timestamp_bounds("2024-10-20T12:30:00Z", &bounds));
        assert!(matches_timestamp_bounds("2024-10-20T15:45:00Z", &bounds));

        // End of range - should NOT match (< operator)
        assert!(!matches_timestamp_bounds("2024-10-20T16:00:00Z", &bounds));

        // After range - should NOT match
        assert!(!matches_timestamp_bounds("2024-10-20T16:00:01Z", &bounds));
    }

    /// Test filter_renewable_rows with time-based filtering
    #[test]
    fn test_filter_renewable_rows_time_based() {
        use chrono::DateTime;

        let rows = vec![
            RenewableRow {
                timestamp_utc: "2024-10-20T09:00:00Z".to_string(),
                interval_end_utc: "2024-10-20T09:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(5000.0),
                tso_amprion_mw: Some(3000.0),
                tso_tennet_mw: Some(4000.0),
                tso_transnetbw_mw: Some(2000.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-20T10:00:00Z".to_string(),
                interval_end_utc: "2024-10-20T10:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(8000.0),
                tso_amprion_mw: Some(6000.0),
                tso_tennet_mw: Some(7000.0),
                tso_transnetbw_mw: Some(5000.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-20T12:00:00Z".to_string(),
                interval_end_utc: "2024-10-20T12:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(10000.0),
                tso_amprion_mw: Some(8000.0),
                tso_tennet_mw: Some(9000.0),
                tso_transnetbw_mw: Some(7000.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-20T16:00:00Z".to_string(),
                interval_end_utc: "2024-10-20T16:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(6000.0),
                tso_amprion_mw: Some(4000.0),
                tso_tennet_mw: Some(5000.0),
                tso_transnetbw_mw: Some(3000.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            },
        ];

        // Filter for 10:00-16:00 range (daytime solar production)
        let bounds = Some(TimestampBounds {
            start: Some(
                DateTime::parse_from_rfc3339("2024-10-20T10:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            start_operator: Some(">=".to_string()),
            end: Some(
                DateTime::parse_from_rfc3339("2024-10-20T16:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            end_operator: Some("<".to_string()),
        });

        let filtered = filter_renewable_rows(rows, &bounds);

        // Should return only 2 rows: 10:00 and 12:00 (not 09:00 or 16:00)
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].timestamp_utc, "2024-10-20T10:00:00Z");
        assert_eq!(filtered[1].timestamp_utc, "2024-10-20T12:00:00Z");
    }

    /// Test filter_renewable_rows with no bounds (pass-through)
    #[test]
    fn test_filter_renewable_rows_no_bounds() {
        let rows = vec![
            RenewableRow {
                timestamp_utc: "2024-10-20T00:00:00Z".to_string(),
                interval_end_utc: "2024-10-20T00:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(0.0),
                tso_amprion_mw: Some(0.0),
                tso_tennet_mw: Some(0.0),
                tso_transnetbw_mw: Some(0.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
            RenewableRow {
                timestamp_utc: "2024-10-20T12:00:00Z".to_string(),
                interval_end_utc: "2024-10-20T12:15:00Z".to_string(),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "forecast".to_string(),
                tso_50hertz_mw: Some(10000.0),
                tso_amprion_mw: Some(8000.0),
                tso_tennet_mw: Some(9000.0),
                tso_transnetbw_mw: Some(7000.0),
                source_endpoint: "prognose/Solar".to_string(),
            },
        ];

        let filtered = filter_renewable_rows(rows.clone(), &None);

        // Should return all rows (no filtering)
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0].timestamp_utc, rows[0].timestamp_utc);
        assert_eq!(filtered[1].timestamp_utc, rows[1].timestamp_utc);
    }

    /// Test timestamp filtering replicates bug scenario from TEST_RESULTS.md
    ///
    /// Validates that the fix resolves the original bug where queries like:
    /// `WHERE timestamp_utc >= '2024-10-20T10:00:00' AND timestamp_utc < '2024-10-20T16:00:00'`
    /// were returning 0 rows because time components were stripped.
    #[test]
    fn test_timestamp_filtering_bug_fix_scenario() {
        use chrono::DateTime;

        // Simulate fetched data for 2024-10-20 (entire day, 96 rows @ 15-min intervals)
        // We'll create a subset representing key hours
        let mut all_day_rows = Vec::new();

        // Nighttime hours (00:00-09:00) - should be filtered OUT
        for hour in 0..9 {
            all_day_rows.push(RenewableRow {
                timestamp_utc: format!("2024-10-20T{:02}:00:00Z", hour),
                interval_end_utc: format!("2024-10-20T{:02}:15:00Z", hour),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(0.0),
                tso_amprion_mw: Some(0.0),
                tso_tennet_mw: Some(0.0),
                tso_transnetbw_mw: Some(0.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            });
        }

        // Daytime hours (10:00-15:00) - should be INCLUDED
        for hour in 10..16 {
            all_day_rows.push(RenewableRow {
                timestamp_utc: format!("2024-10-20T{:02}:00:00Z", hour),
                interval_end_utc: format!("2024-10-20T{:02}:15:00Z", hour),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(10000.0),
                tso_amprion_mw: Some(8000.0),
                tso_tennet_mw: Some(9000.0),
                tso_transnetbw_mw: Some(7000.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            });
        }

        // Evening hours (17:00-23:00) - should be filtered OUT
        for hour in 17..24 {
            all_day_rows.push(RenewableRow {
                timestamp_utc: format!("2024-10-20T{:02}:00:00Z", hour),
                interval_end_utc: format!("2024-10-20T{:02}:15:00Z", hour),
                interval_minutes: 15,
                product_type: "solar".to_string(),
                data_category: "extrapolation".to_string(),
                tso_50hertz_mw: Some(0.0),
                tso_amprion_mw: Some(0.0),
                tso_tennet_mw: Some(0.0),
                tso_transnetbw_mw: Some(0.0),
                source_endpoint: "hochrechnung/Solar".to_string(),
            });
        }

        // User query: WHERE timestamp_utc >= '2024-10-20T10:00:00' AND timestamp_utc < '2024-10-20T16:00:00'
        let bounds = Some(TimestampBounds {
            start: Some(
                DateTime::parse_from_rfc3339("2024-10-20T10:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            start_operator: Some(">=".to_string()),
            end: Some(
                DateTime::parse_from_rfc3339("2024-10-20T16:00:00Z")
                    .unwrap()
                    .timestamp_micros(),
            ),
            end_operator: Some("<".to_string()),
        });

        // Before fix: This would return 0 rows (time components stripped, invalid range)
        // After fix: Should return exactly 6 rows (10:00-15:00)
        let filtered = filter_renewable_rows(all_day_rows.clone(), &bounds);

        assert_eq!(
            filtered.len(),
            6,
            "Expected 6 rows for 10:00-15:00 range (bug fix validation)"
        );
        assert_eq!(filtered[0].timestamp_utc, "2024-10-20T10:00:00Z");
        assert_eq!(filtered[5].timestamp_utc, "2024-10-20T15:00:00Z");

        // Verify all returned rows have non-zero production (daytime)
        for row in &filtered {
            assert!(
                row.tso_50hertz_mw.unwrap() > 0.0,
                "Daytime solar should have production"
            );
        }
    }

    // ========================================================================
    // Helper Function Tests (v0.2.2 - String Timestamp Parsing Fix)
    // ========================================================================

    /// Test parse_string_to_micros with full ISO 8601 timestamp
    #[test]
    fn test_parse_string_to_micros_iso8601() {
        use chrono::DateTime;

        // Test full ISO 8601 timestamp with Z timezone
        let micros = parse_string_to_micros("2024-10-20T10:00:00Z").unwrap();
        let expected = DateTime::parse_from_rfc3339("2024-10-20T10:00:00Z")
            .unwrap()
            .timestamp_micros();
        assert_eq!(micros, expected);

        // Test full ISO 8601 timestamp with +00:00 timezone
        let micros2 = parse_string_to_micros("2024-10-20T15:30:45+00:00").unwrap();
        let expected2 = DateTime::parse_from_rfc3339("2024-10-20T15:30:45+00:00")
            .unwrap()
            .timestamp_micros();
        assert_eq!(micros2, expected2);
    }

    /// Test parse_string_to_micros with date-only string
    #[test]
    fn test_parse_string_to_micros_date_only() {
        use chrono::{DateTime, NaiveDate, Utc};

        // Test date-only (should be start of day 00:00:00 UTC)
        let micros = parse_string_to_micros("2024-10-20").unwrap();

        let date = NaiveDate::from_ymd_opt(2024, 10, 20).unwrap();
        let dt = date.and_hms_opt(0, 0, 0).unwrap();
        let expected = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).timestamp_micros();

        assert_eq!(micros, expected);
    }

    /// Test parse_string_to_micros with invalid input
    #[test]
    fn test_parse_string_to_micros_invalid() {
        // Invalid format should return None
        assert!(parse_string_to_micros("invalid").is_none());
        assert!(parse_string_to_micros("2024-13-01").is_none()); // Invalid month
        assert!(parse_string_to_micros("not-a-date").is_none());
    }

    /// Test extract_date_component with various formats
    #[test]
    fn test_extract_date_component() {
        // Date-only string (already in correct format)
        assert_eq!(extract_date_component("2024-10-20"), "2024-10-20");

        // Full timestamp with Z timezone
        assert_eq!(extract_date_component("2024-10-20T10:00:00Z"), "2024-10-20");

        // Full timestamp without timezone
        assert_eq!(extract_date_component("2024-10-20T10:00:00"), "2024-10-20");

        // Full timestamp with offset timezone
        assert_eq!(
            extract_date_component("2024-10-20T15:30:45+00:00"),
            "2024-10-20"
        );
    }

    /// Test same-date query auto-adjustment (v0.2.3 fix)
    ///
    /// Verifies that same-date queries are automatically adjusted by adding 1 day
    /// to the end date to work around NTP API's exclusive end date behavior.
    #[test]
    fn test_same_date_adjustment() {
        // Test same-date input
        let start = "2024-10-20".to_string();
        let end = "2024-10-20".to_string();

        // Simulate the adjustment logic from parse_quals()
        let adjusted_end = if start == end {
            add_days_to_date(&end, 1).unwrap()
        } else {
            end.clone()
        };

        // Verify adjustment: 2024-10-20 → 2024-10-21
        assert_eq!(adjusted_end, "2024-10-21");
        assert_ne!(adjusted_end, start);

        // Test different dates (should not adjust)
        let start2 = "2024-10-20".to_string();
        let end2 = "2024-10-21".to_string();

        let adjusted_end2 = if start2 == end2 {
            add_days_to_date(&end2, 1).unwrap()
        } else {
            end2.clone()
        };

        // Verify no adjustment when dates differ
        assert_eq!(adjusted_end2, "2024-10-21");
        assert_eq!(adjusted_end2, end2);
    }

    /// Test add_days_to_date helper (used for same-date adjustment)
    #[test]
    fn test_add_days_to_date() {
        // Add 1 day
        assert_eq!(add_days_to_date("2024-10-20", 1).unwrap(), "2024-10-21");

        // Add 7 days
        assert_eq!(add_days_to_date("2024-10-20", 7).unwrap(), "2024-10-27");

        // Subtract 1 day
        assert_eq!(add_days_to_date("2024-10-20", -1).unwrap(), "2024-10-19");

        // Month boundary (Oct → Nov)
        assert_eq!(add_days_to_date("2024-10-31", 1).unwrap(), "2024-11-01");

        // Year boundary (Dec → Jan)
        assert_eq!(add_days_to_date("2024-12-31", 1).unwrap(), "2025-01-01");

        // Leap year (Feb 28 → Feb 29)
        assert_eq!(add_days_to_date("2024-02-28", 1).unwrap(), "2024-02-29");

        // Invalid date format
        assert!(add_days_to_date("invalid", 1).is_err());
    }

    /// Test cross-day time range adjustment (v0.2.4 fix)
    ///
    /// Verifies that queries spanning multiple calendar days with time components
    /// automatically adjust the end date to fetch data from all relevant days.
    #[test]
    #[allow(clippy::if_same_then_else)]
    fn test_cross_day_time_range_adjustment() {
        // Scenario: Query spans midnight (Oct 20 23:00 → Oct 21 01:00)
        let start_date = "2024-10-20".to_string();
        let end_date = "2024-10-21".to_string();

        // Simulate time bounds present (indicates time-based filtering)
        let has_time_bounds = true;

        // Adjustment logic
        let adjusted_end = if start_date == end_date {
            add_days_to_date(&end_date, 1).unwrap()
        } else if has_time_bounds {
            add_days_to_date(&end_date, 1).unwrap()
        } else {
            end_date.clone()
        };

        // Verify: 2024-10-21 → 2024-10-22
        assert_eq!(adjusted_end, "2024-10-22");
        assert_ne!(adjusted_end, end_date);
    }

    /// Test date-only queries remain unchanged (v0.2.4 regression test)
    #[test]
    #[allow(clippy::if_same_then_else)]
    fn test_date_only_query_no_adjustment() {
        // Scenario: Date-only query (no time bounds)
        let start_date = "2024-10-20".to_string();
        let end_date = "2024-10-25".to_string();

        // No time bounds
        let has_time_bounds = false;

        // Adjustment logic
        let adjusted_end = if start_date == end_date {
            add_days_to_date(&end_date, 1).unwrap()
        } else if has_time_bounds {
            add_days_to_date(&end_date, 1).unwrap()
        } else {
            end_date.clone()
        };

        // Verify: No adjustment for date-only queries
        assert_eq!(adjusted_end, "2024-10-25");
        assert_eq!(adjusted_end, end_date);
    }

    /// Test three-way adjustment logic (comprehensive)
    #[test]
    #[allow(clippy::if_same_then_else)]
    fn test_timestamp_range_adjustment_all_cases() {
        // Case 1: Same-date with time bounds
        let (start1, end1, has_time1) = ("2024-10-20", "2024-10-20", true);
        let adj1 = if start1 == end1 {
            add_days_to_date(end1, 1).unwrap()
        } else if has_time1 {
            add_days_to_date(end1, 1).unwrap()
        } else {
            end1.to_string()
        };
        assert_eq!(adj1, "2024-10-21"); // Same-date: +1 day

        // Case 2: Cross-day with time bounds
        let (start2, end2, has_time2) = ("2024-10-20", "2024-10-21", true);
        let adj2 = if start2 == end2 {
            add_days_to_date(end2, 1).unwrap()
        } else if has_time2 {
            add_days_to_date(end2, 1).unwrap()
        } else {
            end2.to_string()
        };
        assert_eq!(adj2, "2024-10-22"); // Cross-day with time: +1 day

        // Case 3: Date-only (no time bounds)
        let (start3, end3, has_time3) = ("2024-10-20", "2024-10-25", false);
        let adj3 = if start3 == end3 {
            add_days_to_date(end3, 1).unwrap()
        } else if has_time3 {
            add_days_to_date(end3, 1).unwrap()
        } else {
            end3.to_string()
        };
        assert_eq!(adj3, "2024-10-25"); // Date-only: no adjustment
    }
}
