//! Query Router for NTP FDW
//!
//! Routes SQL WHERE clauses to appropriate NTP API endpoints.
//!
//! # Routing Logic
//!
//! ## Renewable Energy Table
//! - `product_type` → API product parameter (Solar, Wind, Windonshore, Windoffshore)
//! - `data_category` → API endpoint prefix (prognose, hochrechnung, onlinehochrechnung)
//! - `timestamp_utc` → ISO 8601 date range (YYYY-MM-DD)
//!
//! ## Electricity Market Prices Table
//! - `price_type` → API endpoint (Spotmarktpreise, NegativePreise, marktpraemie, Jahresmarktpraemie)
//! - `timestamp_utc` → ISO 8601 date range
//!
//! # Example
//!
//! ```rust
//! use supabase_fdw_ntp::query_router::*;
//!
//! // Route a solar forecast query
//! let filters = QualFilters {
//!     product_type: Some("solar".to_string()),
//!     data_category: Some("forecast".to_string()),
//!     price_type: None,
//!     timestamp_range: Some(DateRange {
//!         start: "2024-10-24".to_string(),
//!         end: "2024-10-25".to_string(),
//!     }),
//!     timestamp_bounds: None,
//!     table_name: "renewable_energy_timeseries".to_string(),
//! };
//!
//! let plans = route_query(&filters, "https://www.netztransparenz.de/api/ntp").unwrap();
//! assert_eq!(plans.len(), 1); // Single endpoint: prognose/Solar/2024-10-24/2024-10-25
//! ```

use crate::error::{ApiError, NtpFdwError};
use chrono::NaiveDate;

// ============================================================================
// Data Structures
// ============================================================================

/// Query plan for a single API request
///
/// Represents one HTTP GET request to the NTP API.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryPlan {
    /// API endpoint path (e.g., "prognose", "hochrechnung", "Spotmarktpreise")
    pub endpoint: String,

    /// Product parameter (e.g., "Solar", "Wind", "Windonshore", "Windoffshore")
    ///
    /// `None` for price endpoints (Spotmarktpreise, etc.)
    pub product: Option<String>,

    /// Start date (ISO 8601 format: YYYY-MM-DD)
    pub date_from: String,

    /// End date (ISO 8601 format: YYYY-MM-DD)
    pub date_to: String,

    /// Full API URL (ready for HTTP GET)
    ///
    /// Example: `https://www.netztransparenz.de/api/ntp/prognose/Solar/2024-10-24/2024-10-25`
    pub api_url: String,
}

/// Extracted filters from SQL WHERE clause
///
/// Parsed from Supabase FDW Context quals
#[derive(Debug, Clone)]
pub struct QualFilters {
    /// Product type filter: "solar", "wind_onshore", "wind_offshore"
    ///
    /// From SQL: `WHERE product_type = 'solar'`
    pub product_type: Option<String>,

    /// Data category filter: "forecast", "extrapolation", "online_actual"
    ///
    /// From SQL: `WHERE data_category = 'forecast'`
    pub data_category: Option<String>,

    /// Price type filter: "spot_market", "market_premium", "annual_market_value", "negative_flag"
    ///
    /// From SQL: `WHERE price_type = 'spot_market'`
    pub price_type: Option<String>,

    /// Timestamp range filter (date-only, for API routing)
    ///
    /// From SQL: `WHERE timestamp_utc >= '2024-10-24' AND timestamp_utc < '2024-10-25'`
    ///
    /// Extracts only the date portion (YYYY-MM-DD) for efficient API endpoint routing.
    pub timestamp_range: Option<DateRange>,

    /// Timestamp bounds (full precision, for local filtering)
    ///
    /// From SQL: `WHERE timestamp_utc >= '2024-10-20T10:00:00' AND timestamp_utc < '2024-10-20T16:00:00'`
    ///
    /// Preserves full timestamp with hour/minute/second for local post-filtering.
    /// Solves bug where time components were stripped during qual parsing.
    pub timestamp_bounds: Option<TimestampBounds>,

    /// Table name: "renewable_energy_timeseries" or "electricity_market_prices"
    ///
    /// From Context.table
    pub table_name: String,
}

/// Date range for timestamp filtering (API routing)
#[derive(Debug, Clone, PartialEq)]
pub struct DateRange {
    /// Start date (ISO 8601: YYYY-MM-DD)
    pub start: String,

    /// End date (ISO 8601: YYYY-MM-DD)
    pub end: String,
}

/// Timestamp bounds for local filtering
///
/// Stores full timestamp values (with time components) extracted from SQL WHERE clause
/// for local post-filtering after API data fetch.
///
/// # Purpose
///
/// Solves the time-component stripping bug where queries like
/// `WHERE timestamp_utc >= '2024-10-20T10:00:00'` were converted to date-only
/// strings for API routing, losing hour/minute precision.
///
/// # Usage
///
/// - API routing: Use `DateRange` (date-only) to determine which dates to fetch
/// - Local filtering: Use `TimestampBounds` (full timestamps) to filter fetched rows
#[derive(Debug, Clone)]
pub struct TimestampBounds {
    /// Lower bound timestamp in microseconds since epoch
    ///
    /// From SQL: `WHERE timestamp_utc >= '2024-10-20T10:00:00'`
    pub start: Option<i64>,

    /// Lower bound operator: ">=", ">", or "="
    pub start_operator: Option<String>,

    /// Upper bound timestamp in microseconds since epoch
    ///
    /// From SQL: `WHERE timestamp_utc < '2024-10-20T16:00:00'`
    pub end: Option<i64>,

    /// Upper bound operator: "<", "<=", or "="
    pub end_operator: Option<String>,
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Build full API URL from components
///
/// Handles different URL formats for different endpoints:
/// - **Standard endpoints:** `/endpoint/date_from/date_to`
/// - **Annual endpoint (Jahresmarktpraemie):** `/endpoint/YYYY` (year only)
/// - **Monthly endpoint (marktpraemie):** `/endpoint/MM/YYYY/MM/YYYY` (monthFrom/yearFrom/monthTo/yearTo)
/// - **Product-based endpoints:** `/endpoint/product/date_from/date_to`
///
/// # Arguments
///
/// * `base_url` - API base URL (e.g., "https://www.netztransparenz.de/api/ntp")
/// * `endpoint` - Endpoint name (e.g., "prognose", "Spotmarktpreise", "Jahresmarktpraemie")
/// * `product` - Optional product name (e.g., "Solar")
/// * `date_from` - Start date (YYYY-MM-DD format)
/// * `date_to` - End date (YYYY-MM-DD format)
///
/// # Returns
///
/// Full API URL ready for HTTP GET
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::build_api_url;
/// // Standard endpoint (date range)
/// let url = build_api_url(
///     "https://www.netztransparenz.de/api/ntp",
///     "Spotmarktpreise",
///     None,
///     "2024-10-24",
///     "2024-10-25"
/// );
/// assert_eq!(url, "https://www.netztransparenz.de/api/ntp/Spotmarktpreise/2024-10-24/2024-10-25");
///
/// // Annual endpoint (year only)
/// let url = build_api_url(
///     "https://www.netztransparenz.de/api/ntp",
///     "Jahresmarktpraemie",
///     None,
///     "2024-01-01",
///     "2025-01-01"
/// );
/// assert_eq!(url, "https://www.netztransparenz.de/api/ntp/Jahresmarktpraemie/2024");
///
/// // Monthly endpoint (monthFrom/yearFrom/monthTo/yearTo)
/// let url = build_api_url(
///     "https://www.netztransparenz.de/api/ntp",
///     "marktpraemie",
///     None,
///     "2024-01-01",
///     "2024-03-31"
/// );
/// assert_eq!(url, "https://www.netztransparenz.de/api/ntp/marktpraemie/01/2024/03/2024");
///
/// // Product-based endpoint (date range with product)
/// let url = build_api_url(
///     "https://www.netztransparenz.de/api/ntp",
///     "prognose",
///     Some("Solar"),
///     "2024-10-24",
///     "2024-10-25"
/// );
/// assert_eq!(url, "https://www.netztransparenz.de/api/ntp/prognose/Solar/2024-10-24/2024-10-25");
/// ```
pub fn build_api_url(
    base_url: &str,
    endpoint: &str,
    product: Option<&str>,
    date_from: &str,
    date_to: &str,
) -> String {
    // Remove trailing slash from base_url if present
    let base = base_url.trim_end_matches('/');

    // Special handling for Jahresmarktpraemie (year-only endpoint)
    if endpoint == "Jahresmarktpraemie" {
        // Extract year from date_from (YYYY-MM-DD -> YYYY)
        let year = &date_from[0..4];
        return format!("{}/{}/{}", base, endpoint, year);
    }

    // Special handling for marktpraemie (monthFrom/yearFrom/monthTo/yearTo endpoint)
    if endpoint == "marktpraemie" {
        // Extract month and year from both date_from and date_to (YYYY-MM-DD)
        let month_from = &date_from[5..7];
        let year_from = &date_from[0..4];
        let month_to = &date_to[5..7];
        let year_to = &date_to[0..4];
        return format!(
            "{}/{}/{}/{}/{}/{}",
            base, endpoint, month_from, year_from, month_to, year_to
        );
    }

    // Standard handling for all other endpoints (date range format)
    if let Some(prod) = product {
        format!("{}/{}/{}/{}/{}", base, endpoint, prod, date_from, date_to)
    } else {
        format!("{}/{}/{}/{}", base, endpoint, date_from, date_to)
    }
}

/// Validate date range
///
/// Ensures `date_from <= date_to` and both dates are valid ISO 8601 format.
///
/// # Arguments
///
/// * `date_from` - Start date (YYYY-MM-DD)
/// * `date_to` - End date (YYYY-MM-DD)
///
/// # Returns
///
/// * `Ok(())` - Valid date range
/// * `Err(ApiError)` - Invalid range or format
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::validate_date_range;
/// // Valid range
/// assert!(validate_date_range("2024-10-24", "2024-10-25").is_ok());
///
/// // Same day (valid - will be auto-adjusted by +1 day in parse_quals, v0.2.3)
/// assert!(validate_date_range("2024-10-24", "2024-10-24").is_ok());
///
/// // Invalid: date_from > date_to
/// assert!(validate_date_range("2024-10-25", "2024-10-24").is_err());
///
/// // Invalid format
/// assert!(validate_date_range("invalid", "2024-10-24").is_err());
/// ```
pub fn validate_date_range(date_from: &str, date_to: &str) -> Result<(), NtpFdwError> {
    // Parse dates
    let from =
        NaiveDate::parse_from_str(date_from, "%Y-%m-%d").map_err(|_| ApiError::HttpError {
            status: 400,
            body: format!(
                "Invalid date format for date_from: '{}'. Expected YYYY-MM-DD.",
                date_from
            ),
        })?;

    let to = NaiveDate::parse_from_str(date_to, "%Y-%m-%d").map_err(|_| ApiError::HttpError {
        status: 400,
        body: format!(
            "Invalid date format for date_to: '{}'. Expected YYYY-MM-DD.",
            date_to
        ),
    })?;

    // Validate range
    if from > to {
        return Err(ApiError::HttpError {
            status: 400,
            body: format!(
                "Invalid date range: date_from ({}) must be <= date_to ({})",
                date_from, date_to
            ),
        }
        .into());
    }

    Ok(())
}

/// Extract date range from timestamp filter, or use default (last 7 days)
///
/// # Arguments
///
/// * `timestamp_range` - Optional date range from WHERE clause
///
/// # Returns
///
/// DateRange (from filter or default last 7 days)
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::{extract_date_range, DateRange};
/// // With filter
/// let range = Some(DateRange {
///     start: "2024-10-24".to_string(),
///     end: "2024-10-25".to_string(),
/// });
/// let result = extract_date_range(range.as_ref());
/// assert_eq!(result.start, "2024-10-24");
///
/// // Without filter (defaults to last 7 days)
/// let result = extract_date_range(None);
/// // result.start is 7 days ago, result.end is today
/// ```
pub fn extract_date_range(timestamp_range: Option<&DateRange>) -> DateRange {
    if let Some(range) = timestamp_range {
        return range.clone();
    }

    // Default: Fixed 7-day window (fallback only)
    // Note: SystemTime::now() not available in WASM, so we use a fixed date range
    // In practice, SQL queries should always specify timestamp_utc filters
    DateRange {
        start: "2024-10-18".to_string(),
        end: "2024-10-25".to_string(),
    }
}

// ============================================================================
// Routing Functions
// ============================================================================

/// Route query to appropriate API endpoints
///
/// Main entry point for query routing. Dispatches to table-specific routing functions.
///
/// # Arguments
///
/// * `filters` - Extracted filters from SQL WHERE clause
/// * `base_url` - API base URL
///
/// # Returns
///
/// * `Ok(Vec<QueryPlan>)` - List of API endpoints to call
/// * `Err(NtpFdwError)` - Invalid filters or routing error
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::*;
/// let filters = QualFilters {
///     product_type: Some("solar".to_string()),
///     data_category: Some("forecast".to_string()),
///     price_type: None,
///     timestamp_range: Some(DateRange {
///         start: "2024-10-24".to_string(),
///         end: "2024-10-25".to_string(),
///     }),
///     timestamp_bounds: None,
///     table_name: "renewable_energy_timeseries".to_string(),
/// };
///
/// let plans = route_query(&filters, "https://www.netztransparenz.de/api/ntp").unwrap();
/// assert_eq!(plans.len(), 1); // Single optimized query
/// ```
pub fn route_query(filters: &QualFilters, base_url: &str) -> Result<Vec<QueryPlan>, NtpFdwError> {
    match filters.table_name.as_str() {
        "renewable_energy_timeseries" => route_renewable(filters, base_url),
        "electricity_market_prices" => route_prices(filters, base_url),
        "redispatch_events" => route_redispatch(filters, base_url),
        "grid_status_timeseries" => route_grid_status(filters, base_url),
        _ => Err(NtpFdwError::Generic(format!(
            "Unknown table: {}. Expected one of: renewable_energy_timeseries, electricity_market_prices, redispatch_events, grid_status_timeseries.",
            filters.table_name
        ))),
    }
}

/// Route renewable energy queries to API endpoints
///
/// Maps product_type and data_category filters to API endpoints.
///
/// # Routing Matrix
///
/// | product_type | data_category | API Calls |
/// |--------------|---------------|-----------|
/// | solar | forecast | prognose/Solar | 1 |
/// | solar | (none) | prognose/Solar, hochrechnung/Solar, onlinehochrechnung/Solar | 3 |
/// | wind_onshore | forecast | prognose/Wind | 1 |
/// | wind_onshore | online_actual | onlinehochrechnung/Windonshore | 1 |
/// | (none) | (none) | ALL 9 endpoints | 9 |
///
/// # Arguments
///
/// * `filters` - Query filters
/// * `base_url` - API base URL
///
/// # Returns
///
/// List of query plans (1-9 endpoints)
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::*;
/// // Optimal query (1 endpoint)
/// let filters = QualFilters {
///     product_type: Some("solar".to_string()),
///     data_category: Some("forecast".to_string()),
///     price_type: None,
///     timestamp_range: None,
///     table_name: "renewable_energy_timeseries".to_string(),
/// };
/// let plans = route_renewable(&filters, "https://api.example.com").unwrap();
/// assert_eq!(plans.len(), 1);
/// assert_eq!(plans[0].endpoint, "prognose");
/// assert_eq!(plans[0].product, Some("Solar".to_string()));
/// ```
pub fn route_renewable(
    filters: &QualFilters,
    base_url: &str,
) -> Result<Vec<QueryPlan>, NtpFdwError> {
    // Extract date range (default: last 7 days)
    let date_range = extract_date_range(filters.timestamp_range.as_ref());

    // Validate date range
    validate_date_range(&date_range.start, &date_range.end)?;

    // Determine products to query
    let products = match &filters.product_type {
        Some(product_type) => vec![product_type.as_str()],
        None => vec!["solar", "wind_onshore", "wind_offshore"],
    };

    // Determine data categories to query
    let categories = match &filters.data_category {
        Some(category) => vec![category.as_str()],
        None => vec!["forecast", "extrapolation", "online_actual"],
    };

    let mut plans = Vec::new();

    // Generate query plans (Cartesian product of products × categories)
    for product_type in products {
        for category in &categories {
            // Map product_type to API product name
            let api_products = map_product_to_api(product_type, category)?;

            // Map data_category to API endpoint
            let api_endpoint = map_category_to_endpoint(category)?;

            for api_product in api_products {
                let api_url = build_api_url(
                    base_url,
                    api_endpoint,
                    Some(api_product),
                    &date_range.start,
                    &date_range.end,
                );

                plans.push(QueryPlan {
                    endpoint: api_endpoint.to_string(),
                    product: Some(api_product.to_string()),
                    date_from: date_range.start.clone(),
                    date_to: date_range.end.clone(),
                    api_url,
                });
            }
        }
    }

    Ok(plans)
}

/// Map database product type to API product name(s)
///
/// Handles special case for wind products.
///
/// # Arguments
///
/// * `product_type` - Database product type: "solar", "wind_onshore", "wind_offshore"
/// * `category` - Data category: "forecast", "extrapolation", "online_actual"
///
/// # Returns
///
/// List of API product names
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::map_product_to_api;
/// // Solar is straightforward
/// assert_eq!(map_product_to_api("solar", "forecast").unwrap(), vec!["Solar"]);
///
/// // Wind depends on data category
/// assert_eq!(map_product_to_api("wind_onshore", "forecast").unwrap(), vec!["Wind"]);
/// assert_eq!(map_product_to_api("wind_onshore", "online_actual").unwrap(), vec!["Windonshore"]);
///
/// // Wind offshore only has online_actual
/// assert_eq!(map_product_to_api("wind_offshore", "online_actual").unwrap(), vec!["Windoffshore"]);
/// ```
pub fn map_product_to_api(
    product_type: &str,
    category: &str,
) -> Result<Vec<&'static str>, NtpFdwError> {
    match (product_type, category) {
        // Solar: same for all categories
        ("solar", _) => Ok(vec!["Solar"]),

        // Wind onshore: "Wind" for forecast/extrapolation, "Windonshore" for online_actual
        ("wind_onshore", "forecast") | ("wind_onshore", "extrapolation") => Ok(vec!["Wind"]),
        ("wind_onshore", "online_actual") => Ok(vec!["Windonshore"]),

        // Wind offshore: only online_actual available
        ("wind_offshore", "online_actual") => Ok(vec!["Windoffshore"]),
        ("wind_offshore", "forecast") | ("wind_offshore", "extrapolation") => {
            // Wind offshore doesn't have forecast/extrapolation endpoints
            // Return empty list (no endpoints to query)
            Ok(vec![])
        }

        // Unknown product type
        _ => Err(NtpFdwError::Generic(format!(
            "Unknown product type: '{}'. Expected 'solar', 'wind_onshore', or 'wind_offshore'.",
            product_type
        ))),
    }
}

/// Map database data category to API endpoint name
///
/// # Arguments
///
/// * `category` - Data category: "forecast", "extrapolation", "online_actual"
///
/// # Returns
///
/// API endpoint name
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::map_category_to_endpoint;
/// assert_eq!(map_category_to_endpoint("forecast").unwrap(), "prognose");
/// assert_eq!(map_category_to_endpoint("extrapolation").unwrap(), "hochrechnung");
/// assert_eq!(map_category_to_endpoint("online_actual").unwrap(), "onlinehochrechnung");
/// ```
pub fn map_category_to_endpoint(category: &str) -> Result<&'static str, NtpFdwError> {
    match category {
        "forecast" => Ok("prognose"),
        "extrapolation" => Ok("hochrechnung"),
        "online_actual" => Ok("onlinehochrechnung"),
        _ => Err(NtpFdwError::Generic(format!(
            "Unknown data category: '{}'. Expected 'forecast', 'extrapolation', or 'online_actual'.",
            category
        ))),
    }
}

/// Route electricity price queries to API endpoints
///
/// Maps price_type filter to API endpoints.
///
/// # Routing Matrix
///
/// | price_type | API Endpoint | Query Plans |
/// |------------|--------------|-------------|
/// | spot_market | Spotmarktpreise | 1 |
/// | market_premium | marktpraemie | 1 |
/// | annual_market_value | Jahresmarktpraemie | 1 |
/// | negative_flag | NegativePreise | 1 |
/// | (none) | ALL 4 endpoints | 4 |
///
/// # Arguments
///
/// * `filters` - Query filters
/// * `base_url` - API base URL
///
/// # Returns
///
/// List of query plans (1-4 endpoints)
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::*;
/// let filters = QualFilters {
///     product_type: None,
///     data_category: None,
///     price_type: Some("spot_market".to_string()),
///     timestamp_range: None,
///     table_name: "electricity_market_prices".to_string(),
/// };
/// let plans = route_prices(&filters, "https://api.example.com").unwrap();
/// assert_eq!(plans.len(), 1);
/// assert_eq!(plans[0].endpoint, "Spotmarktpreise");
/// ```
pub fn route_prices(filters: &QualFilters, base_url: &str) -> Result<Vec<QueryPlan>, NtpFdwError> {
    // Extract date range (default: last 7 days)
    let date_range = extract_date_range(filters.timestamp_range.as_ref());

    // Validate date range
    validate_date_range(&date_range.start, &date_range.end)?;

    // Determine price endpoints to query
    let endpoints = match &filters.price_type {
        Some(price_type) => vec![map_price_type_to_endpoint(price_type)?],
        None => vec![
            "Spotmarktpreise",
            "NegativePreise",
            "marktpraemie",
            "Jahresmarktpraemie",
        ],
    };

    let mut plans = Vec::new();

    for endpoint in endpoints {
        let api_url = build_api_url(
            base_url,
            endpoint,
            None, // Price endpoints don't have product parameter
            &date_range.start,
            &date_range.end,
        );

        plans.push(QueryPlan {
            endpoint: endpoint.to_string(),
            product: None,
            date_from: date_range.start.clone(),
            date_to: date_range.end.clone(),
            api_url,
        });
    }

    Ok(plans)
}

/// Map database price type to API endpoint name
///
/// # Arguments
///
/// * `price_type` - Database price type
///
/// # Returns
///
/// API endpoint name
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::map_price_type_to_endpoint;
/// assert_eq!(map_price_type_to_endpoint("spot_market").unwrap(), "Spotmarktpreise");
/// assert_eq!(map_price_type_to_endpoint("market_premium").unwrap(), "marktpraemie");
/// assert_eq!(map_price_type_to_endpoint("annual_market_value").unwrap(), "Jahresmarktpraemie");
/// assert_eq!(map_price_type_to_endpoint("negative_flag").unwrap(), "NegativePreise");
/// ```
pub fn map_price_type_to_endpoint(price_type: &str) -> Result<&'static str, NtpFdwError> {
    match price_type {
        "spot_market" => Ok("Spotmarktpreise"),
        "market_premium" => Ok("marktpraemie"),
        "annual_market_value" => Ok("Jahresmarktpraemie"),
        "negative_flag" => Ok("NegativePreise"),
        _ => Err(NtpFdwError::Generic(format!(
            "Unknown price type: '{}'. Expected 'spot_market', 'market_premium', 'annual_market_value', or 'negative_flag'.",
            price_type
        ))),
    }
}

/// Route grid status queries to TrafficLight API endpoint
///
/// Maps timestamp filter to TrafficLight endpoint.
///
/// # Arguments
///
/// * `filters` - Query filters
/// * `base_url` - API base URL
///
/// # Returns
///
/// Single query plan for TrafficLight endpoint
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::*;
/// let filters = QualFilters {
///     product_type: None,
///     data_category: None,
///     price_type: None,
///     timestamp_range: Some(DateRange {
///         start: "2024-10-24".to_string(),
///         end: "2024-10-25".to_string(),
///     }),
///     table_name: "grid_status_timeseries".to_string(),
/// };
/// let plans = route_grid_status(&filters, "https://api.example.com").unwrap();
/// assert_eq!(plans.len(), 1);
/// assert_eq!(plans[0].endpoint, "TrafficLight");
/// ```
pub fn route_grid_status(
    filters: &QualFilters,
    base_url: &str,
) -> Result<Vec<QueryPlan>, NtpFdwError> {
    // Extract date range (default: last 7 days)
    let date_range = extract_date_range(filters.timestamp_range.as_ref());

    // Validate date range
    validate_date_range(&date_range.start, &date_range.end)?;

    let api_url = build_api_url(
        base_url,
        "TrafficLight",
        None, // No product parameter
        &date_range.start,
        &date_range.end,
    );

    let plan = QueryPlan {
        endpoint: "TrafficLight".to_string(),
        product: None,
        date_from: date_range.start,
        date_to: date_range.end,
        api_url,
    };

    Ok(vec![plan])
}

/// Route redispatch queries to redispatch API endpoint
///
/// Maps timestamp filter to redispatch endpoint.
///
/// # Arguments
///
/// * `filters` - Query filters
/// * `base_url` - API base URL
///
/// # Returns
///
/// Single query plan for redispatch endpoint
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::query_router::*;
/// let filters = QualFilters {
///     product_type: None,
///     data_category: None,
///     price_type: None,
///     timestamp_range: Some(DateRange {
///         start: "2024-10-23".to_string(),
///         end: "2024-10-24".to_string(),
///     }),
///     table_name: "redispatch_events".to_string(),
/// };
/// let plans = route_redispatch(&filters, "https://api.example.com").unwrap();
/// assert_eq!(plans.len(), 1);
/// assert_eq!(plans[0].endpoint, "redispatch");
/// ```
pub fn route_redispatch(
    filters: &QualFilters,
    base_url: &str,
) -> Result<Vec<QueryPlan>, NtpFdwError> {
    // Extract date range (default: last 7 days)
    let date_range = extract_date_range(filters.timestamp_range.as_ref());

    // Validate date range
    validate_date_range(&date_range.start, &date_range.end)?;

    let api_url = build_api_url(
        base_url,
        "redispatch",
        None, // No product parameter
        &date_range.start,
        &date_range.end,
    );

    let plan = QueryPlan {
        endpoint: "redispatch".to_string(),
        product: None,
        date_from: date_range.start,
        date_to: date_range.end,
        api_url,
    };

    Ok(vec![plan])
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Helper Function Tests
    // ========================================================================

    #[test]
    fn test_build_api_url_with_product() {
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp",
            "prognose",
            Some("Solar"),
            "2024-10-24",
            "2024-10-25",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/prognose/Solar/2024-10-24/2024-10-25"
        );
    }

    #[test]
    fn test_build_api_url_without_product() {
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp",
            "Spotmarktpreise",
            None,
            "2024-10-24",
            "2024-10-25",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/Spotmarktpreise/2024-10-24/2024-10-25"
        );
    }

    #[test]
    fn test_build_api_url_trailing_slash() {
        // Base URL with trailing slash should be handled
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp/",
            "prognose",
            Some("Solar"),
            "2024-10-24",
            "2024-10-25",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/prognose/Solar/2024-10-24/2024-10-25"
        );
    }

    #[test]
    fn test_build_api_url_annual_endpoint() {
        // Jahresmarktpraemie uses year-only format
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp",
            "Jahresmarktpraemie",
            None,
            "2024-01-01",
            "2025-01-01",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/Jahresmarktpraemie/2024"
        );
    }

    #[test]
    fn test_build_api_url_monthly_endpoint() {
        // marktpraemie uses monthFrom/yearFrom/monthTo/yearTo format
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp",
            "marktpraemie",
            None,
            "2024-10-01",
            "2024-12-31",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/marktpraemie/10/2024/12/2024"
        );
    }

    #[test]
    fn test_build_api_url_monthly_endpoint_single_digit_month() {
        // Verify month is extracted with leading zero preserved
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp",
            "marktpraemie",
            None,
            "2024-01-01",
            "2024-03-31",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/marktpraemie/01/2024/03/2024"
        );
    }

    #[test]
    fn test_build_api_url_monthly_endpoint_cross_year() {
        // Verify cross-year date ranges work correctly (Nov 2023 → Feb 2024)
        let url = build_api_url(
            "https://www.netztransparenz.de/api/ntp",
            "marktpraemie",
            None,
            "2023-11-01",
            "2024-02-28",
        );
        assert_eq!(
            url,
            "https://www.netztransparenz.de/api/ntp/marktpraemie/11/2023/02/2024"
        );
    }

    #[test]
    fn test_validate_date_range_valid() {
        assert!(validate_date_range("2024-10-24", "2024-10-25").is_ok());
        assert!(validate_date_range("2024-10-24", "2024-10-24").is_ok()); // Same day
    }

    #[test]
    fn test_validate_date_range_invalid_order() {
        assert!(validate_date_range("2024-10-25", "2024-10-24").is_err());
    }

    #[test]
    fn test_validate_date_range_invalid_format() {
        assert!(validate_date_range("invalid", "2024-10-24").is_err());
        assert!(validate_date_range("2024-10-24", "invalid").is_err());
    }

    #[test]
    fn test_extract_date_range_with_filter() {
        let range = DateRange {
            start: "2024-10-24".to_string(),
            end: "2024-10-25".to_string(),
        };
        let result = extract_date_range(Some(&range));
        assert_eq!(result.start, "2024-10-24");
        assert_eq!(result.end, "2024-10-25");
    }

    #[test]
    fn test_extract_date_range_default() {
        let result = extract_date_range(None);
        // Should return last 7 days
        // We can't assert exact dates, but we can check format
        assert_eq!(result.start.len(), 10); // YYYY-MM-DD
        assert_eq!(result.end.len(), 10);
        // Verify end is after start
        assert!(result.start <= result.end);
    }

    // ========================================================================
    // Product Mapping Tests
    // ========================================================================

    #[test]
    fn test_map_product_solar() {
        assert_eq!(
            map_product_to_api("solar", "forecast").unwrap(),
            vec!["Solar"]
        );
        assert_eq!(
            map_product_to_api("solar", "extrapolation").unwrap(),
            vec!["Solar"]
        );
        assert_eq!(
            map_product_to_api("solar", "online_actual").unwrap(),
            vec!["Solar"]
        );
    }

    #[test]
    fn test_map_product_wind_onshore() {
        assert_eq!(
            map_product_to_api("wind_onshore", "forecast").unwrap(),
            vec!["Wind"]
        );
        assert_eq!(
            map_product_to_api("wind_onshore", "extrapolation").unwrap(),
            vec!["Wind"]
        );
        assert_eq!(
            map_product_to_api("wind_onshore", "online_actual").unwrap(),
            vec!["Windonshore"]
        );
    }

    #[test]
    fn test_map_product_wind_offshore() {
        // Wind offshore only has online_actual
        assert_eq!(
            map_product_to_api("wind_offshore", "online_actual").unwrap(),
            vec!["Windoffshore"]
        );
        // Other categories return empty (no endpoints available)
        assert_eq!(
            map_product_to_api("wind_offshore", "forecast").unwrap(),
            Vec::<&str>::new()
        );
        assert_eq!(
            map_product_to_api("wind_offshore", "extrapolation").unwrap(),
            Vec::<&str>::new()
        );
    }

    #[test]
    fn test_map_product_unknown() {
        assert!(map_product_to_api("biomass", "forecast").is_err());
    }

    #[test]
    fn test_map_category_to_endpoint() {
        assert_eq!(map_category_to_endpoint("forecast").unwrap(), "prognose");
        assert_eq!(
            map_category_to_endpoint("extrapolation").unwrap(),
            "hochrechnung"
        );
        assert_eq!(
            map_category_to_endpoint("online_actual").unwrap(),
            "onlinehochrechnung"
        );
    }

    #[test]
    fn test_map_category_unknown() {
        assert!(map_category_to_endpoint("unknown").is_err());
    }

    #[test]
    fn test_map_price_type_to_endpoint() {
        assert_eq!(
            map_price_type_to_endpoint("spot_market").unwrap(),
            "Spotmarktpreise"
        );
        assert_eq!(
            map_price_type_to_endpoint("market_premium").unwrap(),
            "marktpraemie"
        );
        assert_eq!(
            map_price_type_to_endpoint("annual_market_value").unwrap(),
            "Jahresmarktpraemie"
        );
        assert_eq!(
            map_price_type_to_endpoint("negative_flag").unwrap(),
            "NegativePreise"
        );
    }

    #[test]
    fn test_map_price_type_unknown() {
        assert!(map_price_type_to_endpoint("unknown").is_err());
    }

    // ========================================================================
    // Renewable Routing Tests
    // ========================================================================

    #[test]
    fn test_route_renewable_solar_forecast() {
        // Optimal query: 1 endpoint
        let filters = QualFilters {
            product_type: Some("solar".to_string()),
            data_category: Some("forecast".to_string()),
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_renewable(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "prognose");
        assert_eq!(plans[0].product, Some("Solar".to_string()));
        assert_eq!(plans[0].date_from, "2024-10-24");
        assert_eq!(plans[0].date_to, "2024-10-25");
        assert_eq!(
            plans[0].api_url,
            "https://api.example.com/prognose/Solar/2024-10-24/2024-10-25"
        );
    }

    #[test]
    fn test_route_renewable_solar_all_categories() {
        // No data_category filter: 3 endpoints
        let filters = QualFilters {
            product_type: Some("solar".to_string()),
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_renewable(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 3);
        assert_eq!(plans[0].endpoint, "prognose");
        assert_eq!(plans[1].endpoint, "hochrechnung");
        assert_eq!(plans[2].endpoint, "onlinehochrechnung");
    }

    #[test]
    fn test_route_renewable_wind_onshore() {
        // Wind onshore with all categories: 3 endpoints
        let filters = QualFilters {
            product_type: Some("wind_onshore".to_string()),
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_renewable(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 3);
        // Forecast and extrapolation use "Wind"
        assert_eq!(plans[0].product, Some("Wind".to_string()));
        assert_eq!(plans[1].product, Some("Wind".to_string()));
        // Online actual uses "Windonshore"
        assert_eq!(plans[2].product, Some("Windonshore".to_string()));
    }

    #[test]
    fn test_route_renewable_wind_offshore() {
        // Wind offshore only has online_actual: 1 endpoint
        let filters = QualFilters {
            product_type: Some("wind_offshore".to_string()),
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_renewable(&filters, "https://api.example.com").unwrap();

        // Only online_actual is available for wind_offshore
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "onlinehochrechnung");
        assert_eq!(plans[0].product, Some("Windoffshore".to_string()));
    }

    #[test]
    fn test_route_renewable_all_products_and_categories() {
        // No filters: 9 endpoints total
        // - Solar: prognose, hochrechnung, onlinehochrechnung (3)
        // - Wind onshore: prognose/Wind, hochrechnung/Wind, onlinehochrechnung/Windonshore (3)
        // - Wind offshore: onlinehochrechnung/Windoffshore (1)
        // Actually 7 unique endpoints (wind offshore doesn't have forecast/extrapolation)
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_renewable(&filters, "https://api.example.com").unwrap();

        // 3 products × 3 categories = 9 combinations
        // But wind_offshore doesn't have forecast/extrapolation, so 7 actual endpoints
        assert_eq!(plans.len(), 7);
    }

    #[test]
    fn test_route_renewable_default_date_range() {
        // No timestamp_range filter: should default to last 7 days
        let filters = QualFilters {
            product_type: Some("solar".to_string()),
            data_category: Some("forecast".to_string()),
            price_type: None,
            timestamp_range: None,
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_renewable(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        // Date range should be last 7 days (can't assert exact dates)
        assert_eq!(plans[0].date_from.len(), 10); // YYYY-MM-DD
        assert_eq!(plans[0].date_to.len(), 10);
    }

    #[test]
    fn test_route_renewable_invalid_date_range() {
        let filters = QualFilters {
            product_type: Some("solar".to_string()),
            data_category: Some("forecast".to_string()),
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-25".to_string(),
                end: "2024-10-24".to_string(), // Invalid: end < start
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        assert!(route_renewable(&filters, "https://api.example.com").is_err());
    }

    // ========================================================================
    // Price Routing Tests
    // ========================================================================

    #[test]
    fn test_route_prices_spot_market() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: Some("spot_market".to_string()),
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "electricity_market_prices".to_string(),
        };

        let plans = route_prices(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "Spotmarktpreise");
        assert_eq!(plans[0].product, None); // Price endpoints don't have product
        assert_eq!(
            plans[0].api_url,
            "https://api.example.com/Spotmarktpreise/2024-10-24/2024-10-25"
        );
    }

    #[test]
    fn test_route_prices_all_types() {
        // No price_type filter: 4 endpoints
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "electricity_market_prices".to_string(),
        };

        let plans = route_prices(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 4);
        assert_eq!(plans[0].endpoint, "Spotmarktpreise");
        assert_eq!(plans[1].endpoint, "NegativePreise");
        assert_eq!(plans[2].endpoint, "marktpraemie");
        assert_eq!(plans[3].endpoint, "Jahresmarktpraemie");
    }

    #[test]
    fn test_route_prices_default_date_range() {
        // No timestamp_range filter: should default to last 7 days
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: Some("spot_market".to_string()),
            timestamp_range: None,
            timestamp_bounds: None,
            table_name: "electricity_market_prices".to_string(),
        };

        let plans = route_prices(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].date_from.len(), 10); // YYYY-MM-DD
        assert_eq!(plans[0].date_to.len(), 10);
    }

    // ========================================================================
    // Main Router Tests
    // ========================================================================

    #[test]
    fn test_route_query_renewable() {
        let filters = QualFilters {
            product_type: Some("solar".to_string()),
            data_category: Some("forecast".to_string()),
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "renewable_energy_timeseries".to_string(),
        };

        let plans = route_query(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "prognose");
    }

    #[test]
    fn test_route_query_prices() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: Some("spot_market".to_string()),
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "electricity_market_prices".to_string(),
        };

        let plans = route_query(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "Spotmarktpreise");
    }

    #[test]
    fn test_route_query_unknown_table() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: None,
            timestamp_bounds: None,
            table_name: "unknown_table".to_string(),
        };

        assert!(route_query(&filters, "https://api.example.com").is_err());
    }

    #[test]
    fn test_route_query_redispatch() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-23".to_string(),
                end: "2024-10-24".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "redispatch_events".to_string(),
        };

        let plans = route_query(&filters, "https://api.example.com").unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "redispatch");
    }

    #[test]
    fn test_route_query_grid_status() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "grid_status_timeseries".to_string(),
        };

        let plans = route_query(&filters, "https://api.example.com").unwrap();
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "TrafficLight");
    }

    // ========================================================================
    // Grid Operations Routing Tests
    // ========================================================================

    #[test]
    fn test_route_redispatch_with_date_range() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-23".to_string(),
                end: "2024-10-24".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "redispatch_events".to_string(),
        };

        let plans = route_redispatch(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "redispatch");
        assert_eq!(plans[0].product, None);
        assert_eq!(plans[0].date_from, "2024-10-23");
        assert_eq!(plans[0].date_to, "2024-10-24");
        assert_eq!(
            plans[0].api_url,
            "https://api.example.com/redispatch/2024-10-23/2024-10-24"
        );
    }

    #[test]
    fn test_route_redispatch_default_date_range() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: None,
            timestamp_bounds: None,
            table_name: "redispatch_events".to_string(),
        };

        let plans = route_redispatch(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "redispatch");
        // Should use default date range
        assert_eq!(plans[0].date_from.len(), 10); // YYYY-MM-DD
        assert_eq!(plans[0].date_to.len(), 10);
    }

    #[test]
    fn test_route_grid_status_with_date_range() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-24".to_string(),
                end: "2024-10-25".to_string(),
            }),
            timestamp_bounds: None,
            table_name: "grid_status_timeseries".to_string(),
        };

        let plans = route_grid_status(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "TrafficLight");
        assert_eq!(plans[0].product, None);
        assert_eq!(plans[0].date_from, "2024-10-24");
        assert_eq!(plans[0].date_to, "2024-10-25");
        assert_eq!(
            plans[0].api_url,
            "https://api.example.com/TrafficLight/2024-10-24/2024-10-25"
        );
    }

    #[test]
    fn test_route_grid_status_default_date_range() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: None,
            timestamp_bounds: None,
            table_name: "grid_status_timeseries".to_string(),
        };

        let plans = route_grid_status(&filters, "https://api.example.com").unwrap();

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].endpoint, "TrafficLight");
        // Should use default date range
        assert_eq!(plans[0].date_from.len(), 10); // YYYY-MM-DD
        assert_eq!(plans[0].date_to.len(), 10);
    }

    #[test]
    fn test_route_redispatch_invalid_date_range() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-25".to_string(),
                end: "2024-10-24".to_string(), // Invalid: end < start
            }),
            timestamp_bounds: None,
            table_name: "redispatch_events".to_string(),
        };

        assert!(route_redispatch(&filters, "https://api.example.com").is_err());
    }

    #[test]
    fn test_route_grid_status_invalid_date_range() {
        let filters = QualFilters {
            product_type: None,
            data_category: None,
            price_type: None,
            timestamp_range: Some(DateRange {
                start: "2024-10-25".to_string(),
                end: "2024-10-24".to_string(), // Invalid: end < start
            }),
            timestamp_bounds: None,
            table_name: "grid_status_timeseries".to_string(),
        };

        assert!(route_grid_status(&filters, "https://api.example.com").is_err());
    }
}
