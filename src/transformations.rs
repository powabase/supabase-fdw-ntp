//! ETL transformation functions for NTP FDW
//!
//! This module provides 11 transformation functions to convert German-formatted CSV data
//! from the NTP API into normalized SQL types for PostgreSQL foreign tables.
//!
//! # Transformations
//!
//! 1. **German decimal conversion**: `"119,5"` → `119.5`
//! 2. **"N.A." → NULL mapping**: `"N.A."` → `None`
//! 3. **Timestamp normalization**: `"23.10.2024" + "22:00"` → `"2024-10-23T22:00:00Z"`
//! 4. **Interval calculation**: `(start, end)` → `15` minutes
//! 5. **TSO zone flattening**: 4 CSV columns → 4 Option<f64> values
//! 6. **Product type normalization**: `"Solar"` → `"solar"`
//! 7. **Data category extraction**: `"prognose"` → `"forecast"`
//! 8. **Price unit conversion**: `8.273 ct/kWh` → `82.73 EUR/MWh`
//! 9. **Price type detection**: `"Spotmarktpreise"` → `"spot_market"`
//! 10. **Source endpoint building**: `"prognose/Solar/2024-10-24/2024-10-25"`
//!
//! # Example
//!
//! ```rust
//! use supabase_fdw_ntp::transformations::*;
//!
//! // Parse German decimal
//! let value = parse_german_decimal("119,5").unwrap();
//! assert_eq!(value, 119.5);
//!
//! // Handle N.A. values
//! let na_value = parse_value("N.A.").unwrap();
//! assert_eq!(na_value, None);
//!
//! // Normalize timestamps
//! let ts = parse_timestamp("23.10.2024", "22:00", "UTC").unwrap();
//! assert_eq!(ts, "2024-10-23T22:00:00Z");
//! ```

use crate::error::ParseError;
use chrono::{DateTime, Duration, NaiveDate, NaiveTime};

/// Helper struct for TSO zone data
///
/// Represents the 4 German Transmission System Operator zones with their production values.
#[derive(Debug, Clone, PartialEq)]
pub struct TsoZones {
    /// 50Hertz zone (Eastern Germany)
    pub tso_50hertz_mw: Option<f64>,
    /// Amprion zone (Western Germany)
    pub tso_amprion_mw: Option<f64>,
    /// TenneT zone (Northern Germany)
    pub tso_tennet_mw: Option<f64>,
    /// TransnetBW zone (Southern Germany)
    pub tso_transnetbw_mw: Option<f64>,
}

// ============================================================================
// Transformation 1: German Decimal Conversion
// ============================================================================

/// Parse German decimal format (comma as decimal separator)
///
/// Converts German decimal notation (comma) to standard notation (period) and parses to f64.
///
/// # Arguments
///
/// * `value` - String value with German decimal format (e.g., "119,5")
///
/// # Returns
///
/// * `Ok(f64)` - Parsed decimal value
/// * `Err(ParseError::InvalidDecimal)` - If value is empty or cannot be parsed
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_german_decimal;
/// assert_eq!(parse_german_decimal("119,5").unwrap(), 119.5);
/// assert_eq!(parse_german_decimal("2025,870").unwrap(), 2025.870);
/// assert_eq!(parse_german_decimal("0,000").unwrap(), 0.0);
/// assert!(parse_german_decimal("").is_err());
/// assert!(parse_german_decimal("abc").is_err());
/// ```
pub fn parse_german_decimal(value: &str) -> Result<f64, ParseError> {
    if value.trim().is_empty() {
        return Err(ParseError::InvalidDecimal(value.to_string()));
    }

    value
        .replace(',', ".")
        .parse::<f64>()
        .map_err(|_| ParseError::InvalidDecimal(value.to_string()))
}

// ============================================================================
// Transformation 2: "N.A." → NULL Mapping
// ============================================================================

/// Parse value with "N.A." and "N.E." handling and negative value validation
///
/// Converts "N.A." (not available), "N.E." (Nicht Erfasst = not recorded), and empty strings
/// to None, otherwise parses as decimal. Rejects negative values as they are physically
/// impossible for electrical production (MW).
///
/// **Note:** This function is specifically for TSO zone production values (MW).
/// For price parsing (which allows negative values), use `parse_german_decimal` directly.
///
/// # Arguments
///
/// * `value` - String value from CSV
///
/// # Returns
///
/// * `Ok(None)` - For "N.A.", "N.E.", or empty strings (represents SQL NULL)
/// * `Ok(Some(f64))` - For valid non-negative numeric values
/// * `Err(ParseError::InvalidDecimal)` - For invalid formats or negative values
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_value;
/// assert_eq!(parse_value("N.A.").unwrap(), None);
/// assert_eq!(parse_value("N.E.").unwrap(), None);
/// assert_eq!(parse_value("").unwrap(), None);
/// assert_eq!(parse_value("   ").unwrap(), None);
/// assert_eq!(parse_value("123,456").unwrap(), Some(123.456));
/// assert_eq!(parse_value("0,000").unwrap(), Some(0.0));
/// assert!(parse_value("-100,5").is_err()); // Negative production values are invalid
/// ```
pub fn parse_value(value: &str) -> Result<Option<f64>, ParseError> {
    let trimmed = value.trim();

    // Case-insensitive NULL variant matching (handles API format changes)
    // "N.A." = Not Available, "N.E." = Nicht Erfasst (Not Recorded)
    let upper = trimmed.to_uppercase();
    if upper == "N.A."
        || upper == "NA"
        || upper == "N.A"
        || upper == "N.E."
        || upper == "NE"
        || upper == "N.E"
        || trimmed.is_empty()
    {
        return Ok(None);
    }

    let parsed = parse_german_decimal(trimmed)?;

    // Validate: electrical production (MW) cannot be negative
    if parsed < 0.0 {
        return Err(ParseError::InvalidDecimal(format!(
            "Negative production value not allowed: {} MW",
            value
        )));
    }

    Ok(Some(parsed))
}

// ============================================================================
// Transformation 3: Timestamp Normalization
// ============================================================================

/// Parse timestamp from German or ISO date format
///
/// Handles both date formats:
/// - German: DD.MM.YYYY (e.g., "23.10.2024")
/// - ISO: YYYY-MM-DD (e.g., "2024-10-24")
///
/// Combines date and time into ISO 8601 format with UTC timezone.
///
/// # Arguments
///
/// * `datum` - Date string (DD.MM.YYYY or YYYY-MM-DD)
/// * `zeit` - Time string (HH:MM)
/// * `timezone` - Timezone indicator (must be "UTC")
///
/// # Returns
///
/// * `Ok(String)` - ISO 8601 timestamp (e.g., "2024-10-23T22:00:00Z")
/// * `Err(ParseError::InvalidTimezone)` - If timezone is not "UTC"
/// * `Err(ParseError::InvalidTimestamp)` - If date or time format is invalid
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_timestamp;
/// // German format
/// assert_eq!(
///     parse_timestamp("23.10.2024", "22:00", "UTC").unwrap(),
///     "2024-10-23T22:00:00Z"
/// );
///
/// // ISO format
/// assert_eq!(
///     parse_timestamp("2024-10-24", "06:30", "UTC").unwrap(),
///     "2024-10-24T06:30:00Z"
/// );
///
/// // Invalid timezone
/// assert!(parse_timestamp("2024-10-24", "06:30", "CET").is_err());
/// ```
pub fn parse_timestamp(datum: &str, zeit: &str, timezone: &str) -> Result<String, ParseError> {
    // Validate timezone
    if timezone != "UTC" {
        return Err(ParseError::InvalidTimezone(timezone.to_string()));
    }

    // Try parsing German format (DD.MM.YYYY) first, then ISO format (YYYY-MM-DD)
    let date = NaiveDate::parse_from_str(datum, "%d.%m.%Y")
        .or_else(|_| NaiveDate::parse_from_str(datum, "%Y-%m-%d"))
        .map_err(|_| ParseError::InvalidTimestamp(format!("{} {}", datum, zeit)))?;

    // Parse time (HH:MM)
    let time = NaiveTime::parse_from_str(zeit, "%H:%M")
        .map_err(|_| ParseError::InvalidTimestamp(format!("{} {}", datum, zeit)))?;

    // Combine into UTC datetime
    let datetime = date.and_time(time).and_utc();

    // Format as ISO 8601
    Ok(datetime.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

/// Parse interval timestamps with midnight-crossing detection (Bug #5 fix)
///
/// Handles the case where interval end time is <= start time, which indicates
/// the interval crosses midnight (e.g., 23:45 - 00:00).
///
/// # Arguments
///
/// * `datum` - Date string (DD.MM.YYYY or YYYY-MM-DD)
/// * `von` - Start time (HH:MM)
/// * `bis` - End time (HH:MM)
/// * `tz_von` - Start timezone (must be "UTC")
/// * `tz_bis` - End timezone (must be "UTC")
///
/// # Returns
///
/// * `Ok((start_timestamp, end_timestamp))` - Both as ISO 8601 strings
/// * `Err(ParseError)` - If parsing fails
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_interval_timestamps;
/// // Normal interval (same day)
/// let (start, end) = parse_interval_timestamps(
///     "20.10.2024", "10:00", "11:00", "UTC", "UTC"
/// ).unwrap();
/// assert_eq!(start, "2024-10-20T10:00:00Z");
/// assert_eq!(end, "2024-10-20T11:00:00Z");
///
/// // Midnight crossing (end time is before start time)
/// let (start, end) = parse_interval_timestamps(
///     "20.10.2024", "23:45", "00:00", "UTC", "UTC"
/// ).unwrap();
/// assert_eq!(start, "2024-10-20T23:45:00Z");
/// assert_eq!(end, "2024-10-21T00:00:00Z");  // Next day!
/// ```
pub fn parse_interval_timestamps(
    datum: &str,
    von: &str,
    bis: &str,
    tz_von: &str,
    tz_bis: &str,
) -> Result<(String, String), ParseError> {
    // Validate timezones
    if tz_von != "UTC" || tz_bis != "UTC" {
        return Err(ParseError::InvalidTimezone(format!(
            "Expected UTC, got von={}, bis={}",
            tz_von, tz_bis
        )));
    }

    // Parse times to detect midnight crossing
    let start_time = NaiveTime::parse_from_str(von, "%H:%M")
        .map_err(|_| ParseError::InvalidTimestamp(format!("Invalid time: {}", von)))?;
    let end_time = NaiveTime::parse_from_str(bis, "%H:%M")
        .map_err(|_| ParseError::InvalidTimestamp(format!("Invalid time: {}", bis)))?;

    // Parse base date (supports both German DD.MM.YYYY and ISO YYYY-MM-DD)
    let base_date = NaiveDate::parse_from_str(datum, "%d.%m.%Y")
        .or_else(|_| NaiveDate::parse_from_str(datum, "%Y-%m-%d"))
        .map_err(|_| ParseError::InvalidTimestamp(format!("Invalid date: {}", datum)))?;

    // Parse start timestamp (always uses base date)
    let start_timestamp = parse_timestamp(datum, von, tz_von)?;

    // Detect midnight crossing: if end_time <= start_time, assume next day
    let end_date = if end_time <= start_time {
        base_date + Duration::days(1)
    } else {
        base_date
    };

    // Format end date for parsing
    let end_date_str = end_date.format("%d.%m.%Y").to_string();
    let end_timestamp = parse_timestamp(&end_date_str, bis, tz_bis)?;

    Ok((start_timestamp, end_timestamp))
}

// ============================================================================
// Transformation 4: Interval Duration Calculation
// ============================================================================

/// Calculate interval duration in minutes
///
/// Computes the difference between two ISO 8601 timestamps in minutes.
///
/// # Arguments
///
/// * `start` - Start timestamp (ISO 8601 format)
/// * `end` - End timestamp (ISO 8601 format)
///
/// # Returns
///
/// * `Ok(i16)` - Duration in minutes (e.g., 15, 60)
/// * `Err(ParseError::InvalidTimestamp)` - If timestamps cannot be parsed
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::calculate_interval_minutes;
/// // 15-minute interval
/// let minutes = calculate_interval_minutes(
///     "2024-10-24T06:00:00Z",
///     "2024-10-24T06:15:00Z"
/// ).unwrap();
/// assert_eq!(minutes, 15);
///
/// // 60-minute interval
/// let minutes = calculate_interval_minutes(
///     "2024-10-24T06:00:00Z",
///     "2024-10-24T07:00:00Z"
/// ).unwrap();
/// assert_eq!(minutes, 60);
/// ```
pub fn calculate_interval_minutes(start: &str, end: &str) -> Result<i16, ParseError> {
    let start_dt = DateTime::parse_from_rfc3339(start)
        .map_err(|_| ParseError::InvalidTimestamp(start.to_string()))?;
    let end_dt = DateTime::parse_from_rfc3339(end)
        .map_err(|_| ParseError::InvalidTimestamp(end.to_string()))?;

    let duration = end_dt.signed_duration_since(start_dt);
    let minutes = duration.num_minutes();

    // Safe conversion with overflow check (i64 → i16)
    // Max i16 = 32,767 minutes = ~22.75 days
    i16::try_from(minutes).map_err(|_| {
        ParseError::InvalidTimestamp(format!(
            "Interval too large: {} minutes (max: {} minutes / ~{} days). Start: {}, End: {}",
            minutes,
            i16::MAX,
            i16::MAX / (60 * 24), // Convert to days for context
            start,
            end
        ))
    })
}

// ============================================================================
// Transformation 5: TSO Zone Flattening
// ============================================================================

/// Parse TSO zone values from CSV row data
///
/// Extracts and parses the 4 German TSO zone production values.
///
/// # Arguments
///
/// * `row_data` - Slice of (column_name, value) tuples
///
/// # Returns
///
/// * `Ok(TsoZones)` - Parsed TSO zone data
/// * `Err(ParseError::MissingColumn)` - If a required column is missing
/// * `Err(ParseError::InvalidDecimal)` - If a value cannot be parsed
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_tso_zones;
/// let row_data = vec![
///     ("50Hertz (MW)", "100,5"),
///     ("Amprion (MW)", "200,3"),
///     ("TenneT TSO (MW)", "300,7"),
///     ("TransnetBW (MW)", "150,2"),
/// ];
///
/// let zones = parse_tso_zones(&row_data).unwrap();
/// assert_eq!(zones.tso_50hertz_mw, Some(100.5));
/// assert_eq!(zones.tso_amprion_mw, Some(200.3));
/// assert_eq!(zones.tso_tennet_mw, Some(300.7));
/// assert_eq!(zones.tso_transnetbw_mw, Some(150.2));
/// ```
pub fn parse_tso_zones(row_data: &[(&str, &str)]) -> Result<TsoZones, ParseError> {
    let mut zones = TsoZones {
        tso_50hertz_mw: None,
        tso_amprion_mw: None,
        tso_tennet_mw: None,
        tso_transnetbw_mw: None,
    };

    for (col_name, value) in row_data {
        match *col_name {
            "50Hertz (MW)" => zones.tso_50hertz_mw = parse_value(value)?,
            "Amprion (MW)" => zones.tso_amprion_mw = parse_value(value)?,
            "TenneT TSO (MW)" => zones.tso_tennet_mw = parse_value(value)?,
            "TransnetBW (MW)" => zones.tso_transnetbw_mw = parse_value(value)?,
            _ => {} // Ignore other columns
        }
    }

    Ok(zones)
}

// ============================================================================
// Transformation 6: Product Type Normalization
// ============================================================================

/// Normalize API product name to database enum
///
/// Maps API product names to standardized lowercase enum values.
///
/// # Arguments
///
/// * `api_product` - Product name from API
///
/// # Returns
///
/// * `Ok(String)` - Normalized product type
/// * `Err(ParseError::UnknownProduct)` - For unknown products
///
/// # Mappings
///
/// - `"Solar"` → `"solar"`
/// - `"Wind"` → `"wind_onshore"`
/// - `"Windonshore"` → `"wind_onshore"`
/// - `"Windoffshore"` → `"wind_offshore"`
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::normalize_product_type;
/// assert_eq!(normalize_product_type("Solar").unwrap(), "solar");
/// assert_eq!(normalize_product_type("Wind").unwrap(), "wind_onshore");
/// assert_eq!(normalize_product_type("Windonshore").unwrap(), "wind_onshore");
/// assert_eq!(normalize_product_type("Windoffshore").unwrap(), "wind_offshore");
/// assert!(normalize_product_type("Biomass").is_err());
/// ```
pub fn normalize_product_type(api_product: &str) -> Result<String, ParseError> {
    match api_product {
        "Solar" => Ok("solar".to_string()),
        "Wind" => Ok("wind_onshore".to_string()),
        "Windonshore" => Ok("wind_onshore".to_string()),
        "Windoffshore" => Ok("wind_offshore".to_string()),
        _ => Err(ParseError::UnknownProduct(api_product.to_string())),
    }
}

// ============================================================================
// Transformation 7: Data Category Extraction
// ============================================================================

/// Extract data category from endpoint path
///
/// Maps German endpoint names to English data categories.
///
/// # Arguments
///
/// * `endpoint` - API endpoint path
///
/// # Returns
///
/// * `Ok(String)` - Data category
/// * `Err(ParseError::UnknownDataCategory)` - For unknown endpoints
///
/// # Mappings
///
/// - `"hochrechnung"` → `"extrapolation"`
/// - `"onlinehochrechnung"` → `"online_actual"`
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::extract_data_category;
/// assert_eq!(extract_data_category("hochrechnung/Wind").unwrap(), "extrapolation");
/// assert_eq!(extract_data_category("onlinehochrechnung/Solar").unwrap(), "online_actual");
/// assert!(extract_data_category("unknown/endpoint").is_err());
/// ```
pub fn extract_data_category(endpoint: &str) -> Result<String, ParseError> {
    let lower = endpoint.to_lowercase();

    // Check in order: onlinehochrechnung must be checked before hochrechnung
    // (since "onlinehochrechnung" contains "hochrechnung")
    if lower.contains("onlinehochrechnung") {
        Ok("online_actual".to_string())
    } else if lower.contains("hochrechnung") {
        Ok("extrapolation".to_string())
    } else {
        Err(ParseError::UnknownDataCategory(endpoint.to_string()))
    }
}

// ============================================================================
// Transformation 8: Price Unit Conversion
// ============================================================================

/// Convert price from ct/kWh to EUR/MWh
///
/// Applies conversion factor: 1 ct/kWh = 10 EUR/MWh
///
/// # Arguments
///
/// * `ct_kwh` - Price in ct/kWh
///
/// # Returns
///
/// * `f64` - Price in EUR/MWh
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::convert_price_to_eur_mwh;
/// assert_eq!(convert_price_to_eur_mwh(8.273), 82.73);
/// assert_eq!(convert_price_to_eur_mwh(-0.201), -2.01);
/// ```
pub fn convert_price_to_eur_mwh(ct_kwh: f64) -> f64 {
    ct_kwh * 10.0
}

// ============================================================================
// Transformation 9: Price Type Detection
// ============================================================================

/// Detect price type from endpoint name
///
/// Maps endpoint names to price type categories.
///
/// # Arguments
///
/// * `endpoint` - API endpoint name
///
/// # Returns
///
/// * `String` - Price type
///
/// # Mappings
///
/// - `"Spotmarktpreise"` → `"spot_market"`
/// - `"marktpraemie"` → `"market_premium"`
/// - `"Jahresmarktpraemie"` → `"annual_market_value"`
/// - `"NegativePreise"` → `"negative_flag"`
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::detect_price_type;
/// assert_eq!(detect_price_type("Spotmarktpreise"), "spot_market");
/// assert_eq!(detect_price_type("marktpraemie"), "market_premium");
/// assert_eq!(detect_price_type("Jahresmarktpraemie"), "annual_market_value");
/// assert_eq!(detect_price_type("NegativePreise"), "negative_flag");
/// ```
pub fn detect_price_type(endpoint: &str) -> String {
    let lower = endpoint.to_lowercase();

    if lower.contains("spotmarktpreise") {
        "spot_market".to_string()
    } else if lower.contains("jahresmarktpraemie") {
        "annual_market_value".to_string()
    } else if lower.contains("marktpraemie") {
        "market_premium".to_string()
    } else if lower.contains("negativepreise") {
        "negative_flag".to_string()
    } else {
        "spot_market".to_string() // Default
    }
}

// ============================================================================
// Transformation 10: Source Endpoint Building
// ============================================================================

/// Build source endpoint path for traceability
///
/// Constructs a standardized endpoint path for the source_endpoint metadata field.
///
/// # Arguments
///
/// * `endpoint` - Endpoint name (e.g., "prognose")
/// * `product` - Product name (e.g., "Solar")
/// * `date_from` - Start date
/// * `date_to` - End date
///
/// # Returns
///
/// * `String` - Formatted endpoint path
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::build_source_endpoint;
/// let endpoint = build_source_endpoint("prognose", "Solar", "2024-10-24", "2024-10-25");
/// assert_eq!(endpoint, "prognose/Solar/2024-10-24/2024-10-25");
/// ```
pub fn build_source_endpoint(
    endpoint: &str,
    product: &str,
    date_from: &str,
    date_to: &str,
) -> String {
    format!("{}/{}/{}/{}", endpoint, product, date_from, date_to)
}

// ============================================================================
// Grid Operations Transformations
// ============================================================================

/// Parse redispatch timestamp from German format
///
/// Handles German date format (DD.MM.YYYY) combined with 24-hour time (HH:MM).
/// Validates timezone is UTC.
///
/// # Arguments
///
/// * `datum` - Date in DD.MM.YYYY format (e.g., "23.10.2024")
/// * `uhrzeit` - Time in HH:MM format (e.g., "22:00")
/// * `zeitzone` - Timezone (must be "UTC")
///
/// # Returns
///
/// * `Ok(String)` - ISO 8601 timestamp (e.g., "2024-10-23T22:00:00Z")
/// * `Err(ParseError::InvalidTimezone)` - If timezone is not "UTC"
/// * `Err(ParseError::InvalidTimestamp)` - If date or time format is invalid
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_redispatch_timestamp;
/// let dt = parse_redispatch_timestamp("23.10.2024", "22:00", "UTC").unwrap();
/// assert_eq!(dt, "2024-10-23T22:00:00Z");
/// ```
pub fn parse_redispatch_timestamp(
    datum: &str,
    uhrzeit: &str,
    zeitzone: &str,
) -> Result<String, ParseError> {
    // Validate timezone
    if zeitzone != "UTC" {
        return Err(ParseError::InvalidTimezone(zeitzone.to_string()));
    }

    // Concatenate date and time
    let dt_string = format!("{} {}", datum, uhrzeit);

    // Parse German date format (DD.MM.YYYY HH:MM)
    use chrono::NaiveDateTime;
    let naive_dt = NaiveDateTime::parse_from_str(&dt_string, "%d.%m.%Y %H:%M")
        .map_err(|_| ParseError::InvalidTimestamp(dt_string.clone()))?;

    // Convert to UTC DateTime
    let utc_dt = naive_dt.and_utc();

    // Format as ISO 8601
    Ok(utc_dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
}

/// Normalize German direction to English enum
///
/// Maps German redispatch direction text to standardized English enum values.
///
/// # Arguments
///
/// * `richtung` - German direction text from CSV
///
/// # Returns
///
/// * `Ok(String)` - "increase_generation" or "reduce_generation"
/// * `Err(ParseError::UnknownDirection)` - For unknown direction values
///
/// # Mappings
///
/// - `"Wirkleistungseinspeisung erhöhen"` → `"increase_generation"`
/// - `"Wirkleistungseinspeisung reduzieren"` → `"reduce_generation"`
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::normalize_direction;
/// assert_eq!(
///     normalize_direction("Wirkleistungseinspeisung erhöhen").unwrap(),
///     "increase_generation"
/// );
/// assert_eq!(
///     normalize_direction("Wirkleistungseinspeisung reduzieren").unwrap(),
///     "reduce_generation"
/// );
/// ```
pub fn normalize_direction(richtung: &str) -> Result<String, ParseError> {
    match richtung {
        "Wirkleistungseinspeisung erhöhen" => Ok("increase_generation".to_string()),
        "Wirkleistungseinspeisung reduzieren" => Ok("reduce_generation".to_string()),
        _ => Err(ParseError::UnknownDirection(richtung.to_string())),
    }
}

/// Parse ISO 8601 timestamp
///
/// Parses ISO 8601 timestamp strings (used by TrafficLight JSON endpoint).
///
/// Handles both RFC 3339 format with timezone (e.g., "2024-10-24T00:00:00Z")
/// and partial ISO 8601 without timezone (e.g., "2024-10-24T00:00:00", assumes UTC).
///
/// # Arguments
///
/// * `iso_string` - ISO 8601 timestamp with or without timezone
///
/// # Returns
///
/// * `Ok(String)` - Normalized ISO 8601 timestamp with Zulu timezone
/// * `Err(ParseError::InvalidTimestamp)` - If timestamp cannot be parsed
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::parse_iso8601_timestamp;
/// // With timezone
/// let dt1 = parse_iso8601_timestamp("2024-10-24T00:00:00Z").unwrap();
/// assert_eq!(dt1, "2024-10-24T00:00:00Z");
///
/// // Without timezone (assumes UTC)
/// let dt2 = parse_iso8601_timestamp("2024-10-24T00:00:00").unwrap();
/// assert_eq!(dt2, "2024-10-24T00:00:00Z");
/// ```
pub fn parse_iso8601_timestamp(iso_string: &str) -> Result<String, ParseError> {
    use chrono::{DateTime, NaiveDateTime};

    // Try parsing RFC 3339 with timezone first (e.g., "2024-10-24T00:00:00Z")
    if let Ok(dt) = DateTime::parse_from_rfc3339(iso_string) {
        return Ok(dt
            .with_timezone(&chrono::Utc)
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string());
    }

    // Fallback: Parse without timezone and assume UTC (e.g., "2024-10-24T00:00:00")
    // This handles the case where TrafficLight API returns timestamps without 'Z' suffix
    NaiveDateTime::parse_from_str(iso_string, "%Y-%m-%dT%H:%M:%S")
        .map(|naive_dt| naive_dt.and_utc().format("%Y-%m-%dT%H:%M:%SZ").to_string())
        .map_err(|parse_err| {
            ParseError::InvalidTimestamp(format!(
                "{} (expected ISO 8601 format 'YYYY-MM-DDTHH:MM:SS' with optional 'Z' suffix. Parse error: {})",
                iso_string,
                parse_err
            ))
        })
}

/// Validate grid status value
///
/// Ensures grid status is one of the three valid traffic light values.
///
/// # Arguments
///
/// * `value` - Grid status string from JSON
///
/// # Returns
///
/// * `Ok(String)` - Same value if valid
/// * `Err(ParseError::InvalidGridStatus)` - For invalid values
///
/// # Valid Values
///
/// - `"GREEN"` = Normal operation
/// - `"GREEN_NEG"` = Normal operation with negative pricing signal
/// - `"YELLOW"` = Elevated stress
/// - `"YELLOW_NEG"` = Elevated stress with negative pricing signal
/// - `"RED"` = Critical stress
/// - `"RED_NEG"` = Critical stress with negative pricing signal
///
/// The `_NEG` suffix indicates a negative pricing signal from the electricity market.
/// These variants were discovered from actual API responses (undocumented).
///
/// # Examples
///
/// ```
/// # use supabase_fdw_ntp::transformations::validate_grid_status;
/// assert_eq!(validate_grid_status("GREEN").unwrap(), "GREEN");
/// assert_eq!(validate_grid_status("YELLOW").unwrap(), "YELLOW");
/// assert_eq!(validate_grid_status("RED").unwrap(), "RED");
/// assert_eq!(validate_grid_status("YELLOW_NEG").unwrap(), "YELLOW_NEG");
/// assert!(validate_grid_status("ORANGE").is_err());
/// ```
pub fn validate_grid_status(value: &str) -> Result<String, ParseError> {
    match value {
        "GREEN" | "GREEN_NEG" | "YELLOW" | "YELLOW_NEG" | "RED" | "RED_NEG" => {
            Ok(value.to_string())
        }
        _ => Err(ParseError::InvalidGridStatus(value.to_string())),
    }
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Tests for parse_german_decimal (4 tests)
    // ========================================================================

    #[test]
    fn test_german_decimal_with_comma() {
        assert_eq!(parse_german_decimal("119,5").unwrap(), 119.5);
        assert_eq!(parse_german_decimal("2025,870").unwrap(), 2025.870);
    }

    #[test]
    fn test_german_decimal_with_period() {
        // Also accept period (standard format)
        assert_eq!(parse_german_decimal("119.5").unwrap(), 119.5);
        assert_eq!(parse_german_decimal("0.000").unwrap(), 0.0);
    }

    #[test]
    fn test_german_decimal_invalid_format() {
        assert!(parse_german_decimal("abc").is_err());
        assert!(parse_german_decimal("119,5a").is_err());
        assert!(parse_german_decimal("1.2.3").is_err());
    }

    #[test]
    fn test_german_decimal_empty_string() {
        assert!(parse_german_decimal("").is_err());
        assert!(parse_german_decimal("   ").is_err());
    }

    // ========================================================================
    // Tests for parse_value (4 tests)
    // ========================================================================

    #[test]
    fn test_parse_value_na() {
        assert_eq!(parse_value("N.A.").unwrap(), None);
    }

    #[test]
    fn test_parse_value_ne() {
        // "N.E." = Nicht Erfasst (Not Recorded) - used for wind offshore data
        assert_eq!(parse_value("N.E.").unwrap(), None);
    }

    #[test]
    fn test_parse_value_empty() {
        assert_eq!(parse_value("").unwrap(), None);
        assert_eq!(parse_value("   ").unwrap(), None);
    }

    #[test]
    fn test_parse_value_valid() {
        assert_eq!(parse_value("123,456").unwrap(), Some(123.456));
        assert_eq!(parse_value("0,000").unwrap(), Some(0.0));
    }

    #[test]
    fn test_parse_value_invalid() {
        assert!(parse_value("abc").is_err());
    }

    #[test]
    fn test_parse_value_negative_rejected() {
        // Negative production values should be rejected (physically impossible)
        assert!(parse_value("-100,5").is_err());
        assert!(parse_value("-0,001").is_err());

        // Zero is valid (no production)
        assert_eq!(parse_value("0,000").unwrap(), Some(0.0));

        // Positive values are valid
        assert_eq!(parse_value("100,5").unwrap(), Some(100.5));
    }

    // ========================================================================
    // Tests for parse_timestamp (6 tests)
    // ========================================================================

    #[test]
    fn test_timestamp_german_format() {
        assert_eq!(
            parse_timestamp("23.10.2024", "22:00", "UTC").unwrap(),
            "2024-10-23T22:00:00Z"
        );
    }

    #[test]
    fn test_timestamp_iso_format() {
        assert_eq!(
            parse_timestamp("2024-10-24", "06:30", "UTC").unwrap(),
            "2024-10-24T06:30:00Z"
        );
    }

    #[test]
    fn test_timestamp_invalid_timezone() {
        assert!(parse_timestamp("2024-10-24", "06:30", "CET").is_err());
    }

    #[test]
    fn test_timestamp_invalid_date_format() {
        assert!(parse_timestamp("32.10.2024", "06:30", "UTC").is_err());
        assert!(parse_timestamp("2024-13-01", "06:30", "UTC").is_err());
    }

    #[test]
    fn test_timestamp_midnight() {
        assert_eq!(
            parse_timestamp("2024-10-24", "00:00", "UTC").unwrap(),
            "2024-10-24T00:00:00Z"
        );
    }

    #[test]
    fn test_timestamp_edge_case() {
        assert_eq!(
            parse_timestamp("2024-10-24", "23:59", "UTC").unwrap(),
            "2024-10-24T23:59:00Z"
        );
    }

    // ========================================================================
    // Tests for calculate_interval_minutes (3 tests)
    // ========================================================================

    #[test]
    fn test_interval_15_minutes() {
        let minutes =
            calculate_interval_minutes("2024-10-24T06:00:00Z", "2024-10-24T06:15:00Z").unwrap();
        assert_eq!(minutes, 15);
    }

    #[test]
    fn test_interval_60_minutes() {
        let minutes =
            calculate_interval_minutes("2024-10-24T06:00:00Z", "2024-10-24T07:00:00Z").unwrap();
        assert_eq!(minutes, 60);
    }

    #[test]
    fn test_interval_cross_day() {
        let minutes =
            calculate_interval_minutes("2024-10-24T23:45:00Z", "2024-10-25T00:00:00Z").unwrap();
        assert_eq!(minutes, 15);
    }

    // ========================================================================
    // Tests for parse_tso_zones (3 tests)
    // ========================================================================

    #[test]
    fn test_tso_zones_all_values() {
        let row_data = vec![
            ("50Hertz (MW)", "100,5"),
            ("Amprion (MW)", "200,3"),
            ("TenneT TSO (MW)", "300,7"),
            ("TransnetBW (MW)", "150,2"),
        ];

        let zones = parse_tso_zones(&row_data).unwrap();
        assert_eq!(zones.tso_50hertz_mw, Some(100.5));
        assert_eq!(zones.tso_amprion_mw, Some(200.3));
        assert_eq!(zones.tso_tennet_mw, Some(300.7));
        assert_eq!(zones.tso_transnetbw_mw, Some(150.2));
    }

    #[test]
    fn test_tso_zones_all_na() {
        let row_data = vec![
            ("50Hertz (MW)", "N.A."),
            ("Amprion (MW)", "N.A."),
            ("TenneT TSO (MW)", "N.A."),
            ("TransnetBW (MW)", "N.A."),
        ];

        let zones = parse_tso_zones(&row_data).unwrap();
        assert_eq!(zones.tso_50hertz_mw, None);
        assert_eq!(zones.tso_amprion_mw, None);
        assert_eq!(zones.tso_tennet_mw, None);
        assert_eq!(zones.tso_transnetbw_mw, None);
    }

    #[test]
    fn test_tso_zones_mixed() {
        let row_data = vec![
            ("50Hertz (MW)", "100,0"),
            ("Amprion (MW)", "N.A."),
            ("TenneT TSO (MW)", "300,0"),
            ("TransnetBW (MW)", "N.A."),
        ];

        let zones = parse_tso_zones(&row_data).unwrap();
        assert_eq!(zones.tso_50hertz_mw, Some(100.0));
        assert_eq!(zones.tso_amprion_mw, None);
        assert_eq!(zones.tso_tennet_mw, Some(300.0));
        assert_eq!(zones.tso_transnetbw_mw, None);
    }

    // ========================================================================
    // Tests for normalize_product_type (5 tests)
    // ========================================================================

    #[test]
    fn test_normalize_solar() {
        assert_eq!(normalize_product_type("Solar").unwrap(), "solar");
    }

    #[test]
    fn test_normalize_wind() {
        assert_eq!(normalize_product_type("Wind").unwrap(), "wind_onshore");
    }

    #[test]
    fn test_normalize_windonshore() {
        assert_eq!(
            normalize_product_type("Windonshore").unwrap(),
            "wind_onshore"
        );
    }

    #[test]
    fn test_normalize_windoffshore() {
        assert_eq!(
            normalize_product_type("Windoffshore").unwrap(),
            "wind_offshore"
        );
    }

    #[test]
    fn test_normalize_unknown_product() {
        assert!(normalize_product_type("Biomass").is_err());
        assert!(normalize_product_type("Nuclear").is_err());
    }

    // ========================================================================
    // Tests for extract_data_category (4 tests)
    // ========================================================================

    #[test]
    fn test_data_category_extrapolation() {
        assert_eq!(
            extract_data_category("hochrechnung/Wind").unwrap(),
            "extrapolation"
        );
    }

    #[test]
    fn test_data_category_online_actual() {
        assert_eq!(
            extract_data_category("onlinehochrechnung/Solar").unwrap(),
            "online_actual"
        );
    }

    #[test]
    fn test_data_category_unknown() {
        assert!(extract_data_category("unknown/endpoint").is_err());
    }

    // ========================================================================
    // Tests for convert_price_to_eur_mwh (2 tests)
    // ========================================================================

    #[test]
    fn test_price_conversion_positive() {
        let result = convert_price_to_eur_mwh(8.273);
        // Use approximate comparison due to floating-point precision
        assert!((result - 82.73).abs() < 0.01);
    }

    #[test]
    fn test_price_conversion_negative() {
        let result = convert_price_to_eur_mwh(-0.201);
        // Use approximate comparison due to floating-point precision
        assert!((result - (-2.01)).abs() < 0.01);
    }

    // ========================================================================
    // Tests for detect_price_type (4 tests)
    // ========================================================================

    #[test]
    fn test_price_type_spot_market() {
        assert_eq!(detect_price_type("Spotmarktpreise"), "spot_market");
    }

    #[test]
    fn test_price_type_market_premium() {
        assert_eq!(detect_price_type("marktpraemie"), "market_premium");
    }

    #[test]
    fn test_price_type_annual_market_value() {
        assert_eq!(
            detect_price_type("Jahresmarktpraemie"),
            "annual_market_value"
        );
    }

    #[test]
    fn test_price_type_negative_flag() {
        assert_eq!(detect_price_type("NegativePreise"), "negative_flag");
    }

    // ========================================================================
    // Tests for build_source_endpoint (1 test)
    // ========================================================================

    #[test]
    fn test_build_source_endpoint() {
        let endpoint = build_source_endpoint("prognose", "Solar", "2024-10-24", "2024-10-25");
        assert_eq!(endpoint, "prognose/Solar/2024-10-24/2024-10-25");
    }

    // ========================================================================
    // Grid Operations Transformation Tests
    // ========================================================================

    #[test]
    fn test_parse_redispatch_timestamp_valid() {
        let dt = parse_redispatch_timestamp("23.10.2024", "22:00", "UTC").unwrap();
        assert_eq!(dt, "2024-10-23T22:00:00Z");
    }

    #[test]
    fn test_parse_redispatch_timestamp_midnight() {
        let dt = parse_redispatch_timestamp("24.10.2024", "00:00", "UTC").unwrap();
        assert_eq!(dt, "2024-10-24T00:00:00Z");
    }

    #[test]
    fn test_parse_redispatch_timestamp_invalid_timezone() {
        let result = parse_redispatch_timestamp("23.10.2024", "22:00", "CET");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_redispatch_timestamp_invalid_date() {
        let result = parse_redispatch_timestamp("32.10.2024", "22:00", "UTC");
        assert!(result.is_err());
    }

    #[test]
    fn test_normalize_direction_increase() {
        let result = normalize_direction("Wirkleistungseinspeisung erhöhen").unwrap();
        assert_eq!(result, "increase_generation");
    }

    #[test]
    fn test_normalize_direction_reduce() {
        let result = normalize_direction("Wirkleistungseinspeisung reduzieren").unwrap();
        assert_eq!(result, "reduce_generation");
    }

    #[test]
    fn test_normalize_direction_unknown() {
        let result = normalize_direction("unknown direction");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_iso8601_timestamp_valid() {
        let dt = parse_iso8601_timestamp("2024-10-24T00:00:00Z").unwrap();
        assert_eq!(dt, "2024-10-24T00:00:00Z");
    }

    #[test]
    fn test_parse_iso8601_timestamp_with_offset() {
        let dt = parse_iso8601_timestamp("2024-10-24T14:30:00+00:00").unwrap();
        assert_eq!(dt, "2024-10-24T14:30:00Z");
    }

    #[test]
    fn test_parse_iso8601_timestamp_invalid() {
        let result = parse_iso8601_timestamp("invalid timestamp");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_iso8601_timestamp_without_timezone_midnight() {
        // Test parsing timestamps without 'Z' suffix at midnight (TrafficLight API edge case)
        let dt = parse_iso8601_timestamp("2024-10-24T00:00:00").unwrap();
        assert_eq!(dt, "2024-10-24T00:00:00Z");
    }

    #[test]
    fn test_parse_iso8601_timestamp_without_timezone_nonzero_time() {
        // Test parsing timestamps without 'Z' suffix with non-zero time
        let dt = parse_iso8601_timestamp("2024-10-24T14:30:45").unwrap();
        assert_eq!(dt, "2024-10-24T14:30:45Z");
    }

    #[test]
    fn test_parse_iso8601_timestamp_without_timezone_day_boundary() {
        // Test parsing at end of day (23:59:59)
        let dt = parse_iso8601_timestamp("2024-10-24T23:59:59").unwrap();
        assert_eq!(dt, "2024-10-24T23:59:59Z");
    }

    #[test]
    fn test_parse_iso8601_timestamp_invalid_hour() {
        // Invalid hour (25 > 23)
        let result = parse_iso8601_timestamp("2024-10-24T25:00:00");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_iso8601_timestamp_invalid_minute() {
        // Invalid minute (60 >= 60)
        let result = parse_iso8601_timestamp("2024-10-24T12:60:00");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_iso8601_timestamp_invalid_month() {
        // Invalid month (13 > 12)
        let result = parse_iso8601_timestamp("2024-13-01T00:00:00");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_iso8601_timestamp_invalid_day() {
        // Invalid day (32 > 31)
        let result = parse_iso8601_timestamp("2024-10-32T00:00:00");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_grid_status_green() {
        let result = validate_grid_status("GREEN").unwrap();
        assert_eq!(result, "GREEN");
    }

    #[test]
    fn test_validate_grid_status_yellow() {
        let result = validate_grid_status("YELLOW").unwrap();
        assert_eq!(result, "YELLOW");
    }

    #[test]
    fn test_validate_grid_status_red() {
        let result = validate_grid_status("RED").unwrap();
        assert_eq!(result, "RED");
    }

    #[test]
    fn test_validate_grid_status_invalid() {
        let result = validate_grid_status("ORANGE");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_grid_status_case_sensitive() {
        // Should be case-sensitive (uppercase only)
        let result = validate_grid_status("green");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_grid_status_green_neg() {
        let result = validate_grid_status("GREEN_NEG").unwrap();
        assert_eq!(result, "GREEN_NEG");
    }

    #[test]
    fn test_validate_grid_status_yellow_neg() {
        let result = validate_grid_status("YELLOW_NEG").unwrap();
        assert_eq!(result, "YELLOW_NEG");
    }

    #[test]
    fn test_validate_grid_status_red_neg() {
        let result = validate_grid_status("RED_NEG").unwrap();
        assert_eq!(result, "RED_NEG");
    }
}
