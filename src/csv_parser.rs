//! CSV parser for NTP API responses
//!
//! This module provides functions to parse German-formatted CSV responses from the NTP API
//! into structured `RenewableRow` and `PriceRow` objects using the csv crate.
//!
//! # CSV Format
//!
//! - **Delimiter:** Semicolon (`;`)
//! - **Encoding:** UTF-8
//! - **Decimal Separator:** Comma (`,`) - German format
//! - **Header Row:** Always present
//! - **Metadata Footer:** Lines starting with `===` are ignored
//!
//! # Examples
//!
//! ```rust
//! use supabase_fdw_ntp::csv_parser::*;
//!
//! let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
//! 2024-10-24;06:00;UTC;06:15;UTC;100,5;200,3;300,7;150,2"#;
//!
//! let rows = parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25").unwrap();
//! assert_eq!(rows.len(), 1);
//! assert_eq!(rows[0].product_type, "solar");
//! ```

use csv::ReaderBuilder;

use crate::csv_utils::get_field;
use crate::error::{ApiError, NtpFdwError, ParseError};
use crate::transformations::*;
use crate::types::{PriceRow, RenewableRow};

// ============================================================================
// Helper Functions
// ============================================================================

/// Validate renewable energy CSV header has all required columns
///
/// # Required Columns
///
/// - Datum, von, bis, Zeitzone von, Zeitzone bis
/// - 50Hertz (MW), Amprion (MW), TenneT TSO (MW), TransnetBW (MW)
fn validate_renewable_header(headers: &csv::StringRecord) -> Result<(), ParseError> {
    let required = vec![
        "Datum",
        "von",
        "bis",
        "Zeitzone von",
        "Zeitzone bis",
        "50Hertz (MW)",
        "Amprion (MW)",
        "TenneT TSO (MW)",
        "TransnetBW (MW)",
    ];

    for col in required {
        if !headers.iter().any(|h| h == col) {
            return Err(ParseError::MissingColumn(col.to_string()));
        }
    }

    Ok(())
}

/// Validate price CSV header has all required columns
///
/// # Required Columns
///
/// - Datum, von, bis, Zeitzone von, Zeitzone bis
/// - Spotmarktpreis in ct/kWh
fn validate_price_header(headers: &csv::StringRecord) -> Result<(), ParseError> {
    let required = vec![
        "Datum",
        "von",
        "bis",
        "Zeitzone von",
        "Zeitzone bis",
        "Spotmarktpreis in ct/kWh",
    ];

    for col in required {
        if !headers.iter().any(|h| h == col) {
            return Err(ParseError::MissingColumn(col.to_string()));
        }
    }

    Ok(())
}

// ============================================================================
// Main Parsing Functions
// ============================================================================

/// Parse renewable energy CSV into RenewableRow structs
///
/// Uses csv crate for robust parsing of semicolon-delimited German CSV format.
///
/// # Arguments
///
/// * `csv_content` - Raw CSV string from API
/// * `endpoint` - Endpoint name ("prognose", "hochrechnung", "onlinehochrechnung")
/// * `product` - Product name ("Solar", "Wind", "Windonshore", "Windoffshore")
/// * `date_from` - Start date for source_endpoint metadata
/// * `date_to` - End date for source_endpoint metadata
///
/// # Returns
///
/// * `Ok(Vec<RenewableRow>)` - Parsed rows
/// * `Err(NtpFdwError)` - Parse error, missing columns, invalid data
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::csv_parser::parse_renewable_csv;
/// let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
/// 2024-10-24;06:00;UTC;06:15;UTC;100,5;200,3;300,7;150,2
/// 2024-10-24;06:15;UTC;06:30;UTC;N.A.;N.A.;N.A.;N.A."#;
///
/// let rows = parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25").unwrap();
/// assert_eq!(rows.len(), 2);
/// assert_eq!(rows[0].tso_50hertz_mw, Some(100.5));
/// assert_eq!(rows[1].tso_50hertz_mw, None); // N.A. → None
/// ```
pub fn parse_renewable_csv(
    csv_content: &str,
    endpoint: &str,
    product: &str,
    date_from: &str,
    date_to: &str,
) -> Result<Vec<RenewableRow>, NtpFdwError> {
    // Stop at metadata footer (=== marker)
    let csv_data = csv_content.split("===").next().unwrap_or(csv_content);

    // Configure CSV reader for German format
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .flexible(false) // Strict column count
        .trim(csv::Trim::All) // Trim whitespace
        .from_reader(csv_data.as_bytes());

    // Get headers for column indexing
    // Note: We clone the headers here to avoid borrow checker conflicts.
    // The csv::Reader requires mutable access during iteration (reader.records()),
    // but we also need immutable access to headers for column lookups in each row.
    // Cloning is the simplest solution and has minimal performance impact
    // (headers are a small StringRecord with ~9 column names).
    let headers = reader
        .headers()
        .map_err(|e| {
            // Provide detailed error instead of generic "EmptyResponse"
            if csv_data.is_empty() {
                NtpFdwError::from(ApiError::EmptyResponse)
            } else {
                NtpFdwError::from(ParseError::CsvFormat(format!(
                    "Failed to read CSV headers: {}",
                    e
                )))
            }
        })?
        .clone();

    // Validate required columns
    validate_renewable_header(&headers)?;

    // Pre-compute metadata (same for all rows)
    // Note: These clones are necessary because we push each row into the Vec,
    // transferring ownership. We could use Rc<String> to eliminate per-row clones,
    // but the performance gain would be negligible (~100 rows × 3 strings = 300 allocations),
    // and code simplicity is more valuable here.
    let product_type = normalize_product_type(product)?;
    let data_category = extract_data_category(endpoint)?;
    let source_endpoint = build_source_endpoint(endpoint, product, date_from, date_to);

    let mut rows = Vec::new();

    // Parse each data row
    for result in reader.records() {
        let record =
            result.map_err(|e| ParseError::CsvFormat(format!("CSV parse error: {}", e)))?;

        // Extract timestamp fields
        let datum = get_field(&record, &headers, "Datum")?;
        let von = get_field(&record, &headers, "von")?;
        let bis = get_field(&record, &headers, "bis")?;
        let tz_von = get_field(&record, &headers, "Zeitzone von")?;
        let tz_bis = get_field(&record, &headers, "Zeitzone bis")?;

        // Parse timestamps with midnight-crossing detection (Bug #5 fix)
        let (timestamp_utc, interval_end_utc) =
            parse_interval_timestamps(datum, von, bis, tz_von, tz_bis)?;
        let interval_minutes = calculate_interval_minutes(&timestamp_utc, &interval_end_utc)?;

        // Extract TSO zone values
        let tso_50hertz = get_field(&record, &headers, "50Hertz (MW)")?;
        let tso_amprion = get_field(&record, &headers, "Amprion (MW)")?;
        let tso_tennet = get_field(&record, &headers, "TenneT TSO (MW)")?;
        let tso_transnetbw = get_field(&record, &headers, "TransnetBW (MW)")?;

        // Parse TSO zones with transformation functions
        let tso_data = vec![
            ("50Hertz (MW)", tso_50hertz),
            ("Amprion (MW)", tso_amprion),
            ("TenneT TSO (MW)", tso_tennet),
            ("TransnetBW (MW)", tso_transnetbw),
        ];
        let zones = parse_tso_zones(&tso_data)?;

        rows.push(RenewableRow {
            timestamp_utc,
            interval_end_utc,
            interval_minutes,
            product_type: product_type.clone(),
            data_category: data_category.clone(),
            tso_50hertz_mw: zones.tso_50hertz_mw,
            tso_amprion_mw: zones.tso_amprion_mw,
            tso_tennet_mw: zones.tso_tennet_mw,
            tso_transnetbw_mw: zones.tso_transnetbw_mw,
            source_endpoint: source_endpoint.clone(),
        });
    }

    Ok(rows)
}

/// Parse spot market price CSV into PriceRow structs
///
/// # Arguments
///
/// * `csv_content` - Raw CSV string from API
/// * `endpoint` - Endpoint name ("Spotmarktpreise", "NegativePreise", etc.)
/// * `date_from` - Start date for source_endpoint metadata
/// * `date_to` - End date for source_endpoint metadata
///
/// # Returns
///
/// * `Ok(Vec<PriceRow>)` - Parsed rows
/// * `Err(NtpFdwError)` - Parse error
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::csv_parser::parse_price_csv;
/// let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
/// 23.10.2024;00:00;UTC;01:00;UTC;8,273
/// 23.10.2024;01:00;UTC;02:00;UTC;-0,201"#;
///
/// let rows = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24").unwrap();
/// assert_eq!(rows.len(), 2);
/// assert_eq!(rows[0].price_type, "spot_market");
/// assert!(rows[0].price_eur_mwh.unwrap() > 0.0);
/// assert!(rows[1].price_eur_mwh.unwrap() < 0.0); // Negative price
/// ```
pub fn parse_price_csv(
    csv_content: &str,
    endpoint: &str,
    date_from: &str,
    date_to: &str,
) -> Result<Vec<PriceRow>, NtpFdwError> {
    // Stop at metadata footer
    let csv_data = csv_content.split("===").next().unwrap_or(csv_content);

    // Configure CSV reader
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(csv_data.as_bytes());

    // Get and validate headers
    // Note: Clone to avoid borrow checker conflict (see renewable parser above for details)
    let headers = reader
        .headers()
        .map_err(|e| {
            // Provide detailed error instead of generic "EmptyResponse"
            if csv_data.is_empty() {
                NtpFdwError::from(ApiError::EmptyResponse)
            } else {
                NtpFdwError::from(ParseError::CsvFormat(format!(
                    "Failed to read CSV headers: {}",
                    e
                )))
            }
        })?
        .clone();
    validate_price_header(&headers)?;

    // Pre-compute metadata (cloned per row - see note above)
    let price_type = detect_price_type(endpoint);
    let source_endpoint = format!("{}/{}/{}", endpoint, date_from, date_to);

    let mut rows = Vec::new();

    // Parse each data row
    for result in reader.records() {
        let record =
            result.map_err(|e| ParseError::CsvFormat(format!("CSV parse error: {}", e)))?;

        // Extract fields
        let datum = get_field(&record, &headers, "Datum")?;
        let von = get_field(&record, &headers, "von")?;
        let bis = get_field(&record, &headers, "bis")?;
        let tz_von = get_field(&record, &headers, "Zeitzone von")?;
        let tz_bis = get_field(&record, &headers, "Zeitzone bis")?;
        let price_ct_kwh = get_field(&record, &headers, "Spotmarktpreis in ct/kWh")?;

        // Parse timestamps with midnight-crossing detection (Bug #5 fix)
        let (timestamp_utc, interval_end_utc) =
            parse_interval_timestamps(datum, von, bis, tz_von, tz_bis)?;

        // Parse and convert price
        let price_ct = parse_german_decimal(price_ct_kwh)?;
        let price_eur_mwh = convert_price_to_eur_mwh(price_ct);

        rows.push(PriceRow {
            timestamp_utc,
            interval_end_utc,
            granularity: "hourly".to_string(),
            price_type: price_type.clone(),
            price_eur_mwh: Some(price_eur_mwh),
            product_category: None,
            negative_logic_hours: None,
            negative_flag_value: None,
            source_endpoint: source_endpoint.clone(),
        });
    }

    Ok(rows)
}

/// Parse NegativePreise CSV (different format from spot prices) - Bug #7 fix
///
/// The NegativePreise endpoint has a completely different CSV structure:
/// - Combined datetime column: "2024-10-20 00:00" (not separate Datum/von/bis)
/// - Duration flag columns: Stunde1, Stunde3, Stunde4, Stunde6
/// - Boolean format: "1" (true) or "0" (false)
///
/// # Arguments
///
/// * `csv_content` - CSV string from NegativePreise endpoint
/// * `_date_from` - Start date (for logging, not used in parsing)
/// * `_date_to` - End date (for logging, not used in parsing)
///
/// # Returns
///
/// * `Ok(Vec<PriceRow>)` - Parsed price rows with negative_logic_hours populated
/// * `Err(NtpFdwError)` - If CSV parsing or validation fails
///
/// # CSV Format
///
/// ```csv
/// Datum;Stunde1;Stunde3;Stunde4;Stunde6
/// 2024-10-20 00:00;1;1;1;1
/// 2024-10-20 11:00;0;1;1;1
/// ```
pub fn parse_negative_price_flags_csv(
    csv_content: &str,
    _date_from: &str,
    _date_to: &str,
) -> Result<Vec<PriceRow>, NtpFdwError> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_reader(csv_content.as_bytes());

    let headers = reader
        .headers()
        .map_err(|e| {
            if csv_content.is_empty() {
                NtpFdwError::from(ApiError::EmptyResponse)
            } else {
                NtpFdwError::from(ParseError::CsvFormat(format!(
                    "Failed to read CSV headers: {}",
                    e
                )))
            }
        })?
        .clone();
    let mut rows = Vec::new();

    // Validate required columns
    let required_columns = ["Datum", "Stunde1", "Stunde3", "Stunde4", "Stunde6"];
    for col in &required_columns {
        if !headers.iter().any(|h| h == *col) {
            return Err(ParseError::MissingColumn(col.to_string()).into());
        }
    }

    for result in reader.records() {
        let record = result.map_err(|e| {
            ParseError::CsvFormat(format!("Failed to read CSV record: {}", e))
        })?;

        // Parse combined datetime (format: "2024-10-20 00:00")
        let datum_zeit = get_field(&record, &headers, "Datum")?;

        // Split datetime into date and time components
        let parts: Vec<&str> = datum_zeit.split(' ').collect();
        if parts.len() != 2 {
            return Err(ParseError::InvalidTimestamp(format!(
                "Expected 'YYYY-MM-DD HH:MM' format, got: {}",
                datum_zeit
            ))
            .into());
        }

        // Parse timestamp (format: "2024-10-20T00:00:00Z")
        let timestamp_utc = format!("{}T{}:00Z", parts[0], parts[1]);

        // Calculate end timestamp (+1 hour, using chrono)
        let dt = chrono::DateTime::parse_from_rfc3339(&timestamp_utc)
            .map_err(|_| ParseError::InvalidTimestamp(timestamp_utc.clone()))?;
        let interval_end_utc = (dt + chrono::Duration::hours(1))
            .format("%Y-%m-%dT%H:%M:%SZ")
            .to_string();

        // Parse duration flags (1=true, 0=false)
        let flag_1h = get_field(&record, &headers, "Stunde1")? == "1";
        let flag_3h = get_field(&record, &headers, "Stunde3")? == "1";
        let flag_4h = get_field(&record, &headers, "Stunde4")? == "1";
        let flag_6h = get_field(&record, &headers, "Stunde6")? == "1";

        // Determine longest negative duration (priority: 6h > 4h > 3h > 1h)
        let negative_logic_hours = if flag_6h {
            Some("6h".to_string())
        } else if flag_4h {
            Some("4h".to_string())
        } else if flag_3h {
            Some("3h".to_string())
        } else if flag_1h {
            Some("1h".to_string())
        } else {
            None
        };

        // Check if any negative flag is set
        let negative_flag_value = Some(flag_1h || flag_3h || flag_4h || flag_6h);

        rows.push(PriceRow {
            timestamp_utc,
            interval_end_utc,
            price_type: "negative_flag".to_string(),
            granularity: "hourly".to_string(),
            price_eur_mwh: None, // Not provided in NegativePreise CSV
            product_category: None,
            negative_logic_hours,
            negative_flag_value,
            source_endpoint: "NegativePreise".to_string(),
        });
    }

    Ok(rows)
}

/// Parse annual market value response (Jahresmarktpraemie)
///
/// The Jahresmarktpraemie endpoint returns line-separated key-value pairs instead of CSV:
/// ```text
/// Alle Werte in ct/kWh;2024
/// JW;7,946
/// JW Wind an Land;6,293
/// JW Wind auf See;6,777
/// JW Solar;4,624
/// ```
///
/// # Format
///
/// - **Record separator:** Newline (`\n`)
/// - **Field separator:** Semicolon (`;`)
/// - **Decimal separator:** Comma (`,`) - German format
/// - **Header:** First line contains metadata (filtered out)
/// - **Structure:** `category;value` (one per line)
///
/// # Arguments
///
/// * `content` - Raw response body from API
/// * `year` - Year for the data (e.g., "2024")
///
/// # Returns
///
/// Vector of PriceRow with:
/// - `timestamp_utc`: January 1st of year (e.g., "2024-01-01T00:00:00Z")
/// - `interval_end_utc`: December 31st of year (e.g., "2024-12-31T23:59:59Z")
/// - `granularity`: "annual"
/// - `price_type`: "annual_market_value"
/// - `price_eur_mwh`: Converted from API (ct/kWh × 10 = EUR/MWh)
/// - `product_category`: Normalized category
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::csv_parser::parse_annual_price_response;
/// let response = "JW;7,946\nJW Solar;4,624";
/// let rows = parse_annual_price_response(response, "2024").unwrap();
/// assert_eq!(rows.len(), 2);
/// assert_eq!(rows[0].price_eur_mwh, Some(79.46)); // 7.946 ct/kWh × 10
/// assert_eq!(rows[0].product_category, Some("annual_overall".to_string()));
/// assert_eq!(rows[1].product_category, Some("solar".to_string()));
/// ```
pub fn parse_annual_price_response(
    content: &str,
    year: &str,
) -> Result<Vec<PriceRow>, NtpFdwError> {
    // Handle empty response
    if content.trim().is_empty() {
        return Ok(Vec::new());
    }

    // Filter out header lines (e.g., "Alle Werte in ct/kWh;2024")
    // Header lines typically contain words like "Alle", "Werte", or year only
    let cleaned_content = content
        .lines()
        .filter(|line| {
            let trimmed = line.trim();
            // Skip empty lines
            if trimmed.is_empty() {
                return false;
            }
            // Skip header lines (contains "Alle" or starts with metadata)
            if trimmed.to_lowercase().contains("alle") || trimmed.to_lowercase().contains("werte") {
                return false;
            }
            // Skip lines that are just a year
            if trimmed.len() == 4 && trimmed.parse::<i32>().is_ok() {
                return false;
            }
            true
        })
        .collect::<Vec<&str>>()
        .join("\n");

    // Split by newlines to get individual items (not pipes!)
    let items: Vec<&str> = cleaned_content
        .lines()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .collect();

    let mut rows = Vec::new();

    // Parse each item
    for item in items {
        // Split by semicolon to get category and value
        let parts: Vec<&str> = item.split(';').map(|s| s.trim()).collect();

        if parts.len() != 2 {
            return Err(NtpFdwError::from(ParseError::CsvFormat(format!(
                "Invalid annual format: expected 'category;value', got '{}' (parts: {})",
                item,
                parts.len()
            ))));
        }

        let category = parts[0];
        let price_str = parts[1];

        // Parse German decimal (comma → period)
        let price_ct_kwh = parse_german_decimal(price_str)?;

        // Convert ct/kWh → EUR/MWh (multiply by 10)
        let price_eur_mwh = price_ct_kwh * 10.0;

        // Generate timestamps for full year
        let timestamp_utc = format!("{}-01-01T00:00:00Z", year);
        let interval_end_utc = format!("{}-12-31T23:59:59Z", year);

        // Normalize product category
        let product_category = normalize_annual_product(category);

        rows.push(PriceRow {
            timestamp_utc,
            interval_end_utc,
            granularity: "annual".to_string(),
            price_type: "annual_market_value".to_string(),
            price_eur_mwh: Some(price_eur_mwh),
            product_category: Some(product_category),
            negative_logic_hours: None,
            negative_flag_value: None,
            source_endpoint: "Jahresmarktpraemie".to_string(),
        });
    }

    Ok(rows)
}

/// Normalize annual product category names
///
/// Converts German category names from Jahresmarktpraemie API to consistent product names.
///
/// # Mappings
///
/// - `"JW"` → `"annual_overall"` (comprehensive annual value)
/// - `"JW Wind an Land"` → `"wind_onshore"`
/// - `"JW Wind auf See"` → `"wind_offshore"`
/// - `"JW Solar"` → `"solar"`
/// - Other → Lowercased with underscores
fn normalize_annual_product(category: &str) -> String {
    match category.trim() {
        "JW" => "annual_overall".to_string(),
        "JW Wind an Land" => "wind_onshore".to_string(),
        "JW Wind auf See" => "wind_offshore".to_string(),
        "JW Solar" => "solar".to_string(),
        other => other.to_lowercase().replace(' ', "_"),
    }
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
    // Note: get_field tests moved to csv_utils.rs module

    #[test]
    fn test_validate_renewable_header_valid() {
        let headers = csv::StringRecord::from(vec![
            "Datum",
            "von",
            "bis",
            "Zeitzone von",
            "Zeitzone bis",
            "50Hertz (MW)",
            "Amprion (MW)",
            "TenneT TSO (MW)",
            "TransnetBW (MW)",
        ]);

        assert!(validate_renewable_header(&headers).is_ok());
    }

    #[test]
    fn test_validate_renewable_header_missing_column() {
        let headers = csv::StringRecord::from(vec![
            "Datum",
            "von",
            "bis",
            // Missing Zeitzone columns
            "50Hertz (MW)",
        ]);

        assert!(validate_renewable_header(&headers).is_err());
    }

    #[test]
    fn test_validate_price_header_valid() {
        let headers = csv::StringRecord::from(vec![
            "Datum",
            "von",
            "bis",
            "Zeitzone von",
            "Zeitzone bis",
            "Spotmarktpreis in ct/kWh",
        ]);

        assert!(validate_price_header(&headers).is_ok());
    }

    #[test]
    fn test_validate_price_header_missing_column() {
        let headers = csv::StringRecord::from(vec!["Datum", "von", "bis"]);

        assert!(validate_price_header(&headers).is_err());
    }

    #[test]
    fn test_validate_headers_with_extra_columns() {
        // Forward compatibility: extra columns should not cause failure
        let headers = csv::StringRecord::from(vec![
            "Datum",
            "von",
            "bis",
            "Zeitzone von",
            "Zeitzone bis",
            "50Hertz (MW)",
            "Amprion (MW)",
            "TenneT TSO (MW)",
            "TransnetBW (MW)",
            "ExtraColumn", // Extra column - should be ignored
        ]);

        assert!(validate_renewable_header(&headers).is_ok());
    }

    // ========================================================================
    // parse_renewable_csv Tests
    // ========================================================================

    #[test]
    fn test_parse_renewable_csv_valid_solar() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-24;06:00;UTC;06:15;UTC;100,5;200,3;300,7;150,2
2024-10-24;06:15;UTC;06:30;UTC;105,0;210,0;310,0;160,0"#;

        let rows =
            parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25").unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].product_type, "solar");
        assert_eq!(rows[0].data_category, "forecast");
        assert_eq!(rows[0].interval_minutes, 15);
        assert_eq!(rows[0].tso_50hertz_mw, Some(100.5));
        assert_eq!(rows[0].tso_amprion_mw, Some(200.3));
        assert_eq!(rows[1].tso_50hertz_mw, Some(105.0));
    }

    #[test]
    fn test_parse_renewable_csv_with_na_values() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-24;00:00;UTC;00:15;UTC;N.A.;N.A.;N.A.;N.A."#;

        let rows =
            parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tso_50hertz_mw, None);
        assert_eq!(rows[0].tso_amprion_mw, None);
        assert_eq!(rows[0].tso_tennet_mw, None);
        assert_eq!(rows[0].tso_transnetbw_mw, None);
    }

    #[test]
    fn test_parse_renewable_csv_wind_extrapolation() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-23;12:00;UTC;12:15;UTC;500,0;600,0;700,0;200,0"#;

        let rows =
            parse_renewable_csv(csv, "hochrechnung", "Wind", "2024-10-23", "2024-10-24").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].product_type, "wind_onshore");
        assert_eq!(rows[0].data_category, "extrapolation");
    }

    #[test]
    fn test_parse_renewable_csv_online_actual() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-24;10:00;UTC;11:00;UTC;1000,0;1100,0;1200,0;900,0"#;

        let rows = parse_renewable_csv(
            csv,
            "onlinehochrechnung",
            "Windonshore",
            "2024-10-24",
            "2024-10-25",
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].product_type, "wind_onshore");
        assert_eq!(rows[0].data_category, "online_actual");
        assert_eq!(rows[0].interval_minutes, 60); // Hourly
    }

    #[test]
    fn test_parse_renewable_csv_empty() {
        let csv = "";
        let result = parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_renewable_csv_missing_column() {
        let csv = r#"Datum;von;bis
2024-10-24;06:00;06:15"#;

        let result = parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_renewable_csv_invalid_timestamp() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
invalid-date;06:00;UTC;06:15;UTC;100,5;200,3;300,7;150,2"#;

        let result = parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_renewable_csv_invalid_decimal() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-24;06:00;UTC;06:15;UTC;abc;200,3;300,7;150,2"#;

        let result = parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_renewable_csv_with_metadata_footer() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-24;06:00;UTC;06:15;UTC;100,5;200,3;300,7;150,2

===
HTTP_STATUS:200
SIZE:1234"#;

        let rows =
            parse_renewable_csv(csv, "prognose", "Solar", "2024-10-24", "2024-10-25").unwrap();

        // Should parse 1 row and stop at ===
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].tso_50hertz_mw, Some(100.5));
    }

    // ========================================================================
    // parse_price_csv Tests
    // ========================================================================

    #[test]
    fn test_parse_price_csv_valid() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
23.10.2024;00:00;UTC;01:00;UTC;8,273
23.10.2024;01:00;UTC;02:00;UTC;7,884"#;

        let rows = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24").unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].price_type, "spot_market");
        assert_eq!(rows[0].granularity, "hourly");
        // 8.273 ct/kWh × 10 = 82.73 EUR/MWh
        assert!((rows[0].price_eur_mwh.unwrap() - 82.73).abs() < 0.01);
    }

    #[test]
    fn test_parse_price_csv_negative_prices() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
23.10.2024;13:00;UTC;14:00;UTC;-0,201"#;

        let rows = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24").unwrap();

        assert_eq!(rows.len(), 1);
        assert!(rows[0].price_eur_mwh.unwrap() < 0.0);
        // -0.201 ct/kWh × 10 = -2.01 EUR/MWh
        assert!((rows[0].price_eur_mwh.unwrap() - (-2.01)).abs() < 0.01);
    }

    #[test]
    fn test_parse_price_csv_midnight_rollover() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
23.10.2024;23:00;UTC;00:00;UTC;8,242"#;

        let rows = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp_utc, "2024-10-23T23:00:00Z");
        // Bug #5 fix: Midnight crossing detected (00:00 <= 23:00), so end date is next day
        assert_eq!(rows[0].interval_end_utc, "2024-10-24T00:00:00Z");
    }

    #[test]
    fn test_parse_price_csv_german_date_format() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
23.10.2024;12:00;UTC;13:00;UTC;10,5"#;

        let rows = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp_utc, "2024-10-23T12:00:00Z");
    }

    #[test]
    fn test_parse_price_csv_empty() {
        let csv = "";
        let result = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_price_csv_missing_column() {
        let csv = r#"Datum;von;bis
23.10.2024;00:00;01:00"#;

        let result = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_price_csv_invalid_price() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
23.10.2024;00:00;UTC;01:00;UTC;invalid"#;

        let result = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_price_csv_with_metadata_footer() {
        let csv = r#"Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
23.10.2024;00:00;UTC;01:00;UTC;8,273

===
HTTP_STATUS:200
SIZE:142"#;

        let rows = parse_price_csv(csv, "Spotmarktpreise", "2024-10-23", "2024-10-24").unwrap();

        // Should parse 1 row and stop at ===
        assert_eq!(rows.len(), 1);
    }

    // ========================================================================
    // parse_annual_price_response Tests
    // ========================================================================

    #[test]
    fn test_parse_annual_price_response_valid() {
        // Actual format from Jahresmarktpraemie endpoint (line-separated with header)
        let response = "Alle Werte in ct/kWh;2024\nJW;7,946\nJW Wind an Land;6,293\nJW Wind auf See;6,777\nJW Solar;4,624";

        let rows = parse_annual_price_response(response, "2024").unwrap();

        assert_eq!(rows.len(), 4);

        // Check first row (JW - overall annual value)
        assert_eq!(rows[0].timestamp_utc, "2024-01-01T00:00:00Z");
        assert_eq!(rows[0].interval_end_utc, "2024-12-31T23:59:59Z");
        assert_eq!(rows[0].granularity, "annual");
        assert_eq!(rows[0].price_type, "annual_market_value");
        assert_eq!(rows[0].product_category, Some("annual_overall".to_string()));
        // Price conversion: 7.946 ct/kWh × 10 = 79.46 EUR/MWh
        assert_eq!(rows[0].price_eur_mwh, Some(79.46));
        assert_eq!(rows[0].source_endpoint, "Jahresmarktpraemie");

        // Check product normalization
        assert_eq!(rows[1].product_category, Some("wind_onshore".to_string()));
        assert_eq!(rows[2].product_category, Some("wind_offshore".to_string()));
        assert_eq!(rows[3].product_category, Some("solar".to_string()));
    }

    #[test]
    fn test_parse_annual_price_response_german_decimals() {
        // Test German decimal format (comma as decimal separator)
        let response = "JW;10,5\nJW Solar;3,142";

        let rows = parse_annual_price_response(response, "2023").unwrap();

        assert_eq!(rows.len(), 2);
        // 10.5 ct/kWh × 10 = 105.0 EUR/MWh
        assert_eq!(rows[0].price_eur_mwh, Some(105.0));
        // 3.142 ct/kWh × 10 = 31.42 EUR/MWh (use approximate comparison for floating point)
        assert!((rows[1].price_eur_mwh.unwrap() - 31.42).abs() < 0.001);
    }

    #[test]
    fn test_parse_annual_price_response_empty() {
        let response = "";
        let rows = parse_annual_price_response(response, "2024").unwrap();
        assert_eq!(rows.len(), 0);

        // Test whitespace-only
        let response2 = "   \n  \t  ";
        let rows2 = parse_annual_price_response(response2, "2024").unwrap();
        assert_eq!(rows2.len(), 0);
    }

    #[test]
    fn test_parse_annual_price_response_malformed() {
        // Missing semicolon separator
        let response1 = "JW 7,946";
        let result1 = parse_annual_price_response(response1, "2024");
        assert!(result1.is_err());

        // Too many parts (extra semicolons)
        let response2 = "JW;7;946";
        let result2 = parse_annual_price_response(response2, "2024");
        assert!(result2.is_err());

        // Empty lines between valid lines (should be filtered)
        let response3 = "JW;7,946\n\nJW Solar;4,624";
        let rows3 = parse_annual_price_response(response3, "2024").unwrap();
        // Should skip empty lines, parse 2 valid ones
        assert_eq!(rows3.len(), 2);
    }

    #[test]
    fn test_parse_annual_price_response_product_normalization() {
        let response = "JW;1,0\nJW Wind an Land;2,0\nJW Wind auf See;3,0\nJW Solar;4,0\nUnknown Category;5,0";

        let rows = parse_annual_price_response(response, "2024").unwrap();

        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0].product_category, Some("annual_overall".to_string()));
        assert_eq!(rows[1].product_category, Some("wind_onshore".to_string()));
        assert_eq!(rows[2].product_category, Some("wind_offshore".to_string()));
        assert_eq!(rows[3].product_category, Some("solar".to_string()));
        // Unknown category should be lowercased with underscores
        assert_eq!(rows[4].product_category, Some("unknown_category".to_string()));
    }

    #[test]
    fn test_parse_annual_price_response_timestamp_generation() {
        let response = "JW;7,946";

        // Test different years
        let rows_2024 = parse_annual_price_response(response, "2024").unwrap();
        assert_eq!(rows_2024[0].timestamp_utc, "2024-01-01T00:00:00Z");
        assert_eq!(rows_2024[0].interval_end_utc, "2024-12-31T23:59:59Z");

        let rows_2020 = parse_annual_price_response(response, "2020").unwrap();
        assert_eq!(rows_2020[0].timestamp_utc, "2020-01-01T00:00:00Z");
        assert_eq!(rows_2020[0].interval_end_utc, "2020-12-31T23:59:59Z");
    }
}
