//! Grid operations parsers for NTP FDW
//!
//! This module provides parsers for grid operations endpoints:
//! - Redispatch CSV: Grid intervention events (semicolon-delimited, German format)
//! - TrafficLight JSON: Real-time grid status (minute-by-minute, JSON array)
//!
//! # Features
//!
//! - **CSV parsing**: Reuses existing German format transformations
//! - **JSON parsing**: First JSON endpoint in NTP FDW (uses serde_json)
//! - **Error handling**: Fail-fast validation with detailed error messages

use csv::ReaderBuilder;
use serde::Deserialize;

use crate::csv_utils::get_field;
use crate::error::{ApiError, NtpFdwError, ParseError};
use crate::transformations::*;
use crate::types_grid::{GridStatusRow, RedispatchRow};

// ============================================================================
// Redispatch CSV Parser
// ============================================================================

/// Parse redispatch CSV response
///
/// Parses German-formatted CSV with grid intervention events.
///
/// # CSV Format
///
/// - **Delimiter:** Semicolon (`;`)
/// - **Decimal separator:** Comma (`,`) - German format
/// - **Date format:** DD.MM.YYYY
/// - **Time format:** HH:MM (24-hour)
/// - **Timezone:** UTC
/// - **Columns:** 15 German columns (BEGINN_DATUM, RICHTUNG, etc.)
///
/// # Arguments
///
/// * `csv_content` - Raw CSV response body
/// * `date_from` - Start date (for metadata)
/// * `date_to` - End date (for metadata)
///
/// # Returns
///
/// * `Ok(Vec<RedispatchRow>)` - Parsed rows
/// * `Err(NtpFdwError)` - Parse error, missing columns, invalid data
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::grid_parsers::parse_redispatch_csv;
/// let csv = r#"BEGINN_DATUM;BEGINN_UHRZEIT;ZEITZONE_VON;ENDE_DATUM;ENDE_UHRZEIT;ZEITZONE_BIS;GRUND_DER_MASSNAHME;RICHTUNG;MITTLERE_LEISTUNG_MW;MAXIMALE_LEISTUNG_MW;GESAMTE_ARBEIT_MWH;ANWEISENDER_UENB;ANFORDERNDER_UENB;BETROFFENE_ANLAGE;PRIMAERENERGIEART
/// 23.10.2024;22:00;UTC;24.10.2024;08:00;UTC;Probestart (NetzRes);Wirkleistungseinspeisung erhöhen;119,5;120;1195;TransnetBW;TransnetBW;Grosskraftwerk Mannheim Block 8;Konventionell"#;
///
/// let rows = parse_redispatch_csv(csv, "2024-10-23", "2024-10-24").unwrap();
/// assert_eq!(rows.len(), 1);
/// assert_eq!(rows[0].direction, "increase_generation");
/// ```
pub fn parse_redispatch_csv(
    csv_content: &str,
    _date_from: &str,
    _date_to: &str,
) -> Result<Vec<RedispatchRow>, NtpFdwError> {
    // Configure CSV reader for German format
    let mut reader = ReaderBuilder::new()
        .delimiter(b';')
        .has_headers(true)
        .flexible(false) // Strict column count
        .trim(csv::Trim::All)
        .from_reader(csv_content.as_bytes());

    // Get headers for column indexing
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

    // Parse each data row
    for result in reader.records() {
        let record =
            result.map_err(|e| ParseError::CsvFormat(format!("CSV parse error: {}", e)))?;

        // Extract timestamp fields
        let beginn_datum = get_field(&record, &headers, "BEGINN_DATUM")?;
        let beginn_uhrzeit = get_field(&record, &headers, "BEGINN_UHRZEIT")?;
        let zeitzone_von = get_field(&record, &headers, "ZEITZONE_VON")?;
        let ende_datum = get_field(&record, &headers, "ENDE_DATUM")?;
        let ende_uhrzeit = get_field(&record, &headers, "ENDE_UHRZEIT")?;
        let zeitzone_bis = get_field(&record, &headers, "ZEITZONE_BIS")?;

        // Parse timestamps (German format → ISO 8601)
        let timestamp_utc = parse_redispatch_timestamp(beginn_datum, beginn_uhrzeit, zeitzone_von)?;
        let interval_end_utc = parse_redispatch_timestamp(ende_datum, ende_uhrzeit, zeitzone_bis)?;

        // Extract event details
        let grund = get_field(&record, &headers, "GRUND_DER_MASSNAHME")?;
        let richtung = get_field(&record, &headers, "RICHTUNG")?;

        // Normalize direction (German → English enum)
        let direction = normalize_direction(richtung)?;

        // Extract power metrics (German decimal format)
        let mittlere_leistung = get_field(&record, &headers, "MITTLERE_LEISTUNG_MW")?;
        let maximale_leistung = get_field(&record, &headers, "MAXIMALE_LEISTUNG_MW")?;
        let gesamte_arbeit = get_field(&record, &headers, "GESAMTE_ARBEIT_MWH")?;

        let avg_power_mw = if mittlere_leistung.trim().is_empty() {
            None
        } else {
            Some(parse_german_decimal(mittlere_leistung)?)
        };

        let max_power_mw = if maximale_leistung.trim().is_empty() {
            None
        } else {
            Some(parse_german_decimal(maximale_leistung)?)
        };

        let total_energy_mwh = if gesamte_arbeit.trim().is_empty() {
            None
        } else {
            Some(parse_german_decimal(gesamte_arbeit)?)
        };

        // Extract TSO and facility info
        let anweisender = get_field(&record, &headers, "ANWEISENDER_UENB")?;
        let anfordernder = get_field(&record, &headers, "ANFORDERNDER_UENB")?;
        let anlage = get_field(&record, &headers, "BETROFFENE_ANLAGE")?;
        let energieart = get_field(&record, &headers, "PRIMAERENERGIEART")?;

        rows.push(RedispatchRow {
            timestamp_utc,
            interval_end_utc,
            reason: grund.to_string(),
            direction,
            avg_power_mw,
            max_power_mw,
            total_energy_mwh,
            requesting_tso: anfordernder.to_string(),
            instructing_tso: if anweisender.trim().is_empty() {
                None
            } else {
                Some(anweisender.to_string())
            },
            affected_facility: if anlage.trim().is_empty() {
                None
            } else {
                Some(anlage.to_string())
            },
            energy_type: if energieart.trim().is_empty() {
                None
            } else {
                Some(energieart.to_string())
            },
            source_endpoint: "redispatch".to_string(),
        });
    }

    Ok(rows)
}

// ============================================================================
// TrafficLight JSON Parser
// ============================================================================

/// JSON object structure for TrafficLight endpoint
///
/// Represents one minute of grid status data.
#[derive(Debug, Deserialize)]
struct TrafficLightRecord {
    /// Start timestamp (ISO 8601 with Zulu timezone)
    /// Example: "2024-10-24T00:00:00Z"
    #[serde(rename = "From")]
    from: String,

    /// End timestamp (ISO 8601 with Zulu timezone)
    /// Always 1 minute after "From"
    /// Example: "2024-10-24T00:01:00Z"
    #[serde(rename = "To")]
    to: String,

    /// Grid status value
    /// Values: "GREEN" | "YELLOW" | "RED"
    #[serde(rename = "Value")]
    value: String,
}

/// Parse TrafficLight JSON response
///
/// Parses JSON array with minute-by-minute grid status.
///
/// # JSON Format
///
/// Array of objects:
/// ```json
/// [
///   {"From":"2024-10-24T00:00:00Z","To":"2024-10-24T00:01:00Z","Value":"GREEN"},
///   {"From":"2024-10-24T00:01:00Z","To":"2024-10-24T00:02:00Z","Value":"GREEN"}
/// ]
/// ```
///
/// # Arguments
///
/// * `json_content` - Raw JSON response body
/// * `date_from` - Start date (for validation)
/// * `date_to` - End date (for validation)
///
/// # Returns
///
/// * `Ok(Vec<GridStatusRow>)` - Parsed rows (typically 1,440 for full day)
/// * `Err(NtpFdwError)` - Parse error, invalid JSON, invalid status values
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::grid_parsers::parse_trafficlight_json;
/// let json = r#"[
///   {"From":"2024-10-24T00:00:00Z","To":"2024-10-24T00:01:00Z","Value":"GREEN"},
///   {"From":"2024-10-24T00:01:00Z","To":"2024-10-24T00:02:00Z","Value":"YELLOW"}
/// ]"#;
///
/// let rows = parse_trafficlight_json(json, "2024-10-24", "2024-10-25").unwrap();
/// assert_eq!(rows.len(), 2);
/// assert_eq!(rows[0].grid_status, "GREEN");
/// assert_eq!(rows[1].grid_status, "YELLOW");
/// ```
pub fn parse_trafficlight_json(
    json_content: &str,
    _date_from: &str,
    _date_to: &str,
) -> Result<Vec<GridStatusRow>, NtpFdwError> {
    // Parse JSON array
    let records: Vec<TrafficLightRecord> = serde_json::from_str(json_content)
        .map_err(|e| ParseError::CsvFormat(format!("Failed to parse TrafficLight JSON: {}", e)))?;

    let mut rows = Vec::new();

    for record in records {
        // Parse ISO 8601 timestamps
        let timestamp_utc = parse_iso8601_timestamp(&record.from)?;
        let interval_end_utc = parse_iso8601_timestamp(&record.to)?;

        // Validate grid status value
        let grid_status = validate_grid_status(&record.value)?;

        rows.push(GridStatusRow {
            timestamp_utc,
            interval_end_utc,
            grid_status,
            source_endpoint: "TrafficLight".to_string(),
        });
    }

    Ok(rows)
}

// ============================================================================
// TESTS
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Redispatch CSV Parser Tests
    // ========================================================================

    #[test]
    fn test_parse_redispatch_csv_single_row() {
        let csv = r#"BEGINN_DATUM;BEGINN_UHRZEIT;ZEITZONE_VON;ENDE_DATUM;ENDE_UHRZEIT;ZEITZONE_BIS;GRUND_DER_MASSNAHME;RICHTUNG;MITTLERE_LEISTUNG_MW;MAXIMALE_LEISTUNG_MW;GESAMTE_ARBEIT_MWH;ANWEISENDER_UENB;ANFORDERNDER_UENB;BETROFFENE_ANLAGE;PRIMAERENERGIEART
23.10.2024;22:00;UTC;24.10.2024;08:00;UTC;Probestart (NetzRes);Wirkleistungseinspeisung erhöhen;119,5;120;1195;TransnetBW;TransnetBW;Grosskraftwerk Mannheim Block 8;Konventionell"#;

        let rows = parse_redispatch_csv(csv, "2024-10-23", "2024-10-24").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].timestamp_utc, "2024-10-23T22:00:00Z");
        assert_eq!(rows[0].interval_end_utc, "2024-10-24T08:00:00Z");
        assert_eq!(rows[0].reason, "Probestart (NetzRes)");
        assert_eq!(rows[0].direction, "increase_generation");
        assert_eq!(rows[0].avg_power_mw, Some(119.5));
        assert_eq!(rows[0].max_power_mw, Some(120.0));
        assert_eq!(rows[0].total_energy_mwh, Some(1195.0));
        assert_eq!(rows[0].requesting_tso, "TransnetBW");
        assert_eq!(rows[0].instructing_tso, Some("TransnetBW".to_string()));
        assert_eq!(
            rows[0].affected_facility,
            Some("Grosskraftwerk Mannheim Block 8".to_string())
        );
        assert_eq!(rows[0].energy_type, Some("Konventionell".to_string()));
    }

    #[test]
    fn test_parse_redispatch_csv_reduce_direction() {
        let csv = r#"BEGINN_DATUM;BEGINN_UHRZEIT;ZEITZONE_VON;ENDE_DATUM;ENDE_UHRZEIT;ZEITZONE_BIS;GRUND_DER_MASSNAHME;RICHTUNG;MITTLERE_LEISTUNG_MW;MAXIMALE_LEISTUNG_MW;GESAMTE_ARBEIT_MWH;ANWEISENDER_UENB;ANFORDERNDER_UENB;BETROFFENE_ANLAGE;PRIMAERENERGIEART
24.10.2024;14:30;UTC;24.10.2024;20:45;UTC;Strombedingter Redispatch;Wirkleistungseinspeisung reduzieren;228;300;741;TenneT DE;50Hertz & Amprion & TenneT DE & TransnetBW;OWP UW Büttel;Erneuerbar"#;

        let rows = parse_redispatch_csv(csv, "2024-10-24", "2024-10-25").unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].direction, "reduce_generation");
        assert_eq!(rows[0].avg_power_mw, Some(228.0));
        assert_eq!(rows[0].energy_type, Some("Erneuerbar".to_string()));
    }

    #[test]
    fn test_parse_redispatch_csv_empty_response() {
        // Empty CSV returns empty vector (CSV reader treats it as valid with no rows)
        let csv = "";
        let result = parse_redispatch_csv(csv, "2024-10-24", "2024-10-25");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);

        // CSV with only headers should also return empty Vec
        let csv_headers_only = "BEGINN_DATUM;BEGINN_UHRZEIT;ZEITZONE_VON;ENDE_DATUM;ENDE_UHRZEIT;ZEITZONE_BIS;GRUND_DER_MASSNAHME;RICHTUNG;MITTLERE_LEISTUNG_MW;MAXIMALE_LEISTUNG_MW;GESAMTE_ARBEIT_MWH;ANWEISENDER_UENB;ANFORDERNDER_UENB;BETROFFENE_ANLAGE;PRIMAERENERGIEART";
        let result2 = parse_redispatch_csv(csv_headers_only, "2024-10-24", "2024-10-25");
        assert!(result2.is_ok());
        assert_eq!(result2.unwrap().len(), 0);
    }

    #[test]
    fn test_parse_redispatch_csv_missing_column() {
        let csv = r#"BEGINN_DATUM;BEGINN_UHRZEIT
23.10.2024;22:00"#;

        let result = parse_redispatch_csv(csv, "2024-10-23", "2024-10-24");
        assert!(result.is_err());
    }

    // ========================================================================
    // TrafficLight JSON Parser Tests
    // ========================================================================

    #[test]
    fn test_parse_trafficlight_json_two_records() {
        let json = r#"[
  {"From":"2024-10-24T00:00:00Z","To":"2024-10-24T00:01:00Z","Value":"GREEN"},
  {"From":"2024-10-24T00:01:00Z","To":"2024-10-24T00:02:00Z","Value":"YELLOW"}
]"#;

        let rows = parse_trafficlight_json(json, "2024-10-24", "2024-10-25").unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].timestamp_utc, "2024-10-24T00:00:00Z");
        assert_eq!(rows[0].interval_end_utc, "2024-10-24T00:01:00Z");
        assert_eq!(rows[0].grid_status, "GREEN");
        assert_eq!(rows[1].grid_status, "YELLOW");
    }

    #[test]
    fn test_parse_trafficlight_json_all_statuses() {
        let json = r#"[
  {"From":"2024-10-24T00:00:00Z","To":"2024-10-24T00:01:00Z","Value":"GREEN"},
  {"From":"2024-10-24T00:01:00Z","To":"2024-10-24T00:02:00Z","Value":"YELLOW"},
  {"From":"2024-10-24T00:02:00Z","To":"2024-10-24T00:03:00Z","Value":"RED"}
]"#;

        let rows = parse_trafficlight_json(json, "2024-10-24", "2024-10-25").unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].grid_status, "GREEN");
        assert_eq!(rows[1].grid_status, "YELLOW");
        assert_eq!(rows[2].grid_status, "RED");
    }

    #[test]
    fn test_parse_trafficlight_json_invalid_status() {
        let json = r#"[
  {"From":"2024-10-24T00:00:00Z","To":"2024-10-24T00:01:00Z","Value":"ORANGE"}
]"#;

        let result = parse_trafficlight_json(json, "2024-10-24", "2024-10-25");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_trafficlight_json_invalid_json() {
        let json = "invalid json";
        let result = parse_trafficlight_json(json, "2024-10-24", "2024-10-25");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_trafficlight_json_empty_array() {
        let json = "[]";
        let rows = parse_trafficlight_json(json, "2024-10-24", "2024-10-25").unwrap();
        assert_eq!(rows.len(), 0);
    }
}
