//! Shared CSV utilities for NTP FDW parsers
//!
//! This module provides common helper functions used across CSV parsers
//! to avoid code duplication.

use crate::error::ParseError;

/// Helper to get field value by column name from CSV record
///
/// # Arguments
///
/// * `record` - CSV record (row)
/// * `headers` - CSV header row
/// * `field_name` - Column name to look up
///
/// # Returns
///
/// * `Ok(&str)` - Field value
/// * `Err(ParseError::MissingColumn)` - Column not found
///
/// # Example
///
/// ```rust
/// use csv::StringRecord;
/// use supabase_fdw_ntp::csv_utils::get_field;
///
/// let headers = StringRecord::from(vec!["Datum", "von", "bis"]);
/// let record = StringRecord::from(vec!["2024-10-24", "00:00", "01:00"]);
///
/// let datum = get_field(&record, &headers, "Datum").unwrap();
/// assert_eq!(datum, "2024-10-24");
/// ```
pub fn get_field<'a>(
    record: &'a csv::StringRecord,
    headers: &csv::StringRecord,
    field_name: &str,
) -> Result<&'a str, ParseError> {
    let idx = headers
        .iter()
        .position(|h| h == field_name)
        .ok_or_else(|| ParseError::MissingColumn(field_name.to_string()))?;

    record
        .get(idx)
        .ok_or_else(|| ParseError::MissingColumn(field_name.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_field_success() {
        let headers = csv::StringRecord::from(vec!["col1", "col2", "col3"]);
        let record = csv::StringRecord::from(vec!["a", "b", "c"]);

        assert_eq!(get_field(&record, &headers, "col1").unwrap(), "a");
        assert_eq!(get_field(&record, &headers, "col2").unwrap(), "b");
        assert_eq!(get_field(&record, &headers, "col3").unwrap(), "c");
    }

    #[test]
    fn test_get_field_missing_column() {
        let headers = csv::StringRecord::from(vec!["col1", "col2"]);
        let record = csv::StringRecord::from(vec!["a", "b"]);

        assert!(get_field(&record, &headers, "col3").is_err());
    }

    #[test]
    fn test_get_field_empty_value() {
        let headers = csv::StringRecord::from(vec!["col1"]);
        let record = csv::StringRecord::from(vec![""]);

        // Empty string is valid - get_field returns it
        assert_eq!(get_field(&record, &headers, "col1").unwrap(), "");
    }
}
