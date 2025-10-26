//! Row types for grid operations tables
//!
//! This module provides data structures for grid operations monitoring:
//! - RedispatchRow: Grid intervention events from redispatch CSV endpoint
//! - GridStatusRow: Real-time grid stability status from TrafficLight JSON endpoint

/// Represents one row from redispatch_events table
///
/// Grid intervention event where TSO adjusts power plant output to stabilize the grid.
///
/// # CSV Source
///
/// Parsed from redispatch endpoint (semicolon-delimited CSV):
/// - Volume: ~20-100 events/day
/// - Granularity: Hours-long events
/// - Format: German date/time, German text values
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::types_grid::RedispatchRow;
/// let row = RedispatchRow {
///     timestamp_utc: "2024-10-23T22:00:00Z".to_string(),
///     interval_end_utc: "2024-10-24T08:00:00Z".to_string(),
///     reason: "Probestart (NetzRes)".to_string(),
///     direction: "increase_generation".to_string(),
///     avg_power_mw: Some(119.5),
///     max_power_mw: Some(120.0),
///     total_energy_mwh: Some(1195.0),
///     requesting_tso: "TransnetBW".to_string(),
///     instructing_tso: Some("TransnetBW".to_string()),
///     affected_facility: Some("Grosskraftwerk Mannheim Block 8".to_string()),
///     energy_type: Some("Konventionell".to_string()),
///     source_endpoint: "redispatch".to_string(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct RedispatchRow {
    /// Start time of intervention (ISO 8601 format)
    /// Example: "2024-10-23T22:00:00Z"
    pub timestamp_utc: String,

    /// End time of intervention (ISO 8601 format)
    /// Example: "2024-10-24T08:00:00Z"
    pub interval_end_utc: String,

    /// Reason for intervention (German text preserved)
    ///
    /// Common values:
    /// - "Probestart (NetzRes)" = Test start (Network Reserve)
    /// - "Testfahrt (KapRes)" = Test run (Capacity Reserve)
    /// - "Strombedingter Redispatch" = Current-dependent redispatch
    /// - "Strom- und Spannungsbedingter RD" = Current and voltage-dependent
    /// - "Strombedingter Countertrade DE-DK2" = Countertrade with Denmark
    pub reason: String,

    /// Direction of intervention (normalized from German)
    ///
    /// Values:
    /// - "increase_generation" (from "Wirkleistungseinspeisung erhöhen")
    /// - "reduce_generation" (from "Wirkleistungseinspeisung reduzieren")
    pub direction: String,

    /// Average power during intervention in MW
    /// None if data not available
    pub avg_power_mw: Option<f64>,

    /// Maximum power during intervention in MW
    /// None if data not available
    pub max_power_mw: Option<f64>,

    /// Total energy over full duration in MWh
    /// None if data not available
    pub total_energy_mwh: Option<f64>,

    /// TSO requesting intervention
    ///
    /// Values: '50Hertz' | 'Amprion' | 'TenneT' | 'TransnetBW'
    /// Can be combined: '50Hertz & Amprion & TenneT DE & TransnetBW'
    pub requesting_tso: String,

    /// TSO issuing instruction (often same as requesting_tso)
    /// None if not specified
    pub instructing_tso: Option<String>,

    /// Power plant or facility name
    ///
    /// Examples:
    /// - "Grosskraftwerk Mannheim Block 8"
    /// - "Börse" (exchange - market-based adjustment)
    /// - None if not specified
    pub affected_facility: Option<String>,

    /// Energy source type (German text preserved)
    ///
    /// Values:
    /// - "Konventionell" (conventional)
    /// - "Erneuerbar" (renewable)
    /// - "Sonstiges" (other)
    /// - None if not specified
    pub energy_type: Option<String>,

    /// Source API endpoint path for traceability
    /// Example: "redispatch"
    pub source_endpoint: String,
}

/// Represents one row from grid_status_timeseries table
///
/// Minute-by-minute grid stability status (traffic light indicator).
///
/// # JSON Source
///
/// Parsed from TrafficLight endpoint (JSON array):
/// - Volume: 1,440 records/day (one per minute)
/// - Granularity: 1-minute intervals
/// - Format: ISO 8601 timestamps, uppercase status values
///
/// # Example
///
/// ```
/// # use supabase_fdw_ntp::types_grid::GridStatusRow;
/// let row = GridStatusRow {
///     timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
///     interval_end_utc: "2024-10-24T00:01:00Z".to_string(),
///     grid_status: "GREEN".to_string(),
///     source_endpoint: "TrafficLight".to_string(),
/// };
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct GridStatusRow {
    /// Start time of status interval (ISO 8601 format)
    /// Example: "2024-10-24T00:00:00Z"
    pub timestamp_utc: String,

    /// End time of status interval (ISO 8601 format)
    /// Always 1 minute after timestamp_utc
    /// Example: "2024-10-24T00:01:00Z"
    pub interval_end_utc: String,

    /// Grid stability status (traffic light indicator)
    ///
    /// Values:
    /// - "GREEN" = Normal operation (grid stable)
    /// - "GREEN_NEG" = Normal operation with negative pricing signal
    /// - "YELLOW" = Elevated stress (congestion warning)
    /// - "YELLOW_NEG" = Elevated stress with negative pricing signal
    /// - "RED" = Critical stress (high congestion, risk of intervention)
    /// - "RED_NEG" = Critical stress with negative pricing signal
    ///
    /// Note: The `_NEG` suffix indicates negative pricing in the electricity market.
    /// These variants were discovered from actual API responses (undocumented).
    pub grid_status: String,

    /// Source API endpoint path for traceability
    /// Example: "TrafficLight"
    pub source_endpoint: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redispatch_row_creation() {
        let row = RedispatchRow {
            timestamp_utc: "2024-10-23T22:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T08:00:00Z".to_string(),
            reason: "Probestart (NetzRes)".to_string(),
            direction: "increase_generation".to_string(),
            avg_power_mw: Some(119.5),
            max_power_mw: Some(120.0),
            total_energy_mwh: Some(1195.0),
            requesting_tso: "TransnetBW".to_string(),
            instructing_tso: Some("TransnetBW".to_string()),
            affected_facility: Some("Grosskraftwerk Mannheim Block 8".to_string()),
            energy_type: Some("Konventionell".to_string()),
            source_endpoint: "redispatch".to_string(),
        };

        assert_eq!(row.timestamp_utc, "2024-10-23T22:00:00Z");
        assert_eq!(row.direction, "increase_generation");
        assert_eq!(row.avg_power_mw, Some(119.5));
    }

    #[test]
    fn test_redispatch_row_with_nulls() {
        let row = RedispatchRow {
            timestamp_utc: "2024-10-24T14:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T21:00:00Z".to_string(),
            reason: "Strombedingter Redispatch".to_string(),
            direction: "reduce_generation".to_string(),
            avg_power_mw: None,
            max_power_mw: None,
            total_energy_mwh: None,
            requesting_tso: "Amprion".to_string(),
            instructing_tso: None,
            affected_facility: None,
            energy_type: None,
            source_endpoint: "redispatch".to_string(),
        };

        assert!(row.avg_power_mw.is_none());
        assert!(row.instructing_tso.is_none());
        assert!(row.affected_facility.is_none());
    }

    #[test]
    fn test_grid_status_row_creation() {
        let row = GridStatusRow {
            timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T00:01:00Z".to_string(),
            grid_status: "GREEN".to_string(),
            source_endpoint: "TrafficLight".to_string(),
        };

        assert_eq!(row.timestamp_utc, "2024-10-24T00:00:00Z");
        assert_eq!(row.grid_status, "GREEN");
    }

    #[test]
    fn test_grid_status_all_values() {
        let green = GridStatusRow {
            timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T00:01:00Z".to_string(),
            grid_status: "GREEN".to_string(),
            source_endpoint: "TrafficLight".to_string(),
        };

        let yellow = GridStatusRow {
            grid_status: "YELLOW".to_string(),
            ..green.clone()
        };

        let red = GridStatusRow {
            grid_status: "RED".to_string(),
            ..green.clone()
        };

        assert_eq!(green.grid_status, "GREEN");
        assert_eq!(yellow.grid_status, "YELLOW");
        assert_eq!(red.grid_status, "RED");
    }
}
