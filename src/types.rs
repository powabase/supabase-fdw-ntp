//! Data structures for NTP FDW rows
//!
//! These structs represent parsed rows from the NTP API, ready for conversion
//! to PostgreSQL Cell types.

/// Represents one row from renewable energy endpoints
///
/// Consolidates data from 9 API endpoints:
/// - prognose/Solar, prognose/Wind
/// - hochrechnung/Solar, hochrechnung/Wind
/// - onlinehochrechnung/Solar, onlinehochrechnung/Windonshore, onlinehochrechnung/Windoffshore
///
/// Maps to the `renewable_energy_timeseries` foreign table.
#[derive(Debug, Clone)]
pub struct RenewableRow {
    /// Start time of measurement interval (ISO 8601 format)
    /// Example: "2024-10-24T06:00:00Z"
    pub timestamp_utc: String,

    /// End time of measurement interval (ISO 8601 format)
    /// Example: "2024-10-24T06:15:00Z"
    pub interval_end_utc: String,

    /// Duration of interval in minutes
    /// - 15 for prognose/hochrechnung (quarter-hourly)
    /// - 60 for onlinehochrechnung (hourly)
    pub interval_minutes: i16,

    /// Product type (normalized from API)
    /// - "solar" (from API "Solar")
    /// - "wind_onshore" (from API "Wind" or "Windonshore")
    /// - "wind_offshore" (from API "Windoffshore")
    pub product_type: String,

    /// Data category (mapped from endpoint)
    /// - "extrapolation" (from hochrechnung)
    /// - "online_actual" (from onlinehochrechnung)
    pub data_category: String,

    /// 50Hertz TSO zone production in MW
    /// None represents "N.A." values from API (missing/nighttime data)
    pub tso_50hertz_mw: Option<f64>,

    /// Amprion TSO zone production in MW
    pub tso_amprion_mw: Option<f64>,

    /// TenneT TSO zone production in MW
    pub tso_tennet_mw: Option<f64>,

    /// TransnetBW TSO zone production in MW
    pub tso_transnetbw_mw: Option<f64>,

    /// Source API endpoint path for traceability
    /// Example: "hochrechnung/Solar/2024-10-24/2024-10-25"
    pub source_endpoint: String,
}

/// Represents one row from electricity price endpoints
///
/// Consolidates data from 4 API endpoints:
/// - Spotmarktpreise (hourly spot prices)
/// - NegativePreise (negative price flags)
/// - marktpraemie (monthly market premiums)
/// - Jahresmarktpraemie (annual market values)
///
/// Maps to the `electricity_market_prices` foreign table.
#[derive(Debug, Clone)]
pub struct PriceRow {
    /// Start time of price period (ISO 8601 format)
    /// - Hourly: "2024-10-24T14:00:00Z" (for 14:00-15:00 hour)
    /// - Monthly: "2024-10-01T00:00:00Z" (for October 2024)
    /// - Annual: "2024-01-01T00:00:00Z" (for year 2024)
    pub timestamp_utc: String,

    /// End time of price period (ISO 8601 format)
    /// - Hourly: +1 hour
    /// - Monthly: +1 month
    /// - Annual: +1 year
    pub interval_end_utc: String,

    /// Time granularity of this price record
    /// - "hourly" (spot market)
    /// - "monthly" (market premiums)
    /// - "annual" (annual market values)
    pub granularity: String,

    /// Type of price data
    /// - "spot_market" (Spotmarktpreise)
    /// - "market_premium" (marktpraemie)
    /// - "annual_market_value" (Jahresmarktpraemie)
    /// - "negative_flag" (NegativePreise - boolean flags only)
    pub price_type: String,

    /// Price in EUR per MWh (standard unit)
    /// None for negative_flag records (no actual price)
    /// Can be negative (negative prices occur during oversupply)
    /// Source: API ct/kWh × 10 = EUR/MWh
    pub price_eur_mwh: Option<f64>,

    /// Product category (for market premiums and annual values)
    /// - Some("epex") - MW-EPEX
    /// - Some("wind_onshore") - MW Wind Onshore
    /// - Some("wind_offshore") - MW Wind Offshore
    /// - Some("solar") - MW Solar
    /// - None for spot market prices
    pub product_category: Option<String>,

    /// For NegativePreise records: duration of consecutive negative prices
    /// - Some("1h") - at least 1 hour of negative prices
    /// - Some("3h") - at least 3 consecutive hours
    /// - Some("4h") - at least 4 consecutive hours
    /// - Some("6h") - at least 6 consecutive hours
    /// - None for non-negative-flag records
    pub negative_logic_hours: Option<String>,

    /// For NegativePreise records: TRUE if negative price condition is met
    /// - Some(true) - condition met (negative prices occurred)
    /// - Some(false) - condition not met
    /// - None for non-negative-flag records (spot_market, market_premium, etc.)
    pub negative_flag_value: Option<bool>,

    /// Source API endpoint path for traceability
    /// Example: "Spotmarktpreise/2024-10-24/2024-10-24"
    pub source_endpoint: String,
}

impl RenewableRow {
    /// Calculate total Germany production (sum of 4 TSO zones)
    ///
    /// # Semantics of None values
    ///
    /// Treats `None` (N.A. in API) as `0.0` for summation. This matches the PostgreSQL
    /// GENERATED column definition which uses `COALESCE(value, 0)`.
    ///
    /// **Rationale:** In the context of forecasts and extrapolations, "N.A." typically means
    /// "data not available for this zone" rather than "unknown quantity". For aggregation
    /// purposes (total German production), treating unavailable as zero is the most practical
    /// approach, as it allows partial sums even when some zones have missing data.
    ///
    /// **Important distinction:**
    /// - `None` = N.A. (data unavailable, treated as 0.0 for totals)
    /// - `Some(0.0)` = Actual zero production (e.g., nighttime solar)
    ///
    /// Use `has_missing_data()` to check if any zones have N.A. values.
    ///
    /// # Examples
    ///
    /// ```
    /// # use supabase_fdw_ntp::RenewableRow;
    /// let row = RenewableRow {
    ///     tso_50hertz_mw: Some(100.0),
    ///     tso_amprion_mw: None,  // N.A. → treated as 0.0
    ///     tso_tennet_mw: Some(300.0),
    ///     tso_transnetbw_mw: Some(200.0),
    ///     // ... other fields
    /// #     timestamp_utc: "2024-10-24T06:00:00Z".to_string(),
    /// #     interval_end_utc: "2024-10-24T06:15:00Z".to_string(),
    /// #     interval_minutes: 15,
    /// #     product_type: "solar".to_string(),
    /// #     data_category: "extrapolation".to_string(),
    /// #     source_endpoint: "hochrechnung/Solar/2024-10-24/2024-10-25".to_string(),
    /// };
    ///
    /// assert_eq!(row.total_germany_mw(), 600.0); // 100 + 0 + 300 + 200
    /// assert!(row.has_missing_data()); // Amprion is N.A.
    /// ```
    pub fn total_germany_mw(&self) -> f64 {
        self.tso_50hertz_mw.unwrap_or(0.0)
            + self.tso_amprion_mw.unwrap_or(0.0)
            + self.tso_tennet_mw.unwrap_or(0.0)
            + self.tso_transnetbw_mw.unwrap_or(0.0)
    }

    /// Check if any TSO zone has missing data (N.A. value)
    ///
    /// Returns `true` if any of the 4 TSO zones have `None` values (N.A. in API response).
    /// This indicates incomplete data coverage for this timestamp.
    ///
    /// # Use Cases
    ///
    /// - Data quality monitoring
    /// - Filtering out incomplete forecasts
    /// - Alerting when data coverage is below threshold
    ///
    /// # Examples
    ///
    /// ```
    /// # use supabase_fdw_ntp::RenewableRow;
    /// let complete_row = RenewableRow {
    ///     tso_50hertz_mw: Some(100.0),
    ///     tso_amprion_mw: Some(200.0),
    ///     tso_tennet_mw: Some(300.0),
    ///     tso_transnetbw_mw: Some(0.0),  // Zero is valid (not missing)
    ///     // ... other fields
    /// #     timestamp_utc: "2024-10-24T06:00:00Z".to_string(),
    /// #     interval_end_utc: "2024-10-24T06:15:00Z".to_string(),
    /// #     interval_minutes: 15,
    /// #     product_type: "solar".to_string(),
    /// #     data_category: "extrapolation".to_string(),
    /// #     source_endpoint: "hochrechnung/Solar/2024-10-24/2024-10-25".to_string(),
    /// };
    ///
    /// assert!(!complete_row.has_missing_data()); // All zones have data
    /// ```
    pub fn has_missing_data(&self) -> bool {
        self.tso_50hertz_mw.is_none()
            || self.tso_amprion_mw.is_none()
            || self.tso_tennet_mw.is_none()
            || self.tso_transnetbw_mw.is_none()
    }
}

impl PriceRow {
    /// Check if price is negative (oversupply condition)
    pub fn is_negative(&self) -> bool {
        self.price_eur_mwh.is_some_and(|price| price < 0.0)
    }

    /// Convert price to ct/kWh (German standard unit)
    ///
    /// 1 EUR/MWh = 0.1 ct/kWh
    pub fn price_ct_kwh(&self) -> Option<f64> {
        self.price_eur_mwh.map(|price| price / 10.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_renewable_row_total_germany() {
        let row = RenewableRow {
            timestamp_utc: "2024-10-24T06:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T06:15:00Z".to_string(),
            interval_minutes: 15,
            product_type: "solar".to_string(),
            data_category: "forecast".to_string(),
            tso_50hertz_mw: Some(100.0),
            tso_amprion_mw: Some(200.0),
            tso_tennet_mw: Some(300.0),
            tso_transnetbw_mw: Some(400.0),
            source_endpoint: "prognose/Solar/2024-10-24/2024-10-25".to_string(),
        };

        assert_eq!(row.total_germany_mw(), 1000.0);
        assert!(!row.has_missing_data());
    }

    #[test]
    fn test_renewable_row_with_null_values() {
        let row = RenewableRow {
            timestamp_utc: "2024-10-24T00:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T00:15:00Z".to_string(),
            interval_minutes: 15,
            product_type: "solar".to_string(),
            data_category: "forecast".to_string(),
            tso_50hertz_mw: None, // N.A. in CSV
            tso_amprion_mw: None,
            tso_tennet_mw: None,
            tso_transnetbw_mw: None,
            source_endpoint: "prognose/Solar/2024-10-24/2024-10-25".to_string(),
        };

        assert_eq!(row.total_germany_mw(), 0.0); // None treated as 0
        assert!(row.has_missing_data());
    }

    #[test]
    fn test_price_row_negative_detection() {
        let row = PriceRow {
            timestamp_utc: "2024-10-24T13:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T14:00:00Z".to_string(),
            granularity: "hourly".to_string(),
            price_type: "spot_market".to_string(),
            price_eur_mwh: Some(-45.23),
            product_category: None,
            negative_logic_hours: None, // Not a negative_flag record
            negative_flag_value: None,
            source_endpoint: "Spotmarktpreise/2024-10-24/2024-10-24".to_string(),
        };

        assert!(row.is_negative());
        assert_eq!(row.price_ct_kwh(), Some(-4.523));
    }

    #[test]
    fn test_price_row_unit_conversion() {
        let row = PriceRow {
            timestamp_utc: "2024-10-24T14:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T15:00:00Z".to_string(),
            granularity: "hourly".to_string(),
            price_type: "spot_market".to_string(),
            price_eur_mwh: Some(82.73), // API: 8.273 ct/kWh × 10 = 82.73 EUR/MWh
            product_category: None,
            negative_logic_hours: None, // Not a negative_flag record
            negative_flag_value: None,
            source_endpoint: "Spotmarktpreise/2024-10-24/2024-10-24".to_string(),
        };

        assert!(!row.is_negative());
        assert_eq!(row.price_ct_kwh(), Some(8.273));
    }

    #[test]
    fn test_price_row_negative_flags() {
        // Test NegativePreise endpoint data
        let row = PriceRow {
            timestamp_utc: "2024-10-24T13:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T14:00:00Z".to_string(),
            granularity: "hourly".to_string(),
            price_type: "negative_flag".to_string(),
            price_eur_mwh: None, // No actual price in flag records
            product_category: None,
            negative_logic_hours: Some("3h".to_string()),
            negative_flag_value: Some(true),
            source_endpoint: "NegativePreise/2024-10-24/2024-10-24".to_string(),
        };

        // Negative flags don't have prices, so is_negative should be false
        assert!(!row.is_negative()); // price_eur_mwh is None
        assert_eq!(row.negative_logic_hours, Some("3h".to_string()));
        assert_eq!(row.negative_flag_value, Some(true));
    }

    #[test]
    fn test_renewable_row_partial_null_handling() {
        // Test mixed NULL/non-NULL TSO zones (real production scenario)
        let row = RenewableRow {
            timestamp_utc: "2024-10-24T06:00:00Z".to_string(),
            interval_end_utc: "2024-10-24T06:15:00Z".to_string(),
            interval_minutes: 15,
            product_type: "solar".to_string(),
            data_category: "forecast".to_string(),
            tso_50hertz_mw: Some(100.0),
            tso_amprion_mw: None, // <-- One zone missing
            tso_tennet_mw: Some(300.0),
            tso_transnetbw_mw: Some(400.0),
            source_endpoint: "prognose/Solar/2024-10-24/2024-10-25".to_string(),
        };

        // Total should be 100 + 0 + 300 + 400 = 800.0
        assert_eq!(row.total_germany_mw(), 800.0);
        // Should flag as having missing data
        assert!(row.has_missing_data());
    }
}
