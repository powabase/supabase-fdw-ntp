//! Error types for NTP FDW
//!
//! Provides comprehensive error handling for all failure modes:
//! - CSV parsing errors (German format conversion, missing data)
//! - OAuth2 authentication errors
//! - HTTP API errors (network, rate limiting, server errors)

use std::fmt;

/// Top-level error type for NTP FDW
///
/// Supports automatic conversion from specific error types via From trait
#[derive(Debug)]
pub enum NtpFdwError {
    /// CSV parsing error
    Parse(ParseError),

    /// OAuth2 authentication error
    OAuth2(OAuth2Error),

    /// HTTP API error
    Api(ApiError),

    /// Generic error with message
    Generic(String),
}

impl fmt::Display for NtpFdwError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NtpFdwError::Parse(e) => write!(f, "Parse error: {}", e),
            NtpFdwError::OAuth2(e) => write!(f, "OAuth2 error: {}", e),
            NtpFdwError::Api(e) => write!(f, "API error: {}", e),
            NtpFdwError::Generic(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for NtpFdwError {}

impl From<ParseError> for NtpFdwError {
    fn from(err: ParseError) -> Self {
        NtpFdwError::Parse(err)
    }
}

impl From<OAuth2Error> for NtpFdwError {
    fn from(err: OAuth2Error) -> Self {
        NtpFdwError::OAuth2(err)
    }
}

impl From<ApiError> for NtpFdwError {
    fn from(err: ApiError) -> Self {
        NtpFdwError::Api(err)
    }
}

impl From<String> for NtpFdwError {
    fn from(msg: String) -> Self {
        NtpFdwError::Generic(msg)
    }
}

impl From<&str> for NtpFdwError {
    fn from(msg: &str) -> Self {
        NtpFdwError::Generic(msg.to_string())
    }
}

/// CSV parsing errors
///
/// Occurs during transformation of German-formatted CSV data to SQL types
#[derive(Debug, Clone)]
pub enum ParseError {
    /// Failed to parse German decimal format (comma → period conversion)
    ///
    /// Example: "119,5a" (invalid characters after decimal)
    InvalidDecimal(String),

    /// Failed to parse timestamp from German date format
    ///
    /// Example: "32.10.2024" (invalid day)
    InvalidTimestamp(String),

    /// Required CSV column is missing
    ///
    /// Example: Missing "Datum" column in prognose response
    MissingColumn(String),

    /// Invalid timezone (API should always return UTC)
    InvalidTimezone(String),

    /// Unknown product type
    ///
    /// Example: "Biomass" (not in allowed set: Solar, Wind, Windonshore, Windoffshore)
    UnknownProduct(String),

    /// Unknown data category
    ///
    /// Example: Endpoint path doesn't contain prognose/hochrechnung/onlinehochrechnung
    UnknownDataCategory(String),

    /// Unknown redispatch direction
    ///
    /// Example: Direction not in allowed set (increase_generation, reduce_generation)
    UnknownDirection(String),

    /// Invalid grid status value
    ///
    /// Example: Status not in allowed set (GREEN, YELLOW, RED)
    InvalidGridStatus(String),

    /// CSV format error (wrong delimiter, malformed row)
    CsvFormat(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::InvalidDecimal(val) => {
                write!(
                    f,
                    "Invalid decimal format: '{}' (expected German format with comma)",
                    val
                )
            }
            ParseError::InvalidTimestamp(val) => {
                write!(
                    f,
                    "Invalid timestamp format: '{}' (expected DD.MM.YYYY or YYYY-MM-DD)",
                    val
                )
            }
            ParseError::MissingColumn(col) => {
                write!(f, "Missing required column: '{}'", col)
            }
            ParseError::InvalidTimezone(tz) => {
                write!(f, "Invalid timezone: '{}' (expected UTC)", tz)
            }
            ParseError::UnknownProduct(product) => {
                write!(f, "Unknown product type: '{}' (expected Solar, Wind, Windonshore, or Windoffshore)", product)
            }
            ParseError::UnknownDataCategory(endpoint) => {
                write!(f, "Unknown data category from endpoint: '{}'", endpoint)
            }
            ParseError::UnknownDirection(dir) => {
                write!(f, "Unknown redispatch direction: '{}' (expected 'Wirkleistungseinspeisung erhöhen' or 'Wirkleistungseinspeisung reduzieren')", dir)
            }
            ParseError::InvalidGridStatus(status) => {
                write!(
                    f,
                    "Invalid grid status: '{}' (expected GREEN, YELLOW, or RED)",
                    status
                )
            }
            ParseError::CsvFormat(msg) => {
                write!(f, "CSV format error: {}", msg)
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// OAuth2 authentication errors
///
/// Occurs during token fetch or refresh operations
#[derive(Debug, Clone)]
pub enum OAuth2Error {
    /// Failed to fetch access token from identity provider
    ///
    /// Includes HTTP status and response body for debugging
    FetchFailed { status: u16, body: String },

    /// Invalid OAuth2 credentials (client_id or client_secret)
    InvalidCredentials,

    /// Token response missing required fields (access_token, expires_in)
    InvalidTokenResponse(String),

    /// Token has expired and refresh failed
    TokenExpired,
}

impl fmt::Display for OAuth2Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OAuth2Error::FetchFailed { status, body } => {
                write!(f, "Token fetch failed (HTTP {}): {}", status, body)
            }
            OAuth2Error::InvalidCredentials => {
                write!(
                    f,
                    "Invalid OAuth2 credentials (check client_id and client_secret)"
                )
            }
            OAuth2Error::InvalidTokenResponse(msg) => {
                write!(f, "Invalid token response: {}", msg)
            }
            OAuth2Error::TokenExpired => {
                write!(f, "Access token expired and refresh failed")
            }
        }
    }
}

impl std::error::Error for OAuth2Error {}

/// HTTP API errors
///
/// Occurs during NTP API communication
#[derive(Debug, Clone)]
pub enum ApiError {
    /// HTTP error with status code and response body
    ///
    /// Common codes:
    /// - 401: Unauthorized (expired token)
    /// - 404: Not found (invalid endpoint or date range)
    /// - 429: Rate limit exceeded
    /// - 500: Server error
    HttpError { status: u16, body: String },

    /// Rate limit exceeded (HTTP 429)
    ///
    /// Should trigger exponential backoff retry
    RateLimited,

    /// Network error (connection timeout, DNS failure)
    NetworkError(String),

    /// Response body is empty when data expected
    EmptyResponse,

    /// Response is not valid CSV
    InvalidCsvResponse(String),
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::HttpError { status, body } => {
                write!(f, "HTTP {} error: {}", status, body)
            }
            ApiError::RateLimited => {
                write!(
                    f,
                    "Rate limit exceeded (HTTP 429). Implement exponential backoff."
                )
            }
            ApiError::NetworkError(msg) => {
                write!(f, "Network error: {}", msg)
            }
            ApiError::EmptyResponse => {
                write!(f, "API returned empty response")
            }
            ApiError::InvalidCsvResponse(msg) => {
                write!(f, "Invalid CSV response: {}", msg)
            }
        }
    }
}

impl std::error::Error for ApiError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_error_conversion() {
        let err = ParseError::InvalidDecimal("119,5a".to_string());
        let fdw_err: NtpFdwError = err.into();

        match fdw_err {
            NtpFdwError::Parse(ParseError::InvalidDecimal(val)) => {
                assert_eq!(val, "119,5a");
            }
            _ => panic!("Expected Parse error"),
        }
    }

    #[test]
    fn test_oauth2_error_conversion() {
        let err = OAuth2Error::InvalidCredentials;
        let fdw_err: NtpFdwError = err.into();

        match fdw_err {
            NtpFdwError::OAuth2(OAuth2Error::InvalidCredentials) => {
                // Success
            }
            _ => panic!("Expected OAuth2 error"),
        }
    }

    #[test]
    fn test_api_error_conversion() {
        let err = ApiError::RateLimited;
        let fdw_err: NtpFdwError = err.into();

        match fdw_err {
            NtpFdwError::Api(ApiError::RateLimited) => {
                // Success
            }
            _ => panic!("Expected Api error"),
        }
    }

    #[test]
    fn test_error_display_formatting() {
        let err = ParseError::MissingColumn("Datum".to_string());
        let msg = format!("{}", err);
        assert!(msg.contains("Missing required column"));
        assert!(msg.contains("Datum"));
    }

    #[test]
    fn test_http_error_formatting() {
        let err = ApiError::HttpError {
            status: 404,
            body: "Endpoint not found".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("HTTP 404"));
        assert!(msg.contains("Endpoint not found"));
    }
}
