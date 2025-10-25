//! OAuth2 Token Manager for NTP API
//!
//! Implements OAuth2 client credentials flow with token caching for the German NTP API.
//!
//! # Features
//! - Thread-safe token caching with Arc<Mutex<>>
//! - Proactive token refresh (5-minute buffer before expiration)
//! - Uses only WASM-compatible Supabase HTTP interface
//! - No external OAuth2 crates (WASM constraint)
//!
//! # Example
//! ```rust
//! use supabase_fdw_ntp::oauth2::{OAuth2Config, OAuth2Manager};
//!
//! let config = OAuth2Config {
//!     token_url: "https://identity.netztransparenz.de/users/connect/token".to_string(),
//!     client_id: "your_client_id".to_string(),
//!     client_secret: "your_client_secret".to_string(),
//!     scope: "ntpStatistic.read_all_public".to_string(),
//! };
//!
//! let manager = OAuth2Manager::new(config);
//! let token = manager.get_token()?;
//! ```

use crate::bindings::supabase::wrappers::time;
use crate::error::OAuth2Error;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// OAuth2 configuration
///
/// Stores credentials and endpoint information for OAuth2 client credentials flow
#[derive(Debug, Clone)]
pub struct OAuth2Config {
    /// Token endpoint URL
    ///
    /// Example: `https://identity.netztransparenz.de/users/connect/token`
    pub token_url: String,

    /// OAuth2 client ID
    pub client_id: String,

    /// OAuth2 client secret (sensitive!)
    pub client_secret: String,

    /// OAuth2 scope
    ///
    /// Example: `ntpStatistic.read_all_public`
    pub scope: String,
}

/// Cached access token with expiration
#[derive(Debug, Clone)]
struct CachedToken {
    /// JWT access token
    access_token: String,

    /// Unix timestamp when token expires (seconds since epoch)
    expires_at: i64,
}

impl CachedToken {
    /// Check if token is expired or will expire soon
    ///
    /// Uses 5-minute buffer (300 seconds) for proactive refresh
    /// Now uses Supabase time::epoch_secs() instead of SystemTime (WASM-compatible)
    fn is_expired(&self) -> bool {
        let now = time::epoch_secs();

        // Refresh 5 minutes before actual expiration (proactive refresh)
        const REFRESH_BUFFER_SECONDS: i64 = 300;

        now >= self.expires_at.saturating_sub(REFRESH_BUFFER_SECONDS)
    }
}

/// OAuth2 token response from NTP identity provider
///
/// Deserialized from JSON response
#[derive(Debug, Deserialize, Serialize)]
struct TokenResponse {
    /// JWT access token
    access_token: String,

    /// Token lifetime in seconds (typically 3600 = 1 hour)
    expires_in: u64,

    /// Token type (always "Bearer")
    #[serde(default)]
    token_type: String,

    /// Granted scope
    #[serde(default)]
    scope: String,
}

/// OAuth2 token manager with caching
///
/// Thread-safe implementation using Arc<Mutex<>> for concurrent access
pub struct OAuth2Manager {
    /// OAuth2 configuration (credentials, endpoints)
    config: OAuth2Config,

    /// Cached token (None if not yet fetched or expired)
    cached_token: Arc<Mutex<Option<CachedToken>>>,
}

impl OAuth2Manager {
    /// Create new OAuth2 manager
    ///
    /// # Arguments
    /// * `config` - OAuth2 configuration (credentials, token URL, scope)
    ///
    /// # Example
    /// ```
    /// let config = OAuth2Config {
    ///     token_url: "https://identity.netztransparenz.de/users/connect/token".to_string(),
    ///     client_id: "your_client_id".to_string(),
    ///     client_secret: "your_client_secret".to_string(),
    ///     scope: "ntpStatistic.read_all_public".to_string(),
    /// };
    /// let manager = OAuth2Manager::new(config);
    /// ```
    pub fn new(config: OAuth2Config) -> Self {
        Self {
            config,
            cached_token: Arc::new(Mutex::new(None)),
        }
    }

    /// Get valid access token (from cache or fetch new)
    ///
    /// This is the main entry point for token access. It will:
    /// 1. Check cache for valid token
    /// 2. Return cached token if valid (not expired)
    /// 3. Fetch new token if cache empty or expired
    ///
    /// Thread-safe: Multiple concurrent calls will wait for lock
    ///
    /// # Returns
    /// - `Ok(String)` - Valid access token
    /// - `Err(OAuth2Error)` - Token fetch failed
    ///
    /// # Example
    /// ```
    /// let token = manager.get_token()?;
    /// // Use token in Authorization header: format!("Bearer {}", token)
    /// ```
    pub fn get_token(&self) -> Result<String, OAuth2Error> {
        // Lock the cache (thread-safe, handle poisoning gracefully)
        let mut cache = self
            .cached_token
            .lock()
            .map_err(|e| OAuth2Error::FetchFailed {
                status: 0,
                body: format!("Token cache mutex poisoned: {}", e),
            })?;

        // Check if we have a valid cached token
        if let Some(ref token) = *cache {
            if !token.is_expired() {
                // Return cached token (still valid)
                return Ok(token.access_token.clone());
            }
        }

        // Cache empty or expired, fetch new token
        let new_token = self.fetch_token()?;

        // Update cache
        *cache = Some(new_token.clone());

        Ok(new_token.access_token)
    }

    /// Fetch new access token from OAuth2 endpoint
    ///
    /// Performs HTTP POST with client credentials flow
    ///
    /// # Returns
    /// - `Ok(CachedToken)` - Successfully fetched token
    /// - `Err(OAuth2Error)` - HTTP error, invalid response, or parse error
    ///
    /// # Implementation Notes
    /// - Uses Supabase HTTP interface (WASM-compatible)
    /// - Form-urlencoded body (not JSON!)
    /// - Parses JSON response
    fn fetch_token(&self) -> Result<CachedToken, OAuth2Error> {
        // Build form-urlencoded request body
        let body = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}&scope={}",
            urlencoding::encode(&self.config.client_id),
            urlencoding::encode(&self.config.client_secret),
            urlencoding::encode(&self.config.scope)
        );

        // Build HTTP request using Supabase interface
        let request = crate::bindings::supabase::wrappers::http::Request {
            method: crate::bindings::supabase::wrappers::http::Method::Post,
            url: self.config.token_url.clone(),
            headers: vec![(
                "content-type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            )],
            body,
        };

        // Make HTTP POST request
        let response =
            crate::bindings::supabase::wrappers::http::post(&request).map_err(|err| {
                OAuth2Error::FetchFailed {
                    status: 0,
                    body: err.to_string(),
                }
            })?;

        // Check for HTTP errors
        if response.status_code != 200 {
            // Handle specific error codes
            return match response.status_code {
                401 => Err(OAuth2Error::InvalidCredentials),
                _ => Err(OAuth2Error::FetchFailed {
                    status: response.status_code,
                    body: response.body.clone(),
                }),
            };
        }

        // Parse JSON response
        let token_response: TokenResponse =
            serde_json::from_str(&response.body).map_err(|err| {
                OAuth2Error::InvalidTokenResponse(format!(
                    "Failed to parse token response: {}. Body: {}",
                    err, response.body
                ))
            })?;

        // Validate response
        if token_response.access_token.is_empty() {
            return Err(OAuth2Error::InvalidTokenResponse(
                "access_token is empty".to_string(),
            ));
        }

        if token_response.expires_in == 0 {
            return Err(OAuth2Error::InvalidTokenResponse(
                "expires_in is 0".to_string(),
            ));
        }

        // Calculate expiration timestamp using Supabase time interface (WASM-compatible)
        let now = time::epoch_secs();

        let expires_at = now + token_response.expires_in as i64;

        Ok(CachedToken {
            access_token: token_response.access_token,
            expires_at,
        })
    }

    /// Clear cached token (force re-fetch on next get_token call)
    ///
    /// Useful for handling 401 Unauthorized errors from API
    ///
    /// # Example
    /// ```
    /// // API returned 401 - clear cache and retry
    /// manager.clear_cache();
    /// let token = manager.get_token()?;
    /// ```
    pub fn clear_cache(&self) {
        // Handle mutex poisoning gracefully (if poisoned, cache is already invalid)
        if let Ok(mut cache) = self.cached_token.lock() {
            *cache = None;
        }
        // If lock fails (poisoned), cache is already effectively cleared
    }

    /// Check if cached token is near expiry (within 5-minute buffer)
    ///
    /// Used for proactive token refresh before making API calls.
    /// This implements the "proactive" part of the hybrid refresh strategy
    /// specified in Phase 1.
    ///
    /// # Returns
    /// - `true` if token should be refreshed proactively (near expiry or missing)
    /// - `false` if token is still valid with sufficient buffer
    ///
    /// # Proactive Refresh Strategy
    ///
    /// The 5-minute buffer ensures tokens are refreshed BEFORE they expire,
    /// preventing 401 errors during multi-endpoint queries. This is the
    /// "proactive" component of Phase 1's hybrid strategy:
    ///
    /// - **Proactive (this method):** Check before each API call, refresh if near expiry
    /// - **Reactive (fallback):** If 401 occurs, clear cache and retry
    ///
    /// # Example
    /// ```
    /// // Before making API call, check if token needs proactive refresh
    /// if manager.is_near_expiry() {
    ///     manager.clear_cache();
    ///     let token = manager.get_token()?;
    ///     // Use fresh token for API call
    /// }
    /// ```
    pub fn is_near_expiry(&self) -> bool {
        // Handle mutex poisoning gracefully - assume expired on error (safe fallback)
        match self.cached_token.lock() {
            Ok(cache) => match cache.as_ref() {
                Some(token) => token.is_expired(), // Uses 5-min buffer internally
                None => true,                      // No token = needs refresh
            },
            Err(_) => true, // Lock poisoned = assume expired (triggers refresh)
        }
    }
}

// Simple URL encoding for form data
mod urlencoding {
    pub fn encode(input: &str) -> String {
        input
            .chars()
            .map(|c| match c {
                'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // NOTE: Tests that use time::epoch_secs() are removed because they require
    // the WASM host runtime and cannot run in cargo test. These tests should be
    // validated during E2E testing with actual Supabase runtime.
    // See HANDOVER.md line 790-797 for details.

    #[test]
    fn test_oauth2_manager_creation() {
        let config = OAuth2Config {
            token_url: "https://example.com/token".to_string(),
            client_id: "test_client".to_string(),
            client_secret: "test_secret".to_string(),
            scope: "test_scope".to_string(),
        };

        let manager = OAuth2Manager::new(config.clone());

        // Verify cache is initially empty
        let cache = manager.cached_token.lock().unwrap();
        assert!(cache.is_none(), "Cache should be empty on creation");
    }

    #[test]
    fn test_urlencoding_basic() {
        assert_eq!(urlencoding::encode("hello"), "hello");
        assert_eq!(urlencoding::encode("hello world"), "hello+world");
        assert_eq!(urlencoding::encode("a@b.com"), "a%40b.com");
        assert_eq!(
            urlencoding::encode("test_123-abc.xyz~"),
            "test_123-abc.xyz~"
        );
    }

    #[test]
    fn test_urlencoding_special_chars() {
        assert_eq!(urlencoding::encode("a&b=c"), "a%26b%3Dc");
        assert_eq!(urlencoding::encode("100%"), "100%25");
    }

    #[test]
    fn test_token_response_deserialization() {
        let json = r#"{
            "access_token": "eyJhbGciOiJSUzI1NiIs...",
            "expires_in": 3600,
            "token_type": "Bearer",
            "scope": "ntpStatistic.read_all_public"
        }"#;

        let response: TokenResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.access_token, "eyJhbGciOiJSUzI1NiIs...");
        assert_eq!(response.expires_in, 3600);
        assert_eq!(response.token_type, "Bearer");
        assert_eq!(response.scope, "ntpStatistic.read_all_public");
    }

    #[test]
    fn test_token_response_minimal() {
        // Response with only required fields
        let json = r#"{
            "access_token": "token123",
            "expires_in": 7200
        }"#;

        let response: TokenResponse = serde_json::from_str(json).unwrap();

        assert_eq!(response.access_token, "token123");
        assert_eq!(response.expires_in, 7200);
        assert_eq!(response.token_type, ""); // Default empty string
        assert_eq!(response.scope, ""); // Default empty string
    }

    #[test]
    fn test_token_response_invalid_json() {
        let json = r#"{"access_token": "missing_expires_in"}"#;

        let result: Result<TokenResponse, _> = serde_json::from_str(json);
        assert!(result.is_err(), "Should fail on missing expires_in");
    }
}
