# NTP API Technical Specification

**API Name:** Netztransparenz (NTP) Data API
**Base URL:** `https://ds.netztransparenz.de`
**API Version:** v1
**Documentation:** https://netztransparenz.de (German TSO transparency platform)
**OpenAPI Spec:** Available (but format metadata is incorrect)

---

## Authentication

### OAuth2 Client Credentials Flow

**Token Endpoint:** `https://identity.netztransparenz.de/users/connect/token`
**Grant Type:** `client_credentials`
**Required Scope:** `ntpStatistic.read_all_public`
**Token Lifetime:** 3600 seconds (1 hour)
**Token Type:** Bearer (JWT)

### Token Request

```bash
curl -X POST https://identity.netztransparenz.de/users/connect/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials" \
  -d "client_id=YOUR_CLIENT_ID" \
  -d "client_secret=YOUR_CLIENT_SECRET" \
  -d "scope=ntpStatistic.read_all_public"
```

### Response

```json
{
  "access_token": "eyJhbGciOiJSUzI1NiIs...",
  "expires_in": 3600,
  "token_type": "Bearer",
  "scope": "ntpStatistic.read_all_public"
}
```

### Usage in API Calls

```bash
curl -H "Authorization: Bearer YOUR_ACCESS_TOKEN" \
  https://ds.netztransparenz.de/api/v1/data/...
```

---

## Response Format Discovery

### ⚠️ Critical Finding: API Returns CSV, Not JSON

**OpenAPI Spec Claims:** `application/json`
**Actual Response:** `text/csv` with semicolon delimiters

**Exception:** TrafficLight endpoint returns JSON array

### CSV Format Specifications

**Delimiter:** Semicolon (`;`)
**Encoding:** UTF-8
**Decimal Separator:** Comma (`,`) - German format
**Line Ending:** `\n`
**Header Row:** Always present (row 0)

**Example:**
```csv
Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
18.10.2024;00:00;UTC;01:00;UTC;8,273
18.10.2024;01:00;UTC;02:00;UTC;7,884
```

---

## Selected Endpoints for v0.1.0

### 1. Spotmarktpreise (Spot Market Prices)

**Endpoint:** `GET /api/v1/data/Spotmarktpreise/{dateFrom}/{dateTo}`

#### Parameters

| Parameter | Type | Required | Format | Example |
|-----------|------|----------|--------|---------|
| dateFrom | string | Yes | ISO 8601 datetime | `2024-10-18T00:00:00` |
| dateTo | string | Yes | ISO 8601 datetime | `2024-10-25T00:00:00` |

#### Request Example

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://ds.netztransparenz.de/api/v1/data/Spotmarktpreise/2024-10-18T00:00:00/2024-10-25T00:00:00"
```

#### Response Structure (CSV)

```csv
Datum;von;Zeitzone von;bis;Zeitzone bis;Spotmarktpreis in ct/kWh
18.10.2024;00:00;UTC;01:00;UTC;8,273
18.10.2024;01:00;UTC;02:00;UTC;7,884
18.10.2024;02:00;UTC;03:00;UTC;8,119
```

#### Column Definitions

| Column | Type | Description | Unit | Nullable |
|--------|------|-------------|------|----------|
| Datum | Date | Date (German format DD.MM.YYYY) | - | No |
| von | Time | Start time (HH:MM) | - | No |
| Zeitzone von | String | Timezone (always "UTC") | - | No |
| bis | Time | End time (HH:MM) | - | No |
| Zeitzone bis | String | Timezone (always "UTC") | - | No |
| Spotmarktpreis in ct/kWh | Decimal | Spot market price | ct/kWh | No |

#### Response Characteristics

- **Granularity:** Hourly
- **Rows per day:** 24
- **Rows per week:** 168
- **Typical size:** ~38 bytes/row
- **Example size:** 7 days = 6.4 KB (169 rows including header)

#### Data Notes

- Prices can be **negative** during oversupply periods
- Decimal format uses **comma** (`,`) not period (`.`)
- Timestamps are always in **UTC**
- Data available for past several years

---

### 2. Prognose (Renewable Energy Forecasts)

**Endpoint:** `GET /api/v1/data/prognose/{product}/{dateFrom}/{dateTo}`

#### Parameters

| Parameter | Type | Required | Format | Allowed Values | Example |
|-----------|------|----------|--------|----------------|---------|
| product | string | Yes | Enum | `Solar`, `Wind` | `Solar` |
| dateFrom | string | Yes | ISO 8601 datetime | - | `2024-10-25T00:00:00` |
| dateTo | string | Yes | ISO 8601 datetime | - | `2024-10-27T00:00:00` |

#### Request Example

```bash
# Solar forecast
curl -H "Authorization: Bearer $TOKEN" \
  "https://ds.netztransparenz.de/api/v1/data/prognose/Solar/2024-10-25T00:00:00/2024-10-27T00:00:00"

# Wind forecast
curl -H "Authorization: Bearer $TOKEN" \
  "https://ds.netztransparenz.de/api/v1/data/prognose/Wind/2024-10-25T00:00:00/2024-10-27T00:00:00"
```

#### Response Structure (CSV)

```csv
Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-25;00:00;UTC;00:15;UTC;245,123;312,456;428,789;178,234
2024-10-25;00:15;UTC;00:30;UTC;248,567;315,890;432,123;180,456
```

#### Column Definitions

| Column | Type | Description | Unit | Nullable |
|--------|------|-------------|------|----------|
| Datum | Date | Date (ISO format YYYY-MM-DD) | - | No |
| von | Time | Start time (HH:MM) | - | No |
| Zeitzone von | String | Timezone (always "UTC") | - | No |
| bis | Time | End time (HH:MM) | - | No |
| Zeitzone bis | String | Timezone (always "UTC") | - | No |
| 50Hertz (MW) | Decimal | Forecast for 50Hertz TSO zone | MW | Yes (shows "N.A.") |
| Amprion (MW) | Decimal | Forecast for Amprion TSO zone | MW | Yes (shows "N.A.") |
| TenneT TSO (MW) | Decimal | Forecast for TenneT TSO zone | MW | Yes (shows "N.A.") |
| TransnetBW (MW) | Decimal | Forecast for TransnetBW TSO zone | MW | Yes (shows "N.A.") |

#### TSO Zones (German Transmission System Operators)

1. **50Hertz** - Eastern Germany (Berlin, Brandenburg, etc.)
2. **Amprion** - Western Germany (NRW, Rhineland)
3. **TenneT TSO** - Northern Germany (Lower Saxony, etc.)
4. **TransnetBW** - Southern Germany (Baden-Württemberg)

#### Response Characteristics

- **Granularity:** Quarter-hourly (15 minutes)
- **Rows per hour:** 4
- **Rows per day:** 96
- **Typical size:** ~51 bytes/row
- **Example size:** 2 days = 9.8 KB (193 rows including header)

#### Data Notes

- "N.A." values indicate **missing data** or **nighttime for solar** (0 MW)
- TSO columns represent **regional forecasts**
- Total German forecast = sum of 4 TSO zones
- Rolling forecasts updated **multiple times daily**

---

### 3. Hochrechnung (Production Extrapolation)

**Endpoint:** `GET /api/v1/data/hochrechnung/{product}/{dateFrom}/{dateTo}`

#### Parameters

**Identical to prognose endpoint**

| Parameter | Type | Required | Format | Allowed Values | Example |
|-----------|------|----------|--------|----------------|---------|
| product | string | Yes | Enum | `Solar`, `Wind` | `Solar` |
| dateFrom | string | Yes | ISO 8601 datetime | - | `2024-10-24T00:00:00` |
| dateTo | string | Yes | ISO 8601 datetime | - | `2024-10-25T00:00:00` |

#### Request Example

```bash
curl -H "Authorization: Bearer $TOKEN" \
  "https://ds.netztransparenz.de/api/v1/data/hochrechnung/Solar/2024-10-24T00:00:00/2024-10-25T00:00:00"
```

#### Response Structure (CSV)

**Identical structure to prognose:**

```csv
Datum;von;Zeitzone von;bis;Zeitzone bis;50Hertz (MW);Amprion (MW);TenneT TSO (MW);TransnetBW (MW)
2024-10-24;00:00;UTC;00:15;UTC;0,000;0,000;0,000;0,000
2024-10-24;00:15;UTC;00:30;UTC;0,000;0,000;0,000;0,000
```

#### Column Definitions

**Same as prognose** (see above)

#### Response Characteristics

- **Granularity:** Quarter-hourly (15 minutes)
- **Rows per day:** 96
- **Typical size:** ~51 bytes/row
- **Example size:** 1 day = 5.7 KB (97 rows including header)

#### Data Notes

- Represents **extrapolated actual production** from representative plants
- Based on **online measurements** from sample installations
- More accurate than forecasts for **recent periods**
- **Solar production at night = 0,000 MW** (decimal comma!)
- Used for **grid balancing** and **forecast validation**

---

## Data Type Conversions

### German to SQL Data Types

| German Format | Example | SQL Type | Conversion Notes |
|---------------|---------|----------|------------------|
| `DD.MM.YYYY` | `18.10.2024` | `DATE` | Parse to ISO 8601 |
| `YYYY-MM-DD` | `2024-10-25` | `DATE` | Already ISO 8601 |
| `HH:MM` | `13:45` | `TIME` | Parse as time |
| `X,XXX` | `8,273` | `NUMERIC` | Replace `,` → `.` |
| `MW` | `245,123` | `NUMERIC(10,3)` | Megawatts, 3 decimals |
| `ct/kWh` | `8,273` | `NUMERIC(10,3)` | Cents per kWh |
| `N.A.` | `N.A.` | `NULL` | Missing data indicator |

### Parsing Algorithm

```
1. Read HTTP response as UTF-8 text
2. Split by newline (\n) to get rows
3. Row 0 = header (parse column names)
4. For each row (1..N):
   a. Split by semicolon (;)
   b. For each column:
      - If "N.A." → NULL
      - If contains "," → replace with "." then parse as decimal
      - If DD.MM.YYYY → parse as date
      - If HH:MM → parse as time
      - Else → parse as string
```

---

## SQL Foreign Table Schemas

### spotmarktpreise

```sql
CREATE FOREIGN TABLE fdw_ntp.spotmarktpreise (
  datum DATE NOT NULL,
  von_time TIME NOT NULL,
  zeitzone_von TEXT DEFAULT 'UTC',
  bis_time TIME NOT NULL,
  zeitzone_bis TEXT DEFAULT 'UTC',
  spotmarktpreis_ct_kwh NUMERIC(10,3) NOT NULL
)
SERVER ntp_server
OPTIONS (
  endpoint 'Spotmarktpreise',
  date_from_required 'true',
  date_to_required 'true'
);

-- Usage
SELECT * FROM fdw_ntp.spotmarktpreise
WHERE datum >= '2024-10-18' AND datum <= '2024-10-25';
```

### prognose_solar / prognose_wind

```sql
CREATE FOREIGN TABLE fdw_ntp.prognose_solar (
  datum DATE NOT NULL,
  von_time TIME NOT NULL,
  zeitzone_von TEXT DEFAULT 'UTC',
  bis_time TIME NOT NULL,
  zeitzone_bis TEXT DEFAULT 'UTC',
  tso_50hertz_mw NUMERIC(10,3),
  tso_amprion_mw NUMERIC(10,3),
  tso_tennet_mw NUMERIC(10,3),
  tso_transnetbw_mw NUMERIC(10,3),
  total_mw NUMERIC(10,3) GENERATED ALWAYS AS (
    COALESCE(tso_50hertz_mw, 0) +
    COALESCE(tso_amprion_mw, 0) +
    COALESCE(tso_tennet_mw, 0) +
    COALESCE(tso_transnetbw_mw, 0)
  ) STORED
)
SERVER ntp_server
OPTIONS (
  endpoint 'prognose',
  product 'Solar',  -- or 'Wind'
  date_from_required 'true',
  date_to_required 'true'
);
```

### hochrechnung_solar / hochrechnung_wind

```sql
-- Same schema as prognose
CREATE FOREIGN TABLE fdw_ntp.hochrechnung_solar (
  datum DATE NOT NULL,
  von_time TIME NOT NULL,
  zeitzone_von TEXT DEFAULT 'UTC',
  bis_time TIME NOT NULL,
  zeitzone_bis TEXT DEFAULT 'UTC',
  tso_50hertz_mw NUMERIC(10,3),
  tso_amprion_mw NUMERIC(10,3),
  tso_tennet_mw NUMERIC(10,3),
  tso_transnetbw_mw NUMERIC(10,3),
  total_mw NUMERIC(10,3) GENERATED ALWAYS AS (
    COALESCE(tso_50hertz_mw, 0) +
    COALESCE(tso_amprion_mw, 0) +
    COALESCE(tso_tennet_mw, 0) +
    COALESCE(tso_transnetbw_mw, 0)
  ) STORED
)
SERVER ntp_server
OPTIONS (
  endpoint 'hochrechnung',
  product 'Solar',
  date_from_required 'true',
  date_to_required 'true'
);
```

---

## Parameter Mapping (SQL WHERE → API)

### spotmarktpreise

| SQL WHERE Clause | API Parameter | Example |
|------------------|---------------|---------|
| `datum >= '2024-10-18'` | `dateFrom` | `2024-10-18T00:00:00` |
| `datum <= '2024-10-25'` | `dateTo` | `2024-10-25T23:59:59` |

**WASM FDW Implementation:**
```rust
// Extract date range from WHERE clause
let date_from = quals.find("datum", ">=") // e.g., "2024-10-18"
  .format_as_iso8601(); // → "2024-10-18T00:00:00"

let date_to = quals.find("datum", "<=")
  .format_as_iso8601(); // → "2024-10-25T23:59:59"

let url = format!(
  "{}/api/v1/data/Spotmarktpreise/{}/{}",
  base_url, date_from, date_to
);
```

### prognose / hochrechnung

| SQL WHERE Clause | API Parameter | Example |
|------------------|---------------|---------|
| `datum >= '2024-10-25'` | `dateFrom` | `2024-10-25T00:00:00` |
| `datum <= '2024-10-27'` | `dateTo` | `2024-10-27T23:59:59` |
| Table name contains "solar" | `product` | `Solar` |
| Table name contains "wind" | `product` | `Wind` |

**WASM FDW Implementation:**
```rust
// Infer product from table name
let product = if table_name.contains("solar") {
  "Solar"
} else if table_name.contains("wind") {
  "Wind"
} else {
  return error("Table name must contain 'solar' or 'wind'");
};

let url = format!(
  "{}/api/v1/data/prognose/{}/{}/{}",
  base_url, product, date_from, date_to
);
```

---

## Error Handling

### HTTP Status Codes

| Code | Meaning | FDW Response |
|------|---------|--------------|
| 200 | Success | Parse CSV and return rows |
| 401 | Unauthorized | Error: "Invalid or expired token" |
| 403 | Forbidden | Error: "Insufficient permissions" |
| 429 | Too Many Requests | Error: "Rate limit exceeded" |
| 500 | Server Error | Error: "NTP API server error" |

### API-Specific Error Scenarios

**Empty Response:**
- API returns only header row (no data rows)
- FDW should return 0 rows (not error)

**Missing Date Parameters:**
- Some endpoints have "current" variants without dates
- NEVER use these in FDW (unbounded data risk)
- Always require date filters

**Invalid Date Range:**
- `dateFrom > dateTo` → FDW validation error before API call
- Future dates → May return empty (forecasts have limits)

**Invalid Product:**
- `product not in ['Solar', 'Wind']` → FDW validation error
- Case-sensitive! Must be capitalized exactly

---

## Rate Limits & Best Practices

### Rate Limiting

**Observed Limits:**
- ~60 requests per minute
- No explicit rate limit headers in responses
- 429 status code when exceeded

**Recommendations:**
- Implement **exponential backoff** on 429 errors
- **Cache token** for full 3600 seconds (don't refresh on every query)
- **Batch queries** when possible (use wider date ranges)

### Query Optimization

**Good Query:**
```sql
-- One API call, 7 days of data
SELECT * FROM fdw_ntp.spotmarktpreise
WHERE datum >= '2024-10-18' AND datum <= '2024-10-25';
```

**Bad Query:**
```sql
-- Would make 7 API calls (one per day) if not optimized
SELECT * FROM fdw_ntp.spotmarktpreise
WHERE datum = '2024-10-18'
   OR datum = '2024-10-19'
   OR datum = '2024-10-20'
   OR datum = '2024-10-21'
   OR datum = '2024-10-22'
   OR datum = '2024-10-23'
   OR datum = '2024-10-24';
```

**FDW Optimization:** Combine OR'd date predicates into single date range.

---

## Security Considerations

### OAuth2 Credentials

**Storage:**
- Store `client_id` and `client_secret` in **server options** (encrypted by Postgres)
- **Never log** client_secret in application logs
- **Rotate credentials** periodically

**Token Caching:**
- Cache access token in memory (expires in 1 hour)
- Implement automatic refresh **before** expiration
- Handle 401 errors gracefully (refresh + retry)

### Data Sensitivity

- NTP API data is **public** (grid transparency requirement)
- No PII (Personally Identifiable Information)
- Safe for caching, logging, and storage

---

## Appendix: Test Responses

### Example 1: spotmarktpreise (7 days)

**Request:**
```
GET /api/v1/data/Spotmarktpreise/2024-10-18T00:00:00/2024-10-25T00:00:00
```

**Response:** `phase-1-temp/responses/spotmarktpreise_7days.csv`
- **Rows:** 168 (+ 1 header)
- **Size:** 6.4 KB
- **First row:** `18.10.2024;00:00;UTC;01:00;UTC;8,273`

### Example 2: prognose Solar (2 days)

**Request:**
```
GET /api/v1/data/prognose/Solar/2024-10-25T00:00:00/2024-10-27T00:00:00
```

**Response:** `phase-1-temp/responses/prognose_solar_2days.csv`
- **Rows:** 192 (+ 1 header)
- **Size:** 9.8 KB
- **First row:** `2024-10-25;00:00;UTC;00:15;UTC;N.A.;N.A.;N.A.;N.A.` (nighttime)

### Example 3: hochrechnung Solar (1 day)

**Request:**
```
GET /api/v1/data/hochrechnung/Solar/2024-10-24T00:00:00/2024-10-25T00:00:00
```

**Response:** `phase-1-temp/responses/hochrechnung_solar_1day.csv`
- **Rows:** 96 (+ 1 header)
- **Size:** 5.7 KB
- **First row:** `2024-10-24;00:00;UTC;00:15;UTC;0,000;0,000;0,000;0,000`

---

## References

- **NTP Website:** https://www.netztransparenz.de
- **API Base:** https://ds.netztransparenz.de
- **Identity Provider:** https://identity.netztransparenz.de
- **OpenAPI Spec:** Available in project (`phase-1-temp/openapi.json`)
- **Test Responses:** `phase-1-temp/responses/` directory

---

**Specification Version:** 1.0
**Last Updated:** 2024-10-25
**Author:** Phase 1 API Discovery
**Status:** Ready for Phase 2 (Implementation)