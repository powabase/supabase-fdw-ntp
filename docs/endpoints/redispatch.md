# Redispatch Events Endpoint

## Purpose

The `redispatch_events` endpoint tracks grid intervention events where TSOs (Transmission System Operators) adjust power plant output to manage congestion and maintain grid stability. These events represent real-time grid management actions to balance electricity supply and demand across the German transmission network.

**Use Cases:**
- Grid stability monitoring and congestion analysis
- TSO zone performance comparison and stress assessment
- Long-duration event identification (maintenance or severe congestion)
- Conventional vs renewable facility redispatch impact analysis
- Generation increase vs decrease balance tracking
- Total grid intervention energy volume calculation

**Data Characteristics:**
- Event-based data (not regular time-series): 5-50 events per day depending on grid stress
- Variable duration: minutes to hours (typically 2-12 hours per event)
- Geographic scope: Germany (4 TSO zones: 50Hertz, Amprion, TenneT TSO, TransnetBW)
- Query time: ~500ms - 1 second
- API coverage: 1 endpoint (redispatch)

---

## Parameters

### Required Parameters

None - all parameters are optional. If no filters are provided, defaults to last 7 days.

### Optional Parameters

| Parameter | Type | Description | Default | Example | Notes |
|-----------|------|-------------|---------|---------|-------|
| `timestamp_utc` | TIMESTAMPTZ | Date range filter for redispatch events | Last 7 days | `>= '2024-10-24'` | API format: /redispatch/YYYY-MM-DD/YYYY-MM-DD. Fetches events that overlap with date range. |
| `requesting_tso` | TEXT | Filter by TSO that requested intervention | All TSOs | `'TransnetBW'` | Values: `'50Hertz'`, `'Amprion'`, `'TenneT TSO'`, `'TransnetBW'`. German names from API. |
| `direction` | TEXT | Filter by type of power adjustment | All directions | `'increase_generation'` | Values: `'increase_generation'`, `'reduce_generation'`. |

---

## Return Columns

### Timestamp Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `timestamp_utc` | TIMESTAMPTZ | Start time of redispatch event | UTC timestamp | `2024-10-24 22:00:00+00` | Event start. Duration calculated from interval_end_utc. |
| `interval_end_utc` | TIMESTAMPTZ | End time of redispatch event | UTC timestamp | `2024-10-25 08:00:00+00` | Event end. Example shows 10-hour duration event (22:00 to 08:00). |

### Event Characteristics

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `reason` | TEXT | Reason for redispatch intervention (German text) | text | `Probestart (NetzRes)` or `Netzengpass` | German values preserved from API. 'Netzengpass'=grid congestion, 'Probestart (NetzRes)'=test start for grid reserve. |
| `direction` | TEXT | Type of power adjustment | categorical | `increase_generation` | Values: `'increase_generation'` (Wirkleistungseinspeisung erhöhen) or `'reduce_generation'` (reduzieren). English enum from German API text. |

### Power and Energy Metrics

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `avg_power_mw` | NUMERIC | Average power adjustment during event | MW (Megawatts) | `119.5` | Mittlere Leistung (German). NULL if not provided by API. |
| `max_power_mw` | NUMERIC | Peak power adjustment during event | MW (Megawatts) | `150.0` | Maximale Leistung (German). Typically higher than avg_power_mw. |
| `total_energy_mwh` | NUMERIC | Total energy involved in redispatch | MWh (Megawatt-hours) | `1195.0` | Gesamte Arbeit (German). Calculated as avg_power_mw × duration_hours. Example: 119.5 MW × 10 hours = 1195 MWh. |

### TSO and Facility Information

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `requesting_tso` | TEXT | TSO that requested the redispatch (German name) | categorical | `TransnetBW` | Anfordernder ÜNB (German). Values: `'50Hertz'`, `'Amprion'`, `'TenneT TSO'`, `'TransnetBW'`. |
| `instructing_tso` | TEXT | TSO that instructed the redispatch (German name) | categorical | `TransnetBW` | Anweisender ÜNB (German). Often same as requesting_tso. NULL if not specified. |
| `affected_facility` | TEXT | Power plant or facility affected (German name) | text | `Grosskraftwerk Mannheim Block 8` or `Börse` | Betroffene Anlage (German). 'Börse'=exchange (market-based intervention). NULL if not specified. |
| `energy_type` | TEXT | Primary energy source type (German categories) | categorical | `Konventionell` | Primärenergieart (German). Values: `'Konventionell'` (fossil), `'Erneuerbar'` (renewable), `'Sonstiges'` (other). German text preserved. |

### Metadata Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `source_endpoint` | TEXT | Original API endpoint path | text | `redispatch/2024-10-24/2024-10-25` | Data lineage. Always from /redispatch/ endpoint. |
| `fetched_at` | TIMESTAMPTZ | When data was retrieved from API | UTC timestamp | `2024-10-25 10:30:45+00` | DEFAULT NOW(). Cache and freshness tracking. |

**Notes:**
- German text values preserved per ADR-003 (data provenance) while schema uses English column names
- 'Börse' (exchange) indicates market-based intervention, not specific power plant control
- NULL values in power/energy columns indicate API did not provide that metric for the event

---

## Examples

### Example 1: Recent Redispatch Events

**Purpose:** View latest grid interventions with key details

```sql
SELECT
  timestamp_utc as event_start,
  interval_end_utc as event_end,
  reason,
  direction,
  avg_power_mw,
  total_energy_mwh,
  requesting_tso,
  affected_facility
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY timestamp_utc DESC
LIMIT 10;
```

**Expected Output:**

| event_start | event_end | reason | direction | avg_power_mw | total_energy_mwh | requesting_tso | affected_facility |
|-------------|-----------|--------|-----------|--------------|------------------|----------------|-------------------|
| 2024-10-24 22:00:00+00 | 2024-10-25 08:00:00+00 | Netzengpass | increase_generation | 119.5 | 1195.0 | TransnetBW | Grosskraftwerk Mannheim Block 8 |
| 2024-10-24 15:00:00+00 | 2024-10-24 19:00:00+00 | Netzengpass | reduce_generation | 85.2 | 340.8 | TenneT TSO | Börse |

**Insights:**
- 0-10 rows (varies by grid stress)
- Shows most recent interventions
- Duration typically 2-12 hours
- Power: 50-500 MW typical range
- Use to monitor current grid stability - high frequency indicates congestion issues

**Performance:** ~500ms (1 API call: redispatch/7-day range)

---

### Example 2: Redispatch Energy Volume by TSO

**Purpose:** Calculate total energy involved in redispatch per TSO

```sql
SELECT
  requesting_tso,
  COUNT(*) as event_count,
  ROUND(SUM(total_energy_mwh)::numeric, 2) as total_energy_mwh,
  ROUND(AVG(total_energy_mwh)::numeric, 2) as avg_energy_per_event_mwh,
  ROUND(AVG(EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc))/3600)::numeric, 1) as avg_duration_hours
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY requesting_tso
ORDER BY total_energy_mwh DESC;
```

**Expected Output:**

| requesting_tso | event_count | total_energy_mwh | avg_energy_per_event_mwh | avg_duration_hours |
|----------------|-------------|------------------|--------------------------|-------------------|
| TenneT TSO | 142 | 38456.75 | 270.81 | 6.5 |
| Amprion | 98 | 24789.32 | 253.05 | 5.8 |
| 50Hertz | 87 | 19234.56 | 221.09 | 5.2 |
| TransnetBW | 76 | 15678.90 | 206.30 | 4.9 |

**Insights:**
- 4 rows (one per TSO)
- Shows which zones have most congestion
- TenneT often highest due to high wind in North
- Values: 5,000-50,000 MWh/month typical
- Total energy indicates grid stress level - correlate with renewable production peaks

**Performance:** ~500ms (1 API call)

---

### Example 3: Increase vs Decrease Generation Balance

**Purpose:** Analyze balance between generation increases and decreases

```sql
SELECT
  DATE(timestamp_utc) as date,
  direction,
  COUNT(*) as event_count,
  ROUND(SUM(total_energy_mwh)::numeric, 2) as total_energy_mwh
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(timestamp_utc), direction
ORDER BY date DESC, direction;
```

**Expected Output:**

| date | direction | event_count | total_energy_mwh |
|------|-----------|-------------|------------------|
| 2024-10-24 | increase_generation | 5 | 1234.56 |
| 2024-10-24 | reduce_generation | 4 | 987.32 |
| 2024-10-23 | increase_generation | 6 | 1456.78 |
| 2024-10-23 | reduce_generation | 7 | 1523.45 |

**Insights:**
- 60 rows (30 days × 2 directions)
- Should be roughly balanced (equal increases and decreases)
- Imbalance indicates systematic issues (e.g., consistent overproduction or underproduction)
- Balanced redispatch is normal grid management

**Performance:** ~500ms (1 API call)

---

### Example 4: Long-Duration Events

**Purpose:** Identify extended redispatch events (>6 hours)

```sql
SELECT
  timestamp_utc,
  interval_end_utc,
  ROUND(EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc))/3600::numeric, 1) as duration_hours,
  reason,
  direction,
  total_energy_mwh,
  requesting_tso,
  affected_facility
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
  AND EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc))/3600 > 6
ORDER BY duration_hours DESC;
```

**Expected Output:**

| timestamp_utc | interval_end_utc | duration_hours | reason | direction | total_energy_mwh | requesting_tso | affected_facility |
|---------------|------------------|----------------|--------|-----------|------------------|----------------|-------------------|
| 2024-10-18 22:00:00+00 | 2024-10-19 14:00:00+00 | 16.0 | Netzengpass | increase_generation | 2456.8 | TenneT TSO | Kraftwerk Nord |
| 2024-10-12 20:00:00+00 | 2024-10-13 08:00:00+00 | 12.0 | Probestart (NetzRes) | increase_generation | 1534.2 | 50Hertz | Reserve Unit 5 |

**Insights:**
- 0-20 rows (depends on period)
- Long events (10-24 hours) indicate severe congestion or planned maintenance
- Most events are 2-6 hours
- Long-duration events have highest total energy impact
- Often overnight (low demand, high wind production)

**Performance:** ~500ms (1 API call)

---

### Example 5: Conventional vs Renewable Redispatch

**Purpose:** Compare redispatch impact on conventional vs renewable facilities

```sql
SELECT
  energy_type,
  COUNT(*) as event_count,
  ROUND(AVG(avg_power_mw)::numeric, 2) as avg_power_mw,
  ROUND(SUM(total_energy_mwh)::numeric, 2) as total_energy_mwh
FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
  AND energy_type IS NOT NULL
GROUP BY energy_type
ORDER BY total_energy_mwh DESC;
```

**Expected Output:**

| energy_type | event_count | avg_power_mw | total_energy_mwh |
|-------------|-------------|--------------|------------------|
| Konventionell | 245 | 156.34 | 45678.90 |
| Erneuerbar | 87 | 98.23 | 12345.67 |
| Sonstiges | 12 | 45.67 | 789.12 |

**Insights:**
- 2-3 rows (depends on energy types present)
- Conventional facilities often have higher redispatch volumes (more controllable/dispatchable)
- Renewable may be curtailed (reduced generation)
- Shows grid management strategy - high renewable curtailment indicates integration challenges

**Performance:** ~500ms (1 API call)

---

## Performance Notes

### Query Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **API Latency** | 300-600ms | NTP API response time for redispatch endpoint |
| **WASM Overhead** | 100-200ms | CSV parsing, German text handling, row conversion |
| **Total Query Time** | 500ms - 1 second | End-to-end execution time |
| **7-day query** | ~500ms | Typical 35-350 events |
| **30-day query** | ~800ms | Typical 150-1500 events |

### Response Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Response size** | 5-30 KB per API call | CSV payload from NTP API |
| **Rows returned** | 5-50 events per day | Highly variable, depends on grid stress |
| **Scaling** | Event-based, not time-series | 7-day query: 35-350 rows. 30-day query: 150-1500 rows. |
| **Event frequency** | Variable | Low grid stress: 5-10 events/day. High stress: 30-50 events/day. |

### Optimization Tips

1. **Always use timestamp_utc filter to limit date range (API requires dates):**
   ```sql
   WHERE timestamp_utc >= '2024-10-24' AND timestamp_utc < '2024-10-25'
   ```

2. **Filter by requesting_tso for zone-specific analysis:**
   ```sql
   WHERE requesting_tso = 'TenneT TSO'
   ```

3. **Use direction filter to separate generation increases from decreases:**
   ```sql
   WHERE direction = 'increase_generation'
   ```

4. **For energy calculations: SUM(total_energy_mwh) gives total grid intervention:**
   ```sql
   SELECT SUM(total_energy_mwh) as total_intervention_mwh
   ```

5. **Duration calculation: EXTRACT(EPOCH FROM ...) for hours:**
   ```sql
   EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc))/3600 as duration_hours
   ```

6. **Index on timestamp_utc exists (idx_redispatch_timestamp) for efficient filtering**

---

## Troubleshooting

### Issue: No redispatch events found for recent dates

**Symptoms:** Query returns zero rows for recent date range

**Cause:** Low grid stress periods may have zero redispatch events. This is **normal**.

**Solution:** This is expected behavior during stable grid conditions:
```sql
-- Extend date range to 30 days
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'

-- Check grid_status table for correlation
SELECT grid_status, COUNT(*)
FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '1 day'
GROUP BY grid_status;

-- If grid_status shows GREEN (stable), zero redispatch is normal
```

Redispatch is event-driven, not continuous. No events = stable grid.

---

### Issue: reason column shows German text

**Symptoms:** Values like 'Netzengpass', 'Probestart' instead of English

**Cause:** German values preserved per ADR-003 (data provenance). Schema uses English column names.

**Solution:** This is **expected behavior**. Common German terms:

| German Term | English Translation | Meaning |
|-------------|---------------------|---------|
| Netzengpass | Grid congestion | Main reason for redispatch |
| Probestart (NetzRes) | Test start (Grid Reserve) | Reserve capacity testing |
| Wirkleistungseinspeisung erhöhen | Increase active power feed-in | Increase generation |
| Wirkleistungseinspeisung reduzieren | Reduce active power feed-in | Reduce generation |

Document common terms in your application or create translation lookup table.

---

### Issue: total_energy_mwh is NULL

**Symptoms:** NULL values in energy column for some events

**Cause:** API may not provide energy values for all events.

**Solution:** Use COALESCE or filter:
```sql
-- Use COALESCE for aggregations
SELECT SUM(COALESCE(total_energy_mwh, 0)) as total_energy

-- Or filter to complete records
WHERE total_energy_mwh IS NOT NULL

-- Check if avg_power_mw is also NULL (related)
SELECT COUNT(*) FILTER (WHERE total_energy_mwh IS NULL) as null_energy,
       COUNT(*) FILTER (WHERE avg_power_mw IS NULL) as null_power
FROM ntp.redispatch_events;
```

---

### Issue: affected_facility is 'Börse' (exchange)

**Symptoms:** Facility name is 'Börse' instead of power plant name

**Cause:** Market-based redispatch intervention (not specific power plant).

**Solution:** This is **normal and expected**:
- 'Börse' = electricity exchange/market
- Indicates intervention via electricity market trading
- Not direct power plant control
- Common for reduce_generation events (curtailment via market)

Example:
```sql
-- Market-based interventions
SELECT COUNT(*) as market_interventions
FROM ntp.redispatch_events
WHERE affected_facility = 'Börse';

-- Direct plant control
SELECT COUNT(*) as plant_interventions
FROM ntp.redispatch_events
WHERE affected_facility != 'Börse' AND affected_facility IS NOT NULL;
```

---

### Issue: Duration calculation returns large negative values

**Symptoms:** Negative duration hours or unrealistic values (e.g., -10000 hours)

**Cause:** Event crosses date boundary incorrectly or timestamp order wrong.

**Solution:** Use ABS() or check timestamp order:
```sql
-- Use ABS() for protection
SELECT ABS(EXTRACT(EPOCH FROM (interval_end_utc - timestamp_utc))/3600) as duration_hours

-- Check timestamp order (interval_end_utc should always be > timestamp_utc)
SELECT COUNT(*) as invalid_events
FROM ntp.redispatch_events
WHERE interval_end_utc <= timestamp_utc;

-- If invalid_events > 0, report as data quality issue
```

If consistently wrong, check API data quality or parsing logic.

---

## API Constraints

### Rate Limiting

- No documented rate limits for NTP API
- OAuth2 token valid for 1 hour (automatic refresh)
- Recommend caching results for repeated queries

### Data Availability

- **Historical data**: Past ~90 days typically available
- **Event frequency**: Highly variable (5-50 events per day)
- **Granularity**: Event-based (not regular intervals)
- **Update frequency**: Near real-time (events added as they occur)

### Event Characteristics

- **Typical duration**: 2-12 hours
- **Extended events**: Up to 24+ hours (rare, severe congestion/maintenance)
- **Short events**: <1 hour (very rare, emergency interventions)
- **Power range**: 50-500 MW typical, can exceed 1000 MW

---

## Related Documentation

- **[QUICKSTART.md](../../QUICKSTART.md)** - 5-minute setup guide
- **[README.md](../../README.md)** - Project overview
- **[renewable-energy.md](renewable-energy.md)** - Renewable energy timeseries endpoint
- **[electricity-prices.md](electricity-prices.md)** - Electricity market prices endpoint
- **[grid-status.md](grid-status.md)** - Grid stability status endpoint
- **[ARCHITECTURE.md](../reference/ARCHITECTURE.md)** - Complete design reference (15 ADRs)
- **[ETL_LOGIC.md](../reference/ETL_LOGIC.md)** - Data transformation specifications

---

**Built with NTP API** • **Powered by Supabase WASM FDW v0.3.0**
