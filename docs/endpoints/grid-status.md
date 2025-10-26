# Grid Status Timeseries Endpoint

## Purpose

The `grid_status_timeseries` endpoint tracks minute-by-minute grid stability status using a traffic light indicator (GREEN/YELLOW/RED) for real-time grid monitoring. This endpoint provides the most granular view of German grid stability, enabling detection of stress conditions, status transitions, and correlation with other grid events.

**Use Cases:**
- Real-time grid stability monitoring and alerting
- Status transition detection (GREEN→YELLOW/RED escalations)
- Grid stress correlation with redispatch events
- Daily/hourly grid stability summaries and reporting
- Critical situation duration analysis (RED periods)
- Long-term stability trend analysis

**Data Characteristics:**
- 1440 rows per day (minute-level granularity)
- Real-time traffic light status (GREEN/YELLOW/RED)
- Geographic scope: Germany (national grid stability indicator)
- Query time: ~500ms - 2 seconds (depending on date range)
- API coverage: 1 endpoint (TrafficLight - JSON format, only JSON endpoint in v0.2.8)

---

## Parameters

### Required Parameters

None - all parameters are optional. If no filters are provided, defaults to last 7 days.

### Optional Parameters

| Parameter | Type | Description | Default | Example | Notes |
|-----------|------|-------------|---------|---------|-------|
| `timestamp_utc` | TIMESTAMPTZ | Date range filter for grid status | Last 7 days | `>= '2024-10-24'` | API format: /TrafficLight/YYYY-MM-DD/YYYY-MM-DD. Returns 1-minute intervals. **Warning:** Large date ranges return many rows (1440/day). |
| `grid_status` | TEXT | Filter by status level | All statuses | `'RED'` | Values: `'GREEN'`, `'YELLOW'`, `'RED'`. Use to find only problem periods. |

---

## Return Columns

### Timestamp Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `timestamp_utc` | TIMESTAMPTZ | Start of 1-minute status interval | UTC timestamp | `2024-10-24 14:23:00+00` | Minute-level granularity. 1440 rows per day (24 hours × 60 minutes). |
| `interval_end_utc` | TIMESTAMPTZ | End of 1-minute status interval | UTC timestamp | `2024-10-24 14:24:00+00` | Always timestamp_utc + 1 minute. |

### Status Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `grid_status` | TEXT | Traffic light grid stability indicator | categorical | `GREEN` | Values: `'GREEN'` (normal operation), `'YELLOW'` (elevated attention), `'RED'` (critical situation). From TrafficLight JSON endpoint. |

### Metadata Columns

| Column | SQL Type | Description | Units | Example | Notes |
|--------|----------|-------------|-------|---------|-------|
| `source_endpoint` | TEXT | Original API endpoint path | text | `TrafficLight/2024-10-24/2024-10-24` | Always from /TrafficLight/ JSON endpoint (only JSON endpoint in v0.2.8). |
| `fetched_at` | TIMESTAMPTZ | When data was retrieved from API | UTC timestamp | `2024-10-25 10:30:45+00` | DEFAULT NOW(). Cache and freshness tracking. |

**Notes:**
- Minute-level data = 1440 rows per day (very high volume)
- **CRITICAL:** Always use narrow date ranges (1-7 days max) to avoid returning 100,000+ rows
- JSON parsing (first JSON endpoint implemented in v0.2.8)
- Index exists on grid_status for efficient filtering (idx_grid_status_status)

---

## Examples

### Example 1: Grid Status Distribution

**Purpose:** Calculate time spent in each status level for a given day

```sql
SELECT
  grid_status,
  COUNT(*) as minutes,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= '2024-10-24' AND timestamp_utc < '2024-10-25'
GROUP BY grid_status
ORDER BY grid_status;
```

**Expected Output:**

| grid_status | minutes | percentage |
|-------------|---------|------------|
| GREEN | 1382 | 95.97 |
| YELLOW | 56 | 3.89 |
| RED | 2 | 0.14 |

**Insights:**
- 3 rows (one per status level)
- Typical: GREEN 95-98%, YELLOW 2-5%, RED <1%
- High YELLOW/RED percentages indicate grid stress
- Use to assess daily grid stability
- Baseline: >95% GREEN is healthy
- Alert threshold: >10% YELLOW or any RED

**Performance:** ~500ms (1 API call: TrafficLight/1-day, returns 1440 rows)

---

### Example 2: Grid Status Transitions

**Purpose:** Detect status changes (GREEN→YELLOW/RED) with window functions

```sql
WITH status_changes AS (
  SELECT
    timestamp_utc,
    grid_status,
    LAG(grid_status) OVER (ORDER BY timestamp_utc) as prev_status
  FROM ntp.grid_status_timeseries
  WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '24 hours'
)
SELECT
  timestamp_utc as transition_time,
  prev_status || ' → ' || grid_status as status_change,
  CASE
    WHEN prev_status = 'GREEN' AND grid_status = 'YELLOW' THEN 'Elevated Alert'
    WHEN prev_status = 'YELLOW' AND grid_status = 'RED' THEN 'Critical Situation'
    WHEN prev_status = 'RED' AND grid_status = 'GREEN' THEN 'Crisis Resolved'
    WHEN prev_status = 'YELLOW' AND grid_status = 'GREEN' THEN 'Returned to Normal'
  END as transition_description
FROM status_changes
WHERE grid_status != prev_status
  AND prev_status IS NOT NULL
ORDER BY timestamp_utc DESC
LIMIT 20;
```

**Expected Output:**

| transition_time | status_change | transition_description |
|-----------------|---------------|------------------------|
| 2024-10-25 14:45:00+00 | YELLOW → GREEN | Returned to Normal |
| 2024-10-25 13:12:00+00 | GREEN → YELLOW | Elevated Alert |
| 2024-10-25 08:23:00+00 | YELLOW → GREEN | Returned to Normal |
| 2024-10-25 07:58:00+00 | GREEN → YELLOW | Elevated Alert |

**Insights:**
- 0-20 rows showing recent transitions
- GREEN↔YELLOW transitions common (normal grid management)
- YELLOW→RED indicates escalating crisis (requires immediate attention)
- RED→GREEN shows recovery from critical situation
- Use for real-time monitoring - transition to RED should trigger alerts
- Long RED periods indicate severe grid issues

**Performance:** ~500ms (1 API call, 1440 rows processed with LAG window function)

---

### Example 3: Redispatch Events During Grid Stress

**Purpose:** Correlate redispatch interventions with grid status

```sql
SELECT
  DATE(r.timestamp_utc) as date,
  COUNT(DISTINCT r.timestamp_utc) as redispatch_events,
  SUM(r.total_energy_mwh) as total_redispatch_energy_mwh,
  COUNT(*) FILTER (WHERE g.grid_status = 'YELLOW') as yellow_minutes,
  COUNT(*) FILTER (WHERE g.grid_status = 'RED') as red_minutes
FROM ntp.redispatch_events r
LEFT JOIN ntp.grid_status_timeseries g
  ON DATE(r.timestamp_utc) = DATE(g.timestamp_utc)
  AND g.grid_status != 'GREEN'
WHERE r.timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(r.timestamp_utc)
HAVING COUNT(*) FILTER (WHERE g.grid_status IN ('YELLOW', 'RED')) > 0
ORDER BY date DESC;
```

**Expected Output:**

| date | redispatch_events | total_redispatch_energy_mwh | yellow_minutes | red_minutes |
|------|-------------------|----------------------------|----------------|-------------|
| 2024-10-24 | 12 | 3456.78 | 145 | 8 |
| 2024-10-20 | 8 | 2134.56 | 89 | 0 |
| 2024-10-15 | 15 | 4567.89 | 234 | 23 |

**Insights:**
- Variable rows (days with grid stress)
- Shows correlation between redispatch activity and grid stress
- More interventions on high YELLOW/RED days
- Demonstrates grid management effectiveness:
  - High redispatch + GREEN status = interventions working
  - High redispatch + RED status = insufficient capacity
- Use to assess whether redispatch is effective at preventing RED status

**Performance:** ~1-2 seconds (2 API calls + JOIN processing)

---

### Example 4: Critical Situation Duration

**Purpose:** Find longest RED status periods (crisis duration)

```sql
WITH status_changes AS (
  SELECT
    timestamp_utc,
    grid_status,
    CASE WHEN grid_status != LAG(grid_status) OVER (ORDER BY timestamp_utc)
         THEN 1 ELSE 0 END as is_change
  FROM ntp.grid_status_timeseries
  WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'
),
periods AS (
  SELECT
    timestamp_utc,
    grid_status,
    SUM(is_change) OVER (ORDER BY timestamp_utc) as period_id
  FROM status_changes
)
SELECT
  MIN(timestamp_utc) as period_start,
  MAX(timestamp_utc) as period_end,
  COUNT(*) as duration_minutes,
  grid_status
FROM periods
WHERE grid_status = 'RED'
GROUP BY period_id, grid_status
HAVING COUNT(*) > 5
ORDER BY duration_minutes DESC
LIMIT 10;
```

**Expected Output:**

| period_start | period_end | duration_minutes | grid_status |
|--------------|------------|------------------|-------------|
| 2024-10-15 14:23:00+00 | 2024-10-15 15:38:00+00 | 75 | RED |
| 2024-10-08 09:12:00+00 | 2024-10-08 09:45:00+00 | 33 | RED |
| 2024-10-03 18:56:00+00 | 2024-10-03 19:18:00+00 | 22 | RED |

**Insights:**
- 0-10 rows (depends on period - empty result = no critical situations = good)
- RED periods typically 5-60 minutes
- Longer periods (>60 min) indicate severe crisis
- Use for incident analysis - investigate causes
- Correlate with:
  - Redispatch events (were interventions attempted?)
  - Renewable production (high wind/solar causing issues?)
  - Time of day (demand peaks?)

**Performance:** ~800ms (1 API call, window function processing on 43,200 rows for 30 days)

---

### Example 5: Hourly Status Aggregation

**Purpose:** Simplify minute-level data to hourly summary for trends

```sql
SELECT
  DATE_TRUNC('hour', timestamp_utc) as hour,
  COUNT(*) as total_minutes,
  COUNT(*) FILTER (WHERE grid_status = 'GREEN') as green_minutes,
  COUNT(*) FILTER (WHERE grid_status = 'YELLOW') as yellow_minutes,
  COUNT(*) FILTER (WHERE grid_status = 'RED') as red_minutes,
  CASE
    WHEN COUNT(*) FILTER (WHERE grid_status = 'RED') > 0 THEN 'RED'
    WHEN COUNT(*) FILTER (WHERE grid_status = 'YELLOW') > 30 THEN 'YELLOW'
    ELSE 'GREEN'
  END as hourly_status_summary
FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', timestamp_utc)
ORDER BY hour DESC;
```

**Expected Output:**

| hour | total_minutes | green_minutes | yellow_minutes | red_minutes | hourly_status_summary |
|------|---------------|---------------|----------------|-------------|----------------------|
| 2024-10-25 14:00:00+00 | 60 | 58 | 2 | 0 | GREEN |
| 2024-10-25 13:00:00+00 | 60 | 42 | 18 | 0 | GREEN |
| 2024-10-25 12:00:00+00 | 60 | 25 | 33 | 2 | YELLOW |
| 2024-10-25 11:00:00+00 | 60 | 12 | 45 | 3 | RED |

**Insights:**
- 168 rows (7 days × 24 hours)
- Each hour: 60 minutes total
- Hourly summary logic:
  - RED if any RED minutes (critical = entire hour problematic)
  - YELLOW if >30 yellow minutes (majority of hour stressed)
  - GREEN otherwise (normal operation)
- Reduces data volume for dashboards (168 rows vs 10,080 minute-level rows)
- Useful for weekly reports and trend analysis

**Performance:** ~800ms (1 API call, 10,080 rows aggregated)

---

## Performance Notes

### Query Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **API Latency** | 300-800ms | NTP API response time for TrafficLight JSON endpoint |
| **WASM Overhead** | 100-300ms | JSON parsing, timestamp conversion, row generation |
| **Total Query Time** | 500ms - 2 seconds | End-to-end execution time |
| **1-day query** | ~500ms | 1440 rows returned |
| **7-day query** | ~1 second | 10,080 rows returned |
| **30-day query** | ~2 seconds | 43,200 rows returned |

### Response Characteristics

| Metric | Value | Notes |
|--------|-------|-------|
| **Response size** | 50-200 KB per API call | JSON payload from NTP API |
| **Rows returned** | 1440 rows per day | Minute-level granularity |
| **Scaling** | Very high row count | 1-day: 1440 rows. 7-day: 10,080 rows. 30-day: 43,200 rows. **Limit date range!** |
| **Update frequency** | Real-time | Status updates every minute |

### Optimization Tips

1. **CRITICAL: Always use narrow date ranges (1-7 days max) to avoid returning 100,000+ rows:**
   ```sql
   WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '1 day'  -- 1440 rows
   WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'  -- 10,080 rows
   -- NOT: WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '90 days'  -- 129,600 rows!
   ```

2. **Use grid_status filter (has index: idx_grid_status_status) for YELLOW/RED analysis:**
   ```sql
   WHERE grid_status = 'RED'  -- Efficient index scan, returns only RED minutes
   ```

3. **Aggregate to hourly or daily for long-term trends (use DATE_TRUNC):**
   ```sql
   SELECT DATE_TRUNC('hour', timestamp_utc) as hour, ...  -- Reduces 1440 rows to 24
   SELECT DATE_TRUNC('day', timestamp_utc) as day, ...    -- Reduces 10,080 rows to 7
   ```

4. **For transition detection, use LAG() window function with ORDER BY timestamp_utc:**
   ```sql
   LAG(grid_status) OVER (ORDER BY timestamp_utc)  -- Efficient status change detection
   ```

5. **JOIN with redispatch_events: use DATE() or DATE_TRUNC() for alignment:**
   ```sql
   -- ❌ Slow (minute-level JOIN, Cartesian product)
   JOIN redispatch_events r ON r.timestamp_utc = g.timestamp_utc

   -- ✅ Fast (day-level JOIN)
   JOIN redispatch_events r ON DATE(r.timestamp_utc) = DATE(g.timestamp_utc)
   ```

6. **Consider materialized views for common aggregations (hourly status summary):**
   ```sql
   CREATE MATERIALIZED VIEW hourly_grid_status AS
   SELECT DATE_TRUNC('hour', timestamp_utc) as hour, ...;

   -- Refresh periodically
   REFRESH MATERIALIZED VIEW hourly_grid_status;
   ```

---

## Troubleshooting

### Issue: Query returns 100,000+ rows (very slow)

**Symptoms:** Long wait time, massive result set, potential timeout

**Cause:** Date range too wide. Each day returns 1440 rows. 90-day query = 129,600 rows.

**Solution:** Limit to 1-7 days or aggregate:
```sql
-- ❌ Too wide (129,600 rows)
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '90 days'

-- ✅ Narrow range (1440 rows)
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '1 day'

-- ✅ Or aggregate to hourly (2160 rows for 90 days)
SELECT DATE_TRUNC('hour', timestamp_utc) as hour, COUNT(*)
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_TRUNC('hour', timestamp_utc)

-- ✅ Use LIMIT for testing
WHERE timestamp_utc >= '2024-10-24'
LIMIT 100
```

---

### Issue: No RED or YELLOW status found

**Symptoms:** Query with `WHERE grid_status IN ('RED', 'YELLOW')` returns zero rows

**Cause:** Grid is stable. GREEN 99% of the time is **normal** during low-stress periods.

**Solution:** This is expected behavior:
```sql
-- Try wider date range (30 days) to find stress events
WHERE grid_status IN ('RED', 'YELLOW')
  AND timestamp_utc >= CURRENT_DATE - INTERVAL '30 days'

-- Check overall distribution
SELECT grid_status, COUNT(*) FROM ntp.grid_status_timeseries
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY grid_status;

-- Correlate with redispatch_events
SELECT COUNT(*) FROM ntp.redispatch_events
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days';
```

No RED/YELLOW = stable grid = good!

---

### Issue: Status transitions missing (gaps in data)

**Symptoms:** Expected transitions not showing up, or minute-level data has gaps

**Cause:** API may have missing minutes during data collection issues or downtime.

**Solution:** Use COALESCE or generate_series to fill gaps:
```sql
-- Generate expected minute series
WITH expected_minutes AS (
  SELECT generate_series(
    '2024-10-24 00:00:00'::timestamptz,
    '2024-10-24 23:59:00'::timestamptz,
    '1 minute'::interval
  ) as minute
)
SELECT
  e.minute,
  COALESCE(g.grid_status, 'MISSING') as status
FROM expected_minutes e
LEFT JOIN ntp.grid_status_timeseries g ON e.minute = g.timestamp_utc
WHERE COALESCE(g.grid_status, 'MISSING') = 'MISSING';

-- Check fetched_at timestamp for data freshness
SELECT MAX(fetched_at) FROM ntp.grid_status_timeseries;

-- Report persistent gaps to NTP API support
```

---

### Issue: JOIN with redispatch_events returns duplicate rows

**Symptoms:** Expected 10 rows, got 1440 rows (or other unexpected multiplication)

**Cause:** Minute-level grid_status (1440 rows/day) JOINed with event-level redispatch (variable). Cartesian product within time ranges.

**Solution:** Use DATE() or DATE_TRUNC for JOIN:
```sql
-- ❌ Cartesian product (minute × events)
FROM redispatch_events r
JOIN grid_status_timeseries g ON r.timestamp_utc = g.timestamp_utc

-- ✅ Day-level JOIN
FROM redispatch_events r
JOIN grid_status_timeseries g ON DATE(r.timestamp_utc) = DATE(g.timestamp_utc)

-- ✅ Or aggregate grid_status first
WITH daily_stress AS (
  SELECT
    DATE(timestamp_utc) as date,
    COUNT(*) FILTER (WHERE grid_status = 'RED') as red_minutes
  FROM grid_status_timeseries
  GROUP BY DATE(timestamp_utc)
)
SELECT r.*, ds.red_minutes
FROM redispatch_events r
JOIN daily_stress ds ON DATE(r.timestamp_utc) = ds.date;
```

---

### Issue: LAG() window function very slow

**Symptoms:** Query with LAG() takes >10 seconds

**Cause:** Large dataset (10,000+ rows) with window function requires sorting entire partition.

**Solution:** Optimize window function usage:
```sql
-- Ensure ORDER BY in window specification
LAG(grid_status) OVER (ORDER BY timestamp_utc)  -- Correct

-- Limit date range to 7 days max
WHERE timestamp_utc >= CURRENT_DATE - INTERVAL '7 days'

-- Ensure idx_grid_status_timestamp index exists
SELECT indexname FROM pg_indexes
WHERE tablename = 'grid_status_timeseries'
  AND indexname LIKE '%timestamp%';

-- Consider pre-aggregating to hourly for large ranges
WITH hourly AS (
  SELECT DATE_TRUNC('hour', timestamp_utc) as hour,
         mode() WITHIN GROUP (ORDER BY grid_status) as status
  FROM grid_status_timeseries
  GROUP BY DATE_TRUNC('hour', timestamp_utc)
)
SELECT hour, LAG(status) OVER (ORDER BY hour)
FROM hourly;
```

---

## API Constraints

### Rate Limiting

- No documented rate limits for NTP API
- OAuth2 token valid for 1 hour (automatic refresh)
- Recommend caching results for repeated queries (minute-level data = high volume)

### Data Availability

- **Historical data**: Past ~90 days typically available
- **Granularity**: 1-minute intervals (1440 rows per day)
- **Update frequency**: Real-time (updates every minute)
- **Status values**: GREEN, YELLOW, RED (traffic light system)

### Data Volume Warnings

- **1 day**: 1,440 rows
- **7 days**: 10,080 rows
- **30 days**: 43,200 rows
- **90 days**: 129,600 rows ⚠️ **Use aggregation for ranges >7 days**

---

## Related Documentation

- **[QUICKSTART.md](../../QUICKSTART.md)** - 5-minute setup guide
- **[README.md](../../README.md)** - Project overview
- **[renewable-energy.md](renewable-energy.md)** - Renewable energy timeseries endpoint
- **[electricity-prices.md](electricity-prices.md)** - Electricity market prices endpoint
- **[redispatch.md](redispatch.md)** - Grid redispatch events endpoint
- **[ARCHITECTURE.md](../reference/ARCHITECTURE.md)** - Complete design reference (15 ADRs)
- **[ETL_LOGIC.md](../reference/ETL_LOGIC.md)** - Data transformation specifications

---

**Built with NTP API** • **Powered by Supabase WASM FDW v0.3.0**
