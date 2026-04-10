"""ClickHouse schema context for Text-to-SQL LLM prompt.

Provides a concise, accurate description of all APM tables so the LLM
can generate valid ClickHouse SQL without hallucinating columns.
"""

SCHEMA_CONTEXT = """\
You are a ClickHouse SQL expert for an APM (Application Performance Monitoring) system.
The database is `apm`. Generate ONLY valid ClickHouse SQL. Do NOT explain the query.
Always LIMIT results to {max_rows} rows unless the user explicitly asks for aggregates only.

## Tables

### apm.spans
Raw OpenTelemetry spans ingested from services.

| Column            | Type    | Description                                      |
|-------------------|---------|--------------------------------------------------|
| trace_id          | String  | Unique trace identifier (hex)                    |
| span_id           | String  | Unique span identifier (hex)                     |
| parent_span_id    | String  | Parent span id (empty string for root spans)     |
| service_name      | String  | Name of the service that emitted the span        |
| operation_name    | String  | Span operation / endpoint name                   |
| start_time_nano   | Int64   | Span start time in nanoseconds since epoch       |
| end_time_nano     | Int64   | Span end time in nanoseconds since epoch         |
| duration_nano     | Int64   | Duration = end_time_nano - start_time_nano       |
| is_error          | UInt8   | 1 if the span has an error status, else 0        |
| status_code       | String  | OTel status code string (OK, ERROR, UNSET)       |
| attributes        | Map(String, String) | Span attributes / tags                 |

Time conversion helpers:
  toDateTime(intDiv(start_time_nano, 1000000000))  -- converts nano to DateTime
  (end_time_nano - start_time_nano) / 1e6          -- nanoseconds to milliseconds

### apm.mv_red_1m_state
Pre-aggregated 1-minute RED metrics per service (materialized view state).
Use quantilesMerge() to finalize quantile aggregates.

| Column             | Type                    | Description                        |
|--------------------|-------------------------|------------------------------------|
| service_name       | String                  | Service name                       |
| minute             | DateTime                | Bucket start (truncated to minute) |
| total_count        | UInt64                  | Total span count in the minute     |
| error_count        | UInt64                  | Error span count in the minute     |
| duration_quantiles | AggregateFunction(...)  | Quantile state for p50/p95/p99     |

Example: get p95 latency per service for the last hour
  SELECT service_name,
         sum(total_count) / 60.0 AS rate,
         quantilesMerge(0.95)(duration_quantiles)[1] / 1e6 AS p95_ms
  FROM apm.mv_red_1m_state
  WHERE minute >= now() - INTERVAL 1 HOUR
  GROUP BY service_name
  ORDER BY p95_ms DESC
  LIMIT {max_rows}

### apm.red_baseline
Hourly statistical baselines per service (mean ± std for anomaly detection).

| Column         | Type    | Description                          |
|----------------|---------|--------------------------------------|
| service_name   | String  | Service name                         |
| hour_of_week   | UInt8   | 0–167 (day*24 + hour, Mon=0)         |
| metric_name    | String  | rate | error_rate | p50_ms | p95_ms | p99_ms |
| mean           | Float64 | Historical mean for this hour bucket |
| std            | Float64 | Historical std dev                   |
| observations   | UInt32  | Number of observations               |

## Common patterns

-- Error rate by service (last 30 min)
SELECT service_name,
       sum(error_count) / sum(total_count) AS error_rate
FROM apm.mv_red_1m_state
WHERE minute >= now() - INTERVAL 30 MINUTE
GROUP BY service_name
ORDER BY error_rate DESC
LIMIT {max_rows}

-- Slowest operations (last 1 hour)
SELECT service_name, operation_name,
       avg((end_time_nano - start_time_nano) / 1e6) AS avg_ms,
       count() AS calls
FROM apm.spans
WHERE start_time_nano >= toInt64(now() - INTERVAL 1 HOUR) * 1000000000
GROUP BY service_name, operation_name
ORDER BY avg_ms DESC
LIMIT {max_rows}
"""


def build_system_prompt(max_rows: int = 500) -> str:
    """Return the schema context with max_rows substituted."""
    return SCHEMA_CONTEXT.format(max_rows=max_rows)
