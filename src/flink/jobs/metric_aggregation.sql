-- ============================================================================
-- Tapline Metric Aggregation - Flink SQL Job (OPTIONAL)
-- ============================================================================
-- NOTE: Python-based aggregation (src/app/flink_aggregation.py) is now the
-- default and recommended method as it:
--   - Preserves raw values (including non-numeric types like {x,y} points)
--   - Supports all AnalysisKinds (ELLIPSE_2D, CONTOUR_2D, COUNTER, RATE, etc.)
--   - Writes directly to MinIO (no extra Kafka consumer needed)
--
-- To use this Flink SQL job instead, set: TAPLINE_USE_FLINK_SQL=true
--
-- This SQL job reads METRIC_EMITTED events from Kafka, aggregates metrics
-- in tumbling time windows, and writes results to the aggregates topic.
-- LIMITATION: Only handles numeric values; raw values are not preserved.
--
-- Submit via Flink SQL Client:
--   docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh
--   Then paste this SQL
-- ============================================================================

-- Configure idle timeout for source to advance watermarks when no new events
SET 'table.exec.source.idle-timeout' = '10s';

-- Create Kafka source table for metrics with event time
CREATE TABLE metrics_source (
    event_id STRING,
    event_type STRING,
    source_id STRING,
    `timestamp` STRING,
    payload ROW<
        task_id STRING,
        processor_name STRING,
        processor_version STRING,
        metric_name STRING,
        `value` DOUBLE,
        aggregation_mask INT,
        meta MAP<STRING, STRING>
    >,
    -- Parse ISO timestamp and define watermark with 10 second tolerance
    event_time AS TO_TIMESTAMP(SUBSTRING(`timestamp`, 1, 19), 'yyyy-MM-dd''T''HH:mm:ss'),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'tapline.metrics',
    'properties.bootstrap.servers' = 'kafka:9093',
    'properties.group.id' = 'tapline-flink-sql-v2',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- Create Kafka sink table for aggregates
CREATE TABLE aggregates_sink (
    processor_name STRING,
    processor_version STRING,
    metric_name STRING,
    aggregation_mask INT,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    metric_count BIGINT,
    metric_sum DOUBLE,
    metric_avg DOUBLE,
    metric_min DOUBLE,
    metric_max DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'tapline.aggregates',
    'properties.bootstrap.servers' = 'kafka:9093',
    'format' = 'json'
);

-- Run aggregation query with event time windows
-- Note: aggregation_mask is preserved using MAX since it should be constant per metric
INSERT INTO aggregates_sink
SELECT
    payload.processor_name AS processor_name,
    payload.processor_version AS processor_version,
    payload.metric_name AS metric_name,
    MAX(payload.aggregation_mask) AS aggregation_mask,
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    COUNT(*) AS metric_count,
    SUM(payload.`value`) AS metric_sum,
    AVG(payload.`value`) AS metric_avg,
    MIN(payload.`value`) AS metric_min,
    MAX(payload.`value`) AS metric_max
FROM metrics_source
WHERE event_type = 'METRIC_EMITTED'
  AND payload.`value` IS NOT NULL
GROUP BY
    payload.processor_name,
    payload.processor_version,
    payload.metric_name,
    TUMBLE(event_time, INTERVAL '1' MINUTE);
