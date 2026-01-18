-- ============================================================================
-- VisioEval Metric Aggregation - Flink SQL Job
-- ============================================================================
-- This SQL job reads METRIC_EMITTED events from Kafka, aggregates metrics
-- in tumbling time windows, and writes results to the aggregates topic.
--
-- Submit via Flink SQL Client:
--   docker exec -it flink-jobmanager /opt/flink/bin/sql-client.sh
--   Then paste this SQL
-- ============================================================================

-- Create Kafka source table for metrics
CREATE TABLE metrics_source (
    event_id STRING,
    event_type STRING,
    source_id STRING,
    `timestamp` STRING,
    payload ROW<
        job_id STRING,
        algo_name STRING,
        algo_version STRING,
        metric_name STRING,
        `value` DOUBLE,
        analysis_mask INT,
        meta MAP<STRING, STRING>
    >,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'visio.metrics',
    'properties.bootstrap.servers' = 'kafka:9093',
    'properties.group.id' = 'visioeval-flink-sql',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

-- Create Kafka sink table for aggregates
CREATE TABLE aggregates_sink (
    algo_name STRING,
    algo_version STRING,
    metric_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    metric_count BIGINT,
    metric_sum DOUBLE,
    metric_avg DOUBLE,
    metric_min DOUBLE,
    metric_max DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'visio.aggregates',
    'properties.bootstrap.servers' = 'kafka:9093',
    'format' = 'json'
);

-- Run aggregation query
INSERT INTO aggregates_sink
SELECT
    payload.algo_name AS algo_name,
    payload.algo_version AS algo_version,
    payload.metric_name AS metric_name,
    TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(proc_time, INTERVAL '1' MINUTE) AS window_end,
    COUNT(*) AS metric_count,
    SUM(payload.`value`) AS metric_sum,
    AVG(payload.`value`) AS metric_avg,
    MIN(payload.`value`) AS metric_min,
    MAX(payload.`value`) AS metric_max
FROM metrics_source
WHERE event_type = 'METRIC_EMITTED'
  AND payload.`value` IS NOT NULL
GROUP BY
    payload.algo_name,
    payload.algo_version,
    payload.metric_name,
    TUMBLE(proc_time, INTERVAL '1' MINUTE);
