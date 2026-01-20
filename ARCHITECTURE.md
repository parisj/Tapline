# Architecture

## System Overview

VisioEval is a configuration-driven evaluation pipeline built on event sourcing principles.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              VisioEval                                   │
│                                                                         │
│  ┌─────────┐    ┌─────────┐    ┌──────────┐    ┌─────────┐    ┌──────┐ │
│  │ Ingest  │───►│  Kafka  │───►│ Workers  │───►│  MinIO  │───►│ API  │ │
│  │         │    │         │    │          │    │         │    │      │ │
│  │ Files   │    │ Events  │    │ Algos    │    │ Storage │    │ REST │ │
│  └─────────┘    └─────────┘    └──────────┘    └─────────┘    └──────┘ │
│       │              │              │              │              │     │
│       └──────────────┴──────────────┴──────────────┴──────────────┘     │
│                              Observability                              │
│                     (Prometheus, Jaeger, Logging)                       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### Ingest Layer (`src/ingest/`)

Watches directories for new files, ensures stability, deduplicates, and publishes jobs.

- **KafkaObserver** - Directory watcher that publishes to Kafka
- **DedupCache** - Content-hash based deduplication
- **ReadinessChecker** - File stability detection

### Event Backbone (`src/streaming/`)

Kafka-based event streaming with ordered, durable event delivery.

**Topics:**
| Topic | Purpose |
|-------|---------|
| `visio.jobs` | Job lifecycle events |
| `visio.results` | Algorithm results |
| `visio.metrics` | Metric emissions |
| `visio.audit-log` | Immutable audit trail |

### Worker Pool (`src/workers/`)

Parallel job execution with algorithm lifecycle management.

- **KafkaWorkerPool** - Consumes jobs, executes algorithms
- **Lifecycle** - Algorithm initialization and teardown
- **Offload** - Strategy pattern for compute offloading

### Algorithms (`src/algorithms/`)

Pluggable algorithm implementations with registry-based discovery.

- **Algorithm** - Base class defining the contract
- **Registry** - (name, version) -> factory mapping
- **AlgoResult** - Metrics + artifacts output

### Storage (`src/storage/`)

Content-addressed object storage on MinIO.

- **MinIOService** - CRUD operations
- **ObjectRef** - Hash-based path generation
- Path format: `bucket/ab/cd/{full_hash}`

### Aggregation (`src/app/flink_aggregation.py`)

Time-windowed metric aggregation.

- 1-minute tumbling windows
- Preserves raw values for all types
- Writes directly to MinIO

### Visualization (`src/visualization/`)

Flask-based REST API and dashboard.

- Auto-discovers metrics from MinIO
- On-demand artifact computation
- Prometheus integration

## Data Flow

### Job Lifecycle

```
1. File detected in watched directory
2. Readiness check (file stable)
3. Deduplication check (not seen before)
4. JOB_CREATED event published to Kafka
5. Worker consumes job
6. Algorithm executes
7. JOB_COMPLETED/FAILED event published
8. Metrics aggregated in time window
9. Aggregates stored to MinIO
```

### Event Types

| Event | Description |
|-------|-------------|
| `JOB_CREATED` | New job queued |
| `JOB_STARTED` | Worker picked up job |
| `JOB_COMPLETED` | Algorithm finished successfully |
| `JOB_FAILED` | Algorithm failed |
| `METRIC_EMITTED` | Metric value produced |

## Key Abstractions

### Job

Unit of work with lifecycle state machine:

```
created -> started -> completed
                   -> failed
```

### MetricValue

Algorithm output with analysis hints:

```python
MetricValue(
    value=0.87,
    analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
    meta={"range": [0.0, 1.0]}
)
```

### AnalysisKind

Bitmask declaring post-processing recipes:

| Flag | Purpose |
|------|---------|
| SUMMARY | Mean, median, std, etc. |
| DISTRIBUTION_1D | Histogram |
| OUTLIERS_1D | Z-score outlier detection |
| COUNTER | Boolean counting |
| RATE | Boolean rate with CI |
| ELLIPSE_2D | 2D covariance ellipse |
| CONTOUR_2D | 2D density contours |
| INFO | Informational (no aggregation) |

### EventEnvelope

Immutable event with hash chain for audit:

```python
EventEnvelope(
    event_id: UUID,
    event_type: EventType,
    source_id: str,
    timestamp: datetime,
    payload: dict,
    content_hash: str,
    prev_hash: str | None,
)
```

## Design Decisions

### Why Kafka?

- Ordered, durable event delivery
- Consumer groups for scaling
- Replay capability for debugging
- Natural fit for event sourcing

### Why MinIO?

- S3-compatible API
- Content-addressed storage
- Self-hosted, no cloud dependency
- Kubernetes-native

### Why Python Aggregation (Default)?

- Preserves all value types (not just numeric)
- No Flink cluster required
- Simpler deployment
- Sufficient for most workloads

### Why Separate Processes?

- GIL bypass for CPU-bound work
- Independent failure domains
- Easier scaling
- Process isolation for I/O

## Observability

### Tracing

OpenTelemetry spans for:
- Job processing
- Kafka produce/consume
- MinIO operations

### Metrics

Prometheus metrics for:
- Job counts and durations
- Kafka message rates
- Storage latencies

### Logging

Structured JSON logs with:
- Trace context injection
- Correlation IDs
- Request tracking
