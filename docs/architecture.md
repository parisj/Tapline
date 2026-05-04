# Architecture

## System Overview

Tapline is a configuration-driven evaluation pipeline built on event sourcing principles.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Tapline                                     │
│                                                                         │
│  ┌─────────┐    ┌─────────┐    ┌──────────┐    ┌─────────┐    ┌──────┐ │
│  │ Ingest  │───►│  Kafka  │───►│ Workers  │───►│  MinIO  │───►│ API  │ │
│  │         │    │         │    │          │    │         │    │      │ │
│  │ Files   │    │ Events  │    │ Procs    │    │ Storage │    │ REST │ │
│  └─────────┘    └─────────┘    └──────────┘    └─────────┘    └──────┘ │
│       │              │              │              │              │     │
│       └──────────────┴──────────────┴──────────────┴──────────────┘     │
│                              Observability                              │
│                     (Prometheus, Jaeger, Logging)                       │
└─────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### Ingest Layer (`src/ingest/`)

Watches directories for new files, ensures stability, deduplicates, and publishes tasks.

- **KafkaObserver** - Directory watcher that publishes to Kafka
- **DedupCache** - Content-hash based deduplication
- **ReadinessChecker** - File stability detection

### Event Backbone (`src/streaming/`)

Kafka-based event streaming with ordered, durable event delivery.

**Topics:**
| Topic | Purpose |
|-------|---------|
| `tapline.tasks` | Task lifecycle events |
| `tapline.results` | Processor results |
| `tapline.metrics` | Metric emissions |
| `tapline.audit-log` | Immutable audit trail |

### Worker Pool (`src/workers/`)

Parallel task execution with processor lifecycle management.

- **KafkaWorkerPool** - Consumes tasks, executes processors
- **Lifecycle** - Processor initialization and teardown
- **Offload** - Strategy pattern for compute offloading

### Processors (`src/processors/`)

Pluggable processor implementations with registry-based discovery.

- **Processor** - Base class defining the contract
- **Registry** - (name, version) -> factory mapping
- **ProcessorResult** - Metrics + artifacts output

### Storage (`src/storage/`)

Content-addressed object storage on MinIO.

- **MinIOService** - CRUD operations
- **ArtifactRef** - Hash-based path generation
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

### Task Lifecycle

```
1. File detected in watched directory
2. Readiness check (file stable)
3. Deduplication check (not seen before)
4. TASK_CREATED event published to Kafka
5. Worker consumes task
6. Processor executes
7. TASK_COMPLETED/FAILED event published
8. Metrics aggregated in time window
9. Aggregates stored to MinIO
```

### Event Types

| Event | Description |
|-------|-------------|
| `TASK_CREATED` | New task queued |
| `TASK_STARTED` | Worker picked up task |
| `TASK_COMPLETED` | Processor finished successfully |
| `TASK_FAILED` | Processor failed |
| `METRIC_EMITTED` | Metric value produced |

## Key Abstractions

### Task

Unit of work with lifecycle state machine:

```
created -> started -> completed
                   -> failed
```

### Measurement

Processor output with analysis hints:

```python
Measurement(
    value=0.87,
    analysis=AggregationType.STATS | AggregationType.HISTOGRAM,
    meta={"range": [0.0, 1.0]}
)
```

### AggregationType

Bitmask declaring post-processing recipes:

| Flag | Purpose |
|------|---------|
| STATS | Mean, median, std, etc. |
| HISTOGRAM | Histogram |
| OUTLIERS | Z-score outlier detection |
| TALLY | Boolean counting |
| RATE | Boolean rate with CI |
| SCATTER_ELLIPSE | 2D covariance ellipse |
| DENSITY_MAP | 2D density contours |
| RAW | Informational (no aggregation) |

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
- Task processing
- Kafka produce/consume
- MinIO operations

### Metrics

Prometheus metrics for:
- Task counts and durations
- Kafka message rates
- Storage latencies

### Logging

Structured JSON logs with:
- Trace context injection
- Correlation IDs
- Request tracking
