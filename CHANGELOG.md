# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-05-04

Initial public release under the **PolyForm Noncommercial 1.0.0** license.

### Pipeline core

- File ingestion via directory observer with deduplication and stability detection
- Kafka-backed event streaming (`tasks`, `metrics`, `results`, `audit` topics)
- Cooperative-sticky partition assignment to avoid stop-the-world rebalances
- Worker pool with batched offset commits, dedicated I/O thread pool, and parallel artifact uploads
- Per-worker `WORKERS_ACTIVE` gauge with `worker_id` label for accurate busy/idle accounting

### Processor framework

- Base `Processor` interface with `(name, version)` registry
- Built-in processors: analysis_probe, blob_detection, edge_detection, image_quality, histogram_analysis, contour_analysis
- Optional `model_inference` and `model_yolo_segmentation` (gated behind the `ml` pixi environment)
- `model_` prefix triggers slow-task partition pausing

### Metric system

- `Measurement` carries value plus `AggregationType` flags (`STATS`, `HISTOGRAM`, `OUTLIERS`, `TALLY`, `RATE`, `SCATTER_ELLIPSE`, `DENSITY_MAP`, `RAW`)
- Python-based time-windowed aggregation (Flink SQL path optional)
- `AnalyzerRegistry` enables lazy registration of new analyzers

### Storage and audit

- MinIO content-addressed storage with sharded keys (`bucket/ab/cd/{full_hash}`)
- `EventEnvelope` wrapped with hash chain for tamper-evident audit replay
- Configurable `SignatureProvider` interface (HMAC/PKI implementations to follow)

### Dashboard

- Flask + vanilla JS dashboard at `http://localhost:5007`
- Modular blueprints (`prometheus`, `kafka`, `metrics`, `artifacts`, `audit`)
- `ViewerRegistry` dispatches artifact previews by MIME type
- Rate-limiting middleware and security headers
- Hash-based deep linking to dashboard tabs

### Observability

- OpenTelemetry tracing with graceful degradation when no OTLP exporter is reachable
- Prometheus metrics for ingest, workers, and aggregation
- Structured JSON logging with correlation IDs

### CI / quality

- Ruff lint + format, mypy type-check, pytest with coverage
- Bandit static analysis and gitleaks secret scanning
- Codecov coverage reporting

### Documentation

- Self-contained `README.md` with screenshots and quick-start
- `docs/` directory with development, contributing, and architecture references
- Local Inter + JetBrains Mono fonts (no CDN dependency)

### Known limitations

The following items are tracked for future releases and were surfaced by the
multi-agent review used for this release. They are intentionally documented
here rather than silently ignored.

- Pipeline runner crashes when `TAPLINE_USE_FLINK_SQL=true` (HIGH-1, backend report)
- Audit `publish_with_audit` dual-write is not atomic; failed audit writes are silent (HIGH-5)
- Hash chain content hash is keyless and therefore replayable by an attacker with storage write access (security M-3)
- Dashboard binds to `0.0.0.0` with no authentication; tighten before exposing beyond loopback (security H-4)
- Several dashboard `innerHTML` call sites still need defensive escaping (frontend C-1)

See `.omc/reports/` for the full evaluation reports.
