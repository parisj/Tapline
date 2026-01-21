# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Per-worker metrics with `worker_id` label for `WORKERS_ACTIVE` gauge
- Cooperative-sticky Kafka partition assignment strategy (prevents stop-the-world rebalances)
- Dashboard now shows per-worker busy/idle status accurately

### Changed

- Renamed `yolo_segmentation` algorithm to `model_yolo_segmentation` (triggers slow-job partition pausing)
- YOLO algorithm now fails fast at initialization if ultralytics is not installed
- Algorithm naming convention: `model_` prefix for slow/ML algorithms

### Fixed

- Fixed 94% failure rate in YOLO segmentation under stress testing
- Fixed Kafka partition rebalance issues causing worker starvation
- Fixed dashboard worker status display showing incorrect active workers

---

- Claude Code framework with modular documentation
  - Specialized docs in `.claude/docs/`
  - Code standards, testing, security, refactoring guides
- Pre-commit hooks configuration
  - Ruff linting and formatting
  - mypy type checking
  - gitleaks secret scanning
- Enhanced CI/CD pipeline
  - Parallel lint, typecheck, test, security jobs
  - 80% coverage threshold enforcement
  - Codecov integration
- Security scanning
  - pip-audit for dependency vulnerabilities
  - bandit for code security analysis
- Project documentation
  - ARCHITECTURE.md - system design
  - CONTRIBUTING.md - contribution guidelines
  - DEVELOPMENT.md - setup guide
  - CHANGELOG.md - version history

### Changed

- Slimmed CLAUDE.md from ~380 to ~190 lines
- Moved detailed docs to `.claude/docs/` for context efficiency

## [0.1.0] - Initial Release

### Added

- Core pipeline infrastructure
  - File ingestion with directory watching
  - Kafka event streaming
  - Worker pool for parallel execution
  - MinIO content-addressed storage
- Algorithm framework
  - Base Algorithm class
  - Registry-based discovery
  - Configurable routing
- Metric system
  - MetricValue with AnalysisKind flags
  - Python-based time-windowed aggregation
  - Analyzer pipeline
- Built-in analyzers
  - Summary (mean, median, std, etc.)
  - Distribution (histogram)
  - Counter and Rate
  - Ellipse and Contour (2D)
- Observability stack
  - OpenTelemetry tracing
  - Prometheus metrics
  - Structured JSON logging
- Flask dashboard
  - REST API
  - Metric discovery
  - Real-time visualization
- Testing infrastructure
  - Unit tests
  - Integration tests (Kafka, MinIO)
  - E2E tests
