# Tapline

[![CI](https://github.com/yourusername/Tapline/actions/workflows/tests.yml/badge.svg)](https://github.com/yourusername/Tapline/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/yourusername/Tapline/branch/master/graph/badge.svg)](https://codecov.io/gh/yourusername/Tapline)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Configuration-driven evaluation pipeline for continuous ingestion, processing, and time-windowed analysis of data.

<p align="center">
  <img
    src="https://github.com/user-attachments/assets/0f8d68a1-b047-46cd-969c-90db5706cac7"
    alt="Tapline Architecture"
    height="600"
  />
</p>

## Quick Start

```bash
# Install dependencies
pixi install

# Copy and configure environment
cp .env.example .env

# Start infrastructure
docker-compose up -d

# Run the pipeline
pixi run pipeline

# View dashboard
pixi run dashboard  # http://localhost:5007
```

## What It Does

**Ingest** -> **Kafka** -> **Workers** -> **Processors** -> **MinIO** -> **Aggregation** -> **Dashboard**

- Watches directories for new files
- Routes files to processors via configuration
- Executes processors in parallel workers
- Stores results in content-addressed storage
- Aggregates metrics in time windows
- Visualizes via REST API and web dashboard

## Core Concepts

| Concept | Description |
|---------|-------------|
| **Task** | Unit of work with lifecycle (created -> started -> completed/failed) |
| **Processor** | Computes metrics from input data |
| **Measurement** | Value + AggregationType flags for post-processing |
| **AggregationType** | Declares how metrics should be analyzed (STATS, HISTOGRAM, etc.) |
| **Analyzer** | Transforms metrics to summaries and artifacts |

## Tech Stack

| Component | Technology |
|-----------|------------|
| Language | Python 3.12 |
| Package Manager | Pixi |
| Event Streaming | Kafka |
| Object Storage | MinIO |
| Observability | Prometheus, Grafana, Jaeger |
| Dashboard | Flask + Plotly.js |

## Commands

```bash
# Pipeline
pixi run pipeline         # Full pipeline
pixi run run              # Main only
pixi run dashboard        # Web dashboard

# Quality
pixi run test             # Run tests
pixi run lint             # Lint code
pixi run typecheck        # Type check
pixi run precommit-run    # All checks

# Security
pixi run security-audit   # Dependency scan
pixi run bandit-check     # Code analysis
```

## Adding a Processor

1. Create class extending `Processor`
2. Return `Measurement` with `AggregationType` flags
3. Register in `build_default_registry()`
4. Add route in `routes.toml`

That's it. Kafka, storage, and dashboard configuration is automatic.

```python
class MyProcessor(Processor):
    @property
    def name(self) -> str:
        return "my_processor"

    def run(self, data: bytes, settings: Mapping[str, Any]) -> ProcessorResult:
        return ProcessorResult(
            metrics={"score": Measurement(0.87, AggregationType.STATS)},
            artifacts={},
        )
```

## Performance & Security

**Performance Tuning** (`src/config/pipeline.toml`):
- Batch offset commits for reduced Kafka overhead
- Dedicated I/O thread pool for non-blocking file reads
- Parallel artifact uploads to MinIO

**Security Features**:
- Kafka TLS/SASL authentication support
- API rate limiting (token bucket)
- Input validation on all endpoints
- Content Security Policy headers

See `.claude/docs/security.md` for detailed configuration.

## Documentation

| Document | Description |
|----------|-------------|
| [ARCHITECTURE.md](ARCHITECTURE.md) | System design and data flow |
| [DEVELOPMENT.md](DEVELOPMENT.md) | Setup and development guide |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines |
| [CHANGELOG.md](CHANGELOG.md) | Version history |

## Services

| Service | Port | URL |
|---------|------|-----|
| Dashboard | 5007 | http://localhost:5007 |
| MinIO Console | 9001 | http://localhost:9001 |
| Grafana | 3000 | http://localhost:3000 |
| Prometheus | 9091 | http://localhost:9091 |
| Jaeger | 16686 | http://localhost:16686 |

## Project Status

Under active development. Core pipeline, analyzers, and dashboard implemented.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT
