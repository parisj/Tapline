# Development Setup

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.12 | Runtime (pinned for PyFlink compatibility) |
| Pixi | Latest | Package management |
| Docker | 20+ | Infrastructure services |
| Git | 2.0+ | Version control |

## Quick Start

```bash
# Clone the repository
git clone https://github.com/yourusername/Tapline.git
cd Tapline

# Install dependencies
pixi install

# Copy environment file
cp .env.example .env
# Edit .env with your values

# Start infrastructure
docker-compose up -d

# Install pre-commit hooks
pixi run precommit-install

# Run tests to verify setup
pixi run test
```

## Environment Configuration

Copy `.env.example` to `.env` and configure:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=your-access-key
MINIO_SECRET_KEY=your-secret-key
LOG_LEVEL=INFO
```

## Running the Pipeline

```bash
# Full pipeline (recommended)
pixi run pipeline

# Individual components
pixi run run              # Main pipeline
pixi run run-flink-agg    # Aggregation
pixi run aggregate-sink   # Metric collector

# Dashboard
pixi run dashboard        # http://localhost:5007
```

## Development Workflow

### Before Coding

1. Pull latest changes: `git pull`
2. Create feature branch: `git checkout -b feature/my-feature`
3. Ensure tests pass: `pixi run test`

### While Coding

1. Run linter frequently: `pixi run lint`
2. Format code: `pixi run format`
3. Check types: `pixi run typecheck`

### Before Committing

```bash
# Run all checks
pixi run precommit-run

# Or individually
pixi run lint
pixi run typecheck
pixi run test
```

## Common Tasks

### Add a New Processor

See [extending guide](.claude/docs/extending.md).

### Add a New Test

```bash
# Run specific test file
pixi run pytest tests/unit/test_my_module.py -v

# Run with coverage
pixi run test
```

### Debug a Failing Test

```bash
# Run with verbose output
pixi run pytest tests/unit/test_my_module.py -v -s

# Run single test
pixi run pytest tests/unit/test_my_module.py::test_specific_function -v
```

### Check Security

```bash
pixi run security-audit  # Dependency vulnerabilities
pixi run bandit-check    # Code analysis
```

## Infrastructure Services

### Start/Stop

```bash
docker-compose up -d      # Start all
docker-compose down       # Stop all
docker-compose restart    # Restart all
```

### View Logs

```bash
docker-compose logs -f kafka
docker-compose logs -f minio
```

### Service URLs

| Service | URL |
|---------|-----|
| MinIO Console | http://localhost:9001 |
| Flink UI | http://localhost:8081 |
| Grafana | http://localhost:3000 |
| Prometheus | http://localhost:9091 |
| Jaeger | http://localhost:16686 |
| Dashboard | http://localhost:5007 |

## IDE Configuration

### VSCode

Settings are in `.vscode/settings.json`:
- Python interpreter: pixi environment
- Testing: pytest
- Formatting: ruff

### PyCharm

1. Set interpreter to pixi environment
2. Configure pytest as test runner
3. Enable ruff external tool

## Troubleshooting

### Pixi Issues

```bash
# Clear cache and reinstall
pixi clean
pixi install
```

### Docker Issues

```bash
# Reset containers
docker-compose down -v
docker-compose up -d
```

### Test Failures

```bash
# Check infrastructure is running
docker-compose ps

# Run with debug output
pixi run pytest -v -s --tb=long
```

### Import Errors

Ensure you're in the pixi environment:

```bash
pixi shell
python -c "import src.app.main"
```
