# **VisioEval**

**VisioEval** is an open-source, configuration-driven evaluation pipeline for continuously ingesting data, executing algorithms, extracting metrics, and performing structured analysis over time.

It has a strong natural fit for **computer vision** and **machine learning**, but it is **not limited to CV/ML**. Any system that produces files, signals, or measurable outputs that must be evaluated **consistently, reproducibly, and over time** can use VisioEval as its evaluation backbone.

This repository intentionally focuses on **pipeline infrastructure and analysis primitives**. Domain-specific algorithms are expected to live in **separate repositories** or be contributed via pull requests. VisioEval provides the execution, aggregation, and analysis framework around those algorithms.


## **Why VisioEval Exists**

Evaluation systems often start small and grow until they become fragile:

- ingestion logic tightly coupled to algorithms
- aggregation mixed with execution
- ad-hoc scripts for metrics and plots
- no separation between raw data and interpretation
- difficult to scale across teams and machines

**VisioEval enforces separation of concerns**:

- **Algorithms compute metrics only** (per job)
- **Analyzers interpret metrics** (per time window via Flink)
- **Configuration routes data to algorithms**
- **Content-addressed storage separates artifacts from events**
- **Read-only consumers** (dashboards/clients) query via Kafka topics


## **What VisioEval Solves**

VisioEval provides a foundation for:

- continuous evaluation of incoming data
- deterministic and reproducible metric computation
- multi-worker parallel execution
- time-windowed aggregation and analysis via Flink
- content-addressed artifact storage for deduplication
- multiple consumers (local dashboards or hosted services)
- clean extension points for algorithms and analysis logic


## **Core Concepts**
<p align="center">
  <img
    src="https://github.com/user-attachments/assets/0f8d68a1-b047-46cd-969c-90db5706cac7"
    alt="VisioEval Architecture"
    height="900"
  />
</p>


### **Job**

A **job** represents processing of a single input item. Jobs exist to:

- enable parallel execution
- track lifecycle state (created -> started -> completed/failed)
- isolate failures
- support deterministic replay

Each algorithm instance is **created once per worker** and reused across jobs.


### **Algorithm**

An algorithm:

- receives raw input (for example image bytes)
- computes metrics
- does **not** aggregate
- does **not** persist results
- does **not** depend on global state

Algorithms are routed entirely through TOML configuration.

```python
class ExampleAlgo(Algorithm):
    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:
        ...
```

---

### **Metric**

A metric is a single value produced by an algorithm and wrapped in `MetricValue`.

```python
MetricValue(
    value=0.87,
    analysis=AnalysisKind.SUMMARY | AnalysisKind.DISTRIBUTION_1D,
    meta={"range": [0.0, 1.0]}
)
```

Each metric explicitly declares **how it should be analyzed**, not how it is visualized.


### **AnalysisKind**

`AnalysisKind` defines post-processing recipes applied during aggregation windows.

```python
class AnalysisKind(IntFlag):
    SUMMARY = auto()
    DISTRIBUTION_1D = auto()
    OUTLIERS_1D = auto()
    COUNTER = auto()
    RATE = auto()
    ELLIPSE_2D = auto()
    CONTOUR_2D = auto()
    INFO = auto()
```

Multiple analysis kinds can be combined per metric.


## **Algorithm Registry (Routing Without Hardcoding)**

VisioEval includes an **AlgorithmRegistry** so `routes.toml` can stay declarative and `app/main.py` does not need to hardcode which class to instantiate.

- Algorithms are registered as **(name, version) -> factory**
- The dispatcher resolves routes by reading `routes.toml` and asking the registry for the matching algorithm instance

```python
from collections.abc import Callable

from src.algorithms.base import Algorithm

AlgorithmFactory = Callable[[], Algorithm]

class AlgorithmRegistry:
    """Simple registry: (name, version) -> factory."""

    def __init__(self) -> None:
        self._factories: dict[tuple[str, str], AlgorithmFactory] = {}

    def register(self, name: str, version: str, factory: AlgorithmFactory) -> None:
        key = (name, version)
        if key in self._factories:
            raise ValueError(f"Algorithm already registered: {name} {version}")
        self._factories[key] = factory

    def create(self, name: str, version: str) -> Algorithm:
        key = (name, version)
        if key not in self._factories:
            raise KeyError(key)
        return self._factories[key]()
```

A default registry can be provided for the "built-in" algorithms that exist in this repo:

```python
def build_default_registry() -> AlgorithmRegistry:
    reg = AlgorithmRegistry()
    reg.register("analysis_probe", "1.0.0", AnalysisProbeAlgo)
    reg.register("model_inference", "0.1.0", ModelInferenceAlgo)
    return reg
```

This keeps the system extensible and makes algorithm selection a configuration concern.


## **Analyzer Pipeline (AnalysisKind -> Analyzer Implementation)**

Metrics declare **what** analysis they need via `AnalysisKind`. The Flink jobs apply aggregation by using analysis kind mappings.

This mapping lives in:

- `src/evaluation/pipeline.py`

```python
from collections.abc import Callable
from typing import Any

from src.domain.evaluation import AnalysisKind
from src.evaluation.analyzers import (
    ContourAnalyzer,
    CounterAnalyzer,
    CovEllipseAnalyzer,
    HistogramAnalyzer,
    RateAnalyzer,
    SummaryAnalyzer,
)
from src.evaluation.analyzers.base import Analyzer

AnalyzerFactory = Callable[[dict[str, Any]], Analyzer]

ANALYSIS_PIPELINE: dict[AnalysisKind, list[AnalyzerFactory]] = {
    AnalysisKind.SUMMARY: [
        lambda: SummaryAnalyzer(),
    ],
    AnalysisKind.DISTRIBUTION_1D: [
        lambda: HistogramAnalyzer(),
    ],
    AnalysisKind.COUNTER: [
        lambda: CounterAnalyzer(),
    ],
    AnalysisKind.RATE: [
        lambda: RateAnalyzer(),
    ],
    AnalysisKind.ELLIPSE_2D: [
        lambda: CovEllipseAnalyzer(),
    ],
    AnalysisKind.CONTOUR_2D: [
        lambda: ContourAnalyzer(),
    ],
}
```

This design is intentionally simple:

- Adding a new analysis kind is a **domain change**
- Implementing the analyzer is a **module change**
- Wiring it up is a **single-line change** in `ANALYSIS_PIPELINE`


## **Built-in Analyzers**

This repository implements the analysis logic. Algorithms only emit metrics -- analyzers interpret them over time.

Currently included analyzers:

### **Summary**
- count, missing, mean, median, min, max, std, variance
- **NPZ artifact** of raw numeric values

### **Rate**
- count, missing, yes/no, rate, confidence interval
- **NPZ artifact**

### **Distribution / Histogram**
- bins, edges, counts, mean, std
- **NPZ artifact**

### **Outliers (1D)**
- z-score based outlier detection (for example 3 sigma)
- **NPZ artifact**

### **Ellipse 2D**
- covariance ellipse parameters
- **NPZ artifact**

### **Contour 2D**
- 2D density grid for contour plotting
- **NPZ artifact**

Each analyzer returns:

- a **compact JSON summary** for fast filtering/grouping
- an optional **binary artifact** for later recomposition across larger time ranges


## **Artifacts (Binary, Time-Windowed, Merge-Friendly)**

Artifacts are stored in **MinIO** as content-addressed objects.

They are designed to:

- preserve numeric fidelity (no lossy JSON conversion)
- enable re-aggregation over larger time windows
- support flexible visualization and drill-down later
- keep the evaluation layer independent from the visualization layer

Artifacts are **time-windowed** and can be recombined by visualization/query layers to interpret data across custom ranges.

## **Pipeline Configuration**

VisioEval is fully configuration-driven using TOML files.

### **`src/config/pipeline.toml`**

Defines runtime behavior and directory monitoring.

```toml
[ingest]
queue_maxsize = 10000
poll_interval_sec = 0.1
allowed_image_exts = [".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"]

[readiness]
stable_window_sec = 0.05
max_wait_sec = 0.3

[workers]
max_workers = "auto"
auto_divisor = 2
min_workers = 2

[evaluation]
interval_sec = 10.0

[directories]
path0 = "/home/elliot/repos/VisioEval/paths_test/path0"
path1 = "/home/elliot/repos/VisioEval/paths_test/path1"
```

This file is the **single source of truth** for directory keys and their real filesystem paths.


### **`src/config/routes.toml`**

Defines **directory -> algorithm** routing, including the algorithm settings file.

```toml
[route.path0]
algorithm = "analysis_probe"
version = "1.0.0"
settings = "algorithms/algo_template.toml"

[route.path1]
algorithm = "model_inference"
version = "0.1.0"
settings = "algorithms/model_template.toml"
```

The `directory_key` must match the key in `[directories]`.


### **Algorithm Settings Files**

These TOML files are passed into algorithms as `settings`.

**`src/config/algorithms/algo_template.toml`**

```toml
[algorithm]
threshold = 0.4
blur_kernel = 3
use_canny = true
```

**`src/config/algorithms/model_template.toml`**

```toml
[model]
artifact_path = "/models/model.onnx"
device = "cuda:0"
batch_size = 16

[preprocessing]
resize_width = 640
resize_height = 640
normalize = true

[postprocessing]
score_threshold = 0.5
```


## **Reference Algorithm: `analysis_probe`**

VisioEval includes a reference algorithm that emits metrics across multiple `AnalysisKind` flags.

Its purpose:

- exercise the evaluator and analyzers
- provide deterministic outputs for tests
- validate routing and persistence

```python
class AnalysisProbeAlgo(Algorithm):
    @property
    def name(self) -> str:
        return "analysis_probe"

    @property
    def version(self) -> str:
        return "0.1.0"

    def run(self, image_bytes: bytes, settings: Mapping[str, Any]) -> AlgoResult:
        ...
```

This is a test/probe algorithm, not a production CV model.


## **Streaming Architecture**

VisioEval uses a Kafka/MinIO-based architecture for scalable event streaming:

- **Kafka** - Event backbone with topics for jobs, results, metrics, and audit logs
- **Python Aggregation** - Lightweight time-windowed aggregation (default) or Flink SQL (optional)
- **MinIO** - Content-addressed object storage for artifacts and aggregates
- **Prometheus/Grafana** - Metrics collection and dashboards
- **Jaeger** - Distributed tracing via OpenTelemetry

### Pipeline Commands

```bash
# Start infrastructure
docker-compose up -d

# Full pipeline (recommended) - spawns all 3 components as separate processes
pixi run pipeline

# Individual components (for development/debugging)
pixi run run              # Main pipeline only (ingest + workers)
pixi run run-flink-agg    # Python aggregation standalone
pixi run aggregate-sink   # Metric values collector

# Optional: Use Flink SQL instead of Python aggregation
VISIOEVAL_USE_FLINK_SQL=true pixi run pipeline

# Visualization
pixi run dashboard        # Flask dashboard at http://localhost:5007
```

### Pipeline Architecture

The full pipeline (`pixi run pipeline`) spawns three separate processes for better parallelism:

```
┌─────────────────────────────────────────────────────────────────┐
│                    pixi run pipeline                            │
│                  (pipeline_runner.py)                           │
│                                                                 │
│  ┌─────────────────┐  ┌─────────────────┐  ┌────────────────┐  │
│  │  Main Pipeline  │  │ Python Agg Job  │  │ Aggregate Sink │  │
│  │   (main.py)     │  │(flink_agg.py)   │  │                │  │
│  │                 │  │                 │  │                │  │
│  │ • Ingest files  │  │ • 1-min windows │  │ • Metric       │  │
│  │ • Kafka jobs    │  │ • Aggregates    │  │   values       │  │
│  │ • Worker pool   │  │   all metrics   │  │   collector    │  │
│  │ • Algorithm     │  │ • Preserves raw │  │                │  │
│  │   execution     │  │   values & mask │  │                │  │
│  └────────┬────────┘  └────────┬────────┘  └───────┬────────┘  │
│           │                    │                   │            │
└───────────┼────────────────────┼───────────────────┼────────────┘
            │                    │                   │
            ▼                    ▼                   ▼
     ┌──────────┐         ┌──────────────────────────────┐
     │  Kafka   │────────►│           MinIO              │
     │ Topics   │         │  (aggregates + metric-values)│
     └──────────┘         └──────────────────────────────┘
```

**Why Python aggregation (default)?**
- Preserves raw values for all metric types (numeric, boolean, 2D points)
- Supports all AnalysisKinds (ELLIPSE_2D, CONTOUR_2D, COUNTER, RATE, etc.)
- Writes directly to MinIO (simpler data flow)
- No Flink cluster dependency for basic aggregation

**Why separate processes?**
- Python's GIL limits true parallelism within a single process
- I/O-bound operations (Kafka, MinIO) benefit from process isolation
- Independent restart capability (components auto-restart if they crash)
- Better resource utilization across CPU cores

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092, 9093 | Event streaming (external, internal) |
| Schema Registry | 8085 | Avro schemas |
| Flink JobManager | 8081 | Stream processing UI (optional) |
| MinIO | 9000, 9001 | Object storage (API, Console) |
| Zookeeper | 2181 | Kafka coordination |
| Prometheus | 9091 | Metrics collection |
| Grafana | 3000 | Dashboards (admin/admin) |
| Jaeger | 16686 | Distributed tracing UI |
| VisioEval Dashboard | 5007 | Flask API + web dashboard |
| Pipeline Metrics | 8000 | Prometheus metrics endpoint |


## **Observability**

VisioEval includes a comprehensive observability stack:

### **Distributed Tracing (OpenTelemetry + Jaeger)**

- Automatic span creation for job processing, Kafka operations, MinIO storage
- Trace context propagation via Kafka headers
- Correlation IDs for end-to-end request tracking
- View traces at http://localhost:16686

### **Metrics (Prometheus + Grafana)**

Pipeline metrics exposed on port 8000:

| Metric | Type | Description |
|--------|------|-------------|
| `visioeval_pipeline_up` | Gauge | Pipeline running status |
| `visioeval_jobs_created_total` | Counter | Total jobs created |
| `visioeval_jobs_completed_total` | Counter | Total jobs completed |
| `visioeval_jobs_failed_total` | Counter | Total jobs failed |
| `visioeval_job_duration_seconds` | Histogram | Job processing duration |
| `visioeval_kafka_messages_produced_total` | Counter | Kafka messages produced |
| `visioeval_kafka_messages_consumed_total` | Counter | Kafka messages consumed |
| `visioeval_minio_store_duration_seconds` | Histogram | MinIO storage latency |

Access Prometheus at http://localhost:9091 and Grafana dashboards at http://localhost:3000.

### **Structured Logging**

- JSON-formatted logs with trace context injection
- Correlation ID propagation across services
- Configurable via `src/config/observability.toml`


## **Visualization Dashboard**

VisioEval includes a Flask-based REST API and web dashboard with a dark minimal theme.

### Features

- **KPI Cards** - Real-time metrics: jobs processed, throughput, errors, active sources
- **Metrics Explorer** - Browse and visualize aggregated metrics by algorithm/version
- **AnalysisKind Support** - Automatic artifact computation (histograms, ellipses, contours, rates)
- **Time Filtering** - Filter metrics by time range (last 10, 60, 120 minutes)
- **Prometheus Integration** - Live pipeline status and worker stats
- **Audit Chain Verification** - Hash chain integrity checking

### Running the Dashboard

```bash
pixi run dashboard        # Start at http://localhost:5007
```

### REST API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/metrics` | List available metrics with summaries |
| `GET /api/metrics/<name>/data` | Get metric detail with computed artifacts |
| `GET /api/prometheus/query` | Proxy Prometheus instant queries |
| `GET /api/pipeline/status` | Get pipeline worker status |
| `GET /api/audit/verify` | Verify audit chain integrity |

### Architecture

```
src/visualization/
├── api_server.py         # Flask API server
├── static/
│   └── dashboard.html    # Frontend HTML/JS dashboard
├── discovery.py          # Metric discovery from MinIO
├── data/
│   └── prometheus_client.py  # Prometheus HTTP API client
└── readers/
    └── minio_reader.py   # MinIO artifact reader
```

The dashboard reads aggregated metrics from MinIO and computes AnalysisKind-specific artifacts on-demand.


## **Metric Aggregation**

Metrics are aggregated in real-time using 1-minute tumbling windows. By default, VisioEval uses Python-based aggregation which:

- **Preserves raw values** - Including non-numeric types like `{x, y}` points for 2D analysis
- **Maintains analysis_mask** - So dashboard knows which AnalysisKinds to compute
- **Writes directly to MinIO** - No intermediate Kafka topic needed

### Supported Value Types

| Type | AnalysisKind | Example |
|------|--------------|---------|
| Numeric (int, float) | SUMMARY, DISTRIBUTION_1D, OUTLIERS_1D | `0.87` |
| Boolean | COUNTER, RATE | `True/False` (stored as 1/0) |
| 2D Point (dict) | ELLIPSE_2D, CONTOUR_2D | `{"x": 8.65, "y": 5.00}` |
| String | INFO | `"model_v2.onnx"` |

### On-Demand Artifact Computation

AnalysisKind-specific artifacts (histograms, ellipses, contours, rates) are computed **on-demand** by the dashboard API when raw values are available:

```python
# API computes based on analysis_mask
if analysis_mask & AnalysisKind.DISTRIBUTION_1D:
    artifacts["histogram"] = _compute_histogram(raw_values)
if analysis_mask & AnalysisKind.ELLIPSE_2D:
    artifacts["ellipse"] = _compute_ellipse(raw_values)
if analysis_mask & AnalysisKind.CONTOUR_2D:
    artifacts["contour"] = _compute_contour(raw_values)
```

**Topics:**
- Source: `visio.metrics` (METRIC_EMITTED events)
- Storage: MinIO `aggregates` bucket (with raw values)

**Commands:**
```bash
pixi run run-flink-agg    # Run Python aggregation standalone
pixi run pipeline         # Full pipeline (includes aggregation)
```

### Optional: Flink SQL Mode

For high-throughput scenarios, you can use Flink SQL (note: only supports numeric values):

```bash
VISIOEVAL_USE_FLINK_SQL=true pixi run pipeline
```

**Monitor at:** http://localhost:8081 (Flink UI)


## **Requirements**

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.12 | Pinned due to PyFlink/grpcio compatibility |
| NumPy | 1.24-2.1 | Constrained by apache-beam |
| Docker | 20+ | Infrastructure services |


## **Environment Variables**

Required in `.env`:

- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker address
- `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`
- `FLINK_JOBMANAGER` - Flink REST endpoint
- `LOG_LEVEL` (INFO), `LOG_FILE`


## **Logging**

All modules use a shared structured logging approach with JSON output and trace context injection. This provides visibility into:

- ingestion and readiness decisions
- job lifecycle transitions
- worker execution and failures
- evaluation windows and persistence operations

Logs include `trace_id`, `span_id`, and `correlation_id` for distributed tracing correlation.


## **Modularity**

VisioEval is designed for **zero-configuration extensibility**. Adding new algorithms or metrics requires no changes to storage, Kafka, or dashboard code.

### Adding a New Algorithm

1. Create a class extending `Algorithm` base class
2. Return `MetricValue` objects with appropriate `AnalysisKind` flags
3. Register in `build_default_registry()`
4. Add route in `routes.toml`

**That's it.** Everything else happens automatically:

| Layer | Auto-Configured |
|-------|-----------------|
| Kafka events | `METRIC_EMITTED` published with `analysis_mask` |
| Aggregation | Python aggregator handles all value types |
| MinIO storage | Content-addressed, no bucket config needed |
| Dashboard | Metrics auto-discovered from MinIO |
| Visualization | Artifacts computed based on `analysis_mask` |

### How It Works

```
Algorithm.run()
    ↓ returns MetricValue(value, analysis=AnalysisKind.ELLIPSE_2D | AnalysisKind.CONTOUR_2D)
    ↓
KafkaWorkerPool publishes METRIC_EMITTED with analysis_mask=96
    ↓
TumblingWindowAggregator stores to MinIO (preserves raw values + mask)
    ↓
Dashboard API reads analysis_mask, computes ellipse + contour on-demand
```

No hardcoded algorithm names, metric names, or storage paths anywhere in the pipeline.


## **Project Status**

VisioEval is under active development.

- core pipeline implemented
- analyzers implemented
- base unit tests exist (coverage enforced)
- Flask-based visualization dashboard with REST API
- three-process architecture for full pipeline
- Python-based aggregation (default) with Flink SQL optional

No performance claims are made at this stage.


## **Roadmap**

- ~~observability stack (Prometheus, Grafana, Jaeger)~~ done
- ~~distributed Flink aggregation~~ done (Python default, Flink SQL optional)
- ~~read-only query layer for dashboards~~ done
- ~~visualization module (Flask dashboard)~~ done
- ~~modular algorithm/metric discovery~~ done
- richer analysis types (quantiles/sketches, robust stats)
- operational tooling (migrations, admin helpers)


## **Contributing**

Contributions are welcome.

Good contribution areas:

- new analyzers
- robustness improvements
- schema and query improvements
- documentation and examples
- testing and coverage improvements

See `CONTRIBUTING.md` for guidelines.
