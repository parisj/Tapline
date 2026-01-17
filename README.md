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
- **Analyzers interpret metrics** (per time window)
- **Configuration routes data to algorithms**
- **Persistence separates summaries from numeric artifacts**
- **Read-only consumers** (dashboards/clients) query the DB


## **What VisioEval Solves**

VisioEval provides a foundation for:

- continuous evaluation of incoming data
- deterministic and reproducible metric computation
- multi-worker parallel execution
- time-windowed aggregation and analysis
- lossless numeric artifact storage for later recomposition
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
- track lifecycle state (created → started → completed/failed)  
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

- Algorithms are registered as **(name, version) → factory**
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

A default registry can be provided for the “built-in” algorithms that exist in this repo:

```python
def build_default_registry() -> AlgorithmRegistry:
    reg = AlgorithmRegistry()
    reg.register("analysis_probe", "1.0.0", AnalysisProbeAlgo)
    reg.register("model_inference", "0.1.0", ModelInferenceAlgo)
    return reg
```

This keeps the system extensible and makes algorithm selection a configuration concern.


## **Analyzer Pipeline (AnalysisKind → Analyzer Implementation)**

Metrics declare **what** analysis they need via `AnalysisKind`. The evaluator applies that by using a central mapping from analysis kind to analyzers.

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

This repository implements the analysis logic. Algorithms only emit metrics — analyzers interpret them over time.

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
- z-score based outlier detection (for example 3σ)  
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

Artifacts are stored as **binary blobs (`BYTEA`) in PostgreSQL**.

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

Defines **directory → algorithm** routing, including the algorithm settings file.

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


## **Pipeline Modes**

VisioEval supports two operational modes:

### **Legacy Mode** (`PIPELINE_MODE=legacy`)

PostgreSQL-based persistence. The database stores:

- job lifecycle state
- raw algorithm results (`metrics_json` as **JSONB**)
- aggregation summaries (**JSONB**)
- aggregation artifacts (**BYTEA**)

This split keeps queries fast while preserving numeric fidelity for later time-range recomposition.

### **Streaming Mode** (`PIPELINE_MODE=streaming`)

Kafka/Flink/MinIO-based architecture for scalable event streaming:

- **Kafka** - Event backbone with topics for jobs, results, metrics, and audit logs
- **PyFlink** - Stateful stream processing with time-windowed aggregation
- **MinIO** - Content-addressed object storage for artifacts

```bash
# Start infrastructure
docker-compose up -d

# Run in streaming mode
pixi run run-streaming
```

| Service | Port | Purpose |
|---------|------|---------|
| Kafka | 9092 | Event streaming |
| Schema Registry | 8085 | Avro schemas |
| Flink JobManager | 8081 | Stream processing |
| MinIO | 9000, 9001 | Object storage (API, Console) |
| Zookeeper | 2181 | Kafka coordination |


## **Requirements**

| Component | Version | Notes |
|-----------|---------|-------|
| Python | 3.12 | Pinned due to PyFlink/grpcio compatibility |
| NumPy | 1.24-2.1 | Constrained by apache-beam |
| PostgreSQL | 14+ | Legacy mode only |
| Docker | 20+ | Streaming mode infrastructure |


## **Logging and Observability**

All modules use a shared logging approach. This provides visibility into:

- ingestion and readiness decisions
- job lifecycle transitions
- worker execution and failures
- evaluation windows and persistence operations


## **Project Status**

VisioEval is under active development.

- core pipeline implemented
- analyzers implemented
- base unit tests exist (coverage enforced)
- visualization layer not implemented yet (planned)

No performance claims are made at this stage.


## **Roadmap**

- read-only query layer for dashboards
- visualization module (Bokeh or equivalent)
- richer analysis types (quantiles/sketches, robust stats)
- operational tooling (migrations, admin helpers, monitoring)


## **Contributing**

Contributions are welcome.

Good contribution areas:

- new analyzers
- robustness improvements
- schema and query improvements
- documentation and examples
- testing and coverage improvements

See `CONTRIBUTING.md` for guidelines.
