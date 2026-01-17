from typing import Any
from unittest.mock import Mock

import pytest

from src.algorithms.registry import AlgorithmRegistry
from src.dispatch.dispatcher import Dispatcher, DispatchPlan
from src.dispatch.routes import LoadedRoute, build_dispatch_plans, load_routes_toml
from src.domain.jobs import Job


@pytest.fixture
def tomlfile() -> dict[str, Any]:
    return {
        "route": {
            "path0": {
                "algorithm": "algoA",
                "version": "1.0.0",
                "settings": "settings_path0.toml",
            },
            "path1": {
                "algorithm": "algoB",
                "version": "2.0.0",
                "settings": "settings_path1.toml",
            },
        },
    }

@pytest.fixture
def loaded_routes(tmp_path: pytest.TempPathFactory) -> dict[str, LoadedRoute]:
    routes_path = tmp_path / "routes.toml"
    settings_path0 = tmp_path / "settings_path0.toml"
    settings_path1 = tmp_path / "settings_path1.toml"
    routes_path.write_text(
        """[route.path0]
            algorithm = "algoA"
            version = "1.0.0"
            settings = "settings_path0.toml"

            [route.path1]
            algorithm = "algoB"
            version = "0.1.0"
            settings = "settings_path1.toml"
        """)

    settings_path0.write_text(
        """
            [algorithm]
            threshold = 0.4
            blur_kernel = 3
            use_canny = true
        """)

    settings_path1.write_text(
        """
            [model]
            artifact_path = "/models/model.onnx"
            device = "cuda:0"     # or "cpu"
            batch_size = 16

            [preprocessing]
            resize_width = 640
            resize_height = 640
            normalize = true

            [postprocessing]
            score_threshold = 0.5
        """)
    return load_routes_toml(routes_path)

@pytest.fixture
def job_unknown() -> Job:
    return Job(
        job_id="job-unknown",
        directory_key="unknown_path",
        path="/path/to/image.jpg",
        created_at_unix=1_700_000_000.0,
        fingerprint="fp-unknown",
    )


@pytest.fixture
def registry() -> AlgorithmRegistry:
    reg = AlgorithmRegistry()
    # For testing purposes, we can register dummy algorithms
    algo_a = Mock()
    algo_b = Mock()

    reg.register("algoA", "1.0.0", algo_a)
    reg.register("algoB", "0.1.0", algo_b)
    return reg

def test_load_routes_toml(loaded_routes: dict[str, LoadedRoute]) -> None:
    assert len(loaded_routes) == 2
    route0 = loaded_routes["path0"]
    assert route0.directory_key == "path0"
    assert route0.algorithm == "algoA"
    assert route0.version == "1.0.0"
    assert route0.settings_relpath == "settings_path0.toml"
    assert route0.settings["algorithm"]["threshold"] == 0.4

    route1 = loaded_routes["path1"]
    assert route1.directory_key == "path1"
    assert route1.algorithm == "algoB"
    assert route1.version == "0.1.0"
    assert route1.settings_relpath == "settings_path1.toml"
    assert route1.settings["model"]["artifact_path"] == "/models/model.onnx"

def test_load_routes_toml_missing_route_section(tmp_path: pytest.TempPathFactory) -> None:
    routes_path = tmp_path / "routes.toml"
    routes_path.write_text(
        """
        # No [route.*] sections
        """)
    with pytest.raises(ValueError, match="routes.toml must contain non-empty"):
        load_routes_toml(routes_path)

def test_load_routes_toml_missing_settings_file(tmp_path: pytest.TempPathFactory) -> None:
    routes_path = tmp_path / "routes.toml"
    routes_path.write_text(
        """[route.path0]
            algorithm = "algoA"
            version = "1.0.0"
            settings = "non_existent_settings.toml"
        """)
    with pytest.raises(ValueError, match="Settings not found for path0"):
        load_routes_toml(routes_path)

def test_build_dispatch_plans(
    loaded_routes: dict[str, LoadedRoute],
    registry: AlgorithmRegistry,
) -> None:

    registry.register("unknown", "1.0.0", lambda: Mock(name="AlgoA_Instance"))
    dispatch_plans = build_dispatch_plans(
        loaded_routes,
        registry=registry,
    )
    assert len(dispatch_plans) == 2
    plan0 = dispatch_plans["path0"]
    assert isinstance(plan0, DispatchPlan)
    assert plan0.algo is registry.create("algoA", "1.0.0")
    assert plan0.settings["algorithm"]["threshold"] == 0.4
    plan1 = dispatch_plans["path1"]
    assert isinstance(plan1, DispatchPlan)
    assert plan1.algo is registry.create("algoB", "0.1.0")
    assert plan1.settings["model"]["artifact_path"] == "/models/model.onnx"


def test_unknown_directory_key_dispatch_plans(
    loaded_routes: dict[str, LoadedRoute],
    registry: AlgorithmRegistry,
    job_unknown: Job,
) -> None:
    dispatch_plans = build_dispatch_plans(
        loaded_routes,
        registry=registry,
    )
    dispatcher = Dispatcher(dispatch_plans)
    with pytest.raises(KeyError):
        _ = dispatcher.dispatch(job_unknown)


def test_registry_duplicate_entry() -> None:
    reg = AlgorithmRegistry()
    algo_a = Mock()

    reg.register("algoA", "1.0.0", algo_a)
    with pytest.raises(ValueError, match="Algorithm already registered: algoA 1.0.0"):
        reg.register("algoA", "1.0.0", algo_a)


def test_create_unknown_algorithm(registry: AlgorithmRegistry) -> None:
    with pytest.raises(KeyError):
        registry.create("unknownAlgo", "0.1.0")
