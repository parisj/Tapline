from __future__ import annotations

import json
from typing import Any, Callable
from unittest.mock import Mock

import pytest

from src.domain.evaluation import AnalysisKind
from src.evaluation import evaluator as evaluator_mod
from src.evaluation.evaluator import EvaluationModule


def _row(algo_name: str, algo_version: str, metrics_json: Any) -> dict[str, Any]:
    """
    Build a row shaped like PersistenceService.fetch_completed_results output.

    The evaluator supports `metrics_json` being either:
    - a dict (already decoded)
    - a JSON string (common from drivers)
    """
    return {"algo_name": algo_name, "algo_version": algo_version, "metrics_json": metrics_json}


def _analyzer_factory(
    *,
    summary: dict[str, Any] | None = None,
    artifact: Any | None = None,
    record: list[dict[str, Any]] | None = None,
) -> Callable[[], Any]:
    """
    Create an analyzer factory compatible with evaluator expectations.

    Analyzer.run returns:
      {"summary": <dict>, "artifact": <optional>}
    """
    class _Analyzer:
        def run(self, *, values: list[Any], meta: dict[str, Any]) -> dict[str, Any]:
            if record is not None:
                record.append({"values": list(values), "meta": dict(meta)})

            result: dict[str, Any] = {"summary": summary if summary is not None else {"n": len(values), "meta": dict(meta)}}
            if artifact is not None:
                result["artifact"] = artifact
            return result

    return lambda: _Analyzer()


@pytest.fixture()
def persistence() -> Mock:
    """
    Mocked PersistenceService: evaluator must never touch a real DB in unit tests.
    """
    p: Mock = Mock()
    p.fetch_completed_results.return_value = []
    p.persist_aggregate.return_value = 1
    return p


@pytest.fixture()
def sut(persistence: Mock) -> EvaluationModule:
    return EvaluationModule(persistence)


@pytest.fixture()
def pipeline(monkeypatch: pytest.MonkeyPatch) -> dict[AnalysisKind, list[Callable[[], Any]]]:
    """
    Patch the global ANALYSIS_PIPELINE so tests are deterministic and self-contained.
    """
    patched: dict[AnalysisKind, list[Callable[[], Any]]] = {}
    monkeypatch.setattr(evaluator_mod, "ANALYSIS_PIPELINE", patched)
    return patched


def test_calls_fetch_completed_results_with_window(sut: EvaluationModule, persistence: Mock) -> None:
    # Act
    sut.run_window(10.0, 20.0)

    # Assert
    persistence.fetch_completed_results.assert_called_once_with(10.0, 20.0)


def test_no_rows_no_persistence_calls(sut: EvaluationModule, persistence: Mock) -> None:
    # Arrange
    persistence.fetch_completed_results.return_value = []

    # Act
    sut.run_window(1.0, 2.0)

    # Assert
    persistence.persist_aggregate.assert_not_called()
    persistence.persist_aggregate_artifact.assert_not_called()


def test_metrics_json_as_dict_is_supported(sut: EvaluationModule, persistence: Mock, pipeline: dict[AnalysisKind, list[Callable[[], Any]]]) -> None:
    # Arrange
    kind: AnalysisKind = AnalysisKind.SUMMARY
    pipeline[kind] = [_analyzer_factory()]

    persistence.fetch_completed_results.return_value = [
        _row("algo", "v1", {"m": {"value": 1, "analysis_mask": int(kind)}}),
    ]

    # Act
    sut.run_window(100.0, 200.0)

    # Assert
    persistence.persist_aggregate.assert_called_once()
    assert persistence.persist_aggregate.call_args.kwargs["metric_name"] == "m"


def test_metrics_json_as_string_is_parsed(sut: EvaluationModule, persistence: Mock, pipeline: dict[AnalysisKind, list[Callable[[], Any]]]) -> None:
    # Arrange
    kind: AnalysisKind = AnalysisKind.SUMMARY
    pipeline[kind] = [_analyzer_factory()]

    doc: dict[str, Any] = {"m": {"value": 1, "analysis_mask": int(kind)}}
    persistence.fetch_completed_results.return_value = [
        _row("algo", "v1", json.dumps(doc)),
    ]

    # Act
    sut.run_window(100.0, 200.0)

    # Assert
    persistence.persist_aggregate.assert_called_once()
    assert persistence.persist_aggregate.call_args.kwargs["metric_name"] == "m"


@pytest.mark.parametrize(
    "metrics_json",
    [
        json.dumps(["not-a-dict"]),     # decoded -> list
        "not-json",                     # json.loads -> exception => test expects it to propagate? (it currently will)
    ],
)
def test_invalid_metrics_json_string_behavior(
    sut: EvaluationModule,
    persistence: Mock,
    metrics_json: str,
) -> None:
    """
    Production-level tests should lock down the intended behavior.

    Current implementation:
    - If metrics_json is a string and json.loads fails, it will raise.
    - If metrics_json decodes to a non-dict, it is skipped.

    If you want "bad JSON does not fail the window", change code and update this test.
    """
    persistence.fetch_completed_results.return_value = [_row("algo", "v1", metrics_json)]

    if metrics_json == "not-json":
        with pytest.raises(json.JSONDecodeError):
            sut.run_window(1.0, 2.0)
    else:
        sut.run_window(1.0, 2.0)
        persistence.persist_aggregate.assert_not_called()
        persistence.persist_aggregate_artifact.assert_not_called()


@pytest.mark.parametrize(
    "doc",
    [
        {"m": "not-a-dict"},               # payload not dict
        {"m": {"meta": {"x": 1}}},         # missing "value"
        {"m": {"value": 1, "meta": "x"}},  # meta not dict => sanitized to {}
    ],
)
def test_payload_validation_and_meta_sanitization(
    sut: EvaluationModule,
    persistence: Mock,
    pipeline: dict[AnalysisKind, list[Callable[[], Any]]],
    doc: dict[str, Any],
) -> None:
    kind: AnalysisKind = AnalysisKind.SUMMARY
    recorded: list[dict[str, Any]] = []
    pipeline[kind] = [_analyzer_factory(record=recorded)]

    # Ensure the evaluator would run if the payload is valid by forcing analysis_mask
    if isinstance(doc.get("m"), dict):
        doc["m"]["analysis_mask"] = int(kind)

    persistence.fetch_completed_results.return_value = [_row("algo", "v1", json.dumps(doc))]

    sut.run_window(1.0, 2.0)

    if doc.get("m") == "not-a-dict" or (isinstance(doc.get("m"), dict) and "value" not in doc["m"]):
        persistence.persist_aggregate.assert_not_called()
        assert recorded == []
        return

    # meta sanitization case
    persistence.persist_aggregate.assert_called_once()
    assert recorded[0]["values"] == [1]
    assert recorded[0]["meta"] == {}


def test_analysis_mask_gates_pipeline(sut: EvaluationModule, persistence: Mock, pipeline: dict[AnalysisKind, list[Callable[[], Any]]]) -> None:
    # Arrange
    kind: AnalysisKind = AnalysisKind.SUMMARY
    pipeline[kind] = [_analyzer_factory()]

    # Missing analysis_mask => defaults to 0 => pipeline should NOT run
    persistence.fetch_completed_results.return_value = [
        _row("algo", "v1", json.dumps({"m": {"value": 1}})),
    ]

    # Act
    sut.run_window(1.0, 2.0)

    # Assert
    persistence.persist_aggregate.assert_not_called()
    persistence.persist_aggregate_artifact.assert_not_called()


def test_values_are_grouped_by_algo_version_metric_and_meta_last_wins(
    sut: EvaluationModule,
    persistence: Mock,
    pipeline: dict[AnalysisKind, list[Callable[[], Any]]],
) -> None:
    # Arrange
    kind: AnalysisKind = AnalysisKind.SUMMARY
    recorded: list[dict[str, Any]] = []
    pipeline[kind] = [_analyzer_factory(record=recorded)]

    persistence.fetch_completed_results.return_value = [
        _row("algo", "v1", json.dumps({"m": {"value": 1, "meta": {"unit": "%"}, "analysis_mask": int(kind)}})),
        _row("algo", "v1", json.dumps({"m": {"value": 2, "meta": {"unit": "%", "note": "latest"}, "analysis_mask": int(kind)}})),
    ]

    # Act
    sut.run_window(10.0, 20.0)

    # Assert
    assert recorded == [{"values": [1, 2], "meta": {"unit": "%", "note": "latest"}}]
    persistence.persist_aggregate.assert_called_once()
    kw: dict[str, Any] = dict(persistence.persist_aggregate.call_args.kwargs)
    assert kw["algo_name"] == "algo"
    assert kw["algo_version"] == "v1"
    assert kw["metric_name"] == "m"
    assert kw["analysis_kind"] == kind.name


def test_two_metrics_in_one_row_are_processed_independently(
    sut: EvaluationModule,
    persistence: Mock,
    pipeline: dict[AnalysisKind, list[Callable[[], Any]]],
) -> None:
    # Arrange
    kind: AnalysisKind = AnalysisKind.SUMMARY
    pipeline[kind] = [_analyzer_factory()]

    persistence.fetch_completed_results.return_value = [
        _row(
            "algo",
            "v1",
            json.dumps(
                {
                    "m1": {"value": 1, "analysis_mask": int(kind)},
                    "m2": {"value": 2, "analysis_mask": int(kind)},
                }
            ),
        ),
    ]

    # Act
    sut.run_window(1.0, 2.0)

    # Assert: one persist per metric (since one analyzer factory)
    assert persistence.persist_aggregate.call_count == 2
    metric_names: set[str] = {c.kwargs["metric_name"] for c in persistence.persist_aggregate.call_args_list}
    assert metric_names == {"m1", "m2"}


def test_artifact_path_hashes_and_persists_artifact_and_links_hash(
    sut: EvaluationModule,
    persistence: Mock,
    pipeline: dict[AnalysisKind, list[Callable[[], Any]]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Arrange
    kind: AnalysisKind = AnalysisKind.SUMMARY

    artifact: Any = Mock()
    artifact.name = "plot.png"
    artifact.mime = "image/png"
    artifact.data = b"png"

    expected_hash: str = "hash-123"
    monkeypatch.setattr(evaluator_mod, "make_aggregate_artifact_hash", lambda **_kw: expected_hash)

    pipeline[kind] = [_analyzer_factory(artifact=artifact)]

    persistence.fetch_completed_results.return_value = [
        _row("algo", "v1", json.dumps({"m": {"value": 1, "analysis_mask": int(kind)}})),
    ]

    # Act
    sut.run_window(100.0, 200.0)

    # Assert: artifact persisted
    persistence.persist_aggregate_artifact.assert_called_once()
    art_kw: dict[str, Any] = dict(persistence.persist_aggregate_artifact.call_args.kwargs)
    assert art_kw["artifact_hash"] == expected_hash
    assert art_kw["artifact"] is artifact

    # Assert: aggregate persisted with same hash
    persistence.persist_aggregate.assert_called_once()
    agg_kw: dict[str, Any] = dict(persistence.persist_aggregate.call_args.kwargs)
    assert agg_kw["artifact_hash"] == expected_hash


def test_multiple_kinds_run_when_mask_includes_multiple_flags(
    sut: EvaluationModule,
    persistence: Mock,
    pipeline: dict[AnalysisKind, list[Callable[[], Any]]],
) -> None:
    # Arrange
    kinds: list[AnalysisKind] = list(AnalysisKind)
    if len(kinds) < 2:
        pytest.skip("Need at least two AnalysisKind flags to test multi-kind masks.")

    kind_a: AnalysisKind = kinds[0]
    kind_b: AnalysisKind = kinds[1]

    pipeline[kind_a] = [_analyzer_factory(summary={"kind": kind_a.name})]
    pipeline[kind_b] = [_analyzer_factory(summary={"kind": kind_b.name})]

    combined_mask: int = int(kind_a) | int(kind_b)

    persistence.fetch_completed_results.return_value = [
        _row("algo", "v1", json.dumps({"m": {"value": 1, "analysis_mask": combined_mask}})),
    ]

    # Act
    sut.run_window(1.0, 2.0)

    # Assert: one persist per kind (one analyzer per kind)
    assert persistence.persist_aggregate.call_count == 2
    kinds_persisted: set[str] = {c.kwargs["analysis_kind"] for c in persistence.persist_aggregate.call_args_list}
    assert kinds_persisted == {kind_a.name, kind_b.name}
