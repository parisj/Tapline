"""Unit tests for the artifacts route blueprint."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from flask import Flask
from minio.error import S3Error

from src.visualization.routes import artifacts as artifacts_module
from src.visualization.routes.artifacts import (
    _process_artifact_content,
    artifacts_bp,
    init_blueprint,
)


@pytest.fixture
def app():
    """Build a fresh Flask app with the artifacts blueprint registered."""
    app = Flask(__name__)
    app.config["TESTING"] = True
    if "artifacts" not in app.blueprints:
        app.register_blueprint(artifacts_bp)
    yield app


@pytest.fixture
def client(app):
    with app.test_client() as c:
        yield c


@pytest.fixture(autouse=True)
def _reset_blueprint_state():
    # Reset module-level globals between tests
    init_blueprint(None, None, None)
    yield
    init_blueprint(None, None, None)


class TestGetArtifactNotInitialized:
    def test_returns_503_when_storage_none(self, client) -> None:
        resp = client.get("/api/artifact/artifacts/ab/cd/hash")
        assert resp.status_code == 503
        assert "error" in resp.get_json()


class TestGetArtifactValidation:
    def test_invalid_bucket_returns_400(self, client) -> None:
        init_blueprint(MagicMock(), MagicMock(), MagicMock())
        resp = client.get("/api/artifact/forbidden/key123")
        assert resp.status_code == 400
        assert resp.get_json()["error"] == "Invalid bucket"

    def test_path_traversal_returns_400(self, client) -> None:
        init_blueprint(MagicMock(), MagicMock(), MagicMock())
        # Use a key that Flask wouldn't normalize away
        resp = client.get("/api/artifact/artifacts/foo/..bar")
        # The double-dot check is `".." in key`, so this triggers
        assert resp.status_code == 400


class TestGetArtifactSuccess:
    def test_returns_json_content(self, client) -> None:
        storage = MagicMock()
        storage.get_object_info.return_value = {
            "content_type": "application/json",
            "metadata": {"x-amz-meta-name": "f.json"},
        }
        storage.retrieve_by_key.return_value = b'{"a": 1}'
        init_blueprint(storage, MagicMock(), MagicMock())

        resp = client.get("/api/artifact/artifacts/ab/cd/hash")
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["bucket"] == "artifacts"
        assert data["content"]["data"] == {"a": 1}
        assert data["content"]["type"] == "json"

    def test_returns_image_as_base64(self, client) -> None:
        storage = MagicMock()
        storage.get_object_info.return_value = {
            "content_type": "image/png",
            "metadata": {},
        }
        storage.retrieve_by_key.return_value = b"\x89PNG\r\n"
        init_blueprint(storage, MagicMock(), MagicMock())

        resp = client.get("/api/artifact/artifacts/ab/cd/hash")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["content"]["type"] == "image"
        assert body["content"]["mime"] == "image/png"
        assert "data_base64" in body["content"]

    def test_returns_binary_type_for_unknown(self, client) -> None:
        storage = MagicMock()
        storage.get_object_info.return_value = {
            "content_type": "application/octet-stream",
            "metadata": {},
        }
        storage.retrieve_by_key.return_value = b"\x00\x01\x02"
        init_blueprint(storage, MagicMock(), MagicMock())

        resp = client.get("/api/artifact/artifacts/ab/cd/hash")
        assert resp.status_code == 200
        assert resp.get_json()["content"]["type"] == "binary"

    def test_returns_404_when_data_missing(self, client) -> None:
        storage = MagicMock()
        storage.get_object_info.return_value = None
        storage.retrieve_by_key.return_value = None
        init_blueprint(storage, MagicMock(), MagicMock())

        resp = client.get("/api/artifact/artifacts/ab/cd/hash")
        assert resp.status_code == 404

    def test_raw_response_returns_binary_with_content_type(self, client) -> None:
        storage = MagicMock()
        storage.get_object_info.return_value = {
            "content_type": "image/png",
            "metadata": {},
        }
        storage.retrieve_by_key.return_value = b"\x89PNGdata"
        init_blueprint(storage, MagicMock(), MagicMock())

        resp = client.get("/api/artifact/artifacts/ab/cd/hash?raw=true")
        assert resp.status_code == 200
        assert resp.content_type == "image/png"
        assert resp.data == b"\x89PNGdata"

    def test_returns_500_when_storage_raises(self, client) -> None:
        storage = MagicMock()
        storage.get_object_info.side_effect = RuntimeError("boom")
        storage.retrieve_by_key.side_effect = RuntimeError("boom")
        init_blueprint(storage, MagicMock(), MagicMock())

        resp = client.get("/api/artifact/artifacts/ab/cd/hash")
        assert resp.status_code == 500


class TestListTasks:
    def test_returns_503_when_discovery_none(self, client) -> None:
        resp = client.get("/api/tasks")
        assert resp.status_code == 503

    def test_returns_task_list(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = [
            {"task_id": "t1", "processor_name": "p"},
        ]
        init_blueprint(None, discovery, None)

        resp = client.get("/api/tasks")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["count"] == 1
        assert body["tasks"][0]["task_id"] == "t1"

    def test_passes_processor_filter(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = []
        init_blueprint(None, discovery, None)

        client.get("/api/tasks?processor=my_proc")
        discovery.list_recent_tasks.assert_called_once()
        kwargs = discovery.list_recent_tasks.call_args.kwargs
        assert kwargs["processor_name"] == "my_proc"

    def test_clamps_limit_to_max(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = []
        init_blueprint(None, discovery, None)

        client.get("/api/tasks?limit=99999")
        kwargs = discovery.list_recent_tasks.call_args.kwargs
        assert kwargs["limit"] == 200  # MAX_TASK_LIMIT

    def test_invalid_limit_falls_back_to_default(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = []
        init_blueprint(None, discovery, None)

        client.get("/api/tasks?limit=not_a_number")
        kwargs = discovery.list_recent_tasks.call_args.kwargs
        assert kwargs["limit"] == 50  # DEFAULT_TASK_LIMIT

    def test_negative_limit_falls_back_to_default(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = []
        init_blueprint(None, discovery, None)

        client.get("/api/tasks?limit=-5")
        kwargs = discovery.list_recent_tasks.call_args.kwargs
        assert kwargs["limit"] == 50

    def test_negative_offset_clamped_to_zero(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = []
        init_blueprint(None, discovery, None)

        client.get("/api/tasks?offset=-10")
        kwargs = discovery.list_recent_tasks.call_args.kwargs
        assert kwargs["offset"] == 0

    def test_handles_discovery_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)

        resp = client.get("/api/tasks")
        assert resp.status_code == 500


class TestListTaskArtifacts:
    def test_returns_503_when_discovery_none(self, client) -> None:
        resp = client.get("/api/tasks/task-id/artifacts")
        assert resp.status_code == 503

    def test_validates_task_id_format(self, client) -> None:
        init_blueprint(None, MagicMock(), None)
        # Slash inside path traversal in task id - the URL routing will likely
        # 404; use an invalid char like "."
        resp = client.get("/api/tasks/bad.task/artifacts")
        assert resp.status_code == 400

    def test_returns_artifacts(self, client) -> None:
        discovery = MagicMock()
        discovery.list_task_artifacts.return_value = [{"name": "a.json"}]
        init_blueprint(None, discovery, None)

        resp = client.get("/api/tasks/valid-task_id/artifacts")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["count"] == 1
        assert body["task_id"] == "valid-task_id"

    def test_handles_discovery_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.list_task_artifacts.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)

        resp = client.get("/api/tasks/valid_id/artifacts")
        assert resp.status_code == 500


class TestViewArtifact:
    def test_returns_503_when_services_none(self, client) -> None:
        resp = client.get("/api/artifacts/view/artifacts/key")
        assert resp.status_code == 503

    def test_invalid_bucket_returns_400(self, client) -> None:
        init_blueprint(MagicMock(), None, MagicMock())
        resp = client.get("/api/artifacts/view/badbucket/key")
        assert resp.status_code == 400

    def test_path_traversal_returns_400(self, client) -> None:
        init_blueprint(MagicMock(), None, MagicMock())
        resp = client.get("/api/artifacts/view/artifacts/foo/..bar")
        assert resp.status_code == 400

    def test_returns_404_when_data_missing(self, client) -> None:
        storage = MagicMock()
        storage.retrieve_by_key.return_value = None
        storage._client.stat_object.side_effect = S3Error(
            "NoSuchKey",
            "msg",
            "res",
            "req",
            "host",
            MagicMock(),
        )
        viewer_registry = MagicMock()
        init_blueprint(storage, None, viewer_registry)

        resp = client.get("/api/artifacts/view/artifacts/missing/key")
        assert resp.status_code == 404

    def test_renders_through_viewer_registry(self, client) -> None:
        storage = MagicMock()
        storage.retrieve_by_key.return_value = b'{"x":1}'
        stat = MagicMock()
        stat.metadata = {"x-amz-meta-name": "foo.json", "x-amz-meta-task_id": "t1"}
        stat.content_type = "application/json"
        storage._client.stat_object.return_value = stat

        viewer_registry = MagicMock()
        view_result = MagicMock()
        view_result.viewer_type = "json"
        view_result.render_type = "json"
        view_result.data = {"x": 1}
        view_result.metadata = {"name": "foo.json"}
        view_result.error = None
        viewer_registry.view.return_value = view_result

        init_blueprint(storage, None, viewer_registry)

        resp = client.get("/api/artifacts/view/artifacts/some/key.json")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["viewer_type"] == "json"
        assert body["render_type"] == "json"

    def test_invalid_aggregation_mask_defaults_to_zero(self, client) -> None:
        storage = MagicMock()
        storage.retrieve_by_key.return_value = b"{}"
        stat = MagicMock()
        stat.metadata = {}
        stat.content_type = "application/json"
        storage._client.stat_object.return_value = stat

        viewer_registry = MagicMock()
        view_result = MagicMock()
        view_result.viewer_type = "json"
        view_result.render_type = "json"
        view_result.data = {}
        view_result.metadata = {}
        view_result.error = None
        viewer_registry.view.return_value = view_result

        init_blueprint(storage, None, viewer_registry)
        resp = client.get("/api/artifacts/view/artifacts/key?aggregation_mask=not_int")
        assert resp.status_code == 200
        call = viewer_registry.view.call_args
        assert call.kwargs["aggregation_mask"] == 0


class TestListProcessors:
    def test_returns_503_when_discovery_none(self, client) -> None:
        resp = client.get("/api/processors")
        assert resp.status_code == 503

    def test_collects_unique_processors(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.return_value = [
            {"processor_name": "p1", "processor_version": "1"},
            {"processor_name": "p1", "processor_version": "1"},
            {"processor_name": "p2", "processor_version": "1"},
        ]
        init_blueprint(None, discovery, None)

        resp = client.get("/api/processors")
        assert resp.status_code == 200
        body = resp.get_json()
        assert body["count"] == 2
        # p1 should have task_count == 2
        p1 = next(p for p in body["processors"] if p["name"] == "p1")
        assert p1["task_count"] == 2

    def test_handles_discovery_exception(self, client) -> None:
        discovery = MagicMock()
        discovery.list_recent_tasks.side_effect = RuntimeError("boom")
        init_blueprint(None, discovery, None)

        resp = client.get("/api/processors")
        assert resp.status_code == 500


class TestProcessArtifactContent:
    def test_image_content(self) -> None:
        result = _process_artifact_content(b"\x89PNG\r\n", "image/png")
        assert result["type"] == "image"
        assert result["mime"] == "image/png"
        assert "data_base64" in result

    def test_json_content(self) -> None:
        result = _process_artifact_content(b'{"a":1}', "application/json")
        assert result["type"] == "json"
        assert result["data"] == {"a": 1}

    def test_json_inferred_from_data(self) -> None:
        # content_type is None but data starts with { -> tries JSON
        result = _process_artifact_content(b'{"a":1}', None)
        assert result["type"] == "json"

    def test_malformed_json_falls_back_to_binary(self) -> None:
        result = _process_artifact_content(b"{not-json", "application/json")
        assert result["type"] == "binary"

    def test_binary_content(self) -> None:
        result = _process_artifact_content(b"\x00\x01\x02", "application/octet-stream")
        assert result["type"] == "binary"
        assert result["size"] == 3
