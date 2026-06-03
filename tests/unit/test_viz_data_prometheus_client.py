"""Unit tests for src.visualization.data.prometheus_client.

Tests for:
- URL scheme validation in __init__
- is_available with success/failure/cache
- query (instant) happy path + error branches
- query_range happy path + error branches
- Response parsing (success vs error, malformed values)
- Convenience helpers (get_jobs_total, get_jobs_failed, get_throughput,
  get_active_workers, get_metric_value, get_throughput_history)
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from urllib.error import URLError

import pytest

from src.visualization.data.prometheus_client import (
    MetricSample,
    PrometheusClient,
    RangeVector,
)

MODULE = "src.visualization.data.prometheus_client"


def _mock_urlopen_response(payload: dict | str | bytes, *, status: int = 200) -> MagicMock:
    """Build a context-manager mock matching urlopen's return value."""
    response = MagicMock()
    response.status = status
    if isinstance(payload, (dict, list)):
        response.read.return_value = json.dumps(payload).encode("utf-8")
    elif isinstance(payload, str):
        response.read.return_value = payload.encode("utf-8")
    else:
        response.read.return_value = payload

    cm = MagicMock()
    cm.__enter__.return_value = response
    cm.__exit__.return_value = False
    return cm


class TestMetricSample:
    """Tests for MetricSample dataclass."""

    def test_metric_sample_with_defaults(self) -> None:
        sample = MetricSample(
            name="tapline_jobs_completed_total",
            value=42.0,
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
        )

        assert sample.name == "tapline_jobs_completed_total"
        assert sample.value == 42.0
        assert sample.labels == {}

    def test_metric_sample_with_labels(self) -> None:
        sample = MetricSample(
            name="up",
            value=1.0,
            timestamp=datetime(2024, 1, 1),
            labels={"job": "tapline"},
        )

        assert sample.labels == {"job": "tapline"}


class TestRangeVector:
    """Tests for RangeVector dataclass."""

    def test_range_vector_construction(self) -> None:
        ts = datetime(2024, 1, 1)
        rv = RangeVector(
            metric_name="x",
            labels={"job": "y"},
            values=[(ts, 1.0), (ts, 2.0)],
        )

        assert rv.metric_name == "x"
        assert rv.labels == {"job": "y"}
        assert len(rv.values) == 2


class TestPrometheusClientInit:
    """Tests for PrometheusClient.__init__ and URL handling."""

    def test_default_base_url(self) -> None:
        client = PrometheusClient()

        assert client.base_url == "http://localhost:9091"
        assert client._available is None

    def test_https_url_accepted(self) -> None:
        client = PrometheusClient("https://prometheus.example.com")

        assert client.base_url == "https://prometheus.example.com"

    def test_strips_trailing_slash(self) -> None:
        client = PrometheusClient("http://localhost:9091/")

        assert client.base_url == "http://localhost:9091"

    def test_strips_multiple_trailing_slashes(self) -> None:
        # rstrip("/") strips all trailing slashes
        client = PrometheusClient("http://localhost:9091///")

        assert client.base_url == "http://localhost:9091"

    def test_rejects_file_scheme(self) -> None:
        with pytest.raises(ValueError, match="Invalid URL scheme"):
            PrometheusClient("file:///etc/passwd")

    def test_rejects_ftp_scheme(self) -> None:
        with pytest.raises(ValueError, match="Invalid URL scheme"):
            PrometheusClient("ftp://example.com")

    def test_rejects_empty_scheme(self) -> None:
        with pytest.raises(ValueError, match="Invalid URL scheme"):
            PrometheusClient("no-scheme-here")


class TestIsAvailable:
    """Tests for PrometheusClient.is_available."""

    @patch(f"{MODULE}.urlopen")
    def test_returns_true_when_status_200(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = _mock_urlopen_response("ok", status=200)

        client = PrometheusClient()
        assert client.is_available() is True
        assert client._available is True

    @patch(f"{MODULE}.urlopen")
    def test_returns_false_when_status_not_200(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = _mock_urlopen_response("nope", status=500)

        client = PrometheusClient()
        assert client.is_available() is False

    @patch(f"{MODULE}.urlopen")
    def test_returns_false_on_url_error(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = URLError("connection refused")

        client = PrometheusClient()
        assert client.is_available() is False
        assert client._available is False

    @patch(f"{MODULE}.urlopen")
    def test_returns_false_on_timeout(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = TimeoutError("slow")

        client = PrometheusClient()
        assert client.is_available() is False

    @patch(f"{MODULE}.urlopen")
    def test_caches_result(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = _mock_urlopen_response("ok", status=200)

        client = PrometheusClient()
        client.is_available()
        client.is_available()
        client.is_available()

        assert mock_urlopen.call_count == 1

    @patch(f"{MODULE}.urlopen")
    def test_caches_negative_result(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = URLError("down")

        client = PrometheusClient()
        client.is_available()
        client.is_available()

        assert mock_urlopen.call_count == 1

    @patch(f"{MODULE}.urlopen")
    def test_calls_healthy_endpoint(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.return_value = _mock_urlopen_response("ok", status=200)

        client = PrometheusClient("http://example.com:9090")
        client.is_available()

        called_url = mock_urlopen.call_args[0][0]
        assert called_url == "http://example.com:9090/-/healthy"


class TestQuery:
    """Tests for PrometheusClient.query (instant queries)."""

    def test_returns_empty_when_not_available(self) -> None:
        client = PrometheusClient()
        client._available = False

        assert client.query("up") == []

    @patch(f"{MODULE}.urlopen")
    def test_returns_samples_on_success(self, mock_urlopen: MagicMock) -> None:
        payload = {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "up", "job": "tapline"},
                        "value": [1700000000.0, "1"],
                    },
                ],
            },
        }
        # Two responses: healthcheck + query
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            _mock_urlopen_response(payload),
        ]

        client = PrometheusClient()
        samples = client.query("up")

        assert len(samples) == 1
        assert samples[0].name == "up"
        assert samples[0].value == 1.0
        assert samples[0].labels == {"job": "tapline"}

    @patch(f"{MODULE}.urlopen")
    def test_query_url_contains_encoded_promql(self, mock_urlopen: MagicMock) -> None:
        payload = {"status": "success", "data": {"result": []}}
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            _mock_urlopen_response(payload),
        ]

        client = PrometheusClient("http://prom:9090")
        client.query("sum(rate(x[1m]))")

        called_url = mock_urlopen.call_args_list[1][0][0]
        assert called_url.startswith("http://prom:9090/api/v1/query?")
        assert "query=sum%28rate%28x%5B1m%5D%29%29" in called_url

    @patch(f"{MODULE}.urlopen")
    def test_returns_empty_on_url_error(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            URLError("boom"),
        ]

        client = PrometheusClient()
        assert client.query("up") == []

    @patch(f"{MODULE}.urlopen")
    def test_returns_empty_on_timeout(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            TimeoutError("slow"),
        ]

        client = PrometheusClient()
        assert client.query("up") == []

    @patch(f"{MODULE}.urlopen")
    def test_returns_empty_on_invalid_json(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            _mock_urlopen_response("not json at all"),
        ]

        client = PrometheusClient()
        assert client.query("up") == []


class TestParseInstantResponse:
    """Tests for PrometheusClient._parse_instant_response."""

    def test_returns_empty_when_status_error(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {"status": "error", "error": "bad query"},
        )

        assert result == []

    def test_returns_empty_when_no_data(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response({"status": "success"})

        assert result == []

    def test_returns_empty_when_empty_result(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {"status": "success", "data": {"result": []}},
        )

        assert result == []

    def test_parses_single_sample(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "up", "job": "tapline"},
                            "value": [1700000000.0, "1.5"],
                        },
                    ],
                },
            }
        )

        assert len(result) == 1
        assert result[0].name == "up"
        assert result[0].value == 1.5
        assert result[0].labels == {"job": "tapline"}

    def test_defaults_name_to_unknown(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {"metric": {"job": "tapline"}, "value": [1700000000.0, "1"]},
                    ],
                },
            }
        )

        assert len(result) == 1
        assert result[0].name == "unknown"

    def test_skips_sample_with_invalid_value(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {"metric": {"__name__": "a"}, "value": [1700000000.0, "NaN-X"]},
                        {"metric": {"__name__": "b"}, "value": [1700000000.0, "2.0"]},
                    ],
                },
            }
        )

        assert len(result) == 1
        assert result[0].name == "b"

    def test_skips_sample_with_short_value_pair(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {"metric": {"__name__": "a"}, "value": [1700000000.0]},
                    ],
                },
            }
        )

        assert result == []

    def test_parses_multiple_samples(self) -> None:
        client = PrometheusClient()
        result = client._parse_instant_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {"metric": {"__name__": "a"}, "value": [1700000000.0, "1"]},
                        {"metric": {"__name__": "b"}, "value": [1700000000.0, "2"]},
                    ],
                },
            }
        )

        assert len(result) == 2
        assert {s.name for s in result} == {"a", "b"}


class TestQueryRange:
    """Tests for PrometheusClient.query_range."""

    def test_returns_empty_when_not_available(self) -> None:
        client = PrometheusClient()
        client._available = False

        result = client.query_range("up", datetime(2024, 1, 1), datetime(2024, 1, 2))
        assert result == []

    @patch(f"{MODULE}.urlopen")
    def test_returns_vectors_on_success(self, mock_urlopen: MagicMock) -> None:
        payload = {
            "status": "success",
            "data": {
                "result": [
                    {
                        "metric": {"__name__": "rate_x", "job": "tapline"},
                        "values": [
                            [1700000000.0, "1.0"],
                            [1700000060.0, "2.0"],
                        ],
                    },
                ],
            },
        }
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            _mock_urlopen_response(payload),
        ]

        client = PrometheusClient()
        vectors = client.query_range(
            "rate_x",
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            step="60s",
        )

        assert len(vectors) == 1
        assert vectors[0].metric_name == "rate_x"
        assert vectors[0].labels == {"job": "tapline"}
        assert len(vectors[0].values) == 2

    @patch(f"{MODULE}.urlopen")
    def test_url_encodes_range_params(self, mock_urlopen: MagicMock) -> None:
        payload = {"status": "success", "data": {"result": []}}
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            _mock_urlopen_response(payload),
        ]

        client = PrometheusClient("http://prom:9090")
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 1, 1, 0, 0)
        client.query_range("up", start, end, step="30s")

        called_url = mock_urlopen.call_args_list[1][0][0]
        assert called_url.startswith("http://prom:9090/api/v1/query_range?")
        assert "step=30s" in called_url
        assert f"start={start.timestamp()}" in called_url
        assert f"end={end.timestamp()}" in called_url

    @patch(f"{MODULE}.urlopen")
    def test_returns_empty_on_url_error(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            URLError("boom"),
        ]

        client = PrometheusClient()
        assert client.query_range("up", datetime(2024, 1, 1), datetime(2024, 1, 2)) == []

    @patch(f"{MODULE}.urlopen")
    def test_returns_empty_on_timeout(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            TimeoutError("slow"),
        ]

        client = PrometheusClient()
        assert client.query_range("up", datetime(2024, 1, 1), datetime(2024, 1, 2)) == []

    @patch(f"{MODULE}.urlopen")
    def test_returns_empty_on_bad_json(self, mock_urlopen: MagicMock) -> None:
        mock_urlopen.side_effect = [
            _mock_urlopen_response("ok", status=200),
            _mock_urlopen_response("not json"),
        ]

        client = PrometheusClient()
        assert client.query_range("up", datetime(2024, 1, 1), datetime(2024, 1, 2)) == []


class TestParseRangeResponse:
    """Tests for PrometheusClient._parse_range_response."""

    def test_returns_empty_on_error_status(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response(
            {"status": "error", "error": "bad"},
        )

        assert result == []

    def test_returns_empty_on_missing_data(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response({"status": "success"})

        assert result == []

    def test_parses_values(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "m1", "job": "x"},
                            "values": [
                                [1700000000.0, "1.0"],
                                [1700000060.0, "2.5"],
                            ],
                        },
                    ],
                },
            }
        )

        assert len(result) == 1
        assert result[0].metric_name == "m1"
        assert result[0].labels == {"job": "x"}
        assert len(result[0].values) == 2
        assert result[0].values[0][1] == 1.0
        assert result[0].values[1][1] == 2.5

    def test_defaults_name_to_unknown(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {"metric": {"job": "x"}, "values": [[1700000000.0, "1"]]},
                    ],
                },
            }
        )

        assert result[0].metric_name == "unknown"

    def test_skips_short_value_pair(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "m1"},
                            "values": [[1700000000.0]],  # too short
                        },
                    ],
                },
            }
        )

        # No valid values -> vector skipped
        assert result == []

    def test_skips_invalid_value(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "m1"},
                            "values": [
                                [1700000000.0, "not-a-number"],
                                [1700000060.0, "5.0"],
                            ],
                        },
                    ],
                },
            }
        )

        assert len(result) == 1
        assert len(result[0].values) == 1
        assert result[0].values[0][1] == 5.0

    def test_skips_vector_with_no_parsed_values(self) -> None:
        client = PrometheusClient()
        result = client._parse_range_response(
            {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "metric": {"__name__": "m1"},
                            "values": [[1700000000.0, "nope"]],
                        },
                        {
                            "metric": {"__name__": "m2"},
                            "values": [[1700000000.0, "1"]],
                        },
                    ],
                },
            }
        )

        assert len(result) == 1
        assert result[0].metric_name == "m2"


class TestConvenienceMethods:
    """Tests for high-level metric helpers."""

    @patch.object(PrometheusClient, "query")
    def test_get_jobs_total_returns_first_value(self, mock_query: MagicMock) -> None:
        mock_query.return_value = [
            MetricSample(name="x", value=42.0, timestamp=datetime.now()),
        ]
        client = PrometheusClient()

        assert client.get_jobs_total() == 42.0
        mock_query.assert_called_with("sum(tapline_jobs_completed_total)")

    @patch.object(PrometheusClient, "query")
    def test_get_jobs_total_returns_zero_when_empty(self, mock_query: MagicMock) -> None:
        mock_query.return_value = []
        client = PrometheusClient()

        assert client.get_jobs_total() == 0.0

    @patch.object(PrometheusClient, "query")
    def test_get_jobs_failed(self, mock_query: MagicMock) -> None:
        mock_query.return_value = [
            MetricSample(name="x", value=7.0, timestamp=datetime.now()),
        ]
        client = PrometheusClient()

        assert client.get_jobs_failed() == 7.0
        mock_query.assert_called_with("sum(tapline_jobs_failed_total)")

    @patch.object(PrometheusClient, "query")
    def test_get_jobs_failed_default(self, mock_query: MagicMock) -> None:
        mock_query.return_value = []
        client = PrometheusClient()

        assert client.get_jobs_failed() == 0.0

    @patch.object(PrometheusClient, "query")
    def test_get_throughput(self, mock_query: MagicMock) -> None:
        mock_query.return_value = [
            MetricSample(name="x", value=12.5, timestamp=datetime.now()),
        ]
        client = PrometheusClient()

        assert client.get_throughput() == 12.5
        mock_query.assert_called_with("sum(rate(tapline_jobs_completed_total[1m]))")

    @patch.object(PrometheusClient, "query")
    def test_get_throughput_default(self, mock_query: MagicMock) -> None:
        mock_query.return_value = []
        client = PrometheusClient()

        assert client.get_throughput() == 0.0

    @patch.object(PrometheusClient, "query")
    def test_get_active_workers(self, mock_query: MagicMock) -> None:
        mock_query.return_value = [
            MetricSample(name="x", value=4.0, timestamp=datetime.now()),
        ]
        client = PrometheusClient()

        assert client.get_active_workers() == 4.0
        mock_query.assert_called_with("tapline_workers_active")

    @patch.object(PrometheusClient, "query")
    def test_get_active_workers_default(self, mock_query: MagicMock) -> None:
        mock_query.return_value = []
        client = PrometheusClient()

        assert client.get_active_workers() == 0.0

    @patch.object(PrometheusClient, "query")
    def test_get_metric_value_returns_sample_value(self, mock_query: MagicMock) -> None:
        mock_query.return_value = [
            MetricSample(name="m", value=99.0, timestamp=datetime.now()),
        ]
        client = PrometheusClient()

        assert client.get_metric_value("my_metric") == 99.0

    @patch.object(PrometheusClient, "query")
    def test_get_metric_value_returns_default(self, mock_query: MagicMock) -> None:
        mock_query.return_value = []
        client = PrometheusClient()

        assert client.get_metric_value("m", default=3.14) == 3.14

    @patch.object(PrometheusClient, "query")
    def test_get_metric_value_default_is_zero(self, mock_query: MagicMock) -> None:
        mock_query.return_value = []
        client = PrometheusClient()

        assert client.get_metric_value("m") == 0.0


class TestGetThroughputHistory:
    """Tests for PrometheusClient.get_throughput_history."""

    @patch.object(PrometheusClient, "query_range")
    def test_returns_values_from_first_vector(self, mock_qr: MagicMock) -> None:
        ts = datetime(2024, 1, 1, 12, 0, 0)
        mock_qr.return_value = [
            RangeVector(
                metric_name="x",
                labels={},
                values=[(ts, 1.0), (ts + timedelta(seconds=60), 2.0)],
            ),
        ]
        client = PrometheusClient()
        result = client.get_throughput_history()

        assert len(result) == 2
        assert result[0][1] == 1.0
        assert result[1][1] == 2.0

    @patch.object(PrometheusClient, "query_range")
    def test_returns_empty_when_no_vectors(self, mock_qr: MagicMock) -> None:
        mock_qr.return_value = []

        client = PrometheusClient()
        assert client.get_throughput_history() == []

    @patch.object(PrometheusClient, "query_range")
    def test_passes_hours_to_query_range(self, mock_qr: MagicMock) -> None:
        mock_qr.return_value = []
        client = PrometheusClient()

        client.get_throughput_history(hours=2.0, step="5m")

        # Verify args: positional promql + start/end/step
        args, kwargs = mock_qr.call_args
        assert args[0] == "sum(rate(tapline_jobs_completed_total[1m]))"
        assert kwargs["step"] == "5m"
        delta = kwargs["end"] - kwargs["start"]
        # Approximately 2 hours
        assert abs(delta.total_seconds() - 7200) < 5

    @patch.object(PrometheusClient, "query_range")
    def test_default_hours_is_one(self, mock_qr: MagicMock) -> None:
        mock_qr.return_value = []
        client = PrometheusClient()

        client.get_throughput_history()
        _, kwargs = mock_qr.call_args
        delta = kwargs["end"] - kwargs["start"]
        assert abs(delta.total_seconds() - 3600) < 5
