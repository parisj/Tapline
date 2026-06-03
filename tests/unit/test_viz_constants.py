"""Unit tests for visualization constants and validation helpers."""

from __future__ import annotations

import re

from src.visualization import constants


class TestAllowedBuckets:
    def test_is_frozenset(self) -> None:
        assert isinstance(constants.ALLOWED_BUCKETS, frozenset)

    def test_contains_expected_buckets(self) -> None:
        assert (
            frozenset(
                {"artifacts", "inputs", "aggregates", "metric-values"},
            )
            == constants.ALLOWED_BUCKETS
        )

    def test_excludes_internal_buckets(self) -> None:
        assert "private" not in constants.ALLOWED_BUCKETS
        assert "system" not in constants.ALLOWED_BUCKETS
        assert "" not in constants.ALLOWED_BUCKETS


class TestNumericConstants:
    def test_rate_limit_values(self) -> None:
        assert constants.CLEANUP_CUTOFF_SECONDS == 300
        assert constants.RATE_LIMIT_TOKENS_PER_SEC == 10.0
        assert constants.RATE_LIMIT_BUCKET_CAPACITY == 50

    def test_rate_limit_types(self) -> None:
        assert isinstance(constants.RATE_LIMIT_TOKENS_PER_SEC, float)
        assert isinstance(constants.RATE_LIMIT_BUCKET_CAPACITY, int)
        assert isinstance(constants.CLEANUP_CUTOFF_SECONDS, int)

    def test_pagination_limits(self) -> None:
        assert constants.DEFAULT_LIST_LIMIT == 1000
        assert constants.MAX_TOPICS_DISPLAY == 20
        assert constants.DEFAULT_TASK_LIMIT == 50
        assert constants.MAX_TASK_LIMIT == 200
        assert constants.DEFAULT_ARTIFACT_LIMIT == 100
        assert constants.MAX_ARTIFACT_LIMIT == 1000

    def test_default_under_max(self) -> None:
        assert constants.DEFAULT_TASK_LIMIT <= constants.MAX_TASK_LIMIT
        assert constants.DEFAULT_ARTIFACT_LIMIT <= constants.MAX_ARTIFACT_LIMIT

    def test_minio_max_objects(self) -> None:
        assert constants.MINIO_MAX_OBJECTS == 10000

    def test_max_time_range_minutes_is_30_days(self) -> None:
        assert constants.MAX_TIME_RANGE_MINUTES == 30 * 24 * 60


class TestUrlConstants:
    def test_prometheus_url(self) -> None:
        assert constants.PROMETHEUS_URL == "http://localhost:9091"
        assert isinstance(constants.PROMETHEUS_URL, str)


class TestPatterns:
    def test_metric_name_pattern_is_compiled(self) -> None:
        assert isinstance(constants.METRIC_NAME_PATTERN, re.Pattern)

    def test_algorithm_name_pattern_is_compiled(self) -> None:
        assert isinstance(constants.ALGORITHM_NAME_PATTERN, re.Pattern)

    def test_version_pattern_is_compiled(self) -> None:
        assert isinstance(constants.VERSION_PATTERN, re.Pattern)

    def test_task_id_pattern_is_compiled(self) -> None:
        assert isinstance(constants.TASK_ID_PATTERN, re.Pattern)


class TestExtensionToMime:
    def test_known_extensions(self) -> None:
        assert constants.EXTENSION_TO_MIME["png"] == "image/png"
        assert constants.EXTENSION_TO_MIME["jpg"] == "image/jpeg"
        assert constants.EXTENSION_TO_MIME["jpeg"] == "image/jpeg"
        assert constants.EXTENSION_TO_MIME["gif"] == "image/gif"
        assert constants.EXTENSION_TO_MIME["json"] == "application/json"
        assert constants.EXTENSION_TO_MIME["npz"] == "application/x-npz"
        assert constants.EXTENSION_TO_MIME["csv"] == "text/csv"
        assert constants.EXTENSION_TO_MIME["txt"] == "text/plain"
        assert constants.EXTENSION_TO_MIME["html"] == "text/html"

    def test_is_dict(self) -> None:
        assert isinstance(constants.EXTENSION_TO_MIME, dict)


class TestValidateMetricName:
    def test_accepts_simple_name(self) -> None:
        assert constants.validate_metric_name("accuracy") is True

    def test_accepts_underscore(self) -> None:
        assert constants.validate_metric_name("my_metric_v2") is True

    def test_accepts_letters_and_digits(self) -> None:
        assert constants.validate_metric_name("metric123") is True

    def test_rejects_leading_digit(self) -> None:
        assert constants.validate_metric_name("1metric") is False

    def test_rejects_hyphen(self) -> None:
        assert constants.validate_metric_name("my-metric") is False

    def test_rejects_empty_string(self) -> None:
        assert constants.validate_metric_name("") is False

    def test_rejects_too_long(self) -> None:
        assert constants.validate_metric_name("a" * 65) is False

    def test_accepts_max_length(self) -> None:
        assert constants.validate_metric_name("a" + "b" * 63) is True

    def test_rejects_special_chars(self) -> None:
        assert constants.validate_metric_name("metric/path") is False
        assert constants.validate_metric_name("metric.name") is False


class TestValidateAlgorithmName:
    def test_accepts_simple_name(self) -> None:
        assert constants.validate_algorithm_name("analysis") is True

    def test_accepts_hyphen(self) -> None:
        assert constants.validate_algorithm_name("my-algo") is True

    def test_accepts_underscore(self) -> None:
        assert constants.validate_algorithm_name("my_algo") is True

    def test_rejects_leading_digit(self) -> None:
        assert constants.validate_algorithm_name("1algo") is False

    def test_rejects_empty_string(self) -> None:
        assert constants.validate_algorithm_name("") is False

    def test_rejects_period(self) -> None:
        assert constants.validate_algorithm_name("algo.v1") is False

    def test_rejects_too_long(self) -> None:
        assert constants.validate_algorithm_name("a" * 65) is False


class TestValidateVersion:
    def test_accepts_semver(self) -> None:
        assert constants.validate_version("1.0.0") is True

    def test_accepts_with_hyphen(self) -> None:
        assert constants.validate_version("1.0.0-rc1") is True

    def test_accepts_with_underscore(self) -> None:
        assert constants.validate_version("1_0_0") is True

    def test_accepts_alpha_start(self) -> None:
        assert constants.validate_version("v1") is True

    def test_rejects_empty(self) -> None:
        assert constants.validate_version("") is False

    def test_rejects_leading_special(self) -> None:
        assert constants.validate_version(".1.0") is False
        assert constants.validate_version("-1.0") is False

    def test_rejects_too_long(self) -> None:
        assert constants.validate_version("1" * 33) is False

    def test_rejects_slash(self) -> None:
        assert constants.validate_version("1/0") is False


class TestValidateTaskId:
    def test_accepts_uuid_like(self) -> None:
        assert constants.validate_task_id("abc-123_def") is True

    def test_accepts_single_char(self) -> None:
        assert constants.validate_task_id("a") is True

    def test_accepts_max_length(self) -> None:
        assert constants.validate_task_id("a" * 64) is True

    def test_rejects_empty(self) -> None:
        assert constants.validate_task_id("") is False

    def test_rejects_too_long(self) -> None:
        assert constants.validate_task_id("a" * 65) is False

    def test_rejects_path_traversal(self) -> None:
        assert constants.validate_task_id("../etc") is False
        assert constants.validate_task_id("a/b") is False
        assert constants.validate_task_id("a.b") is False

    def test_rejects_whitespace(self) -> None:
        assert constants.validate_task_id("a b") is False
        assert constants.validate_task_id("a\tb") is False


class TestGetMimeFromExtension:
    def test_png(self) -> None:
        assert constants.get_mime_from_extension("foo.png") == "image/png"

    def test_jpg(self) -> None:
        assert constants.get_mime_from_extension("foo.jpg") == "image/jpeg"

    def test_jpeg(self) -> None:
        assert constants.get_mime_from_extension("foo.jpeg") == "image/jpeg"

    def test_json(self) -> None:
        assert constants.get_mime_from_extension("data.json") == "application/json"

    def test_uppercase_extension(self) -> None:
        assert constants.get_mime_from_extension("FOO.PNG") == "image/png"

    def test_no_extension_returns_octet_stream(self) -> None:
        assert constants.get_mime_from_extension("README") == "application/octet-stream"

    def test_unknown_extension_returns_octet_stream(self) -> None:
        assert constants.get_mime_from_extension("foo.xyz") == "application/octet-stream"

    def test_multidot_filename(self) -> None:
        assert constants.get_mime_from_extension("archive.tar.gz") == "application/octet-stream"

    def test_just_dot_returns_octet_stream(self) -> None:
        # "." in name but no real extension. ext "" -> octet-stream.
        assert constants.get_mime_from_extension("foo.") == "application/octet-stream"

    def test_npz(self) -> None:
        assert constants.get_mime_from_extension("model.npz") == "application/x-npz"
