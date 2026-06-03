"""Unit tests for src.flink.jobs.metric_aggregation_job.

PyFlink is an optional dependency. To unit-test the pure-Python logic
(MetricParser, MetricWindowAggregator, get_key, create_job, main) without
requiring apache-flink, stub the pyflink modules in sys.modules BEFORE
importing the module under test.
"""

from __future__ import annotations

import json
import sys
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


def _make_pyflink_stubs() -> dict[str, types.ModuleType]:
    pyflink = types.ModuleType("pyflink")
    pyflink_common = types.ModuleType("pyflink.common")
    pyflink_common.WatermarkStrategy = MagicMock(name="WatermarkStrategy")

    pyflink_common_serialization = types.ModuleType("pyflink.common.serialization")
    pyflink_common_serialization.SimpleStringSchema = MagicMock(name="SimpleStringSchema")

    pyflink_datastream = types.ModuleType("pyflink.datastream")

    class _RuntimeExecutionMode:
        STREAMING = "STREAMING"

    pyflink_datastream.RuntimeExecutionMode = _RuntimeExecutionMode
    pyflink_datastream.StreamExecutionEnvironment = MagicMock(name="StreamExecutionEnvironment")

    pyflink_datastream_connectors = types.ModuleType("pyflink.datastream.connectors")
    pyflink_datastream_connectors_kafka = types.ModuleType("pyflink.datastream.connectors.kafka")
    pyflink_datastream_connectors_kafka.DeliveryGuarantee = MagicMock(name="DeliveryGuarantee")
    pyflink_datastream_connectors_kafka.KafkaOffsetsInitializer = MagicMock(name="KafkaOffsetsInitializer")
    pyflink_datastream_connectors_kafka.KafkaRecordSerializationSchema = MagicMock(
        name="KafkaRecordSerializationSchema"
    )
    pyflink_datastream_connectors_kafka.KafkaSink = MagicMock(name="KafkaSink")
    pyflink_datastream_connectors_kafka.KafkaSource = MagicMock(name="KafkaSource")

    pyflink_datastream_functions = types.ModuleType("pyflink.datastream.functions")

    class _MapFunction:
        pass

    class _ProcessWindowFunction:
        class Context:
            pass

    pyflink_datastream_functions.MapFunction = _MapFunction
    pyflink_datastream_functions.ProcessWindowFunction = _ProcessWindowFunction

    pyflink_datastream_window = types.ModuleType("pyflink.datastream.window")

    class _Time:
        @staticmethod
        def seconds(n: int) -> str:
            return f"seconds:{n}"

    class _TumblingProcessingTimeWindows:
        @staticmethod
        def of(t: Any) -> str:
            return f"tumbling:{t}"

    pyflink_datastream_window.Time = _Time
    pyflink_datastream_window.TumblingProcessingTimeWindows = _TumblingProcessingTimeWindows

    installed = {
        "pyflink": pyflink,
        "pyflink.common": pyflink_common,
        "pyflink.common.serialization": pyflink_common_serialization,
        "pyflink.datastream": pyflink_datastream,
        "pyflink.datastream.connectors": pyflink_datastream_connectors,
        "pyflink.datastream.connectors.kafka": pyflink_datastream_connectors_kafka,
        "pyflink.datastream.functions": pyflink_datastream_functions,
        "pyflink.datastream.window": pyflink_datastream_window,
    }
    for name, fake_mod in installed.items():
        sys.modules[name] = fake_mod
    return installed


_STUBS = _make_pyflink_stubs()

# Now safe to import the module under test
from src.flink.jobs import metric_aggregation_job as mod  # noqa: E402
from src.flink.jobs.metric_aggregation_job import (  # noqa: E402
    MetricParser,
    MetricWindowAggregator,
    create_job,
    get_key,
)


class TestMetricParser:
    def test_parses_metric_emitted(self) -> None:
        parser = MetricParser()
        event = {
            "event_type": "METRIC_EMITTED",
            "timestamp": "2024-01-01T00:00:00Z",
            "payload": {
                "task_id": "t1",
                "processor_name": "p",
                "processor_version": "1.0.0",
                "metric_name": "accuracy",
                "value": 0.95,
                "aggregation_mask": 1,
            },
        }
        result = parser.map(json.dumps(event))
        assert result is not None
        parsed = json.loads(result)
        assert parsed["task_id"] == "t1"
        assert parsed["processor_name"] == "p"
        assert parsed["value"] == 0.95
        assert parsed["aggregation_mask"] == 1
        assert parsed["event_timestamp"] == "2024-01-01T00:00:00Z"

    def test_filters_non_metric_emitted_events(self) -> None:
        parser = MetricParser()
        assert parser.map(json.dumps({"event_type": "JOB_CREATED", "payload": {}})) is None

    def test_filters_non_numeric_values(self) -> None:
        parser = MetricParser()
        event = {
            "event_type": "METRIC_EMITTED",
            "payload": {"value": "not-a-number"},
        }
        assert parser.map(json.dumps(event)) is None

    def test_filters_dict_value(self) -> None:
        parser = MetricParser()
        event = {
            "event_type": "METRIC_EMITTED",
            "payload": {"value": {"x": 1}},
        }
        # dict is not int/float so should be filtered
        assert parser.map(json.dumps(event)) is None

    def test_handles_invalid_json(self) -> None:
        parser = MetricParser()
        assert parser.map("not json") is None

    def test_uses_defaults_for_missing_fields(self) -> None:
        parser = MetricParser()
        event = {
            "event_type": "METRIC_EMITTED",
            "payload": {"value": 3},
        }
        result = parser.map(json.dumps(event))
        assert result is not None
        parsed = json.loads(result)
        assert parsed["task_id"] == ""
        assert parsed["processor_name"] == "unknown"
        assert parsed["processor_version"] == "0.0.0"
        assert parsed["metric_name"] == "unknown"
        assert parsed["aggregation_mask"] == 0
        assert parsed["value"] == 3.0


class TestMetricWindowAggregator:
    def _ctx(self, start: int = 1000, end: int = 2000) -> Any:
        window = MagicMock()
        window.start = start
        window.end = end
        ctx = MagicMock()
        ctx.window.return_value = window
        return ctx

    def _record(self, value: float, **overrides: Any) -> str:
        rec = {
            "task_id": "t",
            "processor_name": "proc",
            "processor_version": "1.0.0",
            "metric_name": "accuracy",
            "value": value,
            "aggregation_mask": 1,
            "event_timestamp": "ts",
        }
        rec.update(overrides)
        return json.dumps(rec)

    def test_empty_window_emits_nothing(self) -> None:
        agg = MetricWindowAggregator()
        out = list(agg.process("key", self._ctx(), []))
        assert out == []

    def test_single_value_stats(self) -> None:
        agg = MetricWindowAggregator()
        out = list(agg.process("key", self._ctx(0, 60000), [self._record(0.5)]))
        assert len(out) == 1
        parsed = json.loads(out[0])
        assert parsed["event_type"] == "AGGREGATE_COMPUTED"
        assert parsed["source_id"] == "flink-metric-aggregation"
        assert parsed["processor_name"] == "proc"
        assert parsed["processor_version"] == "1.0.0"
        assert parsed["metric_name"] == "accuracy"
        assert parsed["aggregation_mask"] == 1
        assert parsed["window_start_ms"] == 0
        assert parsed["window_end_ms"] == 60000
        summary = parsed["summary"]
        assert summary["count"] == 1
        assert summary["sum"] == 0.5
        assert summary["mean"] == 0.5
        assert summary["min"] == 0.5
        assert summary["max"] == 0.5
        assert summary["std"] == 0.0  # n=1

    def test_multi_value_stats(self) -> None:
        agg = MetricWindowAggregator()
        records = [self._record(float(v)) for v in (1, 2, 3, 4, 5)]
        out = list(agg.process("key", self._ctx(), records))
        parsed = json.loads(out[0])
        summary = parsed["summary"]
        assert summary["count"] == 5
        assert summary["sum"] == 15.0
        assert summary["mean"] == 3.0
        assert summary["min"] == 1.0
        assert summary["max"] == 5.0
        assert summary["median"] == 3.0
        # std for [1..5] is sqrt(2) ~= 1.4142
        assert abs(summary["std"] - 1.4142) < 0.001

    def test_percentile_indices(self) -> None:
        agg = MetricWindowAggregator()
        records = [self._record(float(v)) for v in range(1, 101)]
        out = list(agg.process("key", self._ctx(), records))
        summary = json.loads(out[0])["summary"]
        # p95_idx = int(100*0.95) = 95 -> sorted[95] = 96
        assert summary["p95"] == 96
        # p99_idx = int(100*0.99) = 99 -> sorted[99] = 100
        assert summary["p99"] == 100

    def test_invalid_json_records_skipped(self) -> None:
        agg = MetricWindowAggregator()
        records = [
            self._record(1.0),
            "not json",
            self._record(2.0),
        ]
        out = list(agg.process("key", self._ctx(), records))
        assert len(out) == 1
        summary = json.loads(out[0])["summary"]
        # Two valid values, "not json" raises JSONDecodeError and is skipped
        assert summary["count"] == 2
        assert summary["sum"] == 3.0

    def test_partial_record_value_appended_before_keyerror(self) -> None:
        # Characterization: the aggregator appends data["value"] FIRST, then
        # tries to read processor_name/version/metric_name. If those raise
        # KeyError, the value has already been added to values. So a record
        # missing keys but containing "value" still contributes to count/sum.
        agg = MetricWindowAggregator()
        records = [
            self._record(1.0),
            json.dumps({"value": 9.0}),  # has 'value', missing other fields
            self._record(2.0),
        ]
        out = list(agg.process("key", self._ctx(), records))
        summary = json.loads(out[0])["summary"]
        assert summary["count"] == 3
        assert summary["sum"] == 12.0


class TestGetKey:
    def test_constructs_pipe_delimited_key(self) -> None:
        record = json.dumps(
            {
                "processor_name": "proc",
                "processor_version": "1.2.3",
                "metric_name": "accuracy",
            }
        )
        assert get_key(record) == "proc|1.2.3|accuracy"

    def test_missing_field_returns_unknown(self) -> None:
        record = json.dumps({"processor_name": "p"})
        assert get_key(record) == "unknown"

    def test_invalid_json_returns_unknown(self) -> None:
        assert get_key("nope") == "unknown"


class TestCreateJob:
    def test_create_job_uses_env_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Clear any pre-set env vars to exercise defaults
        for var in (
            "KAFKA_BOOTSTRAP_SERVERS",
            "FLINK_CONSUMER_GROUP",
            "KAFKA_TOPIC_METRICS",
            "KAFKA_TOPIC_AGGREGATES",
            "FLINK_WINDOW_SIZE_SEC",
            "FLINK_PARALLELISM",
        ):
            monkeypatch.delenv(var, raising=False)

        mock_env = MagicMock()
        # Make the chained pipeline calls not blow up by returning the same mock.
        # env.from_source(...).map(...).filter(...).key_by(...).window(...).process(...).sink_to(...).name(...)
        chain = MagicMock()
        chain.map.return_value = chain
        chain.filter.return_value = chain
        chain.key_by.return_value = chain
        chain.window.return_value = chain
        chain.process.return_value = chain
        chain.sink_to.return_value = chain
        chain.name.return_value = chain
        mock_env.from_source.return_value = chain

        with patch.object(mod, "StreamExecutionEnvironment") as see_cls:
            see_cls.get_execution_environment.return_value = mock_env
            result = create_job()

        assert result is mock_env
        mock_env.set_parallelism.assert_called_once_with(4)
        mock_env.enable_checkpointing.assert_called_once_with(60000)
        mock_env.set_runtime_mode.assert_called_once()

    def test_create_job_respects_env_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FLINK_PARALLELISM", "11")
        monkeypatch.setenv("FLINK_WINDOW_SIZE_SEC", "30")
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:1234")
        monkeypatch.setenv("FLINK_CONSUMER_GROUP", "grp-x")
        monkeypatch.setenv("KAFKA_TOPIC_METRICS", "mx")
        monkeypatch.setenv("KAFKA_TOPIC_AGGREGATES", "ax")

        mock_env = MagicMock()
        chain = MagicMock()
        chain.map.return_value = chain
        chain.filter.return_value = chain
        chain.key_by.return_value = chain
        chain.window.return_value = chain
        chain.process.return_value = chain
        chain.sink_to.return_value = chain
        chain.name.return_value = chain
        mock_env.from_source.return_value = chain

        with patch.object(mod, "StreamExecutionEnvironment") as see_cls:
            see_cls.get_execution_environment.return_value = mock_env
            create_job()

        mock_env.set_parallelism.assert_called_once_with(11)

    def test_create_job_adds_kafka_jars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # exercise default-env branch
        mock_env = MagicMock()
        chain = MagicMock()
        chain.map.return_value = chain
        chain.filter.return_value = chain
        chain.key_by.return_value = chain
        chain.window.return_value = chain
        chain.process.return_value = chain
        chain.sink_to.return_value = chain
        chain.name.return_value = chain
        mock_env.from_source.return_value = chain

        with patch.object(mod, "StreamExecutionEnvironment") as see_cls:
            see_cls.get_execution_environment.return_value = mock_env
            create_job()

        mock_env.add_jars.assert_called_once()
        args = mock_env.add_jars.call_args.args
        # Expect two jars: flink-connector-kafka and kafka-clients
        assert any("flink-connector-kafka" in a for a in args)
        assert any("kafka-clients" in a for a in args)


class TestMain:
    def test_main_calls_create_job_and_execute(self) -> None:
        mock_env = MagicMock()
        with (
            patch.object(mod, "create_job", return_value=mock_env) as create_mock,
            patch.object(mod, "load_dotenv"),
            patch.object(mod, "_setup_path"),
        ):
            mod.main()
        create_mock.assert_called_once()
        mock_env.execute.assert_called_once_with("Tapline-MetricAggregation")


@pytest.fixture(autouse=True)
def _ensure_pyflink_stubs_present() -> None:
    for name, m in _STUBS.items():
        sys.modules.setdefault(name, m)
