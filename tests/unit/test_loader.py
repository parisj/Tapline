from __future__ import annotations

from pathlib import Path

import pytest

# Adjust to your actual module path
from src.config.loader import (
    ConfigError,
    RuntimeConfig,
    _parse_workers_max,
    load_runtime_config,
)


def _write_toml(tmp_path: Path, text: str) -> Path:
    p = tmp_path / "config.toml"
    p.write_text(text, encoding="utf-8")
    return p


def test_load_runtime_config_success_minimal_valid(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [ingest]
        queue_maxsize = 100
        poll_interval_sec = 0.25
        allowed_image_exts = [".png", " .jpg ", ""]

        [readiness]
        stable_window_sec = 2.0
        max_wait_sec = 30

        [workers]
        max_workers = "auto"
        auto_divisor = 2
        min_workers = 2

        [evaluation]
        interval_sec = 10

        [directories]
        inbox = "/tmp/inbox"
        out = "  /tmp/out   "
        empty = ""
        not_a_string = 123
        """,
    )

    cfg = load_runtime_config(path)

    assert isinstance(cfg, RuntimeConfig)
    assert cfg.ingest_queue_maxsize == 100
    assert cfg.ingest_poll_interval_sec == pytest.approx(0.25)
    assert cfg.allowed_image_exts == [".png", ".jpg"]  # stripped + empty removed
    assert cfg.readiness_stable_window_sec == pytest.approx(2.0)
    assert cfg.readiness_max_wait_sec == pytest.approx(30.0)
    assert cfg.evaluation_interval_sec == pytest.approx(10.0)

    # directories: only non-empty strings, converted to Path (note: value isn't stripped in code)
    assert "inbox" in cfg.directories
    assert cfg.directories["inbox"] == Path("/tmp/inbox")
    assert cfg.directories["out"] == Path("/tmp/out")


def test_load_runtime_config_requires_each_top_level_table(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [ingest]
        queue_maxsize = 1
        poll_interval_sec = 1
        allowed_image_exts = [".png"]

        [workers]
        max_workers = 2

        [evaluation]
        interval_sec = 1

        [directories]
        inbox = "/tmp/inbox"
        """,
    )

    with pytest.raises(ConfigError, match=r"Missing or invalid table \[readiness\]"):
        load_runtime_config(path)


@pytest.mark.parametrize(
    ("ingest_block", "expected_message"),
    [
        (
            """
            queue_maxsize = "100"
            poll_interval_sec = 0.1
            allowed_image_exts = [".png"]
            """,
            "queue_maxsize must be an integer",
        ),
        (
            """
            queue_maxsize = 100
            poll_interval_sec = "0.1"
            allowed_image_exts = [".png"]
            """,
            "poll_interval_sec must be a number",
        ),
        (
            """
            queue_maxsize = 100
            poll_interval_sec = 0.1
            allowed_image_exts = ".png"
            """,
            "allowed_image_exts must be a list of strings",
        ),
        (
            """
            queue_maxsize = 100
            poll_interval_sec = 0.1
            allowed_image_exts = [1, 2]
            """,
            "allowed_image_exts must be a list of strings",
        ),
        (
            """
            queue_maxsize = 100
            allowed_image_exts = [".png"]
            """,
            "poll_interval_sec must be a number",
        ),
        (
            """
            poll_interval_sec = 0.1
            allowed_image_exts = [".png"]
            """,
            "queue_maxsize must be an integer",
        ),
    ],
)
def test_load_runtime_config_validates_ingest_fields(
    tmp_path: Path,
    ingest_block: str,
    expected_message: str,
) -> None:
    path = _write_toml(
        tmp_path,
        f"""
        [ingest]
        {ingest_block}

        [readiness]
        stable_window_sec = 1
        max_wait_sec = 10

        [workers]
        max_workers = 2

        [evaluation]
        interval_sec = 5

        [directories]
        inbox = "/tmp/inbox"
        """,
    )

    with pytest.raises(ConfigError, match=expected_message):
        load_runtime_config(path)


@pytest.mark.parametrize(
    ("readiness_block", "expected_message"),
    [
        (
            """
            stable_window_sec = "1"
            max_wait_sec = 10
            """,
            "stable_window_sec must be a number",
        ),
        (
            """
            stable_window_sec = 1
            max_wait_sec = "10"
            """,
            "max_wait_sec must be a number",
        ),
        (
            """
            max_wait_sec = 10
            """,
            "stable_window_sec must be a number",
        ),
        (
            """
            stable_window_sec = 1
            """,
            "max_wait_sec must be a number",
        ),
    ],
)
def test_load_runtime_config_validates_readiness_fields(
    tmp_path: Path,
    readiness_block: str,
    expected_message: str,
) -> None:
    path = _write_toml(
        tmp_path,
        f"""
        [ingest]
        queue_maxsize = 10
        poll_interval_sec = 0.1
        allowed_image_exts = [".png"]

        [readiness]
        {readiness_block}

        [workers]
        max_workers = 2

        [evaluation]
        interval_sec = 5

        [directories]
        inbox = "/tmp/inbox"
        """,
    )

    with pytest.raises(ConfigError, match=expected_message):
        load_runtime_config(path)


def test_load_runtime_config_validates_evaluation_interval(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [ingest]
        queue_maxsize = 10
        poll_interval_sec = 0.1
        allowed_image_exts = [".png"]

        [readiness]
        stable_window_sec = 1
        max_wait_sec = 10

        [workers]
        max_workers = 2

        [evaluation]
        interval_sec = "nope"

        [directories]
        inbox = "/tmp/inbox"
        """,
    )

    with pytest.raises(ConfigError, match=r"interval_sec must be a number"):
        load_runtime_config(path)


def test_load_runtime_config_directories_must_have_at_least_one_valid_mapping(tmp_path: Path) -> None:
    path = _write_toml(
        tmp_path,
        """
        [ingest]
        queue_maxsize = 10
        poll_interval_sec = 0.1
        allowed_image_exts = [".png"]

        [readiness]
        stable_window_sec = 1
        max_wait_sec = 10

        [workers]
        max_workers = 2

        [evaluation]
        interval_sec = 5

        [directories]
        empty = ""
        num = 123
        whitespace = "   "
        """,
    )

    with pytest.raises(ConfigError, match=r"directories table must contain at least one"):
        load_runtime_config(path)


def test_parse_workers_max_integer_clamped_to_at_least_1() -> None:
    assert _parse_workers_max({"max_workers": 10}) == 10
    assert _parse_workers_max({"max_workers": 1}) == 1
    assert _parse_workers_max({"max_workers": 0}) == 1
    assert _parse_workers_max({"max_workers": -5}) == 1


def test_parse_workers_max_auto_uses_cpu_count(monkeypatch) -> None:
    # cpu=8, divisor=2 => 4; min_workers=2 => 4
    monkeypatch.setattr("os.cpu_count", lambda: 8)
    assert _parse_workers_max({"max_workers": "auto", "auto_divisor": 2, "min_workers": 2}) == 4

    # cpu=3, divisor=2 => 1; min_workers=2 => 2
    monkeypatch.setattr("os.cpu_count", lambda: 3)
    assert _parse_workers_max({"max_workers": "auto", "auto_divisor": 2, "min_workers": 2}) == 2


def test_parse_workers_max_auto_divisor_guardrails(monkeypatch) -> None:
    # divisor=0 => treated as 1 by max(1, divisor)
    monkeypatch.setattr("os.cpu_count", lambda: 8)
    assert _parse_workers_max({"max_workers": "auto", "auto_divisor": 0, "min_workers": 2}) == 8

    # cpu_count None => fallback 4 in code
    monkeypatch.setattr("os.cpu_count", lambda: None)
    assert _parse_workers_max({"max_workers": "auto", "auto_divisor": 2, "min_workers": 2}) == 2


def test_parse_workers_max_rejects_invalid_string() -> None:
    with pytest.raises(ConfigError, match=r"workers\.max_workers must be 'auto' or an integer"):
        _parse_workers_max({"max_workers": "four"})
