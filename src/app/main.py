from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()


import threading
import time
from pathlib import Path

from src.algorithms.registry import build_default_registry
from src.dispatch.dispatcher import Dispatcher
from src.dispatch.routes import build_dispatch_plans, load_routes_toml
from src.evaluation.evaluator import EvaluationModule
from src.ingest.dedup import DedupCache
from src.ingest.observer import DirectoryObserver
from src.ingest.queue import IngestQueue
from src.persistence.service import PersistenceService
from src.utils.logging import configure_logging, get_logger
from src.visualization.bokeh_app import start_bokeh_placeholder
from src.workers.pool import WorkerPool
from src.app.signals import install_signal_handlers
from src.config.loader import load_runtime_config
from src.config.loader import RuntimeConfig
from src.dispatch.routes import RoutesConfigError


logger = get_logger("pipeline.app.main")


def build_app(
    cfg: RuntimeConfig,
) -> tuple[DirectoryObserver, WorkerPool, EvaluationModule]:

    persistence = PersistenceService()
    ingest = IngestQueue(
        maxsize=cfg.ingest_queue_maxsize, timeout_sec=cfg.ingest_poll_interval_sec,
    )
    dedup = DedupCache(ttl_sec=3600.0)

    # Load routes.toml + algorithm settings and build plans
    registry = build_default_registry()

    try:
        loaded_routes = load_routes_toml(
            Path("src/config/routes.toml"), config_root=Path("src/config"),
        )
    except RoutesConfigError as e:
        logger.error("Failed to load routes.toml: %s", e)
        raise

    plans = build_dispatch_plans(loaded_routes, registry=registry)
    dispatcher = Dispatcher(routes=plans)

    observer = DirectoryObserver(
        directories=cfg.directories,
        ingest=ingest,
        dedup=dedup,
        poll_interval_sec=cfg.ingest_poll_interval_sec,
        allowed_exts=set(cfg.allowed_image_exts),
        max_wait_sec=cfg.readiness_max_wait_sec,
        stable_window_sec=cfg.readiness_stable_window_sec,
    )

    worker_pool = WorkerPool(
        ingest=ingest,
        dispatcher=dispatcher,
        persistence=persistence,
        max_workers=cfg.workers_max,
    )

    evaluator = EvaluationModule(persistence=persistence)
    return observer, worker_pool, evaluator


def main() -> None:
    configure_logging()

    cfg = load_runtime_config(Path("src/config/pipeline.toml"))
    observer, worker_pool, evaluator = build_app(cfg=cfg)

    stop = threading.Event()
    install_signal_handlers(stop)

    worker_pool.start()
    observer.start()
    start_bokeh_placeholder()

    logger.info("Pipeline started. Ctrl+C to stop.")

    last_eval = time.time()
    eval_interval = cfg.evaluation_interval_sec

    while not stop.is_set():
        time.sleep(0.2)
        now = time.time()
        if now - last_eval >= eval_interval:
            try:
                evaluator.run_window(window_start_unix=last_eval, window_end_unix=now)
                logger.info("Evaluation completed for window [%s, %s]", last_eval, now)
            except Exception:
                logger.exception("Evaluation failed.")
            last_eval = now

    logger.info("Stopping...")
    observer.stop()
    worker_pool.stop()
    logger.info("Stopped.")


if __name__ == "__main__":
    main()
