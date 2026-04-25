"""Quant Trading System — Task Scheduler.

Async-native scheduling for recurring tasks: daily backfill, instrument
refresh, health checks, and compression triggers.
Uses APScheduler 3.x with AsyncIOScheduler.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.core.config import get_settings
from src.core.logging import get_logger

log = get_logger(__name__)


class PipelineScheduler:
    """Manages scheduled tasks for the data pipeline.

    Scheduled jobs:
    - daily_backfill: After market close, fill any gaps in historical data
    - refresh_instruments: Periodically refresh master contract / exchange info
    - health_check: Frequent connection health monitoring
    - compress_old_data: Weekly TimescaleDB compression trigger
    """

    def __init__(self):
        self._settings = get_settings()
        self._cfg = self._settings.ingestion.scheduler
        self._scheduler = AsyncIOScheduler(timezone="UTC")
        self._registered_jobs: dict[str, Any] = {}

    def register_daily_backfill(
        self,
        callback: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Register the daily backfill job.

        Runs after Indian market close (configurable, default 15:45 IST).

        Args:
            callback: Async function to execute for backfill.
        """
        # Parse time from config (in IST)
        hour, minute = map(int, self._cfg.daily_backfill_time.split(":"))

        # Convert IST to UTC (IST = UTC+5:30)
        utc_hour = (hour - 5) % 24
        utc_minute = (minute - 30) % 60
        if minute < 30:
            utc_hour = (utc_hour - 1) % 24

        job = self._scheduler.add_job(
            self._safe_execute(callback, "daily_backfill"),
            CronTrigger(hour=utc_hour, minute=utc_minute, day_of_week="mon-fri"),
            id="daily_backfill",
            name="Daily Backfill",
            replace_existing=True,
        )
        self._registered_jobs["daily_backfill"] = job
        log.info(
            "scheduler_job_registered",
            job="daily_backfill",
            schedule=f"{utc_hour:02d}:{utc_minute:02d} UTC (Mon-Fri)",
        )

    def register_instrument_refresh(
        self,
        callback: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Register the instrument refresh job.

        Runs every N hours (default: 6).

        Args:
            callback: Async function to refresh instruments.
        """
        job = self._scheduler.add_job(
            self._safe_execute(callback, "instrument_refresh"),
            IntervalTrigger(hours=self._cfg.instrument_refresh_hours),
            id="instrument_refresh",
            name="Instrument Refresh",
            replace_existing=True,
        )
        self._registered_jobs["instrument_refresh"] = job
        log.info(
            "scheduler_job_registered",
            job="instrument_refresh",
            interval_hours=self._cfg.instrument_refresh_hours,
        )

    def register_health_check(
        self,
        callback: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Register the health check job.

        Runs every N seconds (default: 60).

        Args:
            callback: Async function to check system health.
        """
        job = self._scheduler.add_job(
            self._safe_execute(callback, "health_check"),
            IntervalTrigger(seconds=self._cfg.health_check_seconds),
            id="health_check",
            name="Health Check",
            replace_existing=True,
        )
        self._registered_jobs["health_check"] = job
        log.info(
            "scheduler_job_registered",
            job="health_check",
            interval_seconds=self._cfg.health_check_seconds,
        )

    def register_compression(
        self,
        callback: Callable[[], Coroutine[Any, Any, None]],
    ) -> None:
        """Register the weekly compression job.

        Runs once a week at the configured day/time.

        Args:
            callback: Async function to trigger TimescaleDB compression.
        """
        # Map day name to cron day
        day_map = {
            "monday": "mon", "tuesday": "tue", "wednesday": "wed",
            "thursday": "thu", "friday": "fri", "saturday": "sat",
            "sunday": "sun",
        }
        day = day_map.get(self._cfg.compression_day.lower(), "sun")
        hour, minute = map(int, self._cfg.compression_time.split(":"))

        job = self._scheduler.add_job(
            self._safe_execute(callback, "compression"),
            CronTrigger(day_of_week=day, hour=hour, minute=minute),
            id="compression",
            name="Data Compression",
            replace_existing=True,
        )
        self._registered_jobs["compression"] = job
        log.info(
            "scheduler_job_registered",
            job="compression",
            schedule=f"{day} {hour:02d}:{minute:02d} UTC",
        )

    def register_custom(
        self,
        job_id: str,
        callback: Callable[[], Coroutine[Any, Any, None]],
        trigger: Any,
    ) -> None:
        """Register a custom scheduled job.

        Args:
            job_id: Unique job identifier.
            callback: Async function to execute.
            trigger: APScheduler trigger instance.
        """
        job = self._scheduler.add_job(
            self._safe_execute(callback, job_id),
            trigger,
            id=job_id,
            replace_existing=True,
        )
        self._registered_jobs[job_id] = job
        log.info("scheduler_custom_job_registered", job=job_id)

    # -------------------------------------------------------------------------
    # Lifecycle
    # -------------------------------------------------------------------------

    def start(self) -> None:
        """Start the scheduler."""
        if not self._scheduler.running:
            self._scheduler.start()
            log.info(
                "scheduler_started",
                jobs=list(self._registered_jobs.keys()),
            )

    def stop(self) -> None:
        """Stop the scheduler."""
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
            log.info("scheduler_stopped")

    @property
    def is_running(self) -> bool:
        """Check if the scheduler is running."""
        return self._scheduler.running

    @property
    def jobs(self) -> list[dict[str, Any]]:
        """Get info about all registered jobs."""
        result = []
        for job in self._scheduler.get_jobs():
            result.append({
                "id": job.id,
                "name": job.name,
                "next_run": str(job.next_run_time) if job.next_run_time else None,
                "trigger": str(job.trigger),
            })
        return result

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _safe_execute(
        callback: Callable[[], Coroutine[Any, Any, None]],
        job_name: str,
    ) -> Callable:
        """Wrap a callback to catch and log any exceptions.

        Prevents a single job failure from crashing the scheduler.
        """
        async def wrapper():
            try:
                log.debug("scheduler_job_executing", job=job_name)
                await callback()
                log.debug("scheduler_job_completed", job=job_name)
            except Exception as e:
                log.error(
                    "scheduler_job_failed",
                    job=job_name,
                    error=str(e),
                    exc_info=True,
                )

        return wrapper
