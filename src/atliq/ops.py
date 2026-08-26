"""Run audit log written to atliq_dw.ops.pipeline_run_log."""
from __future__ import annotations

from datetime import datetime

from .sqlclient import Sql


def log_run(dw: Sql, run_id, run_date, stage, table_name, rows_in, rows_after,
            status, started_at, error_message=None) -> None:
    finished = datetime.now()
    dw.execute(
        """
        INSERT INTO ops.pipeline_run_log
            (run_id, run_date, stage, table_name, rows_in, rows_after,
             status, error_message, started_at, finished_at, duration_secs)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (run_id, run_date, stage, table_name, rows_in, rows_after, status,
         error_message, started_at, finished,
         round((finished - started_at).total_seconds(), 3)),
    )
