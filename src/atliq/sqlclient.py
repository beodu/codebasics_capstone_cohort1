"""SQL Server access: pymssql for the pipeline, sqlcmd for GO-batch scripts."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path

import pymssql

from . import config as cfg


def sqlcmd_bin() -> str:
    env = cfg.SQLCMD
    if env and Path(env).exists():
        return env
    found = shutil.which("sqlcmd")
    if found:
        return found
    raise FileNotFoundError(
        "sqlcmd not found. Expected ~/mssql/bin/sqlcmd or ATLIQ_SQLCMD."
    )


def run_sqlcmd(script: Path, database: str = "master") -> None:
    cmd = [
        sqlcmd_bin(),
        "-S", f"{cfg.SQL_SERVER},{cfg.SQL_PORT}",
        "-U", cfg.SQL_USER,
        "-P", cfg.SQL_PASSWORD,
        "-C",
        "-b",
        "-d", database,
        "-i", str(script),
    ]
    env = os.environ.copy()
    env.setdefault("PATH", "")
    mssql_bin = str(Path.home() / "mssql" / "bin")
    if mssql_bin not in env["PATH"]:
        env["PATH"] = mssql_bin + os.pathsep + env["PATH"]
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        raise RuntimeError(
            f"sqlcmd failed ({result.returncode}) on {script.name} / {database}\n"
            f"{result.stdout}\n{result.stderr}"
        )


class Sql:
    def __init__(self, database: str):
        self.database = database
        self.conn = pymssql.connect(
            server=cfg.SQL_SERVER,
            port=cfg.SQL_PORT,
            user=cfg.SQL_USER,
            password=cfg.SQL_PASSWORD,
            database=database,
            autocommit=True,
        )

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Sql":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def query(self, sql: str, params=None) -> list[dict]:
        cur = self.conn.cursor(as_dict=True)
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        return rows

    def fetchall(self, sql: str, params=None) -> list:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur.fetchall() or []

    def execute(self, sql: str, params=None) -> int:
        cur = self.conn.cursor()
        cur.execute(sql, params)
        return cur.rowcount if cur.rowcount is not None else 0

    def scalar(self, sql: str, params=None):
        cur = self.conn.cursor()
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None

    def executemany(self, sql: str, rows) -> None:
        if not rows:
            return
        cur = self.conn.cursor()
        cur.executemany(sql, rows)

    def insert_dicts(self, table: str, columns: list[str], rows: list[dict]) -> int:
        if not rows:
            return 0
        placeholders = ",".join(["%s"] * len(columns))
        col_sql = ",".join(columns)
        sql = f"INSERT INTO {table} ({col_sql}) VALUES ({placeholders})"
        tuples = [tuple(_native(r.get(c)) for c in columns) for r in rows]
        self.executemany(sql, tuples)
        return len(tuples)

    def script(self, path: Path) -> None:
        """Run a T-SQL file that may contain GO batches."""
        text = path.read_text(encoding="utf-8")
        batches, current = [], []
        for line in text.splitlines():
            if line.strip().upper() == "GO":
                batches.append("\n".join(current))
                current = []
            else:
                current.append(line)
        batches.append("\n".join(current))
        for batch in batches:
            if batch.strip():
                self.execute(batch)


def _native(v):
    if v is None:
        return None
    if hasattr(v, "to_pydatetime"):
        return v.to_pydatetime()
    if hasattr(v, "item") and not isinstance(v, (bytes, str)):
        try:
            return v.item()
        except Exception:
            return v
    return v
