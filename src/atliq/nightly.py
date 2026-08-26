"""Run ingest, silver, gold, then the tests."""
from __future__ import annotations

import uuid
from datetime import datetime

from . import gold, ingest, quality, silver
from .gold import snapshot
from .loggingutil import header, log


def new_run_id() -> str:
    return uuid.uuid4().hex[:8]


def run_once(verbose: bool = True, label: str = "") -> tuple[str, dict]:
    run_id = new_run_id()
    started = datetime.now()
    if verbose:
        header(f"Nightly run {label}  run_id={run_id}  {started:%Y-%m-%d %H:%M:%S}")
    ingest.run(run_id, started, verbose=verbose)
    silver.run(run_id, started.date(), verbose=verbose)
    gold.run(run_id, started.date(), verbose=verbose)
    snap = snapshot()
    return run_id, snap


def print_snapshot(snap: dict, title: str = "Warehouse") -> None:
    header(title)
    for k, v in snap.items():
        if isinstance(v, float):
            log(f"{k:<22} {v:>16,.2f}")
        else:
            log(f"{k:<22} {v:>16,}")


def compare(a: dict, b: dict) -> bool:
    header("Idempotency  run 1 vs run 2")
    identical = True
    log(f"{'metric':<22} {'run 1':>16} {'run 2':>16}  match")
    for k in a:
        match = a[k] == b[k]
        identical &= match
        if isinstance(a[k], float):
            left, right = f"{a[k]:>16,.2f}", f"{b[k]:>16,.2f}"
        else:
            left, right = f"{a[k]:>16,}", f"{b[k]:>16,}"
        log(f"{k:<22} {left} {right}  {'yes' if match else 'NO'}")
    if identical:
        log("IDEMPOTENT — second run changed nothing")
    else:
        log("NOT IDEMPOTENT — a stage is appending instead of merging")
    return identical


def run_twice(verbose: bool = True) -> bool:
    _, snap1 = run_once(verbose=verbose, label="1")
    _, snap2 = run_once(verbose=verbose, label="2")
    quality.run(verbose=verbose)
    ok = compare(snap1, snap2)
    return ok
