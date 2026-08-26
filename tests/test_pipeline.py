"""End-to-end checks against the local SQL Server warehouse."""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from atliq import config as cfg
from atliq.bootstrap import oltp_counts
from atliq.gold import snapshot
from atliq.nightly import compare, run_once
from atliq.quality import run as quality_run
from atliq.sqlclient import Sql


def sql_up() -> bool:
    try:
        with Sql("master") as db:
            db.scalar("SELECT 1")
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(not sql_up(), reason="SQL Server is not running on localhost,1433")


def test_oltp_seed_counts():
    counts = oltp_counts()
    assert counts["customers"] == 40
    assert counts["products"] == 25
    assert counts["orders"] >= 300
    assert counts["order_items"] >= 783
    assert counts["payments"] >= 246


def test_gold_reference_numbers():
    snap = snapshot()
    assert snap["silver_orders"] >= 300
    assert snap["silver_order_items"] >= 783
    assert snap["fact_sales_rows"] == snap["silver_order_items"]
    assert snap["dim_customer_rows"] == 40
    assert snap["dim_product_rows"] == 25
    assert snap["gross_revenue"] >= 2_076_041


def test_quality_gate():
    results = quality_run(verbose=False)
    failed = [name for name, _, ok in results if not ok]
    assert not failed, failed


def test_idempotent_second_run():
    _, a = run_once(verbose=False, label="idem-a")
    _, b = run_once(verbose=False, label="idem-b")
    assert compare(a, b)
