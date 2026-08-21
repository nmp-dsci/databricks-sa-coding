"""Tests for the synthetic generator.

The generator's contract is that it produces the defects a real feed has. If a
change quietly removes them, the pipeline stops being tested against anything
interesting — so the defects are asserted, not just the happy path.
"""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from lib.generate import STATUSES, make_events
from lib.transforms import clean_events

ROWS = 3000
SEED = 7


@pytest.fixture(scope="module")
def sample(spark):
    return make_events(spark, rows=ROWS, seed=SEED).cache()


def test_rejects_a_nonsense_row_count(spark):
    with pytest.raises(ValueError):
        make_events(spark, rows=0)


def test_is_deterministic_for_a_seed(spark):
    """Same seed, same data. This is what makes a downstream diff mean the
    transform changed rather than the input."""
    a = make_events(spark, rows=500, seed=SEED)
    b = make_events(spark, rows=500, seed=SEED)
    assert a.exceptAll(b).count() == 0
    assert b.exceptAll(a).count() == 0


def test_a_different_seed_gives_different_data(spark):
    a = make_events(spark, rows=500, seed=1)
    b = make_events(spark, rows=500, seed=2)
    assert a.exceptAll(b).count() > 0


def test_emits_duplicate_event_ids(sample):
    dupes = sample.groupBy("event_id").count().filter(F.col("count") > 1).count()
    assert dupes > 0, "no duplicates — the dedupe step is no longer being exercised"


def test_emits_null_customers(sample):
    assert sample.filter(F.col("customer_id").isNull()).count() > 0


def test_emits_late_arrivals(sample):
    late = sample.filter(F.datediff(F.col("ingest_ts"), F.col("event_ts")) > 1).count()
    assert late > 0


def test_only_known_statuses(sample):
    found = {r.status for r in sample.select("status").distinct().collect()}
    assert found <= set(STATUSES)
    # All five should appear at this row count; a missing one means the
    # element_at index arithmetic is off by one and a status is unreachable.
    assert found == set(STATUSES)


def test_the_pipeline_actually_removes_what_was_injected(sample):
    """The generator and the transform are tested together once, because each
    is only meaningful against the other."""
    cleaned = clean_events(sample)
    assert cleaned.filter(F.col("customer_id").isNull()).count() == 0
    assert cleaned.groupBy("event_id").count().filter(F.col("count") > 1).count() == 0
    assert cleaned.count() < sample.count()
