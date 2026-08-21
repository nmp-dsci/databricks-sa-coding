"""Unit tests for the pure transforms.

These run on a local Spark session in a few seconds and need no workspace. The
convention this repo keeps: change a rule in `src/lib/transforms.py`, add a case
here, `make test` — and only then deploy. Verifying a logic change by running the
job is 100x slower and the failure message is worse.
"""

from __future__ import annotations

from datetime import datetime

import pytest

from lib.transforms import MAX_PLAUSIBLE_AMOUNT, clean_events, daily_revenue

SCHEMA = (
    "event_id string, customer_id string, status string, "
    "amount double, event_ts timestamp, ingest_ts timestamp"
)


def ts(day: int, hour: int = 12) -> datetime:
    return datetime(2026, 1, day, hour)


def events(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


# ---------------------------------------------------------------------------
# clean_events
# ---------------------------------------------------------------------------


def test_drops_rows_that_cannot_be_attributed_or_valued(spark):
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(1)),
            ("e2", None, "paid", 10.0, ts(1), ts(1)),  # unattributable
            ("e3", "c1", "paid", 0.0, ts(1), ts(1)),  # zero
            ("e4", "c1", "paid", -5.0, ts(1), ts(1)),  # negative
            ("e5", "c1", "paid", MAX_PLAUSIBLE_AMOUNT + 1, ts(1), ts(1)),  # implausible
        ],
    )
    assert [r.event_id for r in clean_events(df).collect()] == ["e1"]


def test_boundary_amount_is_kept(spark):
    """The cap is inclusive — a filter that is off by one here silently loses
    the single largest order, which is exactly the row someone notices."""
    df = events(spark, [("e1", "c1", "paid", MAX_PLAUSIBLE_AMOUNT, ts(1), ts(1))])
    assert clean_events(df).count() == 1


def test_dedupe_keeps_the_latest_ingest(spark):
    """A re-delivery is more often a correction than an accident, so the newest
    copy wins — including its amount, not just its row."""
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(1)),
            ("e1", "c1", "paid", 99.0, ts(1), ts(3)),  # later ingest, corrected amount
            ("e1", "c1", "paid", 50.0, ts(1), ts(2)),
        ],
    )
    result = clean_events(df).collect()
    assert len(result) == 1
    assert result[0].amount == 99.0


def test_dedupe_is_deterministic_on_an_ingest_tie(spark):
    """Same ingest_ts twice — without the event_ts tie-break the survivor is
    whichever row the shuffle happened to order first, so silver churns between
    runs that read identical bronze."""
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(5)),
            ("e1", "c1", "paid", 20.0, ts(2), ts(5)),  # same ingest, later event
        ],
    )
    assert clean_events(df).collect()[0].amount == 20.0


def test_dedupe_never_resurrects_a_corrupt_row(spark):
    """Filter runs before dedupe. If it ran after, the corrupt later copy would
    win the window and then be dropped, losing the good row entirely."""
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(1)),
            ("e1", None, "paid", 10.0, ts(1), ts(9)),  # later, but unattributable
        ],
    )
    result = clean_events(df).collect()
    assert len(result) == 1
    assert result[0].customer_id == "c1"


# ---------------------------------------------------------------------------
# daily_revenue
# ---------------------------------------------------------------------------


def test_excludes_cancelled_and_refunded(spark):
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 100.0, ts(1), ts(1)),
            ("e2", "c2", "cancelled", 100.0, ts(1), ts(1)),
            ("e3", "c3", "refunded", 100.0, ts(1), ts(1)),
        ],
    )
    row = daily_revenue(df).collect()[0]
    assert row.revenue == 100.0
    assert row.orders == 1


def test_groups_by_event_date_not_ingest_date(spark):
    """A backfill landing today belongs to the day it happened. Grouping on
    ingest_ts would pile a month of history onto one date."""
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(10)),
            ("e2", "c2", "paid", 20.0, ts(2), ts(10)),
        ],
    )
    dates = [str(r.event_date) for r in daily_revenue(df).collect()]
    assert dates == ["2026-01-01", "2026-01-02"]


def test_counts_late_arrivals_separately(spark):
    """More than a day between event and ingest. This column is what makes a
    revenue figure that moved after the fact explainable."""
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(1)),  # same day
            ("e2", "c2", "paid", 10.0, ts(1), ts(5)),  # four days late
        ],
    )
    row = daily_revenue(df).collect()[0]
    assert row.orders == 2
    assert row.late_orders == 1


def test_customers_are_distinct_not_a_row_count(spark):
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(1), ts(1)),
            ("e2", "c1", "paid", 10.0, ts(1), ts(1)),
        ],
    )
    row = daily_revenue(df).collect()[0]
    assert row.orders == 2
    assert row.customers == 1


def test_output_is_ordered_by_date(spark):
    df = events(
        spark,
        [
            ("e1", "c1", "paid", 10.0, ts(3), ts(3)),
            ("e2", "c2", "paid", 10.0, ts(1), ts(1)),
        ],
    )
    dates = [str(r.event_date) for r in daily_revenue(df).collect()]
    assert dates == sorted(dates)


@pytest.mark.parametrize("bad_status", ["cancelled", "refunded"])
def test_a_day_of_only_non_revenue_events_produces_no_row(spark, bad_status):
    """Not a zero row — no row. Worth pinning: the checks notebook asserts gold
    covers every silver date, and it applies the same status filter for exactly
    this reason."""
    df = events(spark, [("e1", "c1", bad_status, 10.0, ts(1), ts(1))])
    assert daily_revenue(df).count() == 0
