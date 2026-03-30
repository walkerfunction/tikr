"""Unit tests for tikr.models — no server needed."""

from tikr.models import Tick, Bar, SeriesInfo


def test_tick_defaults():
    t = Tick(timestamp_ns=1000)
    assert t.timestamp_ns == 1000
    assert t.dimensions == {}
    assert t.fields == {}
    assert t.sequence == 0


def test_tick_with_data():
    t = Tick(
        timestamp_ns=123456789,
        dimensions={"symbol": "AAPL"},
        fields={"price": 17500, "quantity": 100},
        sequence=42,
    )
    assert t.dimensions["symbol"] == "AAPL"
    assert t.fields["price"] == 17500
    assert t.sequence == 42


def test_bar_fields():
    b = Bar(
        series="market_ticks",
        bucket_ts=1000000000,
        dimensions={"symbol": "AAPL"},
        metrics={"open": 100, "high": 110, "low": 90, "close": 105},
        first_timestamp=1000000001,
        last_timestamp=1000000999,
        tick_count=50,
    )
    assert b.series == "market_ticks"
    assert b.metrics["high"] == 110
    assert b.tick_count == 50


def test_series_info():
    s = SeriesInfo(
        name="market_ticks",
        dimensions=["symbol"],
        metrics=["open", "high", "low", "close", "volume"],
        rollup="1s",
    )
    assert s.name == "market_ticks"
    assert len(s.metrics) == 5
    assert s.rollup == "1s"
