# tikr — Python SDK

Python client for [Tikr](https://github.com/walkerfunction/tikr), the schema-agnostic time series edge rollup engine.

## Install

```bash
pip install -e python-sdk/
```

## Quick Start

```python
from tikr import TikrClient, Tick
import time

client = TikrClient("localhost:9876")

# Ingest ticks
now = int(time.time()) * 1_000_000_000
ticks = [
    Tick(timestamp_ns=now + i, dimensions={"symbol": "AAPL"},
         fields={"price": 17500 + i, "quantity": 100})
    for i in range(1000)
]
count = client.ingest("market_ticks", ticks)
print(f"Ingested {count} ticks")

# Query bars
bars = client.query_bars("market_ticks", dimensions={"symbol": "AAPL"},
                         start_ns=now - 60_000_000_000, end_ns=now + 60_000_000_000)
for bar in bars:
    print(f"  {bar.bucket_ts}: {bar.metrics}")

# List series
for s in client.list_series():
    print(f"  {s.name}: {s.dimensions} → {s.metrics} ({s.rollup})")
```

## API

| Method | Description |
|---|---|
| `TikrClient(address)` | Connect to Tikr gRPC server |
| `.ingest(series, ticks)` | Ingest a list of `Tick` objects |
| `.query_bars(series, ...)` | Query rolled-up bars |
| `.query_ticks(series, ...)` | Query raw ticks |
| `.list_series()` | List configured series |
| `.get_info()` | Get server version and uptime |

## License

Apache 2.0
