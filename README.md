**What this does:**

Connects to Binance USD-M futures `<symbol>@depth@0ms` diff streams, assembles a
faithful full order book per symbol (REST snapshot + incremental diffs), and writes
Tardis-style market-data CSVs — `quotes`, `book_snapshot_5`, and/or `book_snapshot_25` —
each row emitted **only when its tracked levels change** (event-driven dedup), with
crossed levels removed.

**How to run:**
```bash
# Collect one or more data types; one CSV file per type is written to --output-dir.
uv run --isolated main.py --data-types quotes book_snapshot_5 book_snapshot_25 \
    --symbols btcusdt ethusdt bnbusdt --output-dir ./data
```
 - `--data-types`: any of `quotes`, `book_snapshot_5`, `book_snapshot_25` (default `book_snapshot_5`).
 - `--output-dir`: directory for the output files (default `.`); files are named `<data_type>.csv`, all symbols sharing each file.

**To simulate packet loss / desync (for testing the resync path):**
```bash
uv run --isolated main.py --symbols btcusdt --simulate-desync
```

**How to run tests:**
```bash
uv run --isolated pytest test_order_book.py
```

**Output formats (Tardis-compatible):**
 - `quotes` (top of book): `exchange,symbol,timestamp,local_timestamp,ask_amount,ask_price,bid_price,bid_amount`
 - `book_snapshot_5` / `book_snapshot_25`: `exchange,symbol,timestamp,local_timestamp,` then `asks[i].price,asks[i].amount,bids[i].price,bids[i].amount` for `i` in `0..N-1`.
 - `exchange` is `binance-futures`, `symbol` is lowercased.
 - `timestamp` is the exchange event time and `local_timestamp` the receive time, both in **microseconds**.
 - Prices/amounts are preserved as the exchange's original decimal strings (no float round-trip).

**Granularity note:**
 - We use `@depth@0ms`, Binance's real-time (dynamically-adjusted) diff-depth stream — **not**
   the throttled `@depth@100ms`. Updates are event-driven and routinely sub-100 ms (~20-40
   rows/sec on BTCUSDT), matching Tardis's own datasets, which reconstruct from the same feed.
 - This mirrors Tardis's approach of reconstructing all top-of-book / snapshot data from the L2
   depth feed rather than native quote feeds (which can be throttled, batched, or absent).

**Libraries used:**
 - pysimdjson - fast json parser (using simd)
 - picows - performant websockets library
 - uvloop - event loop for asyncio
 - beartype - type checking
 - sortedcontainers - for sorted dict

**How to further improve this project:**
 - each instrument has a concept of tickSize and stepSize that can be used to transition to integer calculations
 - handle binance limits on number of requests per second, etc.
 - Handle 24h reconnects
 - Use multiprocessing or threading (each process handles group of instruments)
 - split into more python files
 - more tests
 - better errors handling

**Binance Streams:**
 - [Partial Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams) - snapshot of the order book
 - [Diff. Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams) - partial incremental updates of the order book
