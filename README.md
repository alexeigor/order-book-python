**What this does:**

Connects to Binance **USD-M futures or spot** (`--market`, default futures) `<symbol>@depth`
diff streams, assembles a faithful full order book per symbol (REST snapshot + incremental diffs), and writes
Tardis-style market-data CSVs — `quotes`, `book_snapshot_5`, `book_snapshot_25`, and/or
`incremental_book_L2`. The reconstructed snapshot types emit a row **only when their
tracked levels change** (event-driven dedup), with crossed levels removed;
`incremental_book_L2` instead records the raw L2 feed close to verbatim.

**How to run:**
```bash
# Collect one or more data types; one CSV file per type is written to --output-dir.
uv run --isolated main.py --data-types quotes book_snapshot_5 book_snapshot_25 incremental_book_L2 \
    --symbols btcusdt ethusdt bnbusdt --output-dir ./data
```
 - `--data-types`: any of `quotes`, `book_snapshot_5`, `book_snapshot_25`, `incremental_book_L2` (default `book_snapshot_5`).
 - `--market`: `futures` (default) or `spot`. Spot uses `@depth@100ms` (spot has no `0ms`), the spot REST/WS endpoints, the spot sync rules, and exchange id `binance`. Example: `uv run --isolated main.py --market spot --symbols btcusdt --data-types book_snapshot_5`.
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
 - `incremental_book_L2`: `exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount` — one row per changed level. `side` is `bid`/`ask`; `amount` is the **absolute** new level size (not a delta), `0` = level removed. `is_snapshot=true` rows form the full-book block emitted on connect and on every resync (consumers discard prior state on a `true` block); `is_snapshot=false` rows are diffs.
 - `exchange` is `binance-futures` (futures) or `binance` (spot), `symbol` is lowercased.
 - `timestamp` is the exchange event time and `local_timestamp` the receive time, both in **microseconds**.
 - Prices/amounts are preserved as the exchange's original decimal strings (no float round-trip).

**Granularity note:**
 - On **futures** we use `@depth@0ms`, Binance's real-time (dynamically-adjusted) diff-depth stream — **not**
   the throttled `@depth@100ms`. `0ms` is a documented futures update-speed option for the Diff Book
   Depth stream (alongside `100ms`, `250ms` default, and `500ms`); the Partial Book Depth stream
   is throttled and offers no `0ms`. Updates are event-driven and routinely sub-100 ms (~20-40
   rows/sec on BTCUSDT), matching Tardis's own datasets, which reconstruct from the same feed.
 - **Spot has no `0ms`** (only `100ms`/`1000ms`), so `--market spot` uses `@depth@100ms`. Note Binance
   silently *accepts* a `@depth@0ms` spot subscription but then sends no data — the spot path
   deliberately avoids it.
 - This mirrors Tardis's approach of reconstructing all top-of-book / snapshot data from the L2
   depth feed rather than native quote feeds (which can be throttled, batched, or absent).

**How Tardis derives these data types** ([docs.tardis.dev](https://docs.tardis.dev/downloadable-csv-files/data-types)):
 - `quotes`, `book_snapshot_5`, `book_snapshot_25` are **reconstructed** from the raw L2 WebSocket
   feed: Tardis maintains a local order book and writes a row **only when the tracked top-N levels
   change**, with crossed levels (bid ≥ ask) removed. This collector mirrors that methodology exactly
   — `emit_snapshot` does the on-change dedup and `OrderBook.top_levels` does the read-time crossed
   filter. (Because the crossed filter is monotone, top-5 is exactly the first-5 prefix of top-25, so
   one extraction at the deepest depth feeds every snapshot type consistently.)
 - `incremental_book_L2` is the **raw** L2 feed, not a reconstruction: `is_snapshot,side,price,amount`,
   with absolute amounts (`0` = removal). A leading `is_snapshot=true` block carries the full book
   state; later `is_snapshot=true` blocks mean "discard prior state and resync." This collector emits
   the REST snapshot as the `true` block and every applied diff (buffered catch-up + live) as `false`
   rows, re-emitting a fresh `true` block whenever the `pu`/`prev_u` continuity check forces a resync.

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

Binance offers two order-book WebSocket streams that differ fundamentally — **snapshots vs. diffs**:

 - [Partial Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Partial-Book-Depth-Streams) — a complete *snapshot* of the top **N** levels on every message; each message fully replaces your view of the top of the book.
   - Stream name: `<symbol>@depth<levels>` or `<symbol>@depth<levels>@<speed>` (e.g. `btcusdt@depth5@100ms`).
   - Levels: fixed at **5, 10, or 20**.
   - Update speed: `250ms` (default), `500ms`, or `100ms` — throttled, **no `0ms`**.
   - No client-side bookkeeping: just read the latest message (no REST snapshot, no sequencing, no local book).
 - [Diff. Book Depth Streams](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Diff-Book-Depth-Streams) — *incremental* changes only: the price levels whose quantity changed since the last message (quantity `0` = remove that level). A single message is meaningless on its own.
   - Stream name: `<symbol>@depth`, `<symbol>@depth@0ms`, `<symbol>@depth@100ms`, etc.
   - Levels: no fixed cap — diffs span the **entire book**, not just a top-N window.
   - Update speed: `0ms` (real-time, dynamically adjusted), `100ms`, `250ms` (default), or `500ms`.
   - Payload carries sequencing fields `U` (first update ID), `u` (final update ID), and `pu` (final update ID of the previous event) for chain-continuity checks (`pu` must equal the prior event's `u`).
   - Requires maintaining a local book: fetch a REST depth snapshot, buffer diffs, find the event straddling `lastUpdateId`, then apply diffs in order.

| | Partial Book Depth | Diff Book Depth |
|---|---|---|
| Message content | Full snapshot of top-N | Only changed levels |
| Depth | 5 / 10 / 20 (fixed) | Entire book |
| Update speed | 100 / 250 / 500 ms | **0** / 100 / 250 / 500 ms |
| Client work | None — read latest | Maintain local book + REST sync |
| Sequencing fields | present but unneeded | **required** (`U`/`u`/`pu`) |
| Best for | Simple top-of-book display | Faithful full-book reconstruction |

**This project uses the Diff stream** (`@depth@0ms`): only it provides the full-book depth needed for `book_snapshot_25` (beyond the Partial stream's 20-level cap) and the real-time `0ms` feed. That choice is what makes the REST-snapshot bootstrap, per-symbol diff buffering, and `pu == prev_u` resync logic necessary — we follow Binance's prescribed procedure in [How to manage a local order book correctly](https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly).

**Similar projects / libraries:**

Other Python tools that maintain a live local order book from the same Binance feed (none emit Tardis-format CSVs out of the box, which is this project's distinguishing feature):

 - [unicorn-binance-local-depth-cache](https://github.com/LUCIT-Systems-and-Development/unicorn-binance-local-depth-cache) — closest analog: REST snapshot + `@depth` diffs with automatic out-of-sync detection and resync, for many symbols.
 - [python-binance](https://github.com/sammchardy/python-binance) `DepthCacheManager` / `ThreadedDepthCacheManager` ([docs](https://python-binance.readthedocs.io/en/latest/depth_cache.html)) — long-standing community lib; re-fetches the REST snapshot periodically rather than purely event-driven.
 - [cryptofeed](https://github.com/bmoscon/cryptofeed) — multi-exchange feed handler that normalizes L2/L3 books and writes to pluggable backends (CSV, Parquet, Redis, Kafka, …); the broadest "feed → storage" analog.
 - [CCXT](https://github.com/ccxt/ccxt) `watch_order_book` — unified local-book maintenance across ~100 exchanges (the former "CCXT Pro" is now merged into the open-source package); convenient but hides Binance's exact sync algorithm.
