# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependencies are managed by `uv` (see `pyproject.toml`); Python 3.12 is required.

```bash
# Run the collector: one CSV per data type written to --output-dir (all symbols share each file)
uv run --isolated main.py --data-types quotes book_snapshot_5 book_snapshot_25 --symbols btcusdt ethusdt --output-dir ./data

# Simulate desync / packet loss (~1% chance per frame, for testing the resync path)
uv run --isolated main.py --symbols btcusdt --simulate-desync

# Run all tests
uv run --isolated pytest test_order_book.py

# Run a single test
uv run --isolated pytest test_order_book.py::test_top_levels
```

On exit (including Ctrl-C), a `pyinstrument` profile is always written to `profile_report.html`.

## Architecture

The entire application lives in `main.py`. It maintains faithful live local copies of Binance USD-M futures order books and emits one or more Tardis-style data types (`quotes`, `book_snapshot_5`, `book_snapshot_25`).

**Data flow:** `BinanceStreamListener` (a `picows` `WSListener`) subscribes to `<symbol>@depth@0ms` diff streams. Incoming frames are parsed with `pysimdjson` and applied as diffs to per-symbol `OrderBook` instances held in `OrderBookCollection`. After each applied diff, `emit_snapshot` extracts the top levels **once** at `max_depth` (the deepest selected data type), then for each selected data type slices its prefix and — **only if those levels changed since the last emission** (dedup keyed by `(data_type, symbol)` in `last_emitted`) — enqueues a CSV row. One long-lived `snapshot_writer` task per data type drains its own `asyncio.Queue` and appends to its own file. The queue indirection exists because `on_ws_frame` is a **synchronous** picows callback and cannot `await`; it uses `put_nowait`.

**Data-type registry:** `DATA_TYPES` maps each name to a `DataTypeSpec(name, depth, header, row_builder)`. `quotes` (depth 1) uses `build_quotes_row` with the asymmetric `ask_amount,ask_price,bid_price,bid_amount` column order; `book_snapshot_5`/`_25` share `build_snapshot_row` bound to a depth via `functools.partial`. All builders are pure module-level functions `(symbol, ts_us, local_us, asks, bids) -> str` (unit-tested directly). Headers come from `build_snapshot_header(depth)` / `QUOTES_HEADER`. Because the crossed-filter compares index-0 best bid/ask and is monotone, the top-5 view is exactly the first-5 prefix of the top-25 view — so the same extraction feeds every type consistently. Adding a new data type = one registry entry + a builder + tests.

**The synchronization protocol is the core complexity** — it follows Binance's prescribed "manage a local order book" algorithm and is the part most likely to break if edited carelessly:
- On connect, diff events are buffered per symbol until a REST depth snapshot (`fapi/v1/depth?limit=1000`) arrives. `delayed_snapshot_fetch` deliberately waits ~3s before fetching so a buffer exists.
- `apply_buffered_events` discards events older than the snapshot's `lastUpdateId`, finds the first event that straddles it (`U <= lastUpdateId <= u`), then applies subsequent events.
- After sync, each live event must satisfy `pu == prev_u` (previous event's `u`). A mismatch means a dropped/out-of-order update → the book is cleared, `snapshot_received` reset, `last_emitted` dropped, and the snapshot process restarts. This continuity check appears in both `on_ws_frame` (live path) and `apply_buffered_events` (catch-up path).
- The outer `while True` loop in `main()` reconnects on any WebSocket failure. A reconnect builds a fresh `BinanceStreamListener`, so per-connection state (buffers, dedup) resets — correct, since reconnect forces a full resync.

**Key invariants:**
- `bids` is a `SortedDict` keyed by float price with reversed ordering (highest first); `asks` ascending. Level **values** are the exchange's original `(price_str, qty_str)` decimal strings — kept verbatim so emitted snapshots avoid float round-trip artifacts. The float key is only for ordering.
- The **full** book is retained: `update_from_snapshot` loads all 1000 levels and does **not** trim, and `apply_diff` (full-book diffs, absolute quantities, removal on qty 0) does not trim either. The top-N is extracted only at read time in `top_levels(depth)`, which uses `itertools.islice` (O(depth), not O(N)) and applies a defensive **crossed-level filter** without mutating the stored book. Do not reintroduce trimming of the stored book — it corrupts the assembly (this was a prior bug).
- There are intentionally **no** `best_bid < best_ask` asserts. They previously crashed an empty/one-sided/crossed book during resync (and aborted frames mid-way, leaving `prev_u` unadvanced → spurious desyncs). The read-time crossed filter is the defense instead.

**Stream choice / granularity:** we subscribe to `@depth@0ms` — Binance's real-time, dynamically-adjusted diff-depth stream — **not** the throttled `@depth@100ms`. This yields event-driven, routinely sub-100 ms updates (~20-40 rows/sec on BTCUSDT), matching Tardis's datasets (which reconstruct from the same L2 feed). Do not revert to `@100ms` thinking 100 ms is a hard floor — it isn't; that was an earlier misconception.

**Type checking:** every class and the top-level functions are decorated with `@beartype`, so runtime type errors surface as `BeartypeCallHintParamViolation`. Keep type hints accurate — they are enforced, not decorative. Types are imported from `beartype.typing`. Note the picows listener factory must accept/ignore the negotiated upgrade-request args (`def listener_factory(*_args)`), or beartype rejects them against `__init__`.

## Notes

- Output is Tardis-compatible CSV, one file per `--data-type`. `book_snapshot_N`: `exchange,symbol,timestamp,local_timestamp,` then `asks[i].price,asks[i].amount,bids[i].price,bids[i].amount` for i in 0..N-1. `quotes`: `...,ask_amount,ask_price,bid_price,bid_amount` (note the order). `exchange`=`binance-futures`, lowercase symbol, microsecond timestamps. See `DATA_TYPES`.
- `notebooks/symbols.ipynb` is a scratch notebook for exploring available symbols and is not part of the runtime.
- The codebase targets a single file by design; the README lists splitting into modules and integer-based tick/step-size math as future improvements.
