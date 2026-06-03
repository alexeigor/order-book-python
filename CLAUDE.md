# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Dependencies are managed by `uv` (see `pyproject.toml`); Python 3.12 is required.

```bash
# Run the collector: one CSV per data type written to --output-dir (all symbols share each file)
uv run --isolated main.py --data-types quotes book_snapshot_5 book_snapshot_25 --symbols btcusdt ethusdt --output-dir ./data

# Collect from Binance SPOT instead of the default USD-M futures (--market futures|spot)
uv run --isolated main.py --market spot --symbols btcusdt --data-types book_snapshot_5 --output-dir ./spot

# Simulate desync / packet loss (~1% chance per frame, for testing the resync path)
uv run --isolated main.py --symbols btcusdt --simulate-desync

# Run all tests
uv run --isolated pytest test_order_book.py

# Run a single test
uv run --isolated pytest test_order_book.py::test_top_levels
```

On exit (including Ctrl-C), a `pyinstrument` profile is always written to `profile_report.html`.

## Architecture

The entire application lives in `main.py`. It maintains faithful live local copies of Binance order books — **USD-M futures (default) or spot**, selected via `--market` — and emits one or more Tardis-style data types: the reconstructed top-N snapshots (`quotes`, `book_snapshot_5`, `book_snapshot_25`) and the raw-feed `incremental_book_L2`.

**Market selection (`--market futures|spot`):** all exchange-specific values live in a `MarketSpec` (`MARKETS` registry) — REST depth endpoint, websocket base, per-symbol stream suffix, Tardis `exchange_id`, snapshot limit, and an `is_spot` flag that selects the sync-rule variant. `main()` picks the spec and overrides the module-level `EXCHANGE` (read by the row builders) from it. **Spot and futures differ in four places only**: endpoints (`api.binance.com/api/v3/depth` + `stream.binance.com:9443` vs `fapi.binance.com` + `fstream.binance.com`), exchange id (`binance` vs `binance-futures`), stream speed (spot `@depth@100ms` — **spot has no `0ms`**; futures `@depth@0ms`), and the sync algorithm (below). Everything else (book maintenance, dedup, writers, Tardis output) is exchange-agnostic.

**Data flow:** `BinanceStreamListener` (a `picows` `WSListener`) subscribes to `<symbol>@depth@0ms` diff streams. Incoming frames are parsed with `pysimdjson` and applied as diffs to per-symbol `OrderBook` instances held in `OrderBookCollection`. After each applied diff, `emit_snapshot` extracts the top levels **once** at `max_depth` (the deepest selected data type), then for each selected snapshot data type slices its prefix and — **only if those levels changed since the last emission** (dedup keyed by `(data_type, symbol)` in `last_emitted`) — enqueues a CSV row. One long-lived `snapshot_writer` task per data type drains its own `asyncio.Queue` and appends to its own file. The queue indirection exists because `on_ws_frame` is a **synchronous** picows callback and cannot `await`; it uses `put_nowait`.

**`incremental_book_L2` is a separate path** (not a top-N snapshot, so it bypasses `emit_snapshot`/dedup). When selected (`emit_incremental=True`), `emit_incremental_rows` writes the raw L2 feed verbatim: the REST snapshot is emitted as an `is_snapshot=true` block from inside `fetch_snapshot` (before the ~3s catch-up, so it precedes the deltas), and every **applied** diff is emitted as `is_snapshot=false` rows from both diff-apply sites (`apply_buffered_events` catch-up and the `on_ws_frame` live path). One row per changed level (`side,price,amount`; absolute amount, `0` = removal). A resync simply re-emits a fresh `is_snapshot=true` block. Because buffered events are emitted only when applied (after the snapshot), the file is always ordered snapshot-block → deltas; the buffered `update` dict therefore carries a `local_us` receive-time field so a faithful `local_timestamp` survives the buffering.

**Data-type registry:** `DATA_TYPES` maps each **snapshot** name to a `DataTypeSpec(name, depth, header, row_builder)`. `quotes` (depth 1) uses `build_quotes_row` with the asymmetric `ask_amount,ask_price,bid_price,bid_amount` column order; `book_snapshot_5`/`_25` share `build_snapshot_row` bound to a depth via `functools.partial`. These builders are pure module-level functions `(symbol, ts_us, local_us, asks, bids) -> str` (unit-tested directly). Headers come from `build_snapshot_header(depth)` / `QUOTES_HEADER`. Because the crossed-filter compares index-0 best bid/ask and is monotone, the top-5 view is exactly the first-5 prefix of the top-25 view — so the same extraction feeds every snapshot type consistently. Adding a new **snapshot** data type = one registry entry + a builder + tests. `incremental_book_L2` lives **outside** `DATA_TYPES` (it emits many raw rows per event via `build_incremental_rows`, not one top-N row): `ALL_DATA_TYPE_NAMES` = `list(DATA_TYPES) + [INCREMENTAL_NAME]` is the CLI `--data-types` choice list, and `header_for(name)` resolves the header for either kind.

**The synchronization protocol is the core complexity** — it follows Binance's prescribed "manage a local order book" algorithm and is the part most likely to break if edited carelessly:
- On connect, diff events are buffered per symbol until a REST depth snapshot (`…/depth?limit=1000`) arrives. `delayed_snapshot_fetch` deliberately waits ~3s before fetching so a buffer exists.
- The three sync comparisons that differ between markets are extracted as pure, unit-tested functions — `should_discard`, `straddles`, `is_continuous` — each branching on `is_spot`. `apply_buffered_events` discards stale events (futures `u < lastUpdateId` / spot `u <= lastUpdateId`), finds the first event that straddles the snapshot (futures `U <= lastUpdateId <= u` / spot `U <= lastUpdateId+1 <= u`), then applies subsequent events.
- After sync, each live event must be **continuous**: futures checks `pu == prev_u` (previous event's `u`); **spot has no `pu`** and instead checks `U == prev_u + 1`. A mismatch means a dropped/out-of-order update → the book is cleared, `snapshot_received` reset, `last_emitted` dropped, and the snapshot process restarts. This check appears in both `on_ws_frame` (live path) and `apply_buffered_events` (catch-up path). `--simulate-desync` breaks the right invariant per market (bumps `pu` on futures, `U` on spot).
- The outer `while True` loop in `main()` reconnects on any WebSocket failure. A reconnect builds a fresh `BinanceStreamListener`, so per-connection state (buffers, dedup) resets — correct, since reconnect forces a full resync.

**Key invariants:**
- `bids` is a `SortedDict` keyed by float price with reversed ordering (highest first); `asks` ascending. Level **values** are the exchange's original `(price_str, qty_str)` decimal strings — kept verbatim so emitted snapshots avoid float round-trip artifacts. The float key is only for ordering.
- The book is retained in full **down to `BOOK_DEPTH_LIMIT` (1000) levels per side** — the same depth Binance's REST snapshot seeds. `update_from_snapshot` loads all 1000 levels; `apply_diff` (full-book diffs, absolute quantities, removal on qty 0) then calls `_prune_orphans()` to drop anything deeper than 1000. This is required: the `@depth` diff stream also pushes changes to levels *deeper* than the snapshot ever provided, and those orphaned levels (never seeded, may never get a qty=0 removal) otherwise accumulate without bound — a verified leak that grew BTCUSDT from 1000 to ~3500/side in ~70s. **Critically, this prune is at the snapshot-depth boundary (1000), NOT the top-N display depth.** Do *not* trim the stored book to the display depth (5/25) — that loses diffs to levels just outside the view and corrupts assembly (a prior bug); `BOOK_DEPTH_LIMIT` is far deeper than any emitted snapshot, so pruning there is safe. The top-N is extracted only at read time in `top_levels(depth)`, which uses `itertools.islice` (O(depth), not O(N)) and applies a defensive **crossed-level filter** without mutating the stored book.
- There are intentionally **no** `best_bid < best_ask` asserts. They previously crashed an empty/one-sided/crossed book during resync (and aborted frames mid-way, leaving `prev_u` unadvanced → spurious desyncs). The read-time crossed filter is the defense instead.

**Stream choice / granularity:** on **futures** we subscribe to `@depth@0ms` — Binance's real-time, dynamically-adjusted diff-depth stream — **not** the throttled `@depth@100ms`. `0ms` is a **documented** futures update-speed option for the Diff Book Depth stream (alongside `100ms`, `250ms` default, and `500ms`); the Partial Book Depth stream is throttled and offers no `0ms`. This yields event-driven, routinely sub-100 ms updates (~20-40 rows/sec on BTCUSDT), matching Tardis's datasets (which reconstruct from the same L2 feed). Do not revert futures to `@100ms` thinking 100 ms is a hard floor — it isn't; that was an earlier misconception. **Spot is different: it has no `0ms` (only `100ms`/`1000ms`).** Binance *silently accepts* a `@depth@0ms` spot subscription but never delivers data, so the spot `MarketSpec` uses `@depth@100ms` — do not "unify" the two markets on `0ms`, as that would make spot collect nothing while looking healthy (verified live).

**Type checking:** every class and the top-level functions are decorated with `@beartype`, so runtime type errors surface as `BeartypeCallHintParamViolation`. Keep type hints accurate — they are enforced, not decorative. Types are imported from `beartype.typing`. Note the picows listener factory must accept/ignore the negotiated upgrade-request args (`def listener_factory(*_args)`), or beartype rejects them against `__init__`.

## Notes

- Output is Tardis-compatible CSV, one file per `--data-type`. `book_snapshot_N`: `exchange,symbol,timestamp,local_timestamp,` then `asks[i].price,asks[i].amount,bids[i].price,bids[i].amount` for i in 0..N-1. `quotes`: `...,ask_amount,ask_price,bid_price,bid_amount` (note the order). `incremental_book_L2`: `...,is_snapshot,side,price,amount` (one row per changed level; `side`=`bid`/`ask`, absolute `amount`, `0`=removal). `exchange`=`binance-futures` (futures) or `binance` (spot, `--market spot`), lowercase symbol, microsecond timestamps. See `DATA_TYPES` / `header_for` / `MARKETS`.
- `notebooks/symbols.ipynb` is a scratch notebook for exploring available symbols and is not part of the runtime.
- The codebase targets a single file by design; the README lists splitting into modules and integer-based tick/step-size math as future improvements.
