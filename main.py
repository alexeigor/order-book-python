import asyncio
import aiohttp
import aiofiles
import logging
import random
import argparse
import time
import os
from functools import partial
from itertools import islice

import uvloop

from simdjson import Parser
from sortedcontainers import SortedDict
from collections import deque, defaultdict
from pyinstrument import Profiler
from picows import ws_connect, WSFrame, WSTransport, WSListener, WSMsgType

from beartype import beartype
from beartype.typing import List, Dict, Deque, Tuple, Optional, Any, Callable, NamedTuple

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)


# Identifier for Binance USD-M futures, matching Tardis' exchange id.
EXCHANGE = "binance-futures"
# Number of price levels per side captured in each book_snapshot_5 row.
SNAPSHOT_DEPTH = 5

# A single price level as the exchange's original (price, qty) decimal strings.
Level = Tuple[str, str]


@beartype
class OrderBook:
    """
    A faithful local copy of one symbol's order book, assembled from a REST
    snapshot plus incremental diffs. The full depth is retained; the top-N is
    extracted only at read time. Levels are stored as the exchange's original
    decimal strings (keyed by float price purely for ordering) so emitted
    snapshots preserve exact values without float round-trip artifacts.
    """
    def __init__(self) -> None:
        self.bids: SortedDict[float, Level] = SortedDict(lambda x: -x)
        self.asks: SortedDict[float, Level] = SortedDict()
        self.last_update_id: Optional[int] = None

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.last_update_id = None

    def update_from_snapshot(self, snapshot: Dict[str, Any]) -> None:
        self.last_update_id = snapshot["lastUpdateId"]
        self.bids.clear()
        self.asks.clear()

        for price, qty in snapshot["bids"]:
            self.bids[float(price)] = (price, qty)
        for price, qty in snapshot["asks"]:
            self.asks[float(price)] = (price, qty)

    def apply_diff(self, bids: List[List[str]], asks: List[List[str]]) -> None:
        for price, qty in bids:
            p = float(price)
            if float(qty) == 0:
                self.bids.pop(p, None)
            else:
                self.bids[p] = (price, qty)

        for price, qty in asks:
            p = float(price)
            if float(qty) == 0:
                self.asks.pop(p, None)
            else:
                self.asks[p] = (price, qty)

    def top_levels(self, depth: int = SNAPSHOT_DEPTH) -> Tuple[List[Level], List[Level]]:
        # islice keeps this O(depth) even when the full book holds thousands of levels.
        bid_levels: List[Level] = list(islice(self.bids.values(), depth))
        ask_levels: List[Level] = list(islice(self.asks.values(), depth))

        # Defensive crossed-level removal (Tardis-faithful, read-path only): drop
        # any level that crosses the opposite best. Crossing is rare/transient and
        # self-heals on the next diff, so we never mutate the stored book here.
        if bid_levels and ask_levels:
            best_bid = float(bid_levels[0][0])
            best_ask = float(ask_levels[0][0])
            if best_bid >= best_ask:
                ask_levels = [lvl for lvl in ask_levels if float(lvl[0]) > best_bid]
                bid_levels = [lvl for lvl in bid_levels if float(lvl[0]) < best_ask]

        return bid_levels[:depth], ask_levels[:depth]


@beartype
class OrderBookCollection:
    """
    Manages a collection of OrderBook instances, one for each trading symbol.
    """
    def __init__(self, symbols: List[str]) -> None:
        # Initializes an order book for each symbol provided.
        # Expects symbols to be a list of uppercase symbol strings.
        self.order_books: Dict[str, OrderBook] = {
            symbol.upper(): OrderBook() for symbol in symbols
        }

    def get_book(self, symbol: str) -> Optional[OrderBook]:
        return self.order_books.get(symbol)

    def update_snapshot(self, symbol: str, snapshot_data: Dict[str, Any]) -> None:
        book = self.get_book(symbol)
        if book:
            book.update_from_snapshot(snapshot_data)
        else:
            log.warning(f"[{symbol}] Attempted to update snapshot for non-existent book.")

    def apply_diff(self, symbol: str, bids: List[List[str]], asks: List[List[str]]) -> None:
        book = self.get_book(symbol)
        if book:
            book.apply_diff(bids, asks)
        else:
            log.warning(f"[{symbol}] Attempted to apply diff to non-existent book.")

    def get_last_update_id(self, symbol: str) -> Optional[int]:
        book = self.get_book(symbol)
        return book.last_update_id if book else None

    def get_top_levels(self, symbol: str, depth: int = SNAPSHOT_DEPTH) -> Optional[Tuple[List[Level], List[Level]]]:
        book = self.get_book(symbol)
        return book.top_levels(depth) if book else None


# Fixed preamble shared by every Tardis data type's CSV.
PREAMBLE = ["exchange", "symbol", "timestamp", "local_timestamp"]

# Tardis `quotes` schema: top-of-book only, with the (asymmetric) column order
# ask_amount, ask_price, bid_price, bid_amount.
QUOTES_HEADER = ",".join(PREAMBLE + ["ask_amount", "ask_price", "bid_price", "bid_amount"])


@beartype
def build_snapshot_header(depth: int) -> str:
    """Tardis-style book_snapshot_<depth> header: preamble + per-level columns."""
    return ",".join(
        PREAMBLE
        + [
            f"{side}[{i}].{field}"
            for i in range(depth)
            for side, field in (("asks", "price"), ("asks", "amount"), ("bids", "price"), ("bids", "amount"))
        ]
    )


# Back-compat alias for the book_snapshot_5 header.
SNAPSHOT_HEADER = build_snapshot_header(SNAPSHOT_DEPTH)


@beartype
def build_snapshot_row(
    symbol: str,
    ts_us: int,
    local_us: int,
    asks: List[Level],
    bids: List[Level],
    depth: int = SNAPSHOT_DEPTH,
) -> str:
    """Build one Tardis-format book_snapshot_<depth> CSV line, padding missing levels."""
    fields: List[str] = [EXCHANGE, symbol.lower(), str(ts_us), str(local_us)]
    for i in range(depth):
        ask = asks[i] if i < len(asks) else ("", "")
        bid = bids[i] if i < len(bids) else ("", "")
        fields.extend((ask[0], ask[1], bid[0], bid[1]))
    return ",".join(fields)


@beartype
def build_quotes_row(
    symbol: str,
    ts_us: int,
    local_us: int,
    asks: List[Level],
    bids: List[Level],
) -> str:
    """Build one Tardis-format `quotes` CSV line (top of book only)."""
    ask = asks[0] if asks else ("", "")
    bid = bids[0] if bids else ("", "")
    # Level == (price, qty); quotes column order is ask_amount, ask_price, bid_price, bid_amount.
    fields: List[str] = [EXCHANGE, symbol.lower(), str(ts_us), str(local_us)]
    fields.extend((ask[1], ask[0], bid[0], bid[1]))
    return ",".join(fields)


@beartype
def dedup_key(asks: List[Level], bids: List[Level]) -> Tuple:
    """Hashable key for change-detection: the tracked levels of both sides."""
    return (tuple(asks), tuple(bids))


class DataTypeSpec(NamedTuple):
    name: str
    depth: int
    header: str
    row_builder: Callable[..., str]


# Registry of supported Tardis data types. All reconstruct from the same full
# order book; they differ only in depth and CSV schema.
DATA_TYPES: Dict[str, DataTypeSpec] = {
    "quotes": DataTypeSpec("quotes", 1, QUOTES_HEADER, build_quotes_row),
    "book_snapshot_5": DataTypeSpec(
        "book_snapshot_5", 5, build_snapshot_header(5), partial(build_snapshot_row, depth=5)
    ),
    "book_snapshot_25": DataTypeSpec(
        "book_snapshot_25", 25, build_snapshot_header(25), partial(build_snapshot_row, depth=25)
    ),
}


@beartype
async def snapshot_writer(queue: asyncio.Queue, path: str, header: str) -> None:
    """Single long-lived task that owns one output file and appends its rows."""
    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    async with aiofiles.open(path, mode="a", encoding="utf-8") as f:
        if write_header:
            await f.write(header + "\n")
            await f.flush()
        log.info(f"Snapshot writer started, appending to: {path}")
        while True:
            row = await queue.get()
            # Drain any rows that have piled up so we batch the flush.
            await f.write(row + "\n")
            try:
                while True:
                    await f.write(queue.get_nowait() + "\n")
            except asyncio.QueueEmpty:
                pass
            await f.flush()


@beartype
class BinanceStreamListener(WSListener):
    def __init__(
        self,
        symbols: List[str],
        order_book_collection: OrderBookCollection,
        snapshot_queues: Dict[str, asyncio.Queue],
        data_type_specs: List[DataTypeSpec],
        simulate_desync_flag: bool = False,
    ) -> None:
        super().__init__()
        self.symbols: List[str] = [s.upper() for s in symbols]
        self.order_books_collection = order_book_collection
        self.snapshot_queues = snapshot_queues
        self.data_type_specs = data_type_specs
        # Extract the book once at the deepest configured depth; each type slices a prefix.
        self.max_depth: int = max(spec.depth for spec in data_type_specs)
        self.buffers: Dict[str, Deque[Dict[str, Any]]] = defaultdict(deque)
        self.snapshot_received: Dict[str, bool] = {
            symbol: False for symbol in self.symbols
        }
        self.snapshot_fetching: Dict[str, bool] = {
            symbol: False for symbol in self.symbols
        }
        self.prev_u: Dict[str, int] = {}
        # Last emitted levels key per (data_type, symbol), for on-change dedup.
        self.last_emitted: Dict[Tuple[str, str], Tuple] = {}
        self.max_buffer_size: int = 1000
        self.json_parser = Parser()
        self.simulate_desync_flag: bool = simulate_desync_flag

    async def fetch_snapshot(self, symbol: str) -> None:
        symbol_upper = symbol.upper()
        if self.snapshot_fetching[symbol_upper]:
            return
        self.snapshot_fetching[symbol_upper] = True

        url: str = (
            f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol_upper}&limit=1000"
        )

        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data: Dict[str, Any] = await resp.json()
                    self.order_books_collection.update_snapshot(symbol_upper, data)
                    log.info(
                        f"[{symbol_upper}] Snapshot retrieved. lastUpdateId = {self.order_books_collection.get_last_update_id(symbol_upper)}"
                    )
                    await asyncio.sleep(3.0)
                    self.apply_buffered_events(symbol_upper)
        except Exception as e:
            log.error(f"[{symbol_upper}] Error fetching snapshot: {e}")
        finally:
            self.snapshot_fetching[symbol_upper] = False

    def apply_buffered_events(self, symbol: str) -> None:
        symbol_upper = symbol.upper()
        buffer = self.buffers[symbol_upper]
        new_buffer: Deque[Dict[str, Any]] = deque()

        for update in buffer:
            u = update["u"]
            U = update["U"]

            if u < self.order_books_collection.get_last_update_id(symbol_upper):
                continue
            if U <= self.order_books_collection.get_last_update_id(symbol_upper) <= u:
                log.info(f"[{symbol_upper}] Applying buffered events...")
                self.order_books_collection.apply_diff(symbol_upper, update["b"], update["a"])
                self.prev_u[symbol_upper] = u
                self.snapshot_received[symbol_upper] = True
            elif self.snapshot_received[symbol_upper]:
                if update["pu"] != self.prev_u[symbol_upper]:
                    log.warning(
                        f"[{symbol_upper}] Out of sync! Restarting snapshot process..."
                    )
                    self.snapshot_received[symbol_upper] = False
                    self.order_books_collection.get_book(symbol_upper).clear()
                    asyncio.create_task(self.fetch_snapshot(symbol_upper))
                    return
                self.order_books_collection.apply_diff(symbol_upper, update["b"], update["a"])
                self.prev_u[symbol_upper] = u

        log.info(f"[{symbol_upper}] All buffered events applied.")
        self.buffers[symbol_upper] = new_buffer

        if not self.snapshot_received[symbol_upper]:
            log.info(f"[{symbol_upper}] No snapshot received yet.")
            asyncio.create_task(self.delayed_snapshot_fetch(symbol))

    def on_ws_connected(self, transport: WSTransport) -> None:
        log.info("Connected to Binance WebSocket")
        for symbol in self.symbols:
            asyncio.create_task(self.delayed_snapshot_fetch(symbol))

    async def delayed_snapshot_fetch(self, symbol: str, delay: float = 3.0) -> None:
        log.info(
            f"[{symbol.upper()}] Waiting {delay:.1f} second(s) before fetching snapshot..."
        )
        await asyncio.sleep(delay)
        await self.fetch_snapshot(symbol)

    def emit_snapshot(self, symbol: str, event_time_ms: int, local_us: int) -> None:
        """Emit a row for each configured data type, but only when its levels change."""
        top = self.order_books_collection.get_top_levels(symbol, self.max_depth)
        if top is None:
            return
        bids, asks = top
        ts_us = event_time_ms * 1000
        for spec in self.data_type_specs:
            a = asks[: spec.depth]
            b = bids[: spec.depth]
            key = dedup_key(a, b)
            dkey = (spec.name, symbol)
            if key == self.last_emitted.get(dkey):
                continue
            self.last_emitted[dkey] = key
            row = spec.row_builder(symbol, ts_us, local_us, a, b)
            try:
                self.snapshot_queues[spec.name].put_nowait(row)
            except asyncio.QueueFull:
                log.warning(f"[{symbol}] {spec.name} queue full; dropping row.")

    def on_ws_frame(self, transport: WSTransport, frame: WSFrame) -> None:
        if frame.msg_type != WSMsgType.TEXT:
            return

        local_us: int = time.time_ns() // 1000

        try:
            message: str = frame.get_payload_as_ascii_text()
            parsed: Dict[str, Any] = self.json_parser.parse(message).as_dict()
            data: Dict[str, Any] = parsed.get("data", {})

            if not data:
                return

            symbol: str = data["s"]
            update: Dict[str, Any] = {
                "e": data["e"],
                "E": data["E"],
                "s": symbol,
                "U": data["U"],
                "u": data["u"],
                "pu": data["pu"],
                "b": data.get("b", []),
                "a": data.get("a", []),
            }

            simulate_desync = False
            if self.simulate_desync_flag and self.snapshot_received[symbol]:
                simulate_desync = random.random() < 0.01
                if simulate_desync:
                    log.warning(f"[{symbol}] *** Simulating desync (pu != prev_u) ***")
                    update["pu"] = update["pu"] - 1

            if not self.snapshot_received[symbol]:
                self.buffers[symbol].append(update)
                if len(self.buffers[symbol]) > self.max_buffer_size:
                    self.buffers[symbol].popleft()
            else:
                if update["pu"] != self.prev_u.get(symbol):
                    log.warning(
                        f"[{symbol}] Desync detected. Restarting from snapshot."
                    )
                    self.snapshot_received[symbol] = False
                    self.buffers[symbol].clear()
                    # Drop dedup state for every data type of this symbol so each
                    # re-emits a fresh first row after resync.
                    for dkey in [k for k in self.last_emitted if k[1] == symbol]:
                        del self.last_emitted[dkey]
                    asyncio.create_task(self.delayed_snapshot_fetch(symbol))
                    return

                self.order_books_collection.apply_diff(symbol, update["b"], update["a"])
                self.prev_u[symbol] = update["u"]

                self.emit_snapshot(symbol, update["E"], local_us)

        except Exception as e:
            log.error(f"Error during frame processing: {e}")

    def on_ws_disconnected(self, transport: WSTransport) -> None:
        log.warning("Disconnected from Binance WebSocket")


@beartype
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Binance Tardis-style market-data collector")
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="List of trading pairs (e.g. btcusdt ethusdt bnbusdt)",
    )

    parser.add_argument(
        "--data-types",
        nargs="+",
        choices=list(DATA_TYPES),
        default=["book_snapshot_5"],
        help="Tardis data types to collect (one CSV file per type in --output-dir).",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directory for the output CSV files (one per data type, e.g. book_snapshot_5.csv).",
    )

    parser.add_argument(
        "--simulate-desync",
        action="store_true",
        help="Enable simulation of desynchronization (for testing purposes)"
    )

    return parser.parse_args()


@beartype
async def main(args: argparse.Namespace) -> None:

    symbols = [symbol.upper() for symbol in args.symbols]
    log.info(f"Selected symbols: {', '.join(symbols)}")
    if args.simulate_desync:
        log.warning("Desynchronization simulation is ENABLED.")
    simulate_desync = args.simulate_desync

    specs = [DATA_TYPES[name] for name in args.data_types]
    os.makedirs(args.output_dir, exist_ok=True)
    log.info(f"Collecting data types {[s.name for s in specs]} into: {args.output_dir}")

    order_book_collection = OrderBookCollection(symbols)

    # One queue + one writer task per data type; each writes its own file/header.
    snapshot_queues: Dict[str, "asyncio.Queue[str]"] = {
        spec.name: asyncio.Queue() for spec in specs
    }
    for spec in specs:
        path = os.path.join(args.output_dir, f"{spec.name}.csv")
        asyncio.create_task(snapshot_writer(snapshot_queues[spec.name], path, spec.header))

    while True:
        try:

            streams = "/".join(f"{symbol.lower()}@depth@0ms" for symbol in symbols)
            binance_ws_url = f"wss://fstream.binance.com/stream?streams={streams}"

            log.info(f"Connecting to: {binance_ws_url}")

            # picows passes the negotiated WSUpgradeRequest/Response to the factory;
            # accept and ignore them so they don't collide with __init__ params.
            def listener_factory(*_args):
                return BinanceStreamListener(
                    symbols=symbols,
                    order_book_collection=order_book_collection,
                    snapshot_queues=snapshot_queues,
                    data_type_specs=specs,
                    simulate_desync_flag=simulate_desync,
                )
            # enable_auto_ping lets picows detect a silently-stalled connection
            # (no data, no FIN): after ~10s idle it pings, and with no reply it
            # disconnects, so wait_disconnected() returns and we reconnect+resync.
            transport, _ = await ws_connect(
                listener_factory, binance_ws_url, enable_auto_ping=True
            )
            await transport.wait_disconnected()
        except Exception as e:
            log.error(f"Connection error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    args = parse_args()

    profiler = Profiler()
    profiler.start()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        log.info("Stopped manually.")
    except Exception as e:
        log.critical(f"Critical error in main execution: {e}", exc_info=True)
    finally:
        profiler.stop()
        with open("profile_report.html", "w") as f:
            f.write(profiler.output_html())
        log.info("Saved profiler report to profile_report.html")
