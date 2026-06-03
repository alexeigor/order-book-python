import pytest
from main import (
    OrderBook,
    build_snapshot_row,
    build_snapshot_header,
    build_quotes_row,
    build_incremental_rows,
    dedup_key,
    header_for,
    SNAPSHOT_HEADER,
    QUOTES_HEADER,
    INCREMENTAL_BOOK_L2_HEADER,
    INCREMENTAL_NAME,
    ALL_DATA_TYPE_NAMES,
    SNAPSHOT_DEPTH,
    BOOK_DEPTH_LIMIT,
    DATA_TYPES,
    MARKETS,
    should_discard,
    straddles,
    is_continuous,
)


@pytest.fixture
def order_book():
    return OrderBook()


def test_update_from_snapshot(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.5"], ["99.5", "2.0"], ["99.0", "1.0"]],
        "asks": [["101.0", "1.0"], ["101.5", "2.5"], ["102.0", "1.0"]],
    }

    order_book.update_from_snapshot(snapshot)

    assert order_book.last_update_id == 1001
    assert len(order_book.bids) == 3
    assert len(order_book.asks) == 3
    # Levels are stored as the exchange's original (price, qty) decimal strings.
    assert order_book.bids[100.0] == ("100.0", "1.5")
    assert order_book.asks[101.0] == ("101.0", "1.0")


def test_apply_diff_add_and_remove(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.0"]],
        "asks": [["101.0", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    # Apply diff: update bid, add a bid, remove an ask, add an ask
    order_book.apply_diff(
        bids=[["100.0", "2.0"], ["99.5", "1.0"]],
        asks=[["101.0", "0.0"], ["102.0", "1.5"]],
    )

    assert order_book.bids[100.0] == ("100.0", "2.0")
    assert order_book.bids[99.5] == ("99.5", "1.0")
    assert 101.0 not in order_book.asks
    assert order_book.asks[102.0] == ("102.0", "1.5")


def test_full_book_is_retained(order_book):
    # The snapshot is no longer trimmed; all levels are kept so the book stays
    # faithful as full-book diffs are applied on top of it.
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [[str(100 - i), "1.0"] for i in range(10)],  # 100 down to 91
        "asks": [[str(101 + i), "1.0"] for i in range(10)],  # 101 up to 110
    }
    order_book.update_from_snapshot(snapshot)

    assert len(order_book.bids) == 10
    assert len(order_book.asks) == 10
    # Bids ordered high -> low, asks low -> high.
    assert list(order_book.bids.keys())[:3] == [100.0, 99.0, 98.0]
    assert list(order_book.asks.keys())[:3] == [101.0, 102.0, 103.0]


def test_apply_diff_prunes_orphans_beyond_limit(order_book):
    # Deep levels pushed by diffs (beyond the snapshot's reach) must not
    # accumulate without bound: each side is capped at BOOK_DEPTH_LIMIT, with the
    # deepest levels dropped and the best (top-of-book) levels retained.
    snapshot = {
        "lastUpdateId": 1,
        "bids": [["100.0", "1.0"]],
        "asks": [["101.0", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    # Add far more than the limit of deep levels on each side via a diff.
    extra = BOOK_DEPTH_LIMIT + 50
    order_book.apply_diff(
        bids=[[str(99.0 - i * 0.1), "1.0"] for i in range(extra)],   # below best bid
        asks=[[str(102.0 + i * 0.1), "1.0"] for i in range(extra)],  # above best ask
    )

    assert len(order_book.bids) == BOOK_DEPTH_LIMIT
    assert len(order_book.asks) == BOOK_DEPTH_LIMIT
    # Best levels survive; the deepest were the ones dropped.
    assert order_book.bids.peekitem(0)[1] == ("100.0", "1.0")
    assert order_book.asks.peekitem(0)[1] == ("101.0", "1.0")


def test_apply_diff_under_limit_retains_all(order_book):
    # Below the cap, nothing is pruned — the full book is still retained.
    snapshot = {"lastUpdateId": 1, "bids": [["100.0", "1.0"]], "asks": [["101.0", "1.0"]]}
    order_book.update_from_snapshot(snapshot)
    order_book.apply_diff(
        bids=[[str(99 - i), "1.0"] for i in range(10)],
        asks=[[str(102 + i), "1.0"] for i in range(10)],
    )
    assert len(order_book.bids) == 11
    assert len(order_book.asks) == 11


def test_top_levels(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.0"], ["99.5", "1.5"]],
        "asks": [["101.0", "2.0"], ["101.5", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    bid_levels, ask_levels = order_book.top_levels()

    assert bid_levels == [("100.0", "1.0"), ("99.5", "1.5")]
    assert ask_levels == [("101.0", "2.0"), ("101.5", "1.0")]


def test_top_levels_depth_limit(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [[str(100 - i), "1.0"] for i in range(10)],
        "asks": [[str(101 + i), "1.0"] for i in range(10)],
    }
    order_book.update_from_snapshot(snapshot)

    bid_levels, ask_levels = order_book.top_levels(SNAPSHOT_DEPTH)

    assert len(bid_levels) == SNAPSHOT_DEPTH
    assert len(ask_levels) == SNAPSHOT_DEPTH
    assert [p for p, _ in bid_levels] == ["100", "99", "98", "97", "96"]
    assert [p for p, _ in ask_levels] == ["101", "102", "103", "104", "105"]


def test_apply_diff_zero_quantity_removal(order_book):
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["100.0", "1.0"]],
        "asks": [["101.0", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    order_book.apply_diff(
        bids=[["100.0", "0"]],
        asks=[["101.0", "0"]],
    )

    assert 100.0 not in order_book.bids
    assert 101.0 not in order_book.asks


def test_top_levels_removes_crossed_levels(order_book):
    # A crossed book (best bid >= an ask) should have the crossing levels dropped
    # from the read-time view without mutating the stored book.
    snapshot = {
        "lastUpdateId": 1001,
        "bids": [["101.5", "1.0"], ["100.0", "1.0"]],
        "asks": [["101.0", "1.0"], ["102.0", "1.0"]],
    }
    order_book.update_from_snapshot(snapshot)

    bid_levels, ask_levels = order_book.top_levels()

    # best bid 101.5 crosses ask 101.0: the crossing ask is filtered out.
    assert ("101.0", "1.0") not in ask_levels
    assert ask_levels[0] == ("102.0", "1.0")
    # Stored book is untouched.
    assert 101.0 in order_book.asks


def test_build_snapshot_row_pads_missing_levels():
    asks = [("101.0", "2.0")]
    bids = [("100.0", "1.0")]
    row = build_snapshot_row("BTCUSDT", 1700000000000000, 1700000000000123, asks, bids)
    fields = row.split(",")

    # exchange, symbol(lowercased), timestamp, local_timestamp + 4 fields * depth
    assert len(fields) == 4 + 4 * SNAPSHOT_DEPTH
    assert fields[0] == "binance-futures"
    assert fields[1] == "btcusdt"
    assert fields[2] == "1700000000000000"
    # level 0: asks[0].price, asks[0].amount, bids[0].price, bids[0].amount
    assert fields[4:8] == ["101.0", "2.0", "100.0", "1.0"]
    # level 1 missing -> empty fields
    assert fields[8:12] == ["", "", "", ""]


def test_snapshot_header_shape():
    fields = SNAPSHOT_HEADER.split(",")
    assert fields[:4] == ["exchange", "symbol", "timestamp", "local_timestamp"]
    assert fields[4] == "asks[0].price"
    assert len(fields) == 4 + 4 * SNAPSHOT_DEPTH


# --- Multi-data-type collection ---------------------------------------------

def test_quotes_header_exact():
    assert QUOTES_HEADER == (
        "exchange,symbol,timestamp,local_timestamp,"
        "ask_amount,ask_price,bid_price,bid_amount"
    )


def test_book_snapshot_5_header_exact():
    fields = build_snapshot_header(5).split(",")
    assert fields[:4] == ["exchange", "symbol", "timestamp", "local_timestamp"]
    assert fields[4] == "asks[0].price"
    assert fields[-1] == "bids[4].amount"
    assert len(fields) == 4 + 4 * 5  # 24


def test_book_snapshot_25_header_exact():
    fields = build_snapshot_header(25).split(",")
    assert fields[-1] == "bids[24].amount"
    assert len(fields) == 4 + 4 * 25  # 104


def test_build_quotes_row_column_order():
    asks = [("101.0", "2.0")]
    bids = [("100.0", "1.0")]
    row = build_quotes_row("BTCUSDT", 1700000000000000, 1700000000000123, asks, bids)
    fields = row.split(",")

    assert len(fields) == 8
    assert fields[:4] == ["binance-futures", "btcusdt", "1700000000000000", "1700000000000123"]
    # Tardis quotes order: ask_amount, ask_price, bid_price, bid_amount
    assert fields[4:8] == ["2.0", "101.0", "100.0", "1.0"]


def test_build_quotes_row_empty_sides():
    row = build_quotes_row("BTCUSDT", 1, 2, [], [])
    fields = row.split(",")
    assert len(fields) == 8
    assert fields[4:8] == ["", "", "", ""]


def test_build_snapshot_row_depth_25_padding():
    asks = [("101.0", "2.0")]
    bids = [("100.0", "1.0")]
    row = build_snapshot_row("BTCUSDT", 1, 2, asks, bids, depth=25)
    fields = row.split(",")

    assert len(fields) == 4 + 4 * 25  # 104
    assert fields[4:8] == ["101.0", "2.0", "100.0", "1.0"]  # level 0
    assert fields[8:] == [""] * (4 * 24)  # levels 1..24 all empty


def test_build_snapshot_row_default_depth_is_5():
    asks = [("101.0", "2.0")]
    bids = [("100.0", "1.0")]
    row = build_snapshot_row("BTCUSDT", 1, 2, asks, bids)  # no depth arg
    assert len(row.split(",")) == 24


def test_bs5_is_prefix_of_bs25():
    asks = [(str(101 + i), "1.0") for i in range(25)]
    bids = [(str(100 - i), "1.0") for i in range(25)]
    bs5 = build_snapshot_row("BTCUSDT", 1, 2, asks, bids, depth=5).split(",")
    bs25 = build_snapshot_row("BTCUSDT", 1, 2, asks, bids, depth=25).split(",")

    # Data columns of bs5 equal the first 5 levels (4*5 columns) of bs25.
    assert bs5[4:] == bs25[4 : 4 + 4 * 5]


def test_top_levels_crossed_filter_depth_25():
    order_book = OrderBook()
    # Best bid (101.5) crosses the first two asks (101.0, 101.4); 25+ levels each side.
    snapshot = {
        "lastUpdateId": 1,
        "bids": [["101.5", "1.0"]] + [[str(101 - i), "1.0"] for i in range(25)],
        "asks": [[str(101 + i * 0.1), "1.0"] for i in range(27)],
    }
    order_book.update_from_snapshot(snapshot)

    bid_levels, ask_levels = order_book.top_levels(25)
    best_bid = float(bid_levels[0][0])
    best_ask = float(ask_levels[0][0])

    assert best_bid < best_ask
    assert all(float(p) > best_bid for p, _ in ask_levels)
    assert all(float(p) < best_ask for p, _ in bid_levels)
    # Stored book is untouched (crossing asks still present).
    assert 101.0 in order_book.asks


def test_dedup_key_helper():
    a1 = [("101.0", "2.0")]
    b1 = [("100.0", "1.0")]
    assert dedup_key(a1, b1) == dedup_key([("101.0", "2.0")], [("100.0", "1.0")])
    # A change in amount produces a different key.
    assert dedup_key(a1, b1) != dedup_key([("101.0", "3.0")], b1)


# --- market specs & sync rules (spot vs. futures) ----------------------------

def test_markets_registry():
    spot = MARKETS["spot"]
    fut = MARKETS["futures"]

    assert fut.exchange_id == "binance-futures"
    assert fut.is_spot is False
    assert fut.stream_suffix == "@depth@0ms"
    assert "fapi.binance.com" in fut.rest_depth_url
    assert "fstream.binance.com" in fut.ws_base

    assert spot.exchange_id == "binance"
    assert spot.is_spot is True
    # Spot has no 0ms diff stream — must be 100ms, never the futures-only 0ms.
    assert spot.stream_suffix == "@depth@100ms"
    assert spot.stream_suffix != "@depth@0ms"
    assert "api.binance.com/api/v3/depth" in spot.rest_depth_url
    assert "stream.binance.com" in spot.ws_base

    # Snapshot limit must not exceed the prune cap (orphan-pruning relies on this).
    for spec in (spot, fut):
        assert spec.snapshot_limit <= BOOK_DEPTH_LIMIT


def test_should_discard_spot_vs_futures():
    # Event strictly older than the snapshot is discarded by both.
    assert should_discard(u=99, last_update_id=100, is_spot=True) is True
    assert should_discard(u=99, last_update_id=100, is_spot=False) is True
    # Boundary u == lastUpdateId: spot discards (<=), futures keeps (<).
    assert should_discard(u=100, last_update_id=100, is_spot=True) is True
    assert should_discard(u=100, last_update_id=100, is_spot=False) is False
    # Newer event kept by both.
    assert should_discard(u=101, last_update_id=100, is_spot=True) is False
    assert should_discard(u=101, last_update_id=100, is_spot=False) is False


def test_straddles_spot_vs_futures():
    # Futures bridges when U <= lastUpdateId <= u.
    assert straddles(U=100, u=100, last_update_id=100, is_spot=False) is True
    assert straddles(U=98, u=102, last_update_id=100, is_spot=False) is True
    # Spot bridges when U <= lastUpdateId+1 <= u, so an event starting at id+1 counts.
    assert straddles(U=101, u=105, last_update_id=100, is_spot=True) is True
    # That same event does NOT straddle under futures rules.
    assert straddles(U=101, u=105, last_update_id=100, is_spot=False) is False
    # Event entirely below the pivot bridges for neither.
    assert straddles(U=90, u=99, last_update_id=100, is_spot=True) is False
    assert straddles(U=90, u=99, last_update_id=100, is_spot=False) is False


def test_is_continuous_spot_vs_futures():
    # Futures: continuity holds iff pu == prev_u (ignores U).
    assert is_continuous({"U": 999, "pu": 50}, prev_u=50, is_spot=False) is True
    assert is_continuous({"U": 51, "pu": 49}, prev_u=50, is_spot=False) is False
    # Spot: continuity holds iff U == prev_u + 1 (no pu field present).
    assert is_continuous({"U": 51}, prev_u=50, is_spot=True) is True
    assert is_continuous({"U": 52}, prev_u=50, is_spot=True) is False
    # No previous event yet -> not continuous (both markets).
    assert is_continuous({"U": 1, "pu": 0}, prev_u=None, is_spot=False) is False
    assert is_continuous({"U": 1}, prev_u=None, is_spot=True) is False


# --- incremental_book_L2 -----------------------------------------------------

def test_incremental_book_l2_header_exact():
    assert INCREMENTAL_BOOK_L2_HEADER == (
        "exchange,symbol,timestamp,local_timestamp,is_snapshot,side,price,amount"
    )


def test_build_incremental_rows_snapshot():
    bids = [["100.0", "1.0"], ["99.5", "2.0"]]
    asks = [["101.0", "3.0"]]
    rows = build_incremental_rows("BTCUSDT", 1700000000000000, 1700000000000123, bids, asks, True)

    # One row per level across both sides; asks emitted before bids.
    assert len(rows) == len(bids) + len(asks)
    ask_fields = rows[0].split(",")
    assert ask_fields == [
        "binance-futures", "btcusdt", "1700000000000000", "1700000000000123",
        "true", "ask", "101.0", "3.0",
    ]
    # Remaining rows are the bids, verbatim and in order.
    assert rows[1].split(",")[5:] == ["bid", "100.0", "1.0"]
    assert rows[2].split(",")[5:] == ["bid", "99.5", "2.0"]


def test_build_incremental_rows_diff_and_removal():
    # is_snapshot=false, and a "0" amount (level removal) passes through verbatim.
    bids = [["100.0", "0"]]
    asks = [["101.0", "2.5"]]
    rows = build_incremental_rows("ETHUSDT", 1, 2, bids, asks, False)

    assert rows[0].split(",") == ["binance-futures", "ethusdt", "1", "2", "false", "ask", "101.0", "2.5"]
    assert rows[1].split(",") == ["binance-futures", "ethusdt", "1", "2", "false", "bid", "100.0", "0"]


def test_build_incremental_rows_empty():
    assert build_incremental_rows("BTCUSDT", 1, 2, [], [], False) == []


def test_all_data_type_names_and_header_for():
    # Snapshot registry stays exactly the three reconstructed types.
    assert set(DATA_TYPES) == {"quotes", "book_snapshot_5", "book_snapshot_25"}
    # incremental_book_L2 is selectable but lives outside the snapshot registry.
    assert INCREMENTAL_NAME not in DATA_TYPES
    assert INCREMENTAL_NAME in ALL_DATA_TYPE_NAMES
    assert header_for(INCREMENTAL_NAME) == INCREMENTAL_BOOK_L2_HEADER
    assert header_for("book_snapshot_25") == build_snapshot_header(25)
    assert header_for("quotes") == QUOTES_HEADER


def test_data_types_registry():
    assert DATA_TYPES["quotes"].depth == 1
    assert DATA_TYPES["book_snapshot_5"].depth == 5
    assert DATA_TYPES["book_snapshot_25"].depth == 25
    assert DATA_TYPES["quotes"].header == QUOTES_HEADER
    assert DATA_TYPES["book_snapshot_5"].header == build_snapshot_header(5)
    assert DATA_TYPES["book_snapshot_25"].header == build_snapshot_header(25)
    # Registry row builders are callable with the uniform signature.
    row = DATA_TYPES["book_snapshot_5"].row_builder("BTCUSDT", 1, 2, [("1", "1")], [("0.5", "1")])
    assert len(row.split(",")) == 24
