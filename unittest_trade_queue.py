from tradequeue import Trade, PairedTrades, TradeQueue
import datetime as dt
import unittest
from decimal import Decimal


class TestTradeQueue(unittest.TestCase):
    def test_simple_all_matched(self):
        open_dt = dt.datetime(2020,5,10)
        close_dt = dt.datetime(2020,5,11)

        trades = [
            Trade("BTC", Decimal("1"), 10000, open_dt),
            Trade("BTC", Decimal("-1"), 11000, close_dt),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(trade_queue.open_trades, [])

        closed_trade_pairs = [
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("1"), 10000, open_dt),
                closing_trade=Trade("BTC", Decimal("-1"), 11000, close_dt)
            )
        ]
        self.assertEqual(trade_queue.closed_trade_pairs, closed_trade_pairs)

    def test_simple_some_unmatched(self):
        open_dt = dt.datetime(2020,5,10)
        close_dt = dt.datetime(2020,5,11)
        trades = [
            Trade("BTC", Decimal("1"), 10000, open_dt),
            Trade("BTC", Decimal("-0.5"), 11000, close_dt),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(
            trade_queue.open_trades,
            [Trade("BTC", Decimal("0.5"), 10000, open_dt)],
        )

        closed_trade_pairs = [
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("0.5"), 10000, open_dt),
                closing_trade=Trade("BTC", Decimal("-0.5"), 11000, close_dt)
            )
        ]
        self.assertEqual(trade_queue.closed_trade_pairs, closed_trade_pairs)

    def test_two_paired_all_matched(self):
        open_dt = dt.datetime(2020,5,10)
        close_dt_1 = dt.datetime(2020,5,11)
        close_dt_2 = dt.datetime(2020,5,12)
        trades = [
            Trade("BTC", Decimal("1"), 10000, open_dt),
            Trade("BTC", Decimal("-0.5"), 11000, close_dt_1),
            Trade("BTC", Decimal("-0.5"), 11500, close_dt_2),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(trade_queue.open_trades, [])

        closed_trade_pairs = [
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("0.5"), 10000,  open_dt),
                closing_trade=Trade("BTC", Decimal("-0.5"), 11000, close_dt_1),
            ),
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("0.5"), 10000, open_dt),
                closing_trade=Trade("BTC", Decimal("-0.5"), 11500, close_dt_2),
            ),
        ]
        self.assertEqual(trade_queue.closed_trade_pairs, closed_trade_pairs)

    def test_two_paired_some_unmatched(self):
        open_dt = dt.datetime(2020,5,10)
        close_dt_1 = dt.datetime(2020,5,11)
        close_dt_2 = dt.datetime(2020,5,12)

        trades = [
            Trade("BTC", Decimal("1"), 10000, open_dt),
            Trade("BTC", Decimal("-0.5"), 11000, close_dt_1),
            Trade("BTC", Decimal("-0.4"), 11500, close_dt_2),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(
            trade_queue.open_trades,
            [Trade("BTC", Decimal("0.1"), 10000, open_dt)],
        )

        closed_trade_pairs = [
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("0.5"), 10000, open_dt),
                closing_trade=Trade("BTC", Decimal("-0.5"), 11000, close_dt_1),
            ),
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("0.4"), 10000, open_dt),
                closing_trade=Trade("BTC", Decimal("-0.4"), 11500, close_dt_2),
            ),
        ]
        self.assertEqual(trade_queue.closed_trade_pairs, closed_trade_pairs)

    def test_multiple_matchings(self):
        open_dt = dt.datetime(2020,5,10)
        open_dt_1 = dt.datetime(2020,5,11)
        close_dt = dt.datetime(2020,5,12)

        trades = [
            Trade("BTC", Decimal("1"), 10000, open_dt),
            Trade("BTC", Decimal("0.5"), 11000, open_dt_1),
            Trade("BTC", Decimal("-1.2"), 11500, close_dt),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(
            trade_queue.open_trades,
            [Trade("BTC", Decimal("0.3"), 11000, open_dt_1)],
        )

        closed_trade_pairs = [
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("1"), 10000, open_dt),
                closing_trade=Trade("BTC", Decimal("-1"), 11500, close_dt),
            ),
            PairedTrades(
                opening_trade=Trade("BTC", Decimal("0.2"), 11000, open_dt_1),
                closing_trade=Trade("BTC", Decimal("-0.2"), 11500, close_dt),
            )
        ]
        self.assertEqual(trade_queue.closed_trade_pairs, closed_trade_pairs)

    def test_cannot_match_qty(self):
        trades = [
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today()),
            Trade("BTC", Decimal("-1"), 11000, dt.datetime.today()),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        trade_queue.add_trade(
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today())
        )
        with self.assertRaises(ValueError):
            trade_queue.add_trade(
                Trade("BTC", Decimal("-1.1"), 10000, dt.datetime.today())
            )

    def test_cannot_match_time(self):
        open_dt = dt.datetime(2020,5,10)
        close_dt = dt.datetime(2020,5,9)

        trade_queue = TradeQueue("BTC", is_fifo=True)
        trade_queue.add_trade(
            Trade("BTC", Decimal("1"), 10000, open_dt)
        )
        with self.assertRaises(ValueError):
            trade_queue.add_trade(
                Trade("BTC", Decimal("-.1"), 10000, close_dt)
            )


if __name__ == "__main__":
    unittest.main()
