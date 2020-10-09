from tradequeue import Trade, PairedTrades, TradeQueue
import datetime as dt
import unittest
from decimal import Decimal


class TestTradeQueue(unittest.TestCase):
    def test_simple_all_matched(self):
        trades = [
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today()),
            Trade("BTC", Decimal("-1"), 11000, dt.datetime.today()),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(trade_queue.unpaired_trades, [])

        paired_trades = [
            PairedTrades(
                paired_trades=[
                    Trade("BTC", Decimal("1"), 10000, dt.datetime.today())
                ],
                later_trade=Trade(
                    "BTC", Decimal("-1"), 11000, dt.datetime.today()
                ),
            )
        ]
        self.assertEqual(trade_queue.paired_trades, paired_trades)

    def test_simple_some_unmatched(self):
        trades = [
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today()),
            Trade("BTC", Decimal("-0.5"), 11000, dt.datetime.today()),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(
            trade_queue.unpaired_trades,
            [Trade("BTC", Decimal("0.5"), 10000, dt.datetime.today())],
        )

        paired_trades = [
            PairedTrades(
                paired_trades=[
                    Trade("BTC", Decimal("0.5"), 10000, dt.datetime.today())
                ],
                later_trade=Trade(
                    "BTC", Decimal("-0.5"), 11000, dt.datetime.today()
                ),
            )
        ]
        self.assertEqual(trade_queue.paired_trades, paired_trades)

    def test_two_paired_all_matched(self):
        trades = [
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today()),
            Trade("BTC", Decimal("-0.5"), 11000, dt.datetime.today()),
            Trade("BTC", Decimal("-0.5"), 11500, dt.datetime.today()),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(trade_queue.unpaired_trades, [])

        paired_trades = [
            PairedTrades(
                paired_trades=[
                    Trade("BTC", Decimal("0.5"), 10000, dt.datetime.today())
                ],
                later_trade=Trade(
                    "BTC", Decimal("-0.5"), 11000, dt.datetime.today()
                ),
            ),
            PairedTrades(
                paired_trades=[
                    Trade("BTC", Decimal("0.5"), 10000, dt.datetime.today())
                ],
                later_trade=Trade(
                    "BTC", Decimal("-0.5"), 11500, dt.datetime.today()
                ),
            ),
        ]
        self.assertEqual(trade_queue.paired_trades, paired_trades)

    def test_two_paired_some_unmatched(self):
        trades = [
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today()),
            Trade("BTC", Decimal("-0.5"), 11000, dt.datetime.today()),
            Trade("BTC", Decimal("-0.4"), 11500, dt.datetime.today()),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        for trade in trades:
            trade_queue.add_trade(trade)

        self.assertEqual(trade_queue.all_trades, trades)
        self.assertEqual(
            trade_queue.unpaired_trades,
            [Trade("BTC", Decimal("0.1"), 10000, dt.datetime.today())],
        )

        paired_trades = [
            PairedTrades(
                paired_trades=[
                    Trade("BTC", Decimal("0.5"), 10000, dt.datetime.today())
                ],
                later_trade=Trade(
                    "BTC", Decimal("-0.5"), 11000, dt.datetime.today()
                ),
            ),
            PairedTrades(
                paired_trades=[
                    Trade("BTC", Decimal("0.4"), 10000, dt.datetime.today())
                ],
                later_trade=Trade(
                    "BTC", Decimal("-0.4"), 11500, dt.datetime.today()
                ),
            ),
        ]
        self.assertEqual(trade_queue.paired_trades, paired_trades)

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
        trades = [
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today()),
            Trade("BTC", Decimal("-1"), 11000, dt.datetime.today()),
        ]

        trade_queue = TradeQueue("BTC", is_fifo=True)
        trade_queue.add_trade(
            Trade("BTC", Decimal("1"), 10000, dt.datetime.today())
        )
        with self.assertRaises(ValueError):
            earlier_dt = dt.datetime.today() - dt.timedelta(minutes=1)
            trade_queue.add_trade(
                Trade("BTC", Decimal("-.1"), 10000, earlier_dt,)
            )

if __name__ == "__main__":
    unittest.main()
