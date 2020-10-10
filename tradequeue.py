import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import DefaultDict, Dict, List, Tuple, Union
from copy import deepcopy

@dataclass
class Trade:
    instrument: str
    qty: Decimal
    price: float
    trade_dt: dt.datetime


@dataclass
class PairedTrades:
    opening_trade: Trade
    closing_trade: Trade


class TradeQueue:
    """
    Will pair up trades in a FIFO or LIFO manner
    """

    def __init__(self, instrument: str, is_fifo: bool = True) -> None:
        self.is_fifo = is_fifo
        self.instrument = instrument
        self.all_trades: List[Trade] = []
        self.open_trades: List[Trade] = []
        self.closed_trade_pairs: List[PairedTrades] = []

    def add_trade(self, trade: Trade) -> None:
        if trade.instrument != self.instrument:
            raise ValueError(
                f"Trying to add {trade.instrument} trade to {self.instrument} TradeQueue"
            )
        self.all_trades.append(trade)
        if trade.qty > 0:
            self.open_trades.append(trade)
            self.open_trades.sort(key=lambda x: x.trade_dt)
        else:
            self.pair_trade(trade)

    def pair_trade(self, closing_trade: Trade) -> None:
        unpaired_closing_trade = deepcopy(closing_trade)
        while unpaired_closing_trade.qty < 0:
            idx, next_unclosed_trade = self.get_next_unclosed_trade(
                closing_trade.instrument, closing_trade.trade_dt
            )
            opening_trade_to_pair = deepcopy(next_unclosed_trade)
            closing_trade_to_pair = deepcopy(unpaired_closing_trade)
            if next_unclosed_trade.qty > abs(unpaired_closing_trade.qty):
                # Can close with this unpaired trade. Loop will not iterate again
                opening_trade_to_pair.qty = abs(unpaired_closing_trade.qty)
                excess_opening_qty = next_unclosed_trade.qty + unpaired_closing_trade.qty
                adjusted_unclosed_trade = deepcopy(next_unclosed_trade)
                adjusted_unclosed_trade.qty = excess_opening_qty
                self.open_trades[idx] = adjusted_unclosed_trade
            elif next_unclosed_trade.qty <= abs(unpaired_closing_trade.qty):
                # Loop may need more iterations to close
                closing_trade_to_pair.qty = -opening_trade_to_pair.qty
                del self.open_trades[idx]

            paired_trades = PairedTrades(opening_trade=opening_trade_to_pair, closing_trade=closing_trade_to_pair)
            self.closed_trade_pairs.append(paired_trades)
            unpaired_closing_trade.qty += opening_trade_to_pair.qty
            

    def get_next_unclosed_trade(
        self, instrument: str, trade_dt: dt.datetime
    ) -> Tuple[int, Trade]:
        if not len(self.open_trades):
            raise ValueError(f"No more unpaired trades for {instrument}")

        idx = 0 if self.is_fifo else -1
        next_unclosed_trade = self.open_trades[idx]

        if next_unclosed_trade.trade_dt > trade_dt:
            raise ValueError(
                f"No more unpaired trades for {instrument} before trade time {trade_dt}"
            )
        return idx, self.open_trades[idx]

