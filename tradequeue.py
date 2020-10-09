import datetime as dt
from collections import defaultdict
from dataclasses import dataclass
from decimal import Decimal
from typing import DefaultDict, Dict, List, Tuple, Union


@dataclass
class Trade:
    instrument: str
    qty: Decimal
    price: float
    trade_dt: dt.datetime


@dataclass
class PairedTrades:
    later_trade: Trade
    paired_trades: List[Trade]


class TradeQueue:
    """
    Will pair up trades in a FIFO or LIFO manner
    """

    def __init__(self, instrument: str, is_fifo: bool = True) -> None:
        self.is_fifo = is_fifo
        self.instrument = instrument
        self.all_trades: List[Trade] = []
        self.unpaired_trades: List[Trade] = []
        self.paired_trades: List[PairedTrades] = []

    def add_trade(self, trade: Trade) -> None:
        if trade.instrument != self.instrument:
            raise ValueError(
                f"Trying to add {trade.instrument} trade to {self.instrument} TradeQueue"
            )
        self.all_trades.append(trade)
        if trade.qty > 0:
            self.unpaired_trades.append(trade)
            self.unpaired_trades.sort(key=lambda x: x.trade_dt)
        else:
            self.pair_trade(trade)

    def pair_trade(self, trade: Trade) -> None:
        paired_qty: Decimal = Decimal("0")
        qty_to_pair: Decimal = Decimal(str(abs(trade.qty)))
        paired_trades = PairedTrades(later_trade=trade, paired_trades=[])
        while paired_qty < qty_to_pair:
            idx, next_unpaired_trade = self.get_next_unpaired_trade(
                trade.instrument, trade.trade_dt
            )
            left_to_pair = qty_to_pair - paired_qty
            if next_unpaired_trade.qty > left_to_pair:
                excess_unpaired_qty = next_unpaired_trade.qty - left_to_pair
                paired_trade = Trade(
                    instrument=trade.instrument,
                    qty=left_to_pair,
                    price=next_unpaired_trade.price,
                    trade_dt=next_unpaired_trade.trade_dt,
                )
                paired_trades.paired_trades.append(paired_trade)
                adjusted_unpaired_trade = next_unpaired_trade
                adjusted_unpaired_trade.qty = excess_unpaired_qty
                self.unpaired_trades[idx] = adjusted_unpaired_trade
                self.paired_trades.append(paired_trades)
                return None
            else:
                paired_trades.paired_trades.append(next_unpaired_trade)
                del self.unpaired_trades[idx]
                paired_qty += next_unpaired_trade.qty
                if paired_qty == qty_to_pair:
                    self.paired_trades.append(paired_trades)

    def get_next_unpaired_trade(
        self, instrument: str, trade_dt: dt.datetime
    ) -> Tuple[int, Trade]:
        if not len(self.unpaired_trades):
            raise ValueError(f"No more unpaired trades for {instrument}")

        idx = 0 if self.is_fifo else -1
        next_unpaired_trade = self.unpaired_trades[idx]

        if next_unpaired_trade.trade_dt > trade_dt:
            raise ValueError(
                f"No more unpaired trades for {instrument} before trade time {trade_dt}"
            )
        return idx, self.unpaired_trades[idx]

