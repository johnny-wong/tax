import csv
import datetime as dt
import re
from decimal import Decimal
from typing import List, Optional, Dict
from dateutil.relativedelta import relativedelta

import tradequeue as tq

trades_csv_path = r"C:\Users\johnn\data\tax\2019_2020\commsec_trades.csv"

FYI_START = dt.datetime(2019, 7, 1)
FYI_END = dt.datetime(2020, 6, 30)


class CommsecParser:
    def __init__(self):
        self.trade_details_regex = "(B|S) (\d+) (.*) @ (\d*\.\d*)"
        self.dict_item_to_regex_group = {
            "side": 0,
            "qty": 1,
            "instrument": 2,
            "price": 3,
        }

    def row_to_trade(self, row: List[str]) -> Optional[tq.Trade]:
        details = row[2]
        details_match = re.match(self.trade_details_regex, details)
        trade_dt_str = row[0]
        if details_match:
            trade = self.details_to_trade(
                dt.datetime.strptime(row[0], "%d/%m/%Y"), details_match
            )
            return trade

        return None

    def details_to_trade(
        self, trade_dt: dt.datetime, details: re.Match
    ) -> tq.Trade:
        dict_details = {
            category: details.groups()[regex_group]
            for category, regex_group in self.dict_item_to_regex_group.items()
        }

        if dict_details["side"] == "B":
            qty = Decimal(str(dict_details["qty"]))
        else:
            qty = -Decimal(str(dict_details["qty"]))

        price: Decimal = Decimal(str(dict_details["price"]))
        trade = tq.Trade(
            instrument=str(dict_details["instrument"]),
            qty=Decimal(str(qty)),
            price=price,
            trade_dt=trade_dt,
        )

        return trade

def main():
    commsec_trades: List[tq.Trade] = []
    with open(trades_csv_path, "r") as f:
        commsec_csv = csv.reader(f)
        commsec_parser = CommsecParser()
        for row in commsec_csv:
            potential_trade = commsec_parser.row_to_trade(row)
            if potential_trade:
                commsec_trades.append(potential_trade)

    dict_trade_queues: Dict[str, tq.TradeQueue] = {}

    for trade in reversed(commsec_trades):
        instrument = trade.instrument
        try:
            dict_trade_queues[instrument].add_trade(trade)
        except KeyError:
            dict_trade_queues[instrument] = tq.TradeQueue(instrument, is_fifo=True)
            dict_trade_queues[instrument].add_trade(trade)

    for instrument, instrument_trade_queue in dict_trade_queues.items():
        this_fy_closing_trades = filter(
            lambda paired_trade: FYI_START
            <= paired_trade.closing_trade.trade_dt
            <= FYI_END,
            instrument_trade_queue.closed_trade_pairs,
        )

        cgt_discount_trades = []
        non_cgt_discount_trades = []
        for trade_pair in this_fy_closing_trades:
            if (
                trade_pair.opening_trade.trade_dt
                < trade_pair.closing_trade.trade_dt + relativedelta(months=-12)
            ):
                # Longer than 12 months
                cgt_discount_trades.append(trade_pair)
            else:
                non_cgt_discount_trades.append(trade_pair)

        for discount_eligibility, tradeslist in zip(
            ["CGT discount", "non CGT discount"],
            [cgt_discount_trades, non_cgt_discount_trades],
        ):
            pnl = Decimal("0")
            for trade in tradeslist:
                pnl += (
                    trade.closing_trade.price - trade.opening_trade.price
                ) * trade.opening_trade.qty

            print(f"{instrument} {discount_eligibility}: ${pnl:,.2f}")

if __name__ == "__main__":
    main()