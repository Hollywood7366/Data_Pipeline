import datetime
import logging
import os
import time
from datetime import datetime as dt

import library as ba
import nest_asyncio
import orders as ods
import pandas as pd
import pytz
import schedule
from dateutil.relativedelta import relativedelta
from ib_insync import *

nest_asyncio.apply()

def create_logger(filename, log_path, level=logging.INFO):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)

    logging_filename = filename + '.log'

    if not os.path.exists(log_path):
        os.makedirs(log_path)

    logging_path = os.path.join(log_path, logging_filename)

    # Add logging to file
    handler = logging.FileHandler(logging_path)
    handler.setLevel(level)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    # Add logging to stdout
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger

class Strategy:
    def __init__(self, ipaddress, portid, clientid):
        self.log_path = r'logs'
        self.logger = self.make_logger()
        self.ip_address = ipaddress
        self.port_id = portid
        self.client_id = clientid
        self.ib = IB()

        self.position_size = 0
        self.entry_mode = 0  # 0: long entry mode, 1: short entry mode
        self.max_pnl = 9999999
        self.min_pnl = -9999999
        self.max_trades = 9999999

        self.entry_type = 'LMT'  # 'MKT' or 'STP' or 'LMT'
        self.HH_ON = False
        self.LL_ON = False
        self.PT_ON = 0
        self.SL_ON = 0
        self.TL_ON = 0
        self.atr_length = 20
        self.hh_length = 0
        self.ll_length = 0

        self.Max_Time = 7
        self.Profitable_Closes = 1
        self.kind = "Default"
        self.acct = 100000
        self.init_margin = 1974
        self.pt_mult = 0
        self.sl_mult = 0
        self.tl_mult = 0
        self.duration_str = "1 M"
        self.bar_timeframe = "1 hour"
        self.whatToShow = "TRADES"
        self.RTH = False
        self.contract = Contract()
        self.contract.symbol = "NQ"
        self.contract.secType = "FUT"
        self.contract.currency = "USD"
        self.contract.exchange = "CME"
        self.contract.lastTradeDateOrContractMonth = "202411"
        # self.historical_google_bardata = []
        self.historical_conv_bardata = []

        self.minTickSize = 1.0
        self.pointValue = 1.0

        self.current_month = 0

        self.opens = []
        self.closes = []
        self.highs = []
        self.lows = []
        self.volumes = []
        self.bardates = []
        self.revopens = []
        self.revcloses = []
        self.revhighs = []
        self.revlows = []
        self.revvolumes = []
        self.revbardates = []
        self.TimesEntryA1 = []
        self.DatesEntryB1 = []
        self.QuarterEntryB2 = []

        # Secondary market data
        self.secondary_bardates = {}
        self.secondary_current_open = {}
        self.secondary_current_high = {}
        self.secondary_current_low = {}
        self.secondary_current_close = {}
        self.secondary_current_volume = {}
        self.secondary_current_date = {}
        self.secondary_opens = {}
        self.secondary_closes = {}
        self.secondary_highs = {}
        self.secondary_lows = {}
        self.secondary_volumes = {}

        self.current_date = None
        self.current_open = 0
        self.current_close = 0
        self.current_low = 0
        self.current_high = 0
        self.current_volume = 0
        self.conv = 1
        self.server_time = 0
        self.trade_signal_timeframe = 0

        self.TDOM = [0]
        self.YTD = [0]
        self.QTD = [0]
        self.MTD = [0]

        self.entry_signal = False
        self.exit_signal = False
        self.exit_move = False
        self.position_size_tobe = None
        self.limit_price = None
        self.entry_order = None
        self.exit_order = None
        self.entry_trade = None
        self.sl_order = None
        self.pt_order = None
        self.pt_trade = None
        self.pt_price = None
        self.sl_price = None
        self.sl_trade = None
        self.tl_price = None
        self.order_type = None
        self.hold_count = 0
        self.hh = None
        self.ll = None
        self.atr = None
        self.prof_x = 0
        self.daily_pnl = 0
        self.strategy_pnl = 0
        self.entry_price = None
        self.exec_count = 0
        self.market_closed = False

    def make_logger(self):
        filename = 'bot_' + dt.today().strftime('%Y-%m-%d')
        logger = create_logger(filename, self.log_path, level=logging.DEBUG)
        return logger

    def start_strategy(self):
        self.ib.connect(self.ip_address, self.port_id, self.client_id)
        if not self.ib.isConnected():
            self.logger.info('Cannot login to TWS.')
            exit()

        self.daily_pnl = 0
        self.strategy_pnl = 0

        contracts = self.ib.qualifyContracts(self.contract)
        if not contracts:
            self.logger.info('No contract found for {}'.format(self.contract.symbol))
            exit()

        self.logger.info('Contract: {}, {}, {}, {}'.format(self.contract.symbol,
                                                           self.contract.secType,
                                                           self.contract.currency,
                                                           self.contract.exchange))

        contract_details = self.ib.reqContractDetails(self.contract)[0]
        self.contract_details(contract_details)

        bars = self.ib.reqHistoricalData(self.contract,
                                         '',
                                         self.duration_str,
                                         self.bar_timeframe,
                                         self.whatToShow,
                                         self.RTH,
                                         1,
                                         keepUpToDate=True)
        self.logger.info('Getting historical data: {}, {}, {}'.format(self.duration_str,
                                                                      self.bar_timeframe,
                                                                      self.whatToShow))

        # Add Additional Data Here #
        self.contracts = [
            {'symbol': 'VIX', 'secType': 'IND', 'expiration': '', 'currency': 'USD', 'exchange': 'CBOE', 'depth': '1 M', 'interval': '1 hour'}
        ]

        self.get_secondary_historical_bardata()

        self.opens = [bar.open for bar in bars]
        self.closes = [bar.close for bar in bars]
        self.highs = [bar.high for bar in bars]
        self.lows = [bar.low for bar in bars]
        self.volumes = [bar.volume for bar in bars]
        self.bardates = [bar.date for bar in bars]

        self.revopens = self.opens.copy()
        self.revcloses = self.closes.copy()
        self.revhighs = self.highs.copy()
        self.revlows = self.lows.copy()
        self.revvolumes = self.volumes.copy()
        self.revbardates = self.bardates.copy()

        self.opens.reverse()
        self.closes.reverse()
        self.highs.reverse()
        self.lows.reverse()
        self.volumes.reverse()
        self.bardates.reverse()

        bars.updateEvent += self.on_bar

        while True:
            if self.market_closed:
                self.logger.info('Market closed')
                break
            if isinstance(self.entry_trade, Trade):
                if self.position_size == 0.5:
                    if self.entry_trade.orderStatus.status == 'Filled':
                        self.prof_x = 0
                        self.position_size = 1
                        self.entry_price = self.entry_trade.fills[0].execution.avgPrice
                        self.logger.info('Entry trade filled @: {}'.format(self.entry_price))
                        self.send_protection_order()
                elif self.position_size == 1:
                    if self.order_type == 'oco':
                        if self.pt_trade.orderStatus.status == 'Filled' or self.sl_trade.orderStatus.status == 'Filled':
                            if self.pt_trade.orderStatus.status == 'Filled':
                                filled_trade = self.pt_trade
                                self.ib.cancelOrder(self.sl_trade.order)
                                self.logger.info(
                                    'PT order filled @: {}'.format(filled_trade.fills[0].execution.avgPrice))
                                self.logger.info('EX order cancelled')
                            else:
                                filled_trade = self.sl_trade
                                self.ib.cancelOrder(self.pt_trade.order)
                                self.logger.info(
                                    'EX order filled @: {}'.format(filled_trade.fills[0].execution.avgPrice))
                                self.logger.info('PT order cancelled')
                            self.update_execution(filled_trade)
                    elif self.order_type == 'tp':
                        if self.pt_trade.orderStatus.status == 'Filled':
                            self.logger.info('PT order filled @: {}'.format(self.pt_trade.fills[0].execution.avgPrice))
                            self.update_execution(self.pt_trade)
                    elif self.order_type == 'sl':
                        if self.sl_trade.orderStatus.status == 'Filled':
                            self.logger.info('EX order filled @: {}'.format(self.sl_trade.fills[0].execution.avgPrice))
                            self.update_execution(self.sl_trade)

            self.ib.sleep(0.1)

    def update_execution(self, trade):
        mult = 1 if self.entry_mode == 0 else -1
        self.position_size = 0
        self.logger.info('Position: {}'.format(self.position_size))
        execution_price = trade.fills[0].execution.avgPrice
        self.strategy_pnl = self.strategy_pnl + (
                execution_price - self.entry_price) * float(
            trade.order.totalQuantity) * self.pointValue * mult
        self.logger.info('Strategy PnL: {}'.format(self.strategy_pnl))
        self.daily_pnl = self.daily_pnl + (
                execution_price - self.entry_price) * float(
            trade.order.totalQuantity) * self.pointValue * mult
        self.logger.info('Daily PnL: {}'.format(self.daily_pnl))
        self.entry_price = 0
        self.exec_count = self.exec_count + 1
        self.logger.info('Execution count: {}'.format(self.exec_count))

    def on_bar(self, bars, has_new_bar):
        if has_new_bar:
            if bars[-2].date not in self.bardates:
                self.logger.info('New bar: {}'.format(bars[-2].date))
                self.logger.info('Open   : {}'.format(bars[-2].open))
                self.logger.info('Close  : {}'.format(bars[-2].close))
                self.logger.info('High   : {}'.format(bars[-2].high))
                self.logger.info('Low    : {}'.format(bars[-2].low))
                self.logger.info('Volume : {}'.format(bars[-2].volume))
                self.bardates.insert(0, bars[-2].date)
                self.opens.insert(0, bars[-2].open)
                self.closes.insert(0, bars[-2].close)
                self.highs.insert(0, bars[-2].high)
                self.lows.insert(0, bars[-2].low)
                self.volumes.insert(0, bars[-2].volume)

                self.revopens.append(bars[-2].open)
                self.revhighs.append(bars[-2].high)
                self.revlows.append(bars[-2].low)
                self.revcloses.append(bars[-2].close)
                self.revvolumes.append(bars[-2].volume)

            # reverse to normal: last in the list is the most recent
            # self.revopens = self.opens.copy()
            # self.revopens.reverse()
            # self.revhighs = self.highs.copy()
            # self.revhighs.reverse()
            # self.revlows = self.lows.copy()
            # self.revlows.reverse()
            # self.revcloses = self.closes.copy()
            # self.revcloses.reverse()
            # self.revvolumes = self.volumes.copy()
            # self.revvolumes.reverse()

            self.current_close = bars[-2].close
            self.current_open = bars[-2].open
            self.current_low = bars[-2].low
            self.current_high = bars[-2].high
            self.current_volume = bars[-2].volume
            self.current_date = bars[-2].date

            self.get_current_twstime()

            self.TimesEntryA1 = ba.get_time(self.revbardates)

            self.DatesEntryB1 = ba.get_date(self.revbardates)
            self.QuarterEntryB2 = ba.q_number(self.revbardates)


            # self.market_closed = self.get_current_market_status()

            self.get_TDOM()
            self.get_MTD()
            self.get_QTD()
            self.get_YTD()

            self.execute_strategy()

    def execute_strategy(self):
        self.compute_indicators()
        if self.position_size == 0:
            self.check_entry_condition()
            if self.entry_signal:
                self.place_entry_order()
        elif self.position_size == 0.5:
            self.cancel_pending_order()
            self.position_size = 0
            self.check_entry_condition()
            if self.entry_signal:
                self.place_entry_order()
        elif self.position_size == 1:
            self.check_exit_condition()
            if self.exit_signal:
                self.cancel_pending_order()
                self.place_exit_order()
                self.position_size = 0
                self.check_entry_condition()
                if self.entry_signal:
                    self.place_entry_order()
            else:
                self.hold_count += 1
                self.logger.info('Hold count: {}'.format(self.hold_count))
                if self.position_size == 1:
                    if self.HH_ON or self.LL_ON or self.TL_ON == 1 or self.TL_ON == 2:
                        exit_moved = self.check_exit_move()
                        if self.exit_move:
                            self.edit_protection_order(exit_moved)

    def place_entry_order(self):
        self.compute_position_size()
        self.create_entry_order()
        self.create_protection_order()
        self.send_entry_order()
        self.position_size = 0.5  # entry order placed

    def compute_indicators(self):
        if len(self.highs) >= self.hh_length and len(self.lows) >= self.ll_length:
            self.hh = max(self.highs[0:self.hh_length])
            self.ll = min(self.lows[0:self.ll_length])
            self.atr = ba.atr(self.atr_length, self.highs, self.lows, self.closes)

            if self.HH_ON:
                self.logger.info('HH: {}'.format(self.hh))
            if self.LL_ON:
                self.logger.info('LL: {}'.format(self.ll))
            self.logger.info('ATR: {}'.format(self.atr[0]))

            if self.position_size == 1:
                if self.entry_mode == 0:  # Long entry
                    if self.current_close >= self.entry_price:
                        self.prof_x = self.prof_x + 1
                else:  # Short entry
                    if self.current_close <= self.entry_price:
                        self.prof_x = self.prof_x + 1
                self.logger.info('Count of profitable closes: {}'.format(self.prof_x))

    def check_entry_condition(self):
        # Entry Signal
        self.entry_signal = True if self.revhighs[-6] <= self.revcloses[-9] and (self.TimesEntryA1[-1] >= 0) AND (self.TimesEntryA1[-1] <= 10000) and self.QuarterEntryB2[-1] != 4 else False # todo mock
        # condition = self.exec_count <= self.max_trades and self.strategy_pnl <= self.max_pnl and self.strategy_pnl >= self.min_pnl
        if self.entry_signal:
            self.check_position()
        self.logger.info('Entry signal: {}'.format(self.entry_signal))

    def check_position(self):
        positions = self.ib.positions()
        positions = [p for p in positions if p.contract.conId == self.contract.conId]
        if not positions:
            return
        position = positions[0]
        if position.position == 0:
            return
        else:
            self.logger.info('Current position: {}. Skipping this signal'.format(position.position))
            self.entry_signal = False

    def compute_position_size(self):
        if self.contract.secType == 'CASH' and self.contract.currency != 'BASE':
            self.position_size_tobe = ba.get_forex_position_size(self.conv, self.kind, True,
                                                                 self.acct,
                                                                 self.atr[0],
                                                                 self.SL_ON * self.sl_mult)
        else:
            self.position_size_tobe = ba.get_position_size_tobe(self.current_close,
                                                                self.contract.secType,
                                                                self.kind, self.acct,
                                                                self.init_margin,
                                                                self.atr[0],
                                                                self.SL_ON * self.sl_mult,
                                                                self.pointValue)
        self.logger.info('Position size: {}'.format(self.position_size_tobe))

    def send_protection_order(self):
        if self.entry_type in ['LMT', 'STP']:
            if self.entry_mode == 0 and self.sl_price != 0 and self.limit_price < self.sl_price:
                self.sl_price = self.limit_price - self.minTickSize
                self.logger.info('Adjusting SL price to limit price - tick: {}'.format(self.sl_price))
                self.sl_order.auxPrice = self.sl_price
            if self.entry_mode == 0 and self.pt_price != 0 and self.limit_price > self.pt_price:
                self.pt_price = self.limit_price + self.minTickSize
                self.logger.info('Adjusting PT price to limit price + tick: {}'.format(self.pt_price))
                self.pt_order.lmtPrice = self.pt_price
            if self.entry_mode == 1 and self.sl_price != 0 and self.limit_price > self.sl_price:
                self.sl_price = self.limit_price + self.minTickSize
                self.logger.info('Adjusting SL price to limit price + tick: {}'.format(self.sl_price))
                self.sl_order.auxPrice = self.sl_price
            if self.entry_mode == 1 and self.pt_price != 0 and self.limit_price < self.pt_price:
                self.pt_price = self.limit_price - self.minTickSize
                self.logger.info('Adjusting PT price to limit price - tick: {}'.format(self.pt_price))
                self.pt_order.lmtPrice = self.pt_price

        if self.order_type == 'sl':
            self.sl_trade = self.ib.placeOrder(self.contract, self.sl_order)
            self.ib.sleep(1)
            self.logger.info('Stop order sent: {}, {}, {}'.format(self.sl_order.action,
                                                                  self.sl_order.totalQuantity,
                                                                  self.sl_price))
        elif self.order_type == 'tp':
            self.pt_trade = self.ib.placeOrder(self.contract, self.pt_order)
            self.ib.sleep(1)
            self.logger.info('PT order sent: {}, {}, {}'.format(self.pt_order.action,
                                                                self.pt_order.totalQuantity,
                                                                self.pt_price))
        elif self.order_type == 'oco':
            self.sl_trade = self.ib.placeOrder(self.contract, self.sl_order)
            self.ib.sleep(1)
            self.pt_trade = self.ib.placeOrder(self.contract, self.pt_order)
            self.ib.sleep(1)
            self.logger.info('Stop order sent: {}, {}, {}'.format(self.sl_order.action,
                                                                  self.sl_order.totalQuantity,
                                                                  self.sl_price))
            self.logger.info('PT order sent: {}, {}, {}'.format(self.pt_order.action,
                                                                self.pt_order.totalQuantity,
                                                                self.pt_price))

    def send_entry_order(self):
        self.place_single_entry()
        self.hold_count = 1

    def place_single_entry(self):
        self.entry_trade = self.ib.placeOrder(contract=self.contract, order=self.entry_order)
        self.ib.sleep(1)
        if self.entry_order.orderType == 'MKT':
            self.logger.info('Entry order sent: {}, {}, {}'.format(self.entry_order.action,
                                                                   self.entry_order.orderType,
                                                                   self.entry_order.totalQuantity))
        else:
            self.logger.info('Entry order sent: {}, {}, {}, {} '.format(self.entry_order.action,
                                                                        self.entry_order.orderType,
                                                                        self.entry_order.totalQuantity,
                                                                        self.entry_order.lmtPrice))

    def compute_sl_tp(self):
        self.compute_fixed()
        hh_price, ll_price = self.compute_dynamic()
        self.aggregate_fixed_dynamic(hh_price, ll_price)

    def aggregate_fixed_dynamic(self, hh_price, ll_price):
        if self.entry_mode == 0:  # Long entry
            self.pt_price = min(self.pt_price, hh_price)
            if self.pt_price > self.current_high * 5:
                self.pt_price = 0
            self.sl_price = max(self.sl_price, self.tl_price, ll_price)
            if self.sl_price < self.current_low / 5:
                self.sl_price = 0
        else:  # Short entry
            self.pt_price = max(self.pt_price, ll_price)
            if self.pt_price < self.current_low / 5:
                self.pt_price = 0
            self.sl_price = min(self.sl_price, self.tl_price, hh_price)
            if self.sl_price > self.current_high * 5:
                self.sl_price = 0

        if self.pt_price != 0:
            self.logger.info('PT price: {}'.format(self.pt_price))
        if self.sl_price != 0:
            self.logger.info('SL price: {}'.format(self.sl_price))

    def create_entry_order(self):
        if self.entry_mode == 0:  # Long entry
            if self.entry_type == 'MKT':
                self.entry_order = ods.create_buy_market_order(abs(self.position_size_tobe))
            elif self.entry_type == 'LMT':
                self.limit_price = ba.get_round_to_mintick(self.lows[0], self.minTickSize)
                self.entry_order = ods.create_limit_buy_order(self.limit_price, abs(self.position_size_tobe))
            else:
                self.limit_price = ba.get_round_to_mintick(self.highs[0], self.minTickSize)
                self.entry_order = ods.create_stop_buy_order(self.limit_price, abs(self.position_size_tobe))
        else:  # Short entry
            if self.entry_type == 'MKT':
                self.entry_order = ods.create_sell_market_order(abs(self.position_size_tobe))
            elif self.entry_type == 'LMT':
                self.limit_price = ba.get_round_to_mintick(self.highs[0], self.minTickSize)
                self.entry_order = ods.create_limit_sell_order(self.limit_price, abs(self.position_size_tobe))
            else:
                self.limit_price = ba.get_round_to_mintick(self.lows[0], self.minTickSize)
                self.entry_order = ods.create_stop_sell_order(self.limit_price, abs(self.position_size_tobe))
    def create_protection_order(self):
        self.compute_sl_tp()

        # todo mock
        # self.pt_price += 0.01
        # self.sl_price -= 0.01

        self.order_type = None
        if self.entry_mode == 0:
            if self.pt_price != 0 and self.sl_price != 0:
                self.pt_order = ods.create_limit_sell_order(self.pt_price, self.entry_order.totalQuantity)
                self.sl_order = ods.create_stop_sell_order(self.sl_price, self.entry_order.totalQuantity)
                self.order_type = 'oco'
            elif self.pt_price != 0 and self.sl_price == 0:
                self.pt_order = ods.create_limit_sell_order(self.pt_price, self.entry_order.totalQuantity)
                self.order_type = 'tp'
            elif self.pt_price == 0 and self.sl_price != 0:
                self.sl_order = ods.create_stop_sell_order(self.sl_price, self.entry_order.totalQuantity)
                self.order_type = 'sl'

        else:
            if self.pt_price != 0 and self.sl_price != 0:
                self.pt_order = ods.create_limit_buy_order(self.pt_price, self.entry_order.totalQuantity)
                self.sl_order = ods.create_stop_buy_order(self.sl_price, self.entry_order.totalQuantity)
                self.order_type = 'oco'
            elif self.pt_price != 0 and self.sl_price == 0:
                self.pt_order = ods.create_limit_buy_order(self.pt_price, self.entry_order.totalQuantity)
                self.order_type = 'tp'
            elif self.pt_price == 0 and self.sl_price != 0:
                self.sl_order = ods.create_stop_buy_order(self.sl_price, self.entry_order.totalQuantity)
                self.order_type = 'sl'

    def compute_fixed(self):
        mult = 1 if self.entry_mode == 0 else -1

        base_price_pt = self.current_close
        base_price_sl = self.current_close

        if self.PT_ON == 1:
            self.pt_price = ba.get_round_to_mintick(
                base_price_pt + mult * self.atr[0] * self.pt_mult, self.minTickSize)
            self.logger.info('PT price: {}'.format(self.pt_price))
        elif self.PT_ON == 2:
            pt_as_ticks = self.pt_mult / (self.pointValue * self.minTickSize) / float(
                abs(self.entry_trade.order.totalQuantity))
            self.pt_price = ba.get_round_to_mintick(base_price_pt + mult * pt_as_ticks * self.minTickSize,
                                                    self.minTickSize)
            self.logger.info('PT price: {}'.format(self.pt_price))
        elif self.PT_ON == 3:
            self.pt_price = ba.get_round_to_mintick(
                base_price_pt * (1 + mult * self.pt_mult / 100), self.minTickSize)
            self.logger.info('PT price: {}'.format(self.pt_price))
        else:
            if self.entry_mode == 0:
                self.pt_price = 9999999.0
            else:
                self.pt_price = 0

        mult = -1 if self.entry_mode == 0 else 1
        if self.SL_ON == 1:
            self.sl_price = ba.get_round_to_mintick(
                base_price_sl + mult * self.atr[0] * self.sl_mult, self.minTickSize)
            self.logger.info('SL price: {}'.format(self.sl_price))
        elif self.SL_ON == 2 and self.position_size != 0:
            sl_as_ticks = self.sl_mult / self.pointValue / self.minTickSize / float(
                abs(self.entry_trade.order.totalQuantity))
            self.sl_price = ba.get_round_to_mintick(base_price_sl + mult * sl_as_ticks * self.minTickSize,
                                                    self.minTickSize)
            self.logger.info('SL price: {}'.format(self.sl_price))
        elif self.SL_ON == 3:
            self.sl_price = ba.get_round_to_mintick(
                base_price_sl * (1 + mult * self.sl_mult / 100), self.minTickSize)
            self.logger.info('SL price: {}'.format(self.sl_price))
        else:
            if self.entry_mode == 0:
                self.sl_price = 0
            else:
                self.sl_price = 9999999.0

    def compute_dynamic(self):
        if self.HH_ON:
            hh_price = ba.get_round_to_mintick(self.hh, self.minTickSize)
            self.logger.info('HH price: {}'.format(hh_price))
        else:
            hh_price = 9999999.0

        if self.LL_ON:
            ll_price = ba.get_round_to_mintick(self.ll, self.minTickSize)
            self.logger.info('LL price: {}'.format(ll_price))
        else:
            ll_price = 0.0

        mult = -1 if self.entry_mode == 0 else 1
        mybase = self.current_high if self.entry_mode == 0 else self.current_low
        if self.TL_ON == 1:
            self.tl_price = ba.get_round_to_mintick(
                self.current_close + mult * self.atr[0] * self.tl_mult, self.minTickSize)
            self.logger.info('TL price: {}'.format(self.tl_price))
        elif self.TL_ON == 2 and self.position_size != 0:
            tl_as_ticks = self.tl_mult / float(self.pointValue) / self.minTickSize / float(
                abs(self.entry_trade.order.totalQuantity))
            self.tl_price = ba.get_round_to_mintick(mybase + mult * tl_as_ticks * self.minTickSize,
                                                    self.minTickSize)
            self.logger.info('TL price: {}'.format(self.tl_price))
        elif self.TL_ON == 3:
            self.tl_price = ba.get_round_to_mintick(
                self.current_close * (1 + mult * self.tl_mult / 100), self.minTickSize)
            self.logger.info('TL price: {}'.format(self.tl_price))
        else:
            if self.entry_mode == 0:
                self.tl_price = 0
            else:
                self.tl_price = 9999999.0
        return hh_price, ll_price

    def check_exit_condition(self):
        # Price condition
        price_condition = False
        self.logger.info('Exit price condition: {}'.format(price_condition))

        if (self.hold_count < self.Max_Time and self.Profitable_Closes <= self.prof_x) or \
                (self.hold_count >= self.Max_Time) or (price_condition)  or ( self.current_date.hour >= 16 and self.current_date.minute >= 0 ):
            self.exit_signal = True
        else:
            self.exit_signal = False

        self.logger.info('Exit signal: {}'.format(self.exit_signal))

    def cancel_pending_order(self):
        if isinstance(self.entry_trade, Trade):
            if self.entry_trade.isActive():
                self.ib.cancelOrder(self.entry_trade.order)
                self.logger.info('Entry trade cancelled')
        if isinstance(self.sl_trade, Trade):
            if self.sl_trade.isActive():
                self.ib.cancelOrder(self.sl_trade.order)
                self.logger.info('SL trade cancelled')
        if isinstance(self.pt_trade, Trade):
            if self.pt_trade.isActive():
                self.ib.cancelOrder(self.pt_trade.order)
                self.logger.info('PT trade cancelled')

    def place_exit_order(self):
        if self.entry_mode == 0:  # Long entry
            self.exit_order = ods.create_sell_market_order(abs(self.entry_order.totalQuantity))
        else:  # Short entry
            self.exit_order = ods.create_buy_market_order(abs(self.entry_order.totalQuantity))
        self.ib.placeOrder(self.contract, self.exit_order)
        self.ib.sleep(1)
        self.logger.info('Exit order sent: {}, {}'.format(self.exit_order.action,
                                                          self.exit_order.totalQuantity))

        self.logger.info('Hold count: {}'.format(self.hold_count))
        self.logger.info('Number of profitable closes: {}'.format(self.prof_x))

        self.hold_count = 0
        self.prof_x = 0
        self.entry_trade = None
        self.sl_trade = None
        self.pt_trade = None

    def check_exit_move(self):
        self.exit_move = False
        hh_price, ll_price = self.compute_dynamic()
        self.aggregate_fixed_dynamic(hh_price, ll_price)

        exit_moved = ['SL']

        if self.TL_ON == 1 or self.TL_ON == 2:
            if self.entry_mode == 0:  # Long entry
                self.exit_move = True if self.sl_price > self.sl_order.auxPrice else False
            else:  # Short entry
                self.exit_move = True if self.sl_price < self.sl_order.auxPrice else False

        if self.entry_mode == 0:
            if self.LL_ON:
                if self.sl_price != self.sl_order.auxPrice:
                    self.exit_move = True
            if self.HH_ON:
                if self.pt_price != self.pt_order.lmtPrice:
                    self.exit_move = True
                    exit_moved.append('PT')
        if self.entry_mode == 1:
            if self.LL_ON:
                if self.pt_price != self.pt_order.lmtPrice:
                    self.exit_move = True
                    exit_moved.append('PT')
            if self.HH_ON:
                if self.sl_price != self.sl_order.auxPrice:
                    self.exit_move = True

        self.logger.info('Exit move: {}'.format(self.exit_move))
        return exit_moved

    def edit_protection_order(self, exit_moved):
        if 'PT' in exit_moved:
            if self.pt_trade.isActive():
                self.ib.cancelOrder(self.pt_trade.order)
                self.logger.info('Cancelled PT order')

                if self.pt_price != 0:
                    if self.entry_mode == 0:
                        self.pt_order = ods.create_limit_sell_order(self.pt_price, self.entry_order.totalQuantity)
                    else:
                        self.pt_order = ods.create_limit_buy_order(self.pt_price, self.entry_order.totalQuantity)

                    self.pt_trade = self.ib.placeOrder(contract=self.contract, order=self.pt_order)
                    self.ib.sleep(1)
                    self.logger.info('PT order sent: {}, {}, {}'.format(self.pt_order.action,
                                                                        self.pt_order.totalQuantity,
                                                                        self.pt_price))

        if self.sl_order is None:
            return

        if self.sl_price == self.sl_order.auxPrice:
            return

        if self.sl_trade.isActive():
            self.ib.cancelOrder(self.sl_trade.order)
            self.logger.info('Cancelled SL order')
        else:
            return

        if self.sl_price != 0:
            if self.entry_mode == 0:
                self.sl_order = ods.create_stop_sell_order(self.sl_price, self.entry_order.totalQuantity)
            else:
                self.sl_order = ods.create_stop_buy_order(self.sl_price, self.entry_order.totalQuantity)

            self.sl_trade = self.ib.placeOrder(contract=self.contract, order=self.sl_order)
            self.ib.sleep(1)
            self.logger.info('Stop order sent: {}, {}, {}'.format(self.sl_order.action,
                                                                  self.sl_order.totalQuantity,
                                                                  self.sl_price))

    def get_secondary_historical_bardata(self):
        for contract in self.contracts:
            h_contract = Contract()
            h_contract.symbol = contract.get('symbol')
            h_contract.secType = contract.get('secType')
            h_contract.currency = contract.get('currency')
            h_contract.exchange = contract.get('exchange')
            h_contract.lastTradeDateOrContractMonth = contract.get('expiration')

            h_contracts = self.ib.qualifyContracts(h_contract)
            if not h_contracts:
                self.logger.info('No contract found for {}'.format(h_contract.symbol))
                exit()

            bars = self.ib.reqHistoricalData(h_contract,
                                             '',
                                             contract.get('depth'),
                                             barSizeSetting=contract.get('interval'),
                                             whatToShow='TRADES',
                                             useRTH=False,
                                             formatDate=1,
                                             keepUpToDate=True,
                                             chartOptions=[])

            opens = [bar.open for bar in bars]
            highs = [bar.high for bar in bars]
            lows = [bar.low for bar in bars]
            closes = [bar.close for bar in bars]
            volumes = [bar.volume for bar in bars]
            dates = [bar.date for bar in bars]

            opens.reverse()
            highs.reverse()
            lows.reverse()
            closes.reverse()
            volumes.reverse()
            dates.reverse()

            self.secondary_opens.update({contract['symbol']: opens})
            self.secondary_highs.update({contract['symbol']: highs})
            self.secondary_lows.update({contract['symbol']: lows})
            self.secondary_closes.update({contract['symbol']: closes})
            self.secondary_volumes.update({contract['symbol']: volumes})
            self.secondary_bardates.update({contract['symbol']: dates})

            bars.updateEvent += self.on_bar_secondary

    def on_bar_secondary(self, bars, has_new_bar):
        if has_new_bar:
            symbol = bars.contract.symbol
            if bars[-2].date not in self.secondary_bardates.get(symbol):
                self.logger.info('New bar secondary {}: {}'.format(symbol, bars[-2].date))
                self.logger.info('Open    secondary: {}'.format(bars[-2].open))
                self.logger.info('Close   secondary: {}'.format(bars[-2].close))
                self.logger.info('High    secondary: {}'.format(bars[-2].high))
                self.logger.info('Low     secondary: {}'.format(bars[-2].low))
                self.logger.info('Volume  secondary: {}'.format(bars[-2].volume))

                tmp = self.secondary_bardates.get(symbol)
                tmp.insert(0, bars[-2].date)
                self.secondary_bardates.update({symbol: tmp})
                tmp = self.secondary_opens.get(symbol)
                tmp.insert(0, bars[-2].open)
                self.secondary_opens.update({symbol: tmp})
                tmp = self.secondary_closes.get(symbol)
                tmp.insert(0, bars[-2].close)
                self.secondary_closes.update({symbol: tmp})
                tmp = self.secondary_highs.get(symbol)
                tmp.insert(0, bars[-2].high)
                self.secondary_highs.update({symbol: tmp})
                tmp = self.secondary_lows.get(symbol)
                tmp.insert(0, bars[-2].low)
                self.secondary_lows.update({symbol: tmp})
                tmp = self.secondary_volumes.get(symbol)
                tmp.insert(0, bars[-2].volume)
                self.secondary_volumes.update({symbol: tmp})

            # reverse to normal: last in the list is the most recent
            secondary_revopens = self.secondary_opens.get(symbol).copy()
            secondary_revopens.reverse()
            secondary_revhighs = self.secondary_highs.get(symbol).copy()
            secondary_revhighs.reverse()
            secondary_revlows = self.secondary_lows.get(symbol).copy()
            secondary_revlows.reverse()
            secondary_revcloses = self.secondary_closes.get(symbol).copy()
            secondary_revcloses.reverse()
            secondary_revvolumes = self.secondary_volumes.get(symbol).copy()
            secondary_revvolumes.reverse()
            secondary_revbardates = self.secondary_bardates.get(symbol).copy()
            secondary_revbardates.reverse()


            # self.secondary_valueCloses[symbol].append(ba.value_close(secondary_revhighs, secondary_revlows, secondary_revcloses[-1], 5))
            # self.secondary_valueHighs[symbol].append(ba.value_high(secondary_revhighs, secondary_revlows, secondary_revcloses[-1], 5))

            self.secondary_current_close.update({symbol: bars[-2].close})
            self.secondary_current_open.update({symbol: bars[-2].open})
            self.secondary_current_low.update({symbol: bars[-2].low})
            self.secondary_current_high.update({symbol: bars[-2].high})
            self.secondary_current_volume.update({symbol: bars[-2].volume})
            self.secondary_current_date.update({symbol: bars[-2].date})
            # conversion set for FX sizing
            #if symbol == self.contracts[-1]['symbol']:
            #    self.conv = self.secondary_closes.get('GBP')[0]

    def get_current_market_status(self):
        market_closed = False
        if len(self.bardates) > 0:
            current_server_time = self.server_time.replace(microsecond=0)
            latest_bartime = pd.to_datetime(self.bardates[0])
            current_estime = current_server_time.astimezone(pytz.timezone('US/Eastern')).replace(tzinfo=None).replace(
                microsecond=0)
            time_delta = current_estime - latest_bartime

            historical_timeframe = self.bar_timeframe.split()

            if self.bar_timeframe.endswith("secs") or self.bar_timeframe.endswith("sec"):
                if len(historical_timeframe) > 0 and time_delta > datetime.timedelta(
                        seconds=2 * int(historical_timeframe[0])):
                    market_closed = True
            elif self.bar_timeframe.endswith("min") or self.bar_timeframe.endswith("mins"):
                if len(historical_timeframe) > 0 and time_delta > datetime.timedelta(
                        minutes=2 * int(historical_timeframe[0])):
                    market_closed = True
            elif self.bar_timeframe.endswith("hour") or self.bar_timeframe.endswith("hours"):
                if len(historical_timeframe) > 0 and time_delta > datetime.timedelta(
                        hours=2 * int(historical_timeframe[0])):
                    market_closed = True
            elif self.bar_timeframe.endswith("day"):
                if len(historical_timeframe) > 0 and time_delta > datetime.timedelta(
                        days=2 * int(historical_timeframe[0])):
                    market_closed = True
            elif self.bar_timeframe.endswith("week"):
                if len(historical_timeframe) > 0 and time_delta > datetime.timedelta(
                        weeks=2 * int(historical_timeframe[0])):
                    market_closed = True
            elif self.bar_timeframe.endswith("month"):
                if len(historical_timeframe) > 0 and time_delta > datetime.timedelta(
                        days=2 * int(historical_timeframe[0]) * 31):
                    market_closed = True
        return market_closed

    def get_current_twstime(self):
        self.server_time = self.ib.reqCurrentTime()
        self.logger.info('Server time: {}'.format(self.server_time))

    def get_TDOM(self):
        tdom_day = datetime.datetime.strftime(self.current_date, "%Y%m%d %H:%M:%S")
        bars = self.ib.reqHistoricalData(self.contract,
                                         tdom_day,
                                         "1 M",
                                         "1 day",
                                         self.whatToShow,
                                         True,
                                         1,
                                         False,
                                         [])
        if bars[-2].date.month == self.current_date.month:
            self.TDOM.insert(0, self.closes[0] / bars[-2].close - 1)
            # self.logger.info('TDOM: {}'.format(self.TDOM[0]))

    def get_MTD(self):
        first_day_of_current_month = self.current_date.replace(day=1)
        mtd_day = datetime.datetime.strftime(first_day_of_current_month, "%Y%m%d %H:%M:%S")
        bars = self.ib.reqHistoricalData(self.contract,
                                         mtd_day,
                                         "3 D",
                                         "1 day",
                                         self.whatToShow,
                                         True,
                                         1,
                                         False,
                                         [])
        if bars[-2].date.month == self.current_date.month - 1:
            if len(self.closes) > 0:
                self.MTD.insert(0, self.closes[0] / bars[-2].close - 1)
                # self.logger.info('MTD: {}'.format(self.MTD[0]))

    def get_QTD(self):
        d_month = self.current_date.month
        if d_month % 3 == 0:
            first_day_of_next_qmonth = self.current_date.replace(month=(d_month - 3 + 1), day=1)
        else:
            first_day_of_next_qmonth = self.current_date.replace(month=(d_month - (d_month % 3) + 1), day=1)
        bars = self.ib.reqHistoricalData(self.contract,
                                         datetime.datetime.strftime(first_day_of_next_qmonth, "%Y%m%d %H:%M:%S"),
                                         "3 D",
                                         "1 day",
                                         self.whatToShow,
                                         True,
                                         1,
                                         False,
                                         [])
        if len(self.closes) > 0:
            self.QTD.insert(0, self.closes[0] / bars[-2].close - 1)
            # self.logger.info('QTD: {}'.format(self.QTD[0]))

    def get_YTD(self):
        last_day_of_previous_year = self.current_date.replace(month=12, day=31) - relativedelta(years=1)
        bars = self.ib.reqHistoricalData(self.contract,
                                         datetime.datetime.strftime(last_day_of_previous_year, "%Y%m%d %H:%M:%S"),
                                         "3 D",
                                         "1 day",
                                         self.whatToShow,
                                         True,
                                         1,
                                         False,
                                         [])
        if len(self.closes) > 0:
            self.YTD.insert(0, self.closes[0] / bars[-2].close - 1)
            # self.logger.info('YTD: {}'.format(self.YTD[0]))

    def contract_details(self, contractDetails):
        self.minTickSize = contractDetails.minTick
        self.pointValue = contractDetails.contract.multiplier if contractDetails.contract.multiplier else 1
        self.logger.info('Min tick size: {}'.format(self.minTickSize))
        self.logger.info('Point value: {}'.format(self.pointValue))


def start_strategy_app(app: Strategy):
    app.start_strategy()


def stop_strategy_app(app: Strategy):
    app.ib.disconnect()

def main():
    app_id = 1
    ip_address = "127.0.0.1"
    port = 7497  # TWS WorkStation port
    # port = 4001 # Gateway port
    app = Strategy(ip_address, port, app_id)
    daily_mode = False

    if daily_mode:
        schedule.every().day.at("17:08").do(start_strategy_app, app)
        schedule.every().day.at("17:11").do(stop_strategy_app, app)

        while True:
            schedule.run_pending()
            time.sleep(1)
    else:
        start_strategy_app(app)


if __name__ == '__main__':
    main()

