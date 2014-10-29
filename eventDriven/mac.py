# -*- coding: utf-8 -*-
"""
Created on Tue Sep 16 11:50:53 2014

@author: colin4567
"""

import numpy as np
import datetime

from backtest import Backtest
from data import HistoricCSVDataHandler
from data import MySQLDataHandler
from event import SignalEvent
from execution import SimulatedExecutionHandler
from portfolio import Portfolio
from strategy import Strategy


class MovingAverageCrossStrategy(Strategy):
    """
    Default window is 34/144
    """
    def __init__(self, bars, events, short_window=34, long_window=144):
        """
        Initializes the buy and hold strategy
        
        Parameters:
        bars - DataHandler object that provides bar info
        events - Event queue object
        short/long windows - moving average lookbacks
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.short_window = short_window
        self.long_window = long_window
        
        #set to true if a symbol if in the market
        self.bought = self._calculate_initial_bought()
    
    
    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols and sets them to 'OUT'
        """
        bought = {}        
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought
    
    
    def calculate_signals(self, event):
        """
        Generates a new SignalEvent object and places it on the events 
        Event queue, and updates the bought attribute.
        
        Parameters:
        event - a MarketEvent object
        """
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                bars = self.bars.get_latest_bars_values(symbol, "adj_close", N=self.long_window)
                
                if bars is not None and bars != []:
                    short_sma = np.mean(bars[-self.short_window:])
                    long_sma = np.mean(bars[-self.long_window:])
                    dt = self.bars.get_latest_bar_datetime(symbol)
                    sig_dir = ""
                    strength = 1.0 / len(self.symbol_list) #this is where you set percentage of capital - set to 1/n
                    strategy_id = 1
                    
                    if short_sma > long_sma and self.bought[symbol] == 'OUT':
                        sig_dir = 'LONG'
                        signal = SignalEvent(strategy_id, symbol, dt, sig_dir, strength)
                        self.events.put(signal)
                        self.bought[symbol] = 'LONG'
                    
                    elif short_sma < long_sma and self.bought[symbol] == 'LONG':
                        sig_dir = 'EXIT'
                        signal = SignalEvent(strategy_id, symbol, dt, sig_dir, strength)
                        self.events.put(signal)
                        self.bought[symbol] = 'OUT'


if __name__ == '__main__':
    
    import os
    
    symbol_list = ['aapl', 'ibm', 'xom', 'intc', 'aa']
    initial_capital = 1000000.0
    start_date = datetime.datetime(2001,1,3,0,0,0)
    heartbeat = 0.0
    data_feed = 2 # 1 is csv, 2 is MySQL
    
    if data_feed == 1:
        if os.path.isdir("C:/Users/colin4567/Dropbox/EventTradingEngine/getData/testData"):
            csv_dir = os.path.normpath("C:/Users/colin4567/Dropbox/EventTradingEngine/getData/testData")
        elif os.path.isdir("/Users/colinmark-griffin/Dropbox/EventTradingEngine/getData/testData"):
            csv_dir = os.path.normpath("/Users/colinmark-griffin/Dropbox/EventTradingEngine/getData/testData")
        else:
            raise SystemExit("No csv dir found for windows or mac")
            
        backtest = Backtest(symbol_list, 
                            data_feed,
                            initial_capital, 
                            heartbeat, 
                            start_date, 
                            HistoricCSVDataHandler, 
                            SimulatedExecutionHandler, 
                            Portfolio, 
                            MovingAverageCrossStrategy, 
                            csv_dir=csv_dir)  
                        
    elif data_feed == 2:
        db_host = 'localhost'; db_user = 'sec_user'; db_pass = 'longgamma'; db_name = 'securities_master';
        
        backtest = Backtest(symbol_list, 
                            data_feed,
                            initial_capital, 
                            heartbeat, 
                            start_date, 
                            MySQLDataHandler, 
                            SimulatedExecutionHandler, 
                            Portfolio, 
                            MovingAverageCrossStrategy, 
                            db_host=db_host, db_user=db_user, db_pass=db_pass, db_name=db_name)  

                  
    
    backtest.simulate_trading()
                    
                    
                    
                    
                    
                    
                    
                    
                    
                    
        
        
        
        
        
        
        
        
        

