# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 10:50:35 2014

@author: colin4567
"""

import numpy as np
import pandas as pd
import datetime
import os, os.path
pd.set_option('notebook_repr_html', False)

from abc import ABCMeta, abstractmethod

from event import MarketEvent


class DataHandler(object):
    """
    Abstract base class providing an interface for all subsequent (inherited)
    data handlers both live and historical
    
    Goal: output a generated set of bars (OHLCVI) for each symbol requested.
        Enables historical and live data to be treated identically 
        by the rest of the backtesting suite.
    
    The decorator used below to let Python know the methods will be overridden
    by the subclasses - virtual methods
    """
    __metaclass__ = ABCMeta
    
    @abstractmethod 
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated
        """
        raise NotImplementedError("Should implement get_latest_bar()")
                
    @abstractmethod 
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated
        """
        raise NotImplementedError("Should implement get_latest_bars()")
    
    @abstractmethod 
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")
        
    @abstractmethod
    def get_latest_bar_value(self, symbol):
        """
        Returns one of the OHLCVI values from the last bar
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")
    
    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the latest_symbol list,
        or N-k if less available
        """
        raise NotImplementedError("Should implement get_latest_bar_values()")
    
    @abstractmethod
    def get_latest_ror_value(self, symbol):
        """
        Returns the last ror values for the adj_close series
        """
        raise NotImplementedError("Should implement get_latest_ror_value()")
    
    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol in a tuple 
        OHLCVI format (datetime, open, high, low, etc..)
        """
        raise NotImplementedError("Should implement update_bars()")
        
        
#==============================================================================        


class HistoricCSVDataHandler(DataHandler):
    """
    Reads csv files for each symbol from disk and provide an interface to 
    obtain the "latest" bar in a manner identical to live trading interface.
    
    Parameters:
    events - event queue
    csv_dir - absolute directory to the csv files
    symbol_list - list of symbol strings
    """
    def __init__(self, events, csv_dir, symbol_list, start_date):
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        self.start_date = start_date
        
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        
        self._open_convert_csv_files()
    
    def _open_convert_csv_files(self):
        """
        Opens the CSV files and converts to pd.DataFrame within a symbol dict
        """
        comb_index = None
        for s in self.symbol_list:
            #this is a dict
            self.symbol_data[s] = pd.io.parsers.read_csv(
                                        os.path.join(self.csv_dir,
                                                     '%s.csv' % s),
                                        header=0, index_col=0,
                                        parse_dates=True,
                                        names=['open', 'high', 
                                        'low', 'close', 'adj_close']).sort()
            #combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)
            
            #reset this to None
            self.latest_symbol_data[s] = []
        
        #reindex the df's
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index,
                                                    method='pad').iterrows()
    
    
    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed. Subsequent calls will yeild
        a new bar until the end of the symbol data
        """
        for b in self.symbol_data[symbol]:
            yield b

    #Begin defining virtual classes

    def get_latest_bar(self, symbol):
        """
        Returns last bar from latest_symbol list
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set"
            raise
        else:
            return bars_list[-1]
    
    
    def get_latest_bars(self, symbol, N=1):
        """
        Returns last N bars from latest_symbol list, or N-k if less available
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set"
            raise
        else:
            return bars_list[-N:]
        
    
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "The symbol is not available in the historical data set."
            raise
        else:
            return bars_list[-1][0]
            
    
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the OHLCVI from the pandas Bar object
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set."
            raise
        else:
            #why the [1]?
            return getattr(bars_list[-1][1], val_type)
    
    
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the latest_symbol list, or N-k
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print "That symbol is not available in the historical data set."
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])
            
            
    def get_latest_ror_value(self, symbol):
        """
        Returns the last ror values for the adj_close series
        """
        try:
            bars_list = self.get_latest_bars(symbol, 2)
        except KeyError:
            print "That symbol is not available in the historical data set."
            raise
        else:
            return np.array(pd.Series([getattr(b[1], 'adj_close') for b in bars_list]).pct_change())
    
    
    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list
        """
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s).next()
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())
        
        
#==============================================================================

import MySQLdb as mdb
import pandas.io.sql as psql

        
class MySQLDataHandler(DataHandler):
    
    def __init__(self, events, db_host, db_user, db_pass, db_name, symbol_list, start_date):
        self.events = events        
        self.db_host = db_host
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        self.con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
        
        self.symbol_list = symbol_list
        self.start_date = start_date        
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        
        self._update_symbol_data(self.con)
        #puts bars up to start_date into latest_symbol_data
        self._get_historical_bars()
    
    
    def _update_symbol_data(self, con):
        comb_index = None
        
        for s in self.symbol_list:
            sql_bars = """SELECT dp.price_date, dp.open_price, dp.high_price, dp.low_price, dp.close_price, dp.adj_close_price
                            FROM securities_master.symbol as sym
                            INNER JOIN securities_master.daily_price AS dp
                            ON dp.symbol_id = sym.id
                            WHERE sym.ticker = "%s"
                            ORDER BY dp.price_date ASC;""" %(s,)
            #this is a dict where market name is key and value is df of prices
            self.symbol_data[s] = psql.read_sql(sql_bars, con=con, index_col='price_date', parse_dates=['price_date'])
            self.symbol_data[s].rename(columns={'open_price':'open', 'high_price':'high', 'low_price':'low', 
                                        'close_price':'close', 'adj_close_price': 'adj_close'}, inplace=True)
            
                                        
            #combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)
            
            #reset this to None
            self.latest_symbol_data[s] = []
        
        #reindex the df's
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index,
                                                    method='pad').iterrows()
    
    
        
    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed. Subsequent calls will yeild
        a new bar until the end of the symbol data
        """
        for b in self.symbol_data[symbol]:
            yield b


    #Begin defining virtual classes

    def get_latest_bar(self, symbol):
        """
        Returns last bar from latest_symbol list
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set"
            raise
        else:
            return bars_list[-1]
    
    
    def get_latest_bars(self, symbol, N=1):
        """
        Returns last N bars from latest_symbol list, or N-k if less available
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set"
            raise
        else:
            return bars_list[-N:]
        
    
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "The symbol is not available in the historical data set."
            raise
        else:
            return bars_list[-1][0]
            
    
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the OHLCVI from the pandas Bar object
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print "That symbol is not available in the historical data set."
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
    
    
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the latest_symbol list, or N-k
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print "That symbol is not available in the historical data set."
            raise
        else:
            #b[0] = datetime, b[1] = bars
            return np.array([getattr(b[1], val_type) for b in bars_list])
    

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list
        """
        for s in self.symbol_list:
            try:
                bar = self._get_new_bar(s).next()
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())
        
        
    def get_latest_ror_value(self, symbol):
        """
        Returns the last ror values for the adj_close series
        """
        try:
            bars_list = self.get_latest_bars_values(symbol, 'adj_close', 2)
        except KeyError:
            print "That symbol is not available in the historical data set."
            raise
        else:
            return np.array(pd.Series(bars_list).pct_change())[-1]
                
        
    def _get_historical_bars(self):
        """
        Loads bars from before the start date - issue here is that bar for start_date has already been yielded - need to fix
        """
        for s in self.symbol_list:
            continue_historical = True
            while  continue_historical:
                try:
                    bar = self._get_new_bar(s).next()
                except StopIteration:
                    self.continue_backtest = False
                else :
                    if bar is not None and bar[0].to_datetime() < self.start_date:
                        self.latest_symbol_data[s].append(bar)
                    else:
                        continue_historical = False





#==============================================================================



class WABDataHandler(DataHandler):
    
    def __init__(self, events, symbol_list):
        self.events = events
        self.symbol_list = symbol_list
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        


        
