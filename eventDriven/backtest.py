# -*- coding: utf-8 -*-
"""
Created on Tue Sep 16 10:27:00 2014

@author: colin4567
"""

import datetime
import pprint
import Queue
import time

class Backtest(object):
    """
    Encapsulates the settings and components for carrying out an event-driven
    backtest.
    """
    def __init__(self, symbol_list, data_feed, initial_capital,
                 heartbeat, start_date, data_handler, execution_handler, 
                 portfolio, strategy, db_host=None, db_user=None, db_pass=None, db_name=None, csv_dir=None):
        """
        Initializes the backtest.
        
        Parameters:
        csv_dir - the hard root to the CSV data directory
        symbol_list - list of symbol string
        initial_capital - starting capital
        heartbeat - backtest heartbeat in seconds
        start_date - starting date of strategy
        data_handler - (Class) handles the market data feed
        execution_handler - (Class) handles the orders/fills for trades
        portfolio - (Class) Keeps track of current and prior positions
        strategy - (Class) generates signals based on market data
        """
        self.symbol_list = symbol_list
        self.data_feed = data_feed
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.start_date = start_date
        self.data_handler_cls = data_handler
        self.execution_handler_cls = execution_handler
        self.portfolio_cls = portfolio
        self.strategy_cls = strategy
        
        self.csv_dir = csv_dir
        self.db_host = db_host
        self.db_user = db_user
        self.db_pass = db_pass
        self.db_name = db_name
        
        
        self.events = Queue.Queue()
        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1
        
        self._generate_trading_instances(data_feed)
    

    def _generate_trading_instances(self, data_feed):
        """
        Generates the trading instance objects from their class types
        """
        print "Creating DataHandler, Strategy, Portfolio, and ExecutionHandler..."
        
        if self.data_feed == 1: #HistoricCSVDataHandler
            self.data_handler = self.data_handler_cls(self.events, self.csv_dir, self.symbol_list)
        elif self.data_feed == 2: #MySQLDataHandler
            self.data_handler = self.data_handler_cls(self.events, self.db_host, self.db_user, self.db_pass, self.db_name, self.symbol_list)                                              
                                                  
        self.strategy = self.strategy_cls(self.data_handler, self.events)
        
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, 
                                            self.start_date, self.initial_capital)
                                            
        self.execution_handler = self.execution_handler_cls(self.events)
        
    
    def _run_backtest(self):
        """
        Executes the backtest. Outer loop keeps track of the heartbeat, 
        inner checks if there is an event in the Queue object and sacts on it 
        by calling the appropriate method on the necessary object.
        """
        i = 0
        while True:
            i += 1
            #Update the market bars
            if self.data_handler.continue_backtest == True:
                self.data_handler.update_bars()
            else:
                break
            
            #handle the events
            while True:
                try:
                    event = self.events.get(False) #block=False
                except Queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.portfolio.update_timeindex(event)
                        
                        elif event.type == 'SIGNAL':
                            self.signals += 1
                            self.portfolio.update_signal(event)
                    
                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)
                        
                        elif event.type == 'FILL':
                            self.fills += 1
                            self.portfolio.update_fill(event)
                            
            time.sleep(self.heartbeat)
    
    
    def _output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """

        self.portfolio.create_equity_curve_dataframe()
        self.portfolio.create_positioning_dataframe()
        
        print "Creating summary stats..."
        stats = self.portfolio.output_summary_stats()
        
        print "Creating the equity curve...\n"
        print self.portfolio.equity_curve[50:75]
        print ('')        

        print "Ending equity curve...\n"
        print self.portfolio.equity_curve.tail(10)        
        
        
        print "Creating the historical positioning...\n"
        print self.portfolio.positions.tail(10)
        print ('')
        pprint.pprint(stats)
        
        print ('')
        print "Signals: %s" % self.signals
        print "Orders: %s" % self.orders
        print "Fills: %s" % self.fills
        
        
    
    def simulate_trading(self):
        """
        This is called from __main__.
        Simulates the backtest and outputs portfolio performance
        """
        self._run_backtest()
        self._output_performance()
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
                        
                        
                    
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
        
        
        
        
        
        
        
        
        
        