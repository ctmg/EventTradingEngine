# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 13:07:06 2014

@author: colin4567
"""

import numpy as np
import datetime
import pandas as pd
import Queue
from math import floor

from event import FillEvent, OrderEvent
from performance import create_sharpe_ratio, create_drawdowns


class Portfolio(object):
    """
    Handles the positions and market value of all instruments at a 
    resolution of the 'bar'
    
    Objects:
    positions df - stores time-index of the quantity of positions held
    holdings df - stores the cash and total market holdings value of each symbol 
                    for a particular time-index, as well as the percentage 
                    change in the portfolio across bars.
    """
    def __init__(self, bars, events, start_date, initial_capital=1000000.0):
        """
        Initializes the porfolio with bars and an event queue.
        
        Parameters:
        bars - DataHandler object with current market data
        events - Event Queue object
        start_date - starting bar
        initial_capital - starting USD value
        """
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital
        
        self.all_positions = self.construct_all_positions()
        self.current_positions = dict((k,v) for k,v in [(s,0) for s in self.symbol_list])
        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()    
    
    
    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date to determine 
        when the time index will begin
        
        initializes by creating a dict for each symbol, 
        setting the value to zero, adding a datetime key, & adding it to a list
        """
        d = dict((k,v) for k,v in [(s,0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        return [d] #list of dicts? 
    
    
    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        """
        d = dict((k,v) for k,v in [(s,0.0) for s in self.symbol_list])
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]
        
    
    def construct_current_holdings(self):
        """
        dict that holds instantaneous value of the portfolio across all symbols
        """
        d = dict((k,v) for k,v in [(s,0.0) for s in self.symbol_list])
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d
    
    
    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current market data
        bar. This reflects all info known up this point (previous bar). 
        Uses the MarketEvent from the events que
        """
        latest_datetime = self.bars.get_latest_bar_datetime(self.symbol_list[0])
        
        #Update positions
        #================
        dp = dict((k,v) for k,v in [(s,0) for s in self.symbol_list])
        dp['datetime'] = latest_datetime
        
        for s in self.symbol_list:
            dp[s] = self.current_positions[s]

        #Append the current positions
        self.all_positions.append(dp)
        
        #Update holdings
        #===============
        dh = dict((k,v) for k,v in [(s,0) for s in self.symbol_list])
        dh['datetime'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission'] 
        dh['total'] = self.current_holdings['cash'] 
        
        for s in self.symbol_list:
            #updates value of holdings for both current and all holdings
            market_value = self.current_holdings[s] * (1 + self.bars.get_latest_ror_value(s))          
            self.current_holdings[s] = market_value            
            dh[s] = market_value
            dh['total'] += market_value
            
        
        #Append the current holdings
        self.all_holdings.append(dh)
    
    
    def update_positions_from_fill(self, fill):
        """
        Takes a Fill object and updates the position matrix 
        to reflect the new position.
        
        Parameters:
        fill - the Fill object to update the positions
        """
        #check whether its a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1
            
        #update positions list with new quantities
        self.current_positions[fill.symbol] += fill_dir * fill.quantity
        
    
    def update_holdings_from_fill(self, fill):
        """Takes a fill object and updates the holdings matrix 
        to reflect the holdings value
        
        Parameters:
        fill - the Fill object to update the holdings
        """

       #check whether its a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1
        
        #THIS IS WHERE THE FILL IS HAPPENING!!!! 
        #changed to be same as market_value in update_timeindex
        if fill.position_change == 'EXIT':
            fill_cost = self.current_holdings[fill.symbol] 
            cost = fill_dir * fill_cost
        else:
            fill_cost = self.bars.get_latest_bar_value(fill.symbol, 'close')
            cost = fill_dir * fill_cost * fill.quantity
        
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] -= fill.commission #made negative
        self.current_holdings['cash'] -= (cost + fill.commission) 
        #something is funky here
        self.current_holdings['total'] -= (cost + fill.commission)
    
    
    def update_fill(self, event):
        """
        virtual method of portfolio class that updates positions 
        and holdings when given an event
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)
            
    
    def generate_naive_order(self, signal):
        """
        files an Order object as a function of signal strength (1/N)) and current mrkt values
        without any risk management or position sizing considerations.
        
        Parameters:
        signal - tuple containing Signal info
        
        """
        order = None
        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength
        mkt_quantity = int(floor((self.initial_capital * strength) / self.bars.get_latest_bar_value(symbol, "close")))
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'
        
        #should be able to use direction in place of position_change and get rid of position_change from init
        if direction == "LONG" and cur_quantity == 0:
            position_change = 'LONG_ENTRY'
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY', position_change)
        elif direction == "SHORT" and cur_quantity == 0:
            position_change = 'SHORT_ENTRY'
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL', position_change)
        
        elif direction == 'EXIT' and cur_quantity > 0:
            position_change = direction
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL', position_change)
        elif direction == 'EXIT' and cur_quantity < 0:
            position_change= direction
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY', position_change)
        
        return order
        
    
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders and 
        adds it to the events queue
        """
        if event.type == 'SIGNAL':
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)
    
    
    def create_equity_curve_dataframe(self):
        """
        Creates a pandas df for the all_holdings list of dicts
        """
        from copy import deepcopy
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['equity_curve'] = (1.0 + curve['returns']).cumprod()
        #reorganize columns
        curve_columns = deepcopy(self.symbol_list)
        [curve_columns.append(x) for x in ['cash', 'commission', 'total', 'returns', 'equity_curve']]
        curve = curve[curve_columns]
        curve['equity_curve'].plot(title='equity curve') # <----------added 10/24     
        curve['cash'].plot(secondary_y=True)
        self.equity_curve = curve
        
    
    def create_positioning_dataframe(self):
        positions = pd.DataFrame(self.all_positions)
        positions.set_index('datetime', inplace=True)
        #positions['aapl'].plot(secondary_y=True)
        self.positions = positions
        
        
    
    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']
        
        sharpe_ratio = create_sharpe_ratio(returns)
        max_dd, dd_duration = create_drawdowns(pnl)
        
        stats = [("Total Return", "%0.2f%%" % ((total_return - 1)*100)),
                ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                ("Max Drawdown", "%0.2f%%" % (max_dd*100.0)),
                ("Drawdown Duration", "%d" %dd_duration)]
                
        self.equity_curve.to_csv('equity.csv')
        return stats
        
        
        
        
        
        
        
        
        
        
        
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        