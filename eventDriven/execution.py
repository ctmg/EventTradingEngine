# -*- coding: utf-8 -*-
"""
Created on Tue Sep 16 09:43:01 2014

@author: colin4567
"""

import datetime
import Queue

from abc import ABCMeta, abstractmethod
from event import FillEvent, OrderEvent


class ExecutionHandler(object):
    """
    The ExecutionHandler abstract class handles the interaction between a 
    set of order objects created by a Portfolio and the Fill Objects that 
    occur in the market.
    
    The handlers can be used to subclass simulated brokerages or 
    live brokerages with the same interface.
    """
    __metaclass__ = ABCMeta
    
    @abstractmethod
    def execute_order(self, event):
        """
        Takes an Order event and executes it, producing a Fill event that
        get placed into the Events queue.
        
        Parameters:
        event - contains an event object with order information
        """
        raise NotImplementedError("Should implement execute_order()")


class SimulatedExecutionHandler(ExecutionHandler):
    """
    Converts all order objects into their equivalent fill objects automatically
    without latency or slippage.
    """
    def __init__(self, events):
        """
        Initializes the handler, setting the event queues internally.
       
       Parameters:
        events - The Queue of Event objects
        """
        self.events = events
    
    def execute_order(self, event):
        """
        Convert Order objects into Fill objects
        
        Parameter:
        event - Event object with order information
        
        * Using ARCA as the exchange here as a place holder
        """
        if event.type == 'ORDER':
            fill_event = FillEvent(datetime.datetime.utcnow(), event.symbol,
                                   'ARCA', event.quantity, event.direction, event.position_change, 
                                  None)
            self.events.put(fill_event)