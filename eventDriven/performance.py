# -*- coding: utf-8 -*-
"""
Created on Mon Sep 15 12:45:43 2014

@author: colin4567
"""

import numpy as np
import pandas as pd



def create_sharpe_ratio(returns, periods=252):
    """based on benchmark of zero (risk-free rate=0%)
    
    Parameters:
    returns - pandas Series of ror's
    periods - daily, hourly, minutely, etc.
    """
    return np.sqrt(periods) * (np.mean(returns)) / np.std(returns)


def create_drawdowns(pnl):
    """
    Calculate largest peak-to-trough drawdown of the PnL curve as well as 
    drawdown duration. Requires that the pnl_returns is a pandas Series.
    
    Parameters:
    pnl - pandas Series or ror's
    
    Returns:
    drawdown, duration - Highest peak-to-trough drawdown and duration
    """

    hwm = [0]
    
    #drawdown and duration series
    idx = pnl.index
    drawdown = pd.Series(index=idx)
    duration = pd.Series(index=idx)
    
    #Loop over the index range 
    for t in range(1, len(idx)):
        hwm.append(max(hwm[t-1], pnl[t]))
        drawdown[t] = (hwm[t]-pnl[t])
        duration[t] = (0 if drawdown[t] == 0 else duration[t-1]+1)
    return drawdown.max(), duration.max()
                
    
    
    
    
    
    
    
    
    
    
    