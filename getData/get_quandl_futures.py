# -*- coding: utf-8 -*-
"""
To do:

0. ONCE THE FUTURES_MARKETS TABLE IS UPDATED - GET RID OF MARKET DEFINITIONS BELOW
1. create an update function 
2. figure out how to find or determine expiration dates - use open interest??

@author: colin4567
"""

import datetime
import pandas as pd
import urllib2
pd.set_option('notebook_repr_html', False)


db_host = 'localhost'; db_user = 'sec_user'; db_pass = 'longgamma'; db_name = 'securities_master'
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
   


def construct_futures_symbols(market, months, start_year=2000, end_year=2014):
    """Constructs a list of futures contracts codes for a 
    particlar symbol and timeframe."""
    futures = []
    for y in range(start_year, end_year+1):
        for m in months:
            futures.append("%s%s%s" % (market, m, y))
    return futures


def download_contract_from_quandl(exchange, contract, auth_token, dl_dir):
    """Download an individual futures contract from Quandl 
    and then store it to disk in the directory."""
    
    #API call
    api_call_head = "http://www.quandl.com/api/v1/datasets/%s/%s.csv" % (exchange, contract)
    params = "?&auth_token=%s&sort_order=asc" %auth_token    
    try:    
        data = urllib2.urlopen("%s%s" % (api_call_head, params)).read()
        #store to disk
        fc = open('%s/%s.csv' % (dl_dir, contract), 'w')
        fc.write(data)
        fc.close()
    except Exception, e:
        print "Could not download quandl data: %s" %e
    

  
def download_historical_contracts(market, exchange, months, auth_token, dl_dir, 
                                  start_year=2000, end_year=2014):
    contracts = construct_futures_symbols(market, months, start_year, end_year)
    for symbol in contracts:
        print "Downloading: %s" % symbol 
        download_contract_from_quandl(exchange, symbol, auth_token, dl_dir)






def insert_daily_futures_data_into_db(data_vendor_id, starting_dir, pattern):
    """Takes a list of tuples of daily data and adds it to the db, appending vendor id, symbol id
    daily_data: List of tuples of the OHLC data w/ adj close and volume
    """
    
    import os, glob
    
    #now = datetime.datetime.utcnow()
    
    for (this_dir, dir_names, file_names) in os.walk(starting_dir):
        result = glob.glob(os.path.join(this_dir, pattern))
        if result:
            print this_dir + ':'
            for each in result:
                print '   ' + os.path.split(each)[1]
                fc = open(each, 'r')
                print fc.readlines()[-1].split(',')[0]
                fc.close()
    
     
#    #amend data to include vendor id and symbol id
#    daily_data = [(data_vendor_id, symbol_id, d[0], now, now,
#                    d[1], d[2], d[3], d[4], d[5], d[6]) for d in daily_data]
#    
#    #create insert strings
#    column_str = "data_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, volume, adj_close_price"
#    
#    #why the -2 here - we dont want volume or adj close??
#    insert_str = ("%s, " * 11)[:-2]
#    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)
    
    #now execute insert by symbol
#    with con:
#        cur = con.cursor()
#        cur.executemany(final_str, daily_data)
        

def main():
    
    import os 
    os.chdir('C:\Users\colin4567\Dropbox\Securities_Master\quandl')
    
    rates = {'TY':'CME', 'US':'CME', 'ED':'CME', 'JG': 'SGX', 'FGBL':'EUREX', 
             'FOAT':'EUREX', 'FBTP':'EUREX', 'R':'LIFFE', 'L':'LIFFE', 
             'YT':'ASX', 'IR':'ASX'}
    equities = {'ES':'CME', 'NQ':'CME', 'TF':'ICE', 'FTI':'LIFFE', 'FCE':'LIFFE', 
                'Z':'LIFFE', 'AP':'ASX', 'SXF':'MX', 'IN':'SGX', 'NK':'SGX'}
    currencies = {'AD':'CME', 'BR':'CME', 'BP':'CME', 'CD':'CME', 'EC':'CME', 
                  'JY':'CME', 'MP':'CME', 'NE':'CME', 'RU':'CME', 'RA':'CME', 
                  'SF':'CME', 'TRY':'CME'}
    
    commodities = {'CL':'CME', 'NG':'CME', 'HO':'CME', 'RB':'CME', 'B':'ICE', 
                   'SB':'ICE', 'CC':'ICE', 'CT':'ICE', 'KC':'ICE', 'C':'CME', 
                   'W':'CME', 'S':'CME', 'LC':'CME', 'LN':'CME', 'SM':'CME'}
    metals = {'GC':'CME', 'HG':'CME', 'SI':'CME', 'AL':'SHFE', 'ZN':'SHFE', 
              'CU':'SHFE', 'PB':'SHFE'}
              
    quarterlies = dict((rates.items() + equities.items() + currencies.items())) 
    front_rollers = dict((commodities.items() + metals.items()))
    
#    auth_token = 'zBf9PDxU-ijZnM4nB52L'
#    start_year = 2000
#    end_year = 2014
   
    for market, exchange in quarterlies.iteritems(): 
##        print "Downloading quarterly contracts for %s" % market
##        dl_dir = 'quandl/futures/%s' % market
        months = "HMUZ"
##        download_historical_contracts(market, exchange, months, auth_token, dl_dir,
##                              start_year, end_year)
                            
    for market, exchange in front_rollers.iteritems():
#        print "Downloading all contracts for %s" % market
#        dl_dir = 'futures/%s' % market
        months = "FGHJKMNQUVXZ"
#        download_historical_contracts(market, exchange, months, auth_token, dl_dir,
#                              start_year, end_year)
    
    insert_daily_futures_data_into_db('2', os.getcwd(), "*.csv")
        


if __name__ == '__main__':
    main()
    

        


    

    
    
    
    

