# -*- coding: utf-8 -*-
"""
Created on Fri Sep 12 12:15:58 2014

@author: colin4567
"""

import datetime
import MySQLdb as mdb
import urllib2

db_host = 'localhost'; db_user = 'sec_user'; db_pass = 'longgamma'; db_name = 'securities_master'
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)


def obtain_list_of_db_tickers():    
    """Obtain a list of the tickers in the db"""
    with con:
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol")
        data = cur.fetchall()
        return [(d[0], d[1]) for d in data]




def get_daily_historic_data_yahoo(ticker, 
                                  start_date=(2000,1,1),
                                  end_date=datetime.date.today().timetuple()[0:3]):
    """Obtains data from Yahoo finance returns and a list of tuples
    ticker: Yahoo finance ticker symbol
    start date: (YYY,M,D) format
    end_date: (YYY,M,D) format
    """
    yahoo_url = "http://ichart.finance.yahoo.com/table.csv?s="\
        "%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s" % \
        (ticker, start_date[1] - 1, start_date[2], start_date[0],
         end_date[1] - 1, end_date[2], end_date[0])
    
    """Try connecting to Yahoo finance and print error message if it occurs"""
    try:
        #Ignore the header([1:])
        yf_data = urllib2.urlopen(yahoo_url).readlines()[1:]
        prices = []
        for y in yf_data:
            p = y.strip().split(',')
            prices.append((datetime.datetime.strptime(p[0], '%Y-%m-%d'),
                           p[1], p[2], p[3], p[4], p[5], p[6]))
    except Exception, e:
        print "Could not download Yahoo data: %s" %e
    return prices



def top_off_data_yahoo(ticker,
                       end_date=datetime.date.today().timetuple()[0:3]):
                           
    sql = """SELECT dp.price_date
            FROM symbol as sym
            INNER JOIN daily_price AS dp
            ON dp.symbol_id = sym.id
            WHERE sym.ticker = '%s'
            ORDER BY dp.price_date DESC
            LIMIT 1;""" % ticker
    
    with con:
        cur = con.cursor()
        cur.execute(sql)
        #is there a better fetch for this?
        last_date = cur.fetchall()[0][0].timetuple()

 
    yahoo_url = "http://ichart.finance.yahoo.com/table.csv?s="\
        "%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s" % \
        (ticker, last_date[2], last_date[1], last_date[0],
         end_date[2]-1, end_date[1], end_date[0])
    
    """Try connecting to Yahoo finance and print error message if it occurs"""
    try:
        #Ignore the header([1:])
        yf_data = urllib2.urlopen(yahoo_url).readlines()[1:]
        prices = []
        for y in yf_data:
            p = y.strip().split(',')
            prices.append((datetime.datetime.strptime(p[0], '%Y-%m-%d'),
                           p[1], p[2], p[3], p[4], p[5], p[6]))
    except Exception, e:
        print "Could not download Yahoo data: %s" %e
    return prices
                      



def insert_daily_data_into_db(data_vendor_id, symbol_id, daily_data):
    """Takes a list of tuples of daily data and adds it to the db, appending vendor id, symbol id
    daily_data: List of tuples of the OHLC data w/ adj close and volume
    """
    now = datetime.datetime.utcnow()
     
    #amend data to include vendor id and symbol id
    daily_data = [(data_vendor_id, symbol_id, d[0], now, now,
                    d[1], d[2], d[3], d[4], d[5], d[6]) for d in daily_data]
    
    #create insert strings
    column_str = "data_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, volume, adj_close_price"
    

    insert_str = ("%s, " * 11)[:-2]
    final_str = "INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str)
    
    #now execute insert by symbol
    with con:
        cur = con.cursor()
        cur.executemany(final_str, daily_data)
        




if __name__ == '__main__':
    #Loop through tickers and insert daily historical data in the db
    tickers = obtain_list_of_db_tickers()
    for t in tickers:
        #This if for downloading all data (starting from scratch)
#        print "Adding all data for %s" %t[1]
#        yf_data = get_daily_historic_data_yahoo(t[1])
#        insert_daily_data_into_db('1', t[0], yf_data)

        print "Topping off data for: %s" % t[1]
        yf_data = top_off_data_yahoo(t[1])
        insert_daily_data_into_db('1', t[0], yf_data)
    con.close()
    
            
    
    
    
    
   
    
                                      





