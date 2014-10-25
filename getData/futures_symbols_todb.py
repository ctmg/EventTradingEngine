# -*- coding: utf-8 -*-
"""

DONT USE THIS ANYMORE - DONT WANT TO PUT THE FUTURES SYMBOLS IN THE SYMBOL TABLE
THEY HAVE THEIR OWN TABLE NOW THAT CAN BE JOINED TO THE SYMBOL TABLE

@author: colin4567
"""

import os, glob
import MySQLdb as mdb
import pandas as pd
import datetime
pd.set_option('notebook_repr_html', False)

db_host = 'localhost'; db_user = 'sec_user'; db_pass = 'longgamma'; db_name = 'securities_master'
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
 

def add_futures_toSymbol(exchange_dir):
    """Takes a directory and pattern to look for, and then loops through the 
    directory looking for all csv files and putting them into the exchanges
    table in securities_master."""
    
    now = datetime.datetime.utcnow()
    
    #create insert strings
#    column_str = "ticker, instrument, name, created_date, last_updated_date"
#    insert_str = ("%s, " * 5)[:-2]
#    final_str = "INSERT INTO symbol (%s) VALUES %s " % (column_str, insert_str)

    for f in glob.glob(os.path.join(exchange_dir, '*.csv')):
        with open(f, 'r') as exchange:
            futures = pd.read_csv(exchange)
            futures.columns = ['ticker', 'exchange', 'name', 'months_traded', 'quandl_code']
            futures['instrument'] = 'future'
            futures['created_date'] = now ; futures['last_updated_date'] = now
            
            """This is where you need to decide how to input data - seperately or in a joined table"""
            
            futures_symbols =  futures[["ticker", "instrument", "name", "created_date", "last_updated_date"]]
            futures_markets = futures[["months_traded", "exchange"]]
            #futures_db.to_sql(con=con, name='symbol', if_exists='append', flavor='mysql', index=False)
            exchange.close()
            print len(futures_db)


def obtain_list_of_futures_tickers():    
    """Obtain a list of the futures tickers in the db"""
    with con:
        cur = con.cursor()
        cur.execute("SELECT id, ticker FROM symbol WHERE instrument = 'future'")
        data = cur.fetchall()
        print [(d[0], d[1]) for d in data]



def insert_into_futures_markets():
    #need to get symbol_id and ticker, then make sure to match on that symbol_id when you put in data


def main():
    add_futures_toSymbol(os.path.normpath('C:\Users\colin4567\Dropbox\Securities_Master\quandl\exchanges'))


if __name__ == '__main__':
    main()


