# -*- coding: utf-8 -*-
"""
Script to show how to query the securities_master db

@author: colin4567
"""

import pandas as pd
pd.set_option('notebook_repr_html', False)

import pandas.io.sql as psql
import MySQLdb as mdb

db_host = 'localhost'; db_user = 'sec_user'; db_pass = 'longgamma'; db_name = 'securities_master'
con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)

getTicker = raw_input("What symbols are you looking for? ").split(",")
cleanTickers = tuple([x.strip() for x in getTicker])



sql = """SELECT dp.price_date, sym.ticker, dp.adj_close_price
        FROM symbol as sym
        INNER JOIN daily_price AS dp
        ON dp.symbol_id = sym.id
        WHERE sym.ticker IN %s
        ORDER BY dp.price_date ASC;""" % str(cleanTickers)

#create a df out of the result
dailyPrices = psql.read_sql(sql, con=con, index_col='price_date')
dailyPrices.to_csv('testData/test.csv' )


print dailyPrices.tail()


