import sqlite3
from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
from yahoo_fin import *
from tickers import ticker_list
from datetime import datetime


def add_data(ticker, conn):
    #4 minute run time 
    data = get_data(ticker)
    data.drop(columns = ["ticker"], inplace = True)
    data.to_sql(name=ticker, con=conn)
    conn.commit()


def data_to_db(ticker_list, conn):
    for ticker in ticker_list:
        add_data(ticker, conn)


def create_db():
    db_file = r"C:\Users\sbuca\Desktop\quant trading strats\equity-mean-reversion\database\pricing.db"
    conn = sqlite3.connect(db_file)    

    data_to_db(ticker_list, conn)
    
    
create_db()