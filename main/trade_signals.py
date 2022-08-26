from dataclasses import dataclass
import sqlite3
from statsmodels.tsa.stattools import adfuller
from datetime import timedelta
from datetime import date
import statsmodels.api as sm
import numpy as np
from datetime import datetime
import pandas as pd


def pull_tickers(cursor):
    """Pull all tickers from database"""
    
    sql_query = """SELECT name FROM sqlite_master 
                WHERE type='table'"""

    cursor.execute(sql_query)

    tickers = cursor.fetchall()

    for i in range(len(tickers)):
        tickers[i] = tickers[i][0]

    return tickers


def pull_ticker_data(ticker, current_date):
    #NEED TO CLEAN UP CODE
    start_date = current_date - timedelta(weeks = 281, days = 5)
    end_date = current_date + timedelta(days = 1) 
    

    query = """SELECT [index], [open], [adjclose] 
                FROM [{}]""".format(ticker)

    cursor.execute(query)
    data = cursor.fetchall()

    #Pandas is being fucking retarded and fucks up column
    #Order when passing in the nested data to the dataframe
    index = []
    open_data = []
    close_data = []
    for date_, open_price, close_price in data:
        index.append(date_)
        open_data.append(open_price)
        close_data.append(close_price)


    df = pd.DataFrame(columns = {"DATE", "OPEN", "CLOSE"})
    df["DATE"] = index
    df["OPEN"] = open_data
    df["CLOSE"] = close_data
    df.set_index("DATE", inplace=True)


    #The following code needs to be here and ill figure out why later
    #For live trading
    if current_date == date.today():
        #if use current date it will pull in the previous day and not the current
        return df.loc[str(start_date):str(end_date)] 

    #For backtesting -> will not pull in the t+1 day (end_date)
    return df.loc[str(start_date):str(current_date)]


def calc_halflife(traning_data):
    """Regression on the pairs spread to find lookback
        period for trading"""
    x_lag = np.roll(traning_data,1)
    x_lag[0] = 0
    y_ret = traning_data - x_lag
    y_ret[0] = 0
    
    x_lag_constant = sm.add_constant(x_lag)
    
    res = sm.OLS(y_ret,x_lag_constant).fit()
    halflife = -np.log(2) / res.params[1]
    halflife = int(round(halflife))
    return halflife


def calc_trading_spread(training_data, trading_data):
    halflife = calc_halflife(training_data)
    raw_spread = trading_data[-halflife:]
    standardized_spread = (raw_spread - np.mean(raw_spread)) / np.std(raw_spread)

    return standardized_spread


def find_trades(trading_spread, ticker):
    if trading_spread[-1] > 2 and trading_spread[-2] < 2:
        trade = "Short After Hours " + ticker + " on " + trading_spread.index[-1]
        return trade

    if trading_spread[-1] < -2 and trading_spread[-2] > -2:
        trade = "Long After Hours " + ticker + " on " + trading_spread.index[-1]
        return trade


def generate_trade_signal(ticker, current_date):
    price_data = pull_ticker_data(ticker, current_date)
    training_data = price_data[:-100]
    trading_data = price_data[-100:]

    if len(training_data) >= 1257:
        if adfuller(training_data["CLOSE"])[1] < 0.01:
            trading_spread = calc_trading_spread(training_data["CLOSE"], trading_data["CLOSE"])
        
            return find_trades(trading_spread, ticker)


def main(current_date = date.today(), backtest = False):
    opened_trades = []

    db_file = r"C:\Users\sbuca\Desktop\quant trading strats\equity-mean-reversion\database\pricing.db"
    conn = sqlite3.connect(db_file)
    
    global cursor 
    cursor = conn.cursor()

    tickers = pull_tickers(cursor)
    #for ticker in tickers:
    for ticker in tickers:
        opened_trades.append(generate_trade_signal(ticker, current_date))
    
    all_opened_trades = list(filter(lambda item: item is not None, opened_trades))
    
    return all_opened_trades


#today = datetime.strptime("2022-07-19 00:00:00", '%Y-%m-%d 00:00:00')
main() #About 32 second runtime