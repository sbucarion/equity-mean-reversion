import sqlite3
from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
from yahoo_fin import *
from datetime import timedelta
from datetime import date


def pull_tickers(cursor):
    """Pull all tickers from database"""
    
    sql_query = """SELECT name FROM sqlite_master 
                WHERE type='table';"""

    cursor.execute(sql_query)
    tickers = cursor.fetchall()

    for i in range(len(tickers)):
        tickers[i] = tickers[i][0]

    return tickers


def check_date(ticker, cursor):
    query = """SELECT [index]
                FROM [{}]""".format(ticker)

    cursor.execute(query)
    data = cursor.fetchall()

    for i in range(len(data)):
        data[i] = data[i][0]

    return data[-1]


def format_query_data(df):
    template = {"index": df.name,
                 "open": df["open"], 
                 "high": df["high"], 
                 "low": df["low"], 
                 "close" :df["close"], 
                 "adjclose": df["adjclose"], 
                 "volume": df["volume"]}

    return template


def get_new_price(ticker, cursor):
    start_date = date.today() - timedelta(days = 3)

    last_date = check_date(ticker, cursor)
    data = get_data(ticker, start_date = start_date)
    new_data = format_query_data(data.iloc[-1])

    current_date = new_data["index"]
    
    if str(last_date) != str(current_date):
        return new_data

    return 1


def update_prices():
    db_file = r"C:\Users\sbucarion1\Desktop\quant trading strats\pairs-trading\database\pricing.db"
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    tickers = pull_tickers(cursor)
    for ticker in tickers:
        new_prices = get_new_price(ticker, cursor)

        if new_prices == 1:
            continue

        query = ("INSERT INTO [%s] VALUES('%s',%f,%f,%f,%f,%f,%f)" % (ticker, 
                                                                        str(new_prices["index"]),
                                                                        float(new_prices["open"]),
                                                                        float(new_prices["high"]),
                                                                        float(new_prices["low"]),
                                                                        float(new_prices["close"]),
                                                                        float(new_prices["adjclose"]),
                                                                        float(new_prices["volume"])))
        print(query)
        cursor.execute(query)
        conn.commit()



#update_prices()
db_file = r"C:\Users\sbucarion1\Desktop\quant trading strats\pairs-trading\database\pricing.db"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()
check_date("AAPL", cursor)