import sqlite3
from yahoo_fin import stock_info as si
from yahoo_fin.stock_info import *
from yahoo_fin import *
from datetime import timedelta
from datetime import date
from datetime import datetime


def pull_tickers():
    """Pull all tickers from database"""
    
    sql_query = """SELECT name FROM sqlite_master 
                WHERE type='table';"""

    cursor.execute(sql_query)
    tickers = cursor.fetchall()

    for i in range(len(tickers)):
        tickers[i] = tickers[i][0]

    return tickers


def check_date(ticker):
    query = """SELECT [index]
                FROM [{}]""".format(ticker)

    cursor.execute(query)
    data = cursor.fetchall()


    global most_recent_db_date
    most_recent_db_date = data[-1][0]
    most_recent_api_date = str(get_data(ticker).index[-1])

    return most_recent_db_date == most_recent_api_date


def bulk_update(ticker):
    data = get_data(ticker, start_date=most_recent_db_date)
    data = data.iloc[1:]

    for row in data.iterrows():
        query = ("INSERT INTO [%s] VALUES('%s',%f,%f,%f,%f,%f,%f)" % (ticker, 
                                                                        str(row[0]),
                                                                        float(row[1][0]),
                                                                        float(row[1][1]),
                                                                        float(row[1][2]),
                                                                        float(row[1][3]),
                                                                        float(row[1][4]),
                                                                        float(row[1][5])))

        cursor.execute(query)
        conn.commit()


def update_prices(ticker_list):
    for ticker in ticker_list:
        if ticker == "AAPL":
            continue 
        bulk_update(ticker)


def main():
    db_file = r"C:\Users\sbuca\Desktop\quant trading strats\equity-mean-reversion\database\pricing.db"
    global conn
    conn = sqlite3.connect(db_file)

    global cursor
    cursor = conn.cursor()

    tickers = pull_tickers()
    #tickers = ["AAPL"]
    if not check_date(tickers[0]):
        print("Updating in process...")
        update_prices(tickers)

        cursor.close()
        conn.close()

        return


    cursor.close()
    conn.close()

    print("Up to Date")

#1.5 minute runtime
main()