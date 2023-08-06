#Import package
import numpy as np
import pandas as pd
import requests
import time
import sqlite3

# Some useful functions
#Get Alpha Vantage api key
def get_apikey(filename: str):
    with open(filename) as f:
        api_key = f.read().strip()
    f.close
    return api_key

#Get daily core stock data from Alpha Vantage from 2000-01 to now
def getDailyStockdata(ticker: str, outputsize = 'compact', datatype = 'json'):
    function = 'TIME_SERIES_DAILY'
    datatype = datatype
    outputsize = outputsize
    alpha_vantage_apikey = get_apikey(filename= 'dist/apikey_AlphaVantage')

    url = f'https://www.alphavantage.co/query?function={function}&symbol={ticker}&outputsize={outputsize}&apikey={alpha_vantage_apikey}&datatype={datatype}'
    r = requests.get(url)
    df = pd.DataFrame.from_dict(r.json()['Time Series (Daily)'],orient='index')
    df.columns = ['Open', 'High', 'Low', 'Close', 'Volume']
    df = df.rename_axis('Date').reset_index()
    df.insert(loc = 0, column = 'Ticker', value = ticker, allow_duplicates=True)
    return df

def getCompanyOverview(ticker: str):
    #Delay api call
    time.sleep(15)
    function = 'OVERVIEW'
    alpha_vantage_apikey = get_apikey(filename= 'dist/apikey_AlphaVantage')

    url = f'https://www.alphavantage.co/query?function={function}&symbol={ticker}&apikey={alpha_vantage_apikey}'
    r = requests.get(url)
    return r.json()

def connectToDatabase():
    conn = sqlite3.connect('StockData.db')
    cursor = conn.cursor()
    
    print('SQLite3 is connected')
    return conn, cursor

def isInDatabase(ticker: str, table: str, connection: sqlite3.Connection, cursor: sqlite3.Cursor):
    sql = f'''
    SELECT *
    FROM {table}
    WHERE Ticker = '{ticker}';
    '''
    connection.execute(sql)
    if len(cursor.fetchall()) < 1: 
        print(f'{ticker} is in the Database')
        return False
    else: 
        print(f'{ticker} is not in the Database')
        return True

def pd2sql(table: str, df: pd.DataFrame, connection: sqlite3.Connection, append=False):
    if not append:
        df.to_sql(table, con = connection, if_exists='replace', index = False)
    else: df.to_sql(table, con = connection, if_exists='append', index = False)
    return print('Pandas to SQL finished')

#Create a connection of SQLite3
conn,cursor = connectToDatabase()

#Store the S&P information in pandas dataframe
wiki_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies#Selected_changes_to_the_list_of_S&P_500_components"
tickers_df_list = pd.read_html(wiki_url)
tickers_df = tickers_df_list[0]
industry_dimension = tickers_df.loc[:, ['Symbol','GICS Sector','GICS Sub-Industry']]
industry_dimension = industry_dimension.rename(columns={'Symbol': 'Ticker'})

#Store the industry_dimension to the SQLite3
pd2sql('IndustryDimension',industry_dimension,conn)

#Turn pandas dataframe to the list
tickers = tickers_df['Symbol'].values.tolist()
len(tickers)

#Define the columns and table name in SQLite3
table = 'StockPrice'
stock_df = pd.DataFrame(columns=['Ticker','Date','Open', 'High', 'Low', 'Close', 'Volume'])

#Download the financial data from Alpha vantage
for ticker in tickers:
    if not isInDatabase(ticker, table, conn, cursor):
        try: stock_df = pd.concat([stock_df, getDailyStockdata(ticker,outputsize='full')], ignore_index = True)
        except:
            print(f'Next ticker: {ticker}') 
            continue

#Define the columns
temp_json = getCompanyOverview('IBM')
columns = [key for key in temp_json.keys()]

#Change the first column form Symbol to Ticker
columns[0] = 'Ticker'

#Create a new dict to store the stock overview data
StockOverview_dict = {}
for column in columns:
    StockOverview_dict[column] = []

#Download the company overview from Alpha vantage
#Append the value from api result
table = 'StockOverview'

# for ticker in tickers:
    if not isInDatabase(ticker, table, conn, cursor):
        try: r = getCompanyOverview(ticker)
        except: continue
        for key, value in r.items():
            try: StockOverview_dict[key].append(value)
            except: break


#Convert the dict to pandas dataframe
 StockOverview_df = pd.DataFrame.from_dict(StockOverview_dict)
 StockOverview_df.tail()