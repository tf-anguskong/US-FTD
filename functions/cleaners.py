import pandas as pd
from datetime import datetime
import requests
import json
import decimal

# Will Contain Cleaner functions for each data set collection
# Will be imported into various data collection scripts. 

# Expects CSV data in and returns a pandas dataframe with properly formatted headers
def nasdaq_cleaner(nasdaq_csv):
    listoflist = nasdaq_csv.split('\n')
    listoflist = listoflist[1:]
    listoflist = listoflist[:-1]
    cleandata = []
    database_ready = []
    csv_columns_for_db = ['symbol', 'name_des', 'closeprice', 'netchange', 'pctchange', 'volume', 'marketCap','country','ipoyear','industry','sector','url']
    for line in listoflist:
        duh = line.split(',') # seperates the single list into list of lists
        cleandata.append(duh)
    pddata = pd.DataFrame(cleandata, columns=csv_columns_for_db)
    for index, row in pddata.iterrows():
        try: row.marketCap = int(row.marketCap[:-3])
        except: row.marketCap = 0
        try: row.pctchange = float(row.pctchange[0:-1])
        except: row.pctchange = row.pctchange = 0
        try: row.closeprice = float(row.closeprice[1:])
        except: row.closeprice = '0.00'
        try: row.netchange = decimal.Decimal(row.netchange)
        except: row.netchange = '0.00'
        try: row.volume = int(row.volume)
        except: row.volume = 0
        database_ready.append(row)
    df = pd.DataFrame(database_ready, columns=csv_columns_for_db)
    return(df)    


# Simply expects a list of lists, which is seperated by ','. Which is the only way to consistently break up the SEC FTD data. 
def sec_ftd_cleaner(sec_ftd_data): 
    res = []
    for el in sec_ftd_data:
        sub = el.split('\', \'') # There are ',' within description so have to use the extra '\',\'' to split properly
        for s in sub:
            duh = s.split('|') 
            duh[-1] = '0' if duh[-1] == '.' else duh[-1]        
            res.append(duh)
        return(res)


# Get current market status of the day, returns 'open' or 'closed'. 
def get_market_status():
    today = datetime.today()
    ymd = today.strftime("%Y-%m-%d")
    year = today.strftime("%Y")
    month = today.strftime("%m")
    response = requests.get('https://api.tradier.com/v1/markets/calendar', # Used to tell if market is open/closed today
        params={'month': f'{month}', 'year': f'{year}'},
        headers={'Accept': 'application/json'}
    )
    data = response.json()
    for listitem in data['calendar']['days']['day']:
        if listitem['date'] == ymd:
            status = listitem['status']
    return status