import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import csv
import pandas as pd
import os
import pyodbc
import decimal

import sys 
sys.path.insert(1, f'{os.getcwd()}')

from functions.cleaners import *

'''
This script is meant to run once a day to pull market close data from NASDAQ, determined via github-action scheduler.

The URL that I'm using appears to get updated every 5 minutes, so could get MUCH MORE granular data but am not doing so. 

'''

today = datetime.today()
ymd = today.strftime("%Y-%m-%d")
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'} # Seems to block python user agent :*(
csv_filepath = f'./NASDAQ-scraper/nasdaq-eod-{ymd}.csv'

azsqlpw = os.getenv('SQL_PASSWORD')
azuid = os.getenv('SQL_UID')
azsqlserver = os.getenv('SQL_SERVER')
nddata = os.getenv('ND_DATA')

def import_csv(file_path):
    with open(file_path, 'r') as csv_file:
        csv_content = csv_file.read()
    return csv_content

# Get if the market was open or closed on day
market_status = get_market_status()

if market_status == 'closed':
  print("Markets are closed today")
  exit()
elif market_status == 'open':
  nasdaq_download = requests.get(nddata, headers=headers)
  nasdaq_json = json.loads(nasdaq_download.content)
  nasdaq_list = nasdaq_json['data']['rows']
  csv_columns = ['symbol', 'name', 'lastsale', 'netchange', 'pctchange', 'volume', 'marketCap','country','ipoyear','industry','sector','url']
  with open(csv_filepath, 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
    writer.writeheader()
    for line in nasdaq_list:
      writer.writerow(line)
  
  csv_data = import_csv(csv_filepath)
  pddata = nasdaq_cleaner(csv_data)
  os.remove(csv_filepath)

  connstr = ('Driver={ODBC Driver 18 for SQL Server};' # ODBC Driver 18 for SQL Server for github-actions
            f'Server={azsqlserver}'
            'Database=SEC-FTD-DEV;'
            'Persist Security Info=False;'
            f'Uid={azuid};'
            f'Pwd={azsqlpw};'
            'MultipleActiveResultSets=False;'
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=120;') # Gives Azure SQL time to wake up
  cnxn = pyodbc.connect(connstr, timeout=120)
  cursor = cnxn.cursor()  
  for index, row in pddata.iterrows():
    try: cursor.execute(
                f"INSERT INTO [dbo].[nasdaq_data] ([injestdate],[symbol],[name_des],[closeprice],[netchange],[pctchange],[volume],[marketCap],[country],[ipoyear],[industry],[sector],[uri]) values(?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                ymd,
                row.symbol, 
                row.name_des, 
                row.closeprice,
                row.netchange,
                row.pctchange,
                row.volume,
                row.marketCap,
                row.country,
                row.ipoyear,
                row.industry,
                row.sector,
                row.url
                )
    except: print(row) # If this does break on something, print row with problem value
  cnxn.commit()
  cursor.close()
  
else:
  print("Idk wtf is going on")
  exit()
      

