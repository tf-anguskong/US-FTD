import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import csv
import pandas as pd
import os
import pyodbc
import decimal

'''
This script is meant to run once a day to pull market close data from NASDAQ, determined via github-action scheduler.

The URL that I'm using appears to get updated every 5 minutes, so could get MUCH MORE granular data but am not doing so. 

'''

today = datetime.today()
ymd = today.strftime("%Y-%m-%d")
year = today.strftime("%Y")
month = today.strftime("%m")
response = requests.get('https://api.tradier.com/v1/markets/calendar', # Used to tell me if market is open/closed today
    params={'month': f'{month}', 'year': f'{year}'},
    headers={'Accept': 'application/json'}
)
json_response = response.json()
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36'} # Seems to block python user agent :*(
csv_filepath = f'./NASDAQ-scraper/nasdaq-eod-{ymd}.csv'

azsqlpw = os.getenv('SQL_PASSWORD')
azuid = os.getenv('SQL_UID')
azsqlserver = os.getenv('SQL_SERVER')
nddata = os.getenv('ND_DATA')

def get_trading_day(data,search_date):
    for listitem in data['calendar']['days']['day']:
      if listitem['date'] == search_date:
        return listitem['status']
    return None

def import_csv(file_path):
    with open(file_path, 'r') as csv_file:
        csv_content = csv_file.read()
    return csv_content

def List_of_List(lst):
    res = []
    for line in lst:
      duh = line.split(',')     
      res.append(duh)
    return(res)

market_status = get_trading_day(json_response,ymd)

if market_status == 'closed':
   print("Markets are closed today")
   exit()
elif market_status == 'open':
  nasdaq_download = requests.get(nddata, headers=headers)
  nasdaq_json = json.loads(nasdaq_download.content)
  nasdaq_list = nasdaq_json['data']['rows']
  csv_columns = ['symbol', 'name', 'lastsale', 'netchange', 'pctchange', 'volume', 'marketCap','country','ipoyear','industry','sector','url']
  csv_columns_for_db = ['symbol', 'name_des', 'closeprice', 'netchange', 'pctchange', 'volume', 'marketCap','country','ipoyear','industry','sector','url']
  with open(csv_filepath, 'w', newline='') as csv_file:
    writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
    writer.writeheader()
    for line in nasdaq_list:
      writer.writerow(line)
  
  csv_data = import_csv(csv_filepath)
  precleandata = csv_data.split('\n')
  cleandata = List_of_List(precleandata)[1:]
  os.remove(csv_filepath)

  pddata = pd.DataFrame(cleandata, columns=csv_columns_for_db)
  connstr = ('Driver={ODBC Driver 18 for SQL Server};' # ODBC Driver 18 for SQL Server for github-actions
            f'Server={azsqlserver}'
            'Database=SEC-FTD-DEV;'
            'Persist Security Info=False;'
            f'Uid={azuid};'
            f'Pwd={azsqlpw};'
            'MultipleActiveResultSets=False;'
            'Encrypt=yes;'
            'TrustServerCertificate=no;'
            'Connection Timeout=30;')
  cnxn = pyodbc.connect(connstr)
  cursor = cnxn.cursor()  
  for index, row in pddata.iterrows():
    if index % 10 == 0:
      now = datetime.now()
      current_time = now.strftime("%H:%M:%S")
      print(f'Time: {current_time} : i = {index}')
    #marketCap field is REAL picky
    try:
      if row.marketCap == '':
        row.marketCap = 0
      else: 
       row.marketCap = row.marketCap[:-3]
       row.marketCap = 0 #Yolo
    except:
       row.marketCap = 0
    # pctchange field being picky
    try:
      if row.pctchange == '':
        pct_change = row.pctchange = 0 
      else:
        pct_change = float(row.pctchange[0:-1])
    except:
      pct_change = row.pctchange = 0

    try: close_price_int = float(row.closeprice[1:])
    except: row.closeprice = '0.00'

    try: net_change = decimal.Decimal(row.netchange)
    except: row.netchange = '0.00'

    cursor.execute(
                f"INSERT INTO [dbo].[nasdaq_data] ([injestdate],[symbol],[name_des],[closeprice],[netchange],[pctchange],[volume],[marketCap],[country],[ipoyear],[industry],[sector],[uri]) values(?,?,?,?,?,?,?,?,?,?,?,?,?)", 
                ymd,
                row.symbol, 
                row.name_des, 
                round(close_price_int,2),
                round(net_change,2),
                round(pct_change,3),
                int(row.volume),
                int(row.marketCap),
                row.country,
                row.ipoyear,
                row.industry,
                row.sector,
                row.url
                )
  cnxn.commit()
  cursor.close()
  
else:
  print("Idk wtf is going on")
  exit()
      

