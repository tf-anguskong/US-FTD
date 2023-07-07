import requests
from bs4 import BeautifulSoup
import hashlib
from zipfile import ZipFile
import csv
from io import BytesIO
import pyodbc
import pandas as pd
import os
from datetime import datetime

hashfilename = './SEC-scraper/latest-file-hash.txt' # Stores hash of hash_result for comparison
hashfile = open(hashfilename, 'r+')

# Define the URL to scrape and pull site data
url = 'https://www.sec.gov/data/foiadocsfailsdatahtm'
downloadurl = 'https://www.sec.gov'
response = requests.get(url)

# Parse the HTML content
soup = BeautifulSoup(response.content, "html.parser")

table = soup.find("table", { 'class' : "list" }) # finds the table with FTD urls to download latest data
latest_entry = table.find('a') # First entry in table
filename = latest_entry.get('href').split('/')[-1]

hash_result = hashlib.md5(latest_entry.encode()) # Hash of latest FTD data, used to verify if there is new data or not

'''
######### get_sec_data #########
This will go out to the SEC website and download the latest FTD data. 

It returns a list object that then needs to be cleaned up using List_of_List
'''
def get_sec_data(latest_entry):
  # Download and save the latest ZIP file from SEC GOV Website
  zipdownload = requests.get((downloadurl + latest_entry.get('href')))
  zip_data = BytesIO(zipdownload.content)
  # Empty Array for new data to go into
  lines = []
  newdata = []
  
  with ZipFile(zip_data,'r') as zip_file:
    file_list = zip_file.namelist()
    
    file_to_extract = file_list[0]
    extracted_content = zip_file.read(file_to_extract)
  newdata = extracted_content.decode().split(sep='\n')
  del newdata[0] #Deleting file header, may not be needed
  del newdata[-3:]  
  return newdata

'''
######### list_of_list #########
This cleans up the data provided by SEC into a more useable form. 

This can then be converted into a pandas dataframe. 
'''  
def List_of_List(lst):
    res = []
    for el in lst:
      sub = el.split('\', \'') # There are ',' within description so have to use the extra '\',\'' to split properly
      for s in sub:
        duh = s.split('|') 
        duh[-1] = '0' if duh[-1] == '.' else duh[-1]        
        res.append(duh)
    return(res)
 
#Checks if there is new data based on last hash of url provided, which is stored in hashfilename variable
# If hash downloaded from SEC matches hashfilename, exit scrtipt
if hashfile.read() == hash_result.hexdigest():
  print("No new data from SEC published")
  exit()
# If there is new data, parse and return to latest_data.csv
else:
  # SQL Variables
  azsqlpw = os.getenv('SQL_PASSWORD')
  azuid = os.getenv('SQL_UID')
  azsqlserver = os.getenv('SQL_SERVER')
  
  latestdata = get_sec_data(latest_entry)
  lol = List_of_List(latestdata)
  
  pddata = pd.DataFrame(lol, columns=['setdate','cusip','symbol','qftd','description','price'])
  connstr = (r'Driver={ODBC Driver 18 for SQL Server};'
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
    if index % 10000 == 0:
      now = datetime.now()
      current_time = now.strftime("%H:%M:%S")
      print(f'Time: {current_time} : i = {index}')
    cursor.execute(
                "INSERT INTO [dbo].[FTDData_Python] ([SETTLEMENT DATE],[CUSIP],[SYMBOL],[QUANTITY (FAILS)],[DESCRIPTION],[PRICE]) values(?,?,?,?,?,?)", 
                row.setdate, 
                row.cusip, 
                row.symbol,
                row.qftd,
                row.description,
                row.price
                )
  cnxn.commit()
  cursor.close()
  # write latest hash to latest-file-hash.txt
  hashfile.write(str(hash_result.hexdigest()))
