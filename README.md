# SEC-FTDs

### To-Do
Update Python scripts to use the cleaners.py function. Gotta clean up the scrapers. 

### Goal
Automatically update Azure SQL Database with latest SEC FTD Information. The information is published and pulled from here: https://www.sec.gov/data/foiadocsfailsdatahtm

From SEC Page
```text
Fails Data Availability:

  * The first half of a given month is available at the end of the month
  * The second half of a given month is available at about the 15th of the next month.
```

### Infrastructure Dependencies
Azure SQL Database with network setting exemption set, this allows GitHub-Actions to connect to the database. I was running into issues with Max vCore was set to 1, I upped it to 2 and the github-action completed in 2-3 minutes as opposed to hanging for 30-40 minutes.

![image](https://github.com/tf-anguskong/SEC-FTDs/assets/48041768/16f125b7-6967-4f18-a8c3-44a64539b7da)


### What this is doing
1. Goes to SEC Site, scrapes and pulls latest link from FTD Publishing Data table
2. Downloads (if applicable) new data and extracts then parses data into pandas dataframe
3. Using pyodbc, insert new data into database

### Why Do This
I use the SQL Database to create Power BI Dashboard(s), published here:

[FTD Dashboard](https://app.powerbi.com/view?r=eyJrIjoiNWQwZDJkNjQtYzU5Zi00MzE3LTkzMzQtMDc1NTg2YWZkOTY4IiwidCI6IjYxOTdlMjA4LWRhZTktNDgxNy1iMWE2LTczYTc0MmZiNGZjNSIsImMiOjZ9)

Also, I just wanted to figure all of this out. Hadn't ever used Github-Actions previously, or really any python usage and automation around it. 

### Initial Setup
Since this only appends the latest SEC published data to an existing database, you have to import the initial bulk data going back as far as you want. The provided script wasn't test to work with any data provided by SEC prior to July 2009. 

1. Download ZIP files from SEC that you want to import into the database, leave them zip in a directory and then use the python-compile-sec-data.py to compile every file into a single csv. July 2009 - June 2023 is ~19 Million Row.
2. Take CSV and import into database, I found it easiest to create local SQL database and then use Azure SQL migration to move to Azure SQL database.
3. This Repo's automation should function accordingly

