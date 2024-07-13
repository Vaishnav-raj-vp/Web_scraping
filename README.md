Web Scraping Project
This project involves web scraping, data transformation, and loading data into a SQL server database.

Dependencies
Python 3.x
pandas
requests
BeautifulSoup
pyodbc
sqlalchemy
numpy
datetime


Description
This project extracts data from a Wikipedia page, transforms the data,  loads it into a SQL server database. Additionally for the audit purposes logs the timestamp of each ETL process. 


This project assumes that you have a SQL server database set up with a table named country_assets. You will need to modify the server, database, and table_name variables in the main.py script to match your database configuration.
