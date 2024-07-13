import pandas as pd
import requests
from bs4 import BeautifulSoup
import pyodbc
from sqlalchemy import create_engine
import datetime
import numpy as np

url =r'https://en.wikipedia.org/wiki/List_of_largest_banks'
df=pd.DataFrame(columns=['Bank_Name','Assets'])
csv_path =r'C:\\Users\\vaish\\OneDrive\\Desktop\\country_assets.csv'
server =r'VAISHNAV\SQLEXPRESS'
database='practise'
table_name='country_assets'
log_path  =r'C:\\Users\\vaish\\OneDrive\\Desktop\\country_assets_log.csv'
exchange_rate =r'C:\\Users\\vaish\\Downloads\\exchange_rate.csv'
query1=f'select avg(britishstd) from {table_name}'
query2=f'select *  from {table_name}'

with open(log_path,'w') as log:
    pass
def extract(url):
    response = requests.get(url)
    data = BeautifulSoup(response.text,'html.parser')
    table=data.find('tbody')
    return table

def transform(table):
    global df
    rows = table.find_all('tr')
    # print(rows)
    for row in rows:
        # print(row)
        col= row.find_all('td')
        if len(col)>1:
            # print (col)
            # Bank_name=col[1].text.strip()
            Bank_name=col[1].find_all('a')[1]['title']
            # print (Bank_name)
            Assets=col[2].text.strip()
            # print (Bank_name)
            df2=pd.DataFrame([{'Bank_Name':Bank_name,'Assets':Assets}])
            df=pd.concat([df,df2])

    exchange = pd.read_csv(exchange_rate)
    # print(exchange)

    df.reset_index(inplace=True,drop=True)
    df.index.name = 'ID'
    df['Assets']=df['Assets'].str.replace(',','').astype(float)
    exchange=exchange.set_index('Currency').to_dict()

    # print(df['Assets'].astype(float)*exchange[exchange['Currency']=='EUR']['Rate'].tolist())
    # print (df['Assets']*exchange['Rate']['EUR'])
    df['EuropenSTD']=df['Assets'].apply(lambda x:x*exchange['Rate']['EUR'])
    df['BritishSTD'] = df['Assets'].apply(lambda x: x * exchange['Rate']['GBP'])
    df['IndianSTD'] = df['Assets'].apply(lambda x: x * exchange['Rate']['INR'])
    # print(np.apply(df['Assets'] * x) for x in exchange['Rate']['EUR'])
    return df

def loadtocsv(df):
    df.to_csv(csv_path)

def loadsql(table):
    conn=f'mssql+pyodbc://{server}/{database}?driver=ODBC+driver+17+for+sql+server'

    engine = create_engine(conn)
    df.to_sql(table,con=engine,if_exists='replace')
    engine.dispose()

def runquery(query,query2):
    conn = f'mssql+pyodbc://{server}/{database}?driver=ODBC+driver+17+for+sql+server'

    engine = create_engine(conn)
    queried=pd.read_sql(query,engine)
    print (queried)
    print (pd.read_sql(query2,engine))
    engine.dispose()
def logprogress(messages):
    timestamp = datetime.datetime.now()

    with open (log_path,'a') as f:
        f.write(f"{messages}: {timestamp}\n")

logprogress ('execution started')

logprogress("extraction started")
extracted_data=extract(url)
logprogress("extraction completed")
logprogress("transformation started")
transformed_data = transform(extracted_data)
logprogress("transformation completed")

logprogress("load to csv started")
loadtocsv(transformed_data)
logprogress("load to csv completed")

logprogress("load to sql started")
loadsql(table_name)
logprogress("load to sql completed")

logprogress("read from sql started")
runquery(query1,query2)
logprogress("read from sql completed")

logprogress('execution completed')









