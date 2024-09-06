import os
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import psycopg2

stock_code = os.environ.get('STOCK_CODE')
username = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
host_ip = os.environ.get('HOST_IP')

url = f'https://screener.in/company/{stock_code}/consolidated/'
webpage = requests.get(url)
soup = bs(webpage.text,'html.parser')

data = soup.find('section', id="profit-loss")
tdata= data.find("table")

table_data = []

for row in tdata.find_all('tr'):
    row_data = []
    for cell in row.find_all(['th','td']):
        row_data.append(cell.text.strip())
    table_data.append(row_data)


df_table = pd.DataFrame(table_data)
df_table= df_table.T
df_table.iloc[0,0] = 'Year'
df_table.columns = df_table.iloc[0]
df_table = df_table.iloc[1:]
df_table = df_table.drop(df_table.index[-1])
for i in df_table.iloc[:,1:].columns:
    df_table[i] = df_table[i].str.replace(',','').str.replace('%','').apply(eval)

df_table['Stock'] = f"{stock_code}" 
df_table = df_table.reset_index()
print(df_table)

db_params = {
    'dbname': 'reliance',
    'user': username,
    'password': password,
    'host': host_ip,
    'port': 5432
}

conn = psycopg2.connect(**db_params)

cur = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS profit_loss_data (
    index BIGINT primary key,
    Year TEXT,
    Sales BIGINT,
    Expenses BIGINT,
    Operating_Profit BIGINT,
    OPM_Percent INTEGER,
    Other_Income BIGINT,
    Interest BIGINT,
    Depreciation BIGINT,
    Profit_before_tax BIGINT,
    Tax_Percent INTEGER,
    Net_Profit BIGINT,
    EPS_in_Rs DOUBLE PRECISION,
    Dividend_Payout_Percent INTEGER,
    Stock TEXT
);
'''
cur.execute(create_table_query)
conn.commit()
print("Table created")

insert_query = '''
INSERT INTO profit_loss_data (
    Index,Year, Sales, Expenses, Operating_Profit, OPM_Percent, Other_Income, Interest, Depreciation,
    Profit_before_tax, Tax_Percent, Net_Profit, EPS_in_Rs,Dividend_Payout_Percent, Stock
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s);
'''

for x, row in df_table.iterrows():
    cur.execute(insert_query, tuple(row))
    conn.commit() 
    print(f"Inserted data for year: {row['index']}")
