import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import csv

url = 'https://screener.in/company/RELIANCE/consolidated/'
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


with open("table_data.csv", 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerows(table_data)


df_table = pd.DataFrame(table_data)
df_table.columns = df_table.iloc[0]
df_table = df_table[1:]
df_table = df_table.set_index('')
df_table